"""Spotify recently-played fetch → strict R2 parquet + CSV upsert.

Downloads all four parquets from R2 before any API work (exit on failure).
CSV twins are derived as ``{stem}.csv`` from each parquet basename and uploaded after parquet.
Public-CI safe: counts only in logs; no track/artist names or tokens.
"""

from __future__ import annotations

import base64
import logging
import sys
import time
from pathlib import Path

import pandas as pd
import requests
import spotipy

from spotify_r2 import (
    configure_logging,
    csv_basename_from_parquet_key,
    data_dir,
    discord_user_prefix,
    download_object_or_exit,
    env_int,
    env_required,
    fold_upload_results,
    r2_object_key,
    r2_prefix,
    s3_client,
    send_discord_alert,
    upload_file_if_changed,
)

logger = logging.getLogger("spotify.fetch")

MAX_PAGES = env_int("SPOTIFY_MAX_PAGES", 2)

HISTORY_KEY = env_required("R2_HISTORY_PARQUET_KEY")
SONGS_KEY = env_required("R2_SONGS_PARQUET_KEY")
ALBUMS_KEY = env_required("R2_ALBUMS_PARQUET_KEY")
ARTISTS_KEY = env_required("R2_ARTISTS_PARQUET_KEY")

PARQUET_SPECS: dict[str, str] = {
    "listening_history": HISTORY_KEY,
    "songs": SONGS_KEY,
    "albums": ALBUMS_KEY,
    "artists": ARTISTS_KEY,
}


def upsert_dimension_table(existing_df: pd.DataFrame, new_data: list, key_column: str) -> pd.DataFrame:
    if not new_data:
        return existing_df if existing_df is not None else pd.DataFrame()
    new_df = pd.DataFrame(new_data)
    if existing_df is not None and len(existing_df) > 0:
        updated_keys = set(new_df[key_column])
        existing_df = existing_df[~existing_df[key_column].isin(updated_keys)]
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = new_df
    return combined_df.drop_duplicates(subset=[key_column], keep="first")


def get_spotify_token(client_id: str, client_secret: str, refresh_token: str) -> tuple[str, str | None]:
    max_retries = 3
    auth_str = base64.b64encode(f"{client_id}:{client_secret}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth_str}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    for attempt in range(max_retries):
        try:
            resp = requests.post(
                "https://accounts.spotify.com/api/token",
                headers=headers,
                data={"grant_type": "refresh_token", "refresh_token": refresh_token},
                timeout=10,
            )
            resp.raise_for_status()
            token_data = resp.json()
            access_token = token_data["access_token"]
            new_refresh = token_data.get("refresh_token", refresh_token)
            return access_token, new_refresh
        except requests.RequestException:
            logger.warning("Refresh attempt %d failed.", attempt + 1)
            if attempt < max_retries - 1:
                time.sleep(2**attempt)
            else:
                raise RuntimeError("Spotify token refresh failed after retries.") from None
    raise RuntimeError("Spotify token refresh failed.")


def strict_download_parquets(
    client, bucket: str, prefix: str, base: Path
) -> dict[str, tuple[Path, Path, str, str]]:
    """Download all parquets; exit on any failure.

    Returns per logical name: (local_pq, local_csv, r2_pq_key, r2_csv_key).
    """
    out: dict[str, tuple[Path, Path, str, str]] = {}
    for name, pq_basename in PARQUET_SPECS.items():
        csv_basename = csv_basename_from_parquet_key(pq_basename)
        local_pq = base / pq_basename
        local_csv = base / csv_basename
        key_pq = r2_object_key(prefix, pq_basename)
        key_csv = r2_object_key(prefix, csv_basename)
        download_object_or_exit(client, bucket, key_pq, local_pq)
        out[name] = (local_pq, local_csv, key_pq, key_csv)
    return out


def load_parquet(path: Path) -> pd.DataFrame:
    try:
        return pd.read_parquet(path)
    except Exception:
        logger.error("Failed to read local parquet.")
        sys.exit(1)


def fetch_and_merge(
    existing: dict[str, pd.DataFrame],
) -> tuple[dict[str, pd.DataFrame], int]:
    client_id = env_required("SPOTIFY_CLIENT_ID")
    client_secret = env_required("SPOTIFY_CLIENT_SECRET")
    refresh_token = env_required("SPOTIFY_REFRESH_TOKEN")

    access_token, new_refresh = get_spotify_token(client_id, client_secret, refresh_token)
    if new_refresh and new_refresh != refresh_token:
        logger.warning("Refresh token rotated; update SPOTIFY_REFRESH_TOKEN secret.")

    sp = spotipy.Spotify(auth=access_token)
    logger.info("Spotify authenticated.")

    all_tracks: list = []
    results = sp.current_user_recently_played(limit=50)
    all_tracks.extend(results["items"])
    page_count = 1
    while results.get("next") and page_count < MAX_PAGES:
        last_played = results["items"][-1]["played_at"]
        timestamp_ms = int(pd.to_datetime(last_played).timestamp() * 1000)
        results = sp.current_user_recently_played(limit=50, after=timestamp_ms)
        if results["items"]:
            all_tracks.extend(results["items"])
            page_count += 1
        else:
            break

    logger.info("Fetched %d tracks (%d pages).", len(all_tracks), page_count)

    listening_history_data: list = []
    songs_data: list = []
    albums_data: list = []
    artists_data: list = []
    seen_songs: set = set()
    seen_albums: set = set()
    seen_artists: set = set()

    for item in all_tracks:
        track = item["track"]
        track_id = track["id"]
        played_at = item["played_at"]
        listening_history_data.append({"played_at": played_at, "track_id": track_id})

        if track_id not in seen_songs:
            seen_songs.add(track_id)
            primary_artist_id = track["artists"][0]["id"]
            primary_artist_name = track["artists"][0]["name"]
            featured_artist_ids = [a["id"] for a in track["artists"][1:]]
            featured_artists_str = "|".join(featured_artist_ids) if featured_artist_ids else None
            artist_info = sp.artist(primary_artist_id)
            genres = "|".join(artist_info.get("genres", [])) if artist_info.get("genres") else None
            songs_data.append(
                {
                    "track_id": track_id,
                    "track_name": track["name"],
                    "primary_artist_id": primary_artist_id,
                    "featured_artist_ids": featured_artists_str,
                    "duration_ms": track["duration_ms"],
                    "genres": genres,
                }
            )

            if primary_artist_id not in seen_artists:
                seen_artists.add(primary_artist_id)
                artist_image_url = (
                    artist_info["images"][0]["url"] if artist_info.get("images") else None
                )
                artists_data.append(
                    {
                        "artist_id": primary_artist_id,
                        "artist_name": primary_artist_name,
                        "artist_image_url": artist_image_url,
                    }
                )

            for fa in track["artists"][1:]:
                fa_id = fa["id"]
                if fa_id not in seen_artists:
                    seen_artists.add(fa_id)
                    fa_info = sp.artist(fa_id)
                    fa_image_url = fa_info["images"][0]["url"] if fa_info.get("images") else None
                    artists_data.append(
                        {
                            "artist_id": fa_id,
                            "artist_name": fa["name"],
                            "artist_image_url": fa_image_url,
                        }
                    )

        if track_id not in seen_albums:
            seen_albums.add(track_id)
            album = track["album"]
            album_release_year = (
                album.get("release_date", "")[:4] if album.get("release_date") else None
            )
            album_image_url = album["images"][0]["url"] if album.get("images") else None
            albums_data.append(
                {
                    "track_id": track_id,
                    "album_name": album["name"],
                    "album_release_year": album_release_year,
                    "album_image_url": album_image_url,
                }
            )

    existing_songs_df = existing["songs"]
    songs_before = len(existing_songs_df)
    songs_df = upsert_dimension_table(existing_songs_df, songs_data, "track_id")
    added_songs = max(len(songs_df) - songs_before, 0)

    albums_df = upsert_dimension_table(existing["albums"], albums_data, "track_id")
    artists_df = upsert_dimension_table(existing["artists"], artists_data, "artist_id")

    existing_history_df = existing["listening_history"]
    history_df = pd.DataFrame(listening_history_data)
    if len(existing_history_df) > 0:
        combined_df = pd.concat([existing_history_df, history_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(subset=["played_at"], keep="first")
    else:
        combined_df = history_df
    combined_df = combined_df.sort_values("played_at", ascending=False).reset_index(drop=True)

    logger.info(
        "Merge complete: %d listens, songs +%d.",
        len(combined_df),
        added_songs,
    )

    return (
        {
            "listening_history": combined_df,
            "songs": songs_df,
            "albums": albums_df,
            "artists": artists_df,
        },
        added_songs,
    )


def main() -> tuple[str, int]:
    configure_logging()
    client, bucket = s3_client()
    prefix = r2_prefix()
    base = data_dir()

    paths = strict_download_parquets(client, bucket, prefix, base)
    existing = {name: load_parquet(pq_path) for name, (pq_path, _, _, _) in paths.items()}

    dataframes, added_songs = fetch_and_merge(existing)

    upload_results: list[str] = []
    for name, (pq_path, csv_path, key_pq, key_csv) in paths.items():
        dataframes[name].to_parquet(pq_path, index=False)
        dataframes[name].to_csv(csv_path, index=False)
        upload_results.append(
            upload_file_if_changed(
                client,
                bucket,
                key_pq,
                pq_path,
                content_type="application/vnd.apache.parquet",
                public=True,
            )
        )
        upload_results.append(
            upload_file_if_changed(
                client,
                bucket,
                key_csv,
                csv_path,
                content_type="text/csv",
                public=True,
            )
        )

    return fold_upload_results(*upload_results), added_songs


if __name__ == "__main__":
    ts = int(time.time())
    label = "Spotify Fetch"
    pre = discord_user_prefix()
    try:
        result, added_songs = main()
        if result == "uploaded":
            msg = f"✅ {label} — Uploaded at <t:{ts}:f> | songs +{added_songs}"
            send_discord_alert(msg)
        elif result == "no-change":
            send_discord_alert(f"✅ {label} — No changes at <t:{ts}:f>")
        else:
            send_discord_alert(
                f"{pre}⚠️ {label} — Finished with status {result} at <t:{ts}:f>"
            )
            sys.exit(1)
    except Exception:
        logger.exception("Script failed.")
        send_discord_alert(f"{pre}❌ {label} failed at <t:{ts}:f>")
        sys.exit(1)
