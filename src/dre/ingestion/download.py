"""Download helpers. DLD publishes CSVs on dubailand.gov.ae/en/open-data and Dubai Pulse; some datasets
need a (free) Dubai Pulse account and OAuth token. You can always download manually and pass --file."""
from __future__ import annotations

import time
from pathlib import Path

import requests


def get_dubai_pulse_token(client_id: str, client_secret: str, timeout: int = 30) -> str:
    """Client-credentials flow documented on Dubai Pulse API dataset pages."""
    resp = requests.post(
        "https://api.dubaipulse.gov.ae/oauth/client_credential/accesstoken",
        params={"grant_type": "client_credentials"},
        data={"client_id": client_id, "client_secret": client_secret},
        timeout=timeout,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def download_file(url: str, dest: Path, headers: dict | None = None, retries: int = 3, timeout: int = 60) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=timeout) as r:
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        fh.write(chunk)
            return dest
        except requests.RequestException as err:  # network / HTTP errors -> retry with backoff
            last_err = err
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Download failed after {retries} attempts: {url}") from last_err
