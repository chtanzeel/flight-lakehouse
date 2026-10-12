"""Download the public source files into the landing zone (local filesystem only).

For S3, upload or copy the files into the landing prefix instead; see docs/databricks-aws.md.
"""

from __future__ import annotations

import logging
import time
import urllib.request
from pathlib import Path

from flight_lakehouse.config import AIRPORTS_FILE, FLIGHTS_FILE, Settings

log = logging.getLogger(__name__)

_USER_AGENT = "flight-lakehouse/0.1 (+https://github.com/chtanzeel)"


def download(url: str, dest: Path, *, retries: int = 3, timeout: int = 120) -> None:
    """Download ``url`` to ``dest`` atomically, retrying on transient failures."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
            with (
                urllib.request.urlopen(request, timeout=timeout) as response,
                tmp.open("wb") as out,
            ):
                while chunk := response.read(1 << 20):
                    out.write(chunk)
            if tmp.stat().st_size == 0:
                raise OSError(f"downloaded file is empty: {url}")
            tmp.replace(dest)
            return
        except OSError as error:
            last_error = error
            log.warning("download attempt %d/%d failed for %s: %s", attempt, retries, url, error)
            time.sleep(2 * attempt)
    raise RuntimeError(f"could not download {url}") from last_error


def ingest(settings: Settings, *, force: bool = False) -> list[Path]:
    """Fetch the flights and airports files unless they are already present."""
    if "://" in settings.landing:
        raise ValueError(
            f"ingest only writes to a local directory, but landing is {settings.landing!r}. "
            "Copy the source files to that location yourself (see docs/databricks-aws.md)."
        )

    downloaded: list[Path] = []
    for url, name in (
        (settings.flights_url, FLIGHTS_FILE),
        (settings.airports_url, AIRPORTS_FILE),
    ):
        dest = Path(settings.landing_path(name))
        if dest.exists() and dest.stat().st_size > 0 and not force:
            log.info("landing file already present, skipping: %s", dest)
            continue
        log.info("downloading %s -> %s", url, dest)
        download(url, dest)
        downloaded.append(dest)
    return downloaded


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ingest(Settings.from_env())


if __name__ == "__main__":
    main()
