#!/usr/bin/env python3
"""
Arrow Retail AI Assistant - Component Image Fetcher
===================================================

Companion to ``generate_electronic_components.py``: walks the rows of a
components CSV, runs a DuckDuckGo image search for each product name, and
downloads the first usable result into the path recorded in the CSV's
``image`` column (e.g. ``/images/<slug>.jpg`` resolved with
``--images-root`` defaulting to ``shared/`` → ``shared/images/<slug>.jpg``).

Usage
-----
    pip install -r requirements.txt
    python3 fetch_component_images.py                      # ~100 images, default paths

    python3 fetch_component_images.py \
        --csv ../shared/data/electronic_components.csv \
        --images-root ../shared \
        --limit 100 \
        --concurrency 3

    # Visual / retail-friendly CSV from ``generate_electronic_components.py --preset visual``:
    python3 fetch_component_images.py \
        --csv ../shared/data/electronic_components_visual.csv \
        --query-suffix "product photo" \
        --min-size 200 \
        --max-aspect-ratio 5

Notes
-----
- Uses ``ddgs`` (DuckDuckGo) which does NOT require an API key.
- The script is safe to re-run: rows whose target image already exists on
  disk are skipped. Images that are too small or corrupt are discarded and
  the next search result is tried.
- DuckDuckGo will rate-limit aggressive workers. Concurrency defaults to 3;
  increase at your own risk.
- By default, identical JPEG files are not reused across rows (SHA-256 of
  encoded bytes). Use ``--allow-duplicate-images`` to turn that off.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import logging
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import requests
from PIL import Image, UnidentifiedImageError

try:
    from ddgs import DDGS
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "The 'ddgs' package is required. Install it with:\n"
        "    pip install ddgs"
    ) from exc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("fetch_images")


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)

# Default minimum pixel dimensions; override with --min-size.
_DEFAULT_MIN_DIMENSION = 160
# Default candidates per row; override with --max-candidates.
_DEFAULT_MAX_CANDIDATES = 6
# Reject images taller/wider than this ratio (many datasheet pages are extreme).
_DEFAULT_MAX_ASPECT_RATIO = 12.0


@dataclass
class Task:
    row_index: int           # position in the CSV (0-based, excluding header)
    name: str                # search query
    subcategory: str         # used to enrich the query
    category: str            # optional extra query context
    target_path: Path        # final file path on disk


class ImageContentRegistry:
    """Track SHA-256 hashes of saved JPEG payloads so the same image file is
    not reused for different product rows (exact-byte duplicates from DDG)."""

    __slots__ = ("_hashes", "_lock")

    def __init__(self) -> None:
        self._hashes: set[str] = set()
        self._lock = threading.Lock()

    def preload_jpegs(self, directory: Path) -> int:
        """Register existing ``*.jpg`` under ``directory``; returns count added."""
        if not directory.is_dir():
            return 0
        added = 0
        for path in directory.glob("*.jpg"):
            try:
                data = path.read_bytes()
            except OSError:
                continue
            if len(data) < 500:
                continue
            h = hashlib.sha256(data).hexdigest()
            with self._lock:
                if h not in self._hashes:
                    self._hashes.add(h)
                    added += 1
        if added:
            logger.info("Preloaded %d unique JPEG hashes from %s", added, directory)
        return added

    def claim_jpeg_bytes(self, jpeg_bytes: bytes) -> str | None:
        """If these bytes are new, reserve the hash and return it; else None."""
        h = hashlib.sha256(jpeg_bytes).hexdigest()
        with self._lock:
            if h in self._hashes:
                return None
            self._hashes.add(h)
            return h

    def release_hash(self, digest: str) -> None:
        """Undo a claim if the file was not written successfully."""
        with self._lock:
            self._hashes.discard(digest)


@dataclass
class FetchOptions:
    min_dimension: int
    max_candidates: int
    max_aspect_ratio: float
    query_suffix: str
    content_registry: ImageContentRegistry | None


# ---------------------------------------------------------------------------
# Image search + download
# ---------------------------------------------------------------------------
def search_image_candidates(query: str, max_results: int) -> list[str]:
    """Return a list of candidate image URLs from DuckDuckGo."""
    try:
        with DDGS() as d:
            results = d.images(query, max_results=max_results)
            return [r["image"] for r in results if r.get("image")]
    except Exception as exc:  # noqa: BLE001
        logger.warning("DDG search failed for %r: %s", query, exc)
        return []


def _load_image_bytes(payload: bytes) -> Image.Image | None:
    """Return a Pillow image if ``payload`` is a valid decodable image."""
    try:
        img = Image.open(io.BytesIO(payload))
        img.load()
        return img
    except (UnidentifiedImageError, OSError, ValueError):
        return None


def download_to_jpeg(
    url: str,
    target: Path,
    session: requests.Session,
    opts: FetchOptions,
) -> str:
    """Download ``url`` and write it as JPEG to ``target``.

    Returns ``ok``, ``reject``, or ``duplicate`` (duplicate = same JPEG bytes as
    another product already on disk or saved earlier in this run).
    """
    try:
        resp = session.get(url, timeout=15, stream=True)
        resp.raise_for_status()
    except requests.RequestException as exc:
        logger.debug("HTTP error for %s: %s", url, exc)
        return "reject"

    ctype = resp.headers.get("Content-Type", "").lower()
    if ctype and not ctype.startswith(("image/", "application/octet-stream")):
        logger.debug("Rejecting %s (content-type=%s)", url, ctype)
        return "reject"

    payload = resp.content
    if len(payload) < 2000:
        logger.debug("Rejecting %s (only %d bytes)", url, len(payload))
        return "reject"

    img = _load_image_bytes(payload)
    if img is None:
        logger.debug("Rejecting %s (unreadable image)", url)
        return "reject"

    w, h = img.size
    if w < opts.min_dimension or h < opts.min_dimension:
        logger.debug("Rejecting %s (%dx%d too small)", url, w, h)
        return "reject"

    short_side = min(w, h)
    aspect = max(w, h) / max(short_side, 1)
    if aspect > opts.max_aspect_ratio:
        logger.debug(
            "Rejecting %s (aspect %.1f > %.1f)",
            url,
            aspect,
            opts.max_aspect_ratio,
        )
        return "reject"

    # Normalize everything to RGB JPEG so downstream CLIP / renderers don't
    # have to worry about paletted PNGs or transparency artifacts.
    if img.mode not in ("RGB", "L"):
        img = img.convert("RGB")

    buf = io.BytesIO()
    try:
        img.save(buf, format="JPEG", quality=88)
    except OSError as exc:
        logger.debug("Failed to encode %s: %s", url, exc)
        return "reject"
    jpeg_bytes = buf.getvalue()

    claimed: str | None = None
    if opts.content_registry is not None:
        claimed = opts.content_registry.claim_jpeg_bytes(jpeg_bytes)
        if claimed is None:
            logger.debug("Rejecting %s (duplicate JPEG bytes)", url)
            return "duplicate"

    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        target.write_bytes(jpeg_bytes)
    except OSError as exc:
        logger.debug("Failed to save %s to %s: %s", url, target, exc)
        if opts.content_registry is not None and claimed is not None:
            opts.content_registry.release_hash(claimed)
        return "reject"
    return "ok"


def build_query(
    name: str,
    subcategory: str,
    category: str,
    suffix: str,
    *,
    max_len: int = 180,
) -> str:
    """Compose a concise image-search query from the row fields."""
    hint = subcategory.split(" - ", 1)[0] if subcategory else ""
    cat = (category or "").strip()
    parts: list[str] = [name]
    blob = name.lower()
    if hint and hint.lower() not in blob:
        parts.append(hint)
        blob = f"{blob} {hint.lower()}"
    if cat and cat.lower() not in blob:
        parts.append(cat)
    query = " ".join(parts).strip()
    if suffix:
        query = f"{query} {suffix}".strip()
    return query[:max_len]


def fetch_one(
    task: Task,
    session: requests.Session,
    opts: FetchOptions,
) -> tuple[int, bool, str]:
    """Fetch the image for a single task. Returns (row_index, ok, url_used)."""
    if task.target_path.exists() and task.target_path.stat().st_size > 0:
        logger.info("[skip] %s already exists", task.target_path.name)
        return task.row_index, True, "cached"

    query = build_query(
        task.name,
        task.subcategory,
        task.category,
        opts.query_suffix,
    )
    candidates = search_image_candidates(query, opts.max_candidates)
    if not candidates:
        # Retry with a shorter query if the full line was too niche.
        query_short = build_query(
            task.name,
            "",
            task.category,
            opts.query_suffix,
        )
        if query_short != query:
            candidates = search_image_candidates(query_short, opts.max_candidates)
            query = query_short
    if not candidates:
        # Retry once with just the product name in case the hint is noisy.
        candidates = search_image_candidates(task.name, opts.max_candidates)

    for url in candidates:
        status = download_to_jpeg(url, task.target_path, session, opts)
        if status == "ok":
            logger.info("[ok]   %s  <-  %s", task.target_path.name, url)
            return task.row_index, True, url
        if status == "duplicate":
            logger.debug("[dup]  %s  <-  %s (trying next)", task.target_path.name, url)

    logger.warning("[fail] %s  (query=%r, %d candidates)",
                   task.target_path.name, query, len(candidates))
    return task.row_index, False, ""


# ---------------------------------------------------------------------------
# CSV handling
# ---------------------------------------------------------------------------
CSV_HEADER = ["category", "subcategory", "name", "description", "url", "price", "image"]


def load_tasks(csv_path: Path, images_root: Path, limit: int | None) -> list[Task]:
    with csv_path.open("r", newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    tasks: list[Task] = []
    for i, row in enumerate(rows):
        image_field = (row.get("image") or "").strip()
        if not image_field:
            logger.warning("Row %d has empty 'image' column; skipping", i)
            continue
        # "/images/foo.jpg" -> <images_root>/images/foo.jpg
        relative = image_field.lstrip("/")
        target = images_root / relative
        tasks.append(Task(
            row_index=i,
            name=(row.get("name") or "").strip(),
            subcategory=(row.get("subcategory") or "").strip(),
            category=(row.get("category") or "").strip(),
            target_path=target,
        ))
        if limit and len(tasks) >= limit:
            break
    return tasks


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    repo_root = here.parent
    parser = argparse.ArgumentParser(
        description="Fetch images for an electronic-components CSV using "
                    "DuckDuckGo image search.",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=repo_root / "shared" / "data" / "electronic_components.csv",
        help="Components CSV to walk "
             "(default: ../shared/data/electronic_components.csv)",
    )
    parser.add_argument(
        "--images-root",
        type=Path,
        default=repo_root / "shared",
        help="Root directory against which CSV 'image' paths are resolved "
             "(default: ../shared, so /images/foo.jpg -> shared/images/foo.jpg)",
    )
    parser.add_argument("--limit", type=int, default=100,
                        help="Maximum number of images to fetch (default: 100)")
    parser.add_argument("--concurrency", type=int, default=3,
                        help="Parallel workers (default: 3 — DDG rate-limits)")
    parser.add_argument("--seed", type=int, default=None,
                        help="Optional seed for task shuffle (deterministic runs)")
    parser.add_argument(
        "--query-suffix",
        default="",
        help="Appended to every search query (e.g. 'product photo'). "
             "Helps retail-style hits for visual-preset CSVs.",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=_DEFAULT_MIN_DIMENSION,
        help=f"Minimum width and height in pixels (default: {_DEFAULT_MIN_DIMENSION})",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=_DEFAULT_MAX_CANDIDATES,
        help="DDG image results to try per row (default: "
             f"{_DEFAULT_MAX_CANDIDATES})",
    )
    parser.add_argument(
        "--max-aspect-ratio",
        type=float,
        default=_DEFAULT_MAX_ASPECT_RATIO,
        help="Drop images if max(w,h)/min(w,h) exceeds this — filters many "
             f"datasheet strips (default: {_DEFAULT_MAX_ASPECT_RATIO}).",
    )
    parser.add_argument(
        "--allow-duplicate-images",
        action="store_true",
        help="Disable JPEG byte deduplication (default: reuse of the exact same "
             "image file across products is rejected).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.csv.exists():
        raise SystemExit(f"CSV not found: {args.csv}. "
                         "Run generate_electronic_components.py first.")

    tasks = load_tasks(args.csv, args.images_root, args.limit)
    pending = [t for t in tasks
               if not (t.target_path.exists() and t.target_path.stat().st_size > 0)]
    logger.info("%d rows total, %d images already on disk, %d to fetch",
                len(tasks), len(tasks) - len(pending), len(pending))

    if not pending:
        logger.info("Nothing to do. All target images already exist.")
        return 0

    if args.seed is not None:
        random.Random(args.seed).shuffle(pending)

    session = requests.Session()
    session.headers.update({
        "User-Agent": USER_AGENT,
        "Accept": "image/avif,image/webp,image/png,image/jpeg,image/*;q=0.8,*/*;q=0.5",
        "Accept-Language": "en-US,en;q=0.9",
    })

    registry: ImageContentRegistry | None = None
    if not args.allow_duplicate_images:
        registry = ImageContentRegistry()
        registry.preload_jpegs(args.images_root / "images")

    opts = FetchOptions(
        min_dimension=max(32, args.min_size),
        max_candidates=max(1, args.max_candidates),
        max_aspect_ratio=max(1.0, args.max_aspect_ratio),
        query_suffix=(args.query_suffix or "").strip(),
        content_registry=registry,
    )

    ok = 0
    fail = 0
    start = time.time()
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = {pool.submit(fetch_one, t, session, opts): t for t in pending}
        for fut in as_completed(futures):
            _, success, _ = fut.result()
            ok += int(success)
            fail += int(not success)

    elapsed = time.time() - start
    logger.info("Done in %.1fs.  success=%d  failed=%d  (out of %d)",
                elapsed, ok, fail, len(pending))
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
