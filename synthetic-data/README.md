# Synthetic data (Arrow Retail AI Assistant)

This folder builds a **demo catalog** for the assistant: an LLM generates rows that match `shared/data/products.csv`, then an optional step downloads web images into `shared/images/`. There are **no** wrapper shell scripts—run the Python entrypoints directly from `synthetic-data/`.

## Contents

| File | Purpose |
|------|--------|
| `generate_synthetic_data.py` | LLM-backed CSV generator |
| `fetch_synthetic_data_images.py` | DuckDuckGo image download (companion to the CSV) |
| `requirements.txt` | Python dependencies |
| `README.md` | This file |

## Prerequisites

- Python 3.9+
- From this directory: `pip install -r requirements.txt`
- For generation, an **OpenAI-compatible** chat API:
  - **Local NIM** (default): Llama NIM on the host; `http://llama:8000/v1` in `shared/configs/chain_server/config.yaml` is rewritten to `http://127.0.0.1:8000/v1` when you run the script on the host (start `docker-compose-nim-local.yaml` first).
  - Or **NVIDIA NIM cloud** / **OpenAI** via flags and env vars below.

Set one of: `NGC_API_KEY`, `LLM_API_KEY`, `NVIDIA_API_KEY`, or `OPENAI_API_KEY` (or pass `--api-key`), depending on the endpoint.

## Workflow

### 1. Generate the CSV

Default: **~100 rows**, output `../shared/data/electronic_components.csv`, preset `default`, local NIM URL resolution as above.

```bash
cd synthetic-data
export NGC_API_KEY=nvapi-...   # or the key your endpoint expects
python3 generate_synthetic_data.py
```

More examples:

```bash
# Larger run
python3 generate_synthetic_data.py \
  --count 500 \
  --request-delay 0.35 \
  --output ../shared/data/electronic_components.csv

# NVIDIA NIM cloud
python3 generate_synthetic_data.py --nim-cloud --count 100

# OpenAI
export OPENAI_API_KEY=sk-...
python3 generate_synthetic_data.py \
  --base-url https://api.openai.com/v1 \
  --model gpt-4o-mini

# Taxonomy only (no LLM calls)
python3 generate_synthetic_data.py --dry-run

# Photo-oriented preset → default output ../shared/data/electronic_components_visual.csv
python3 generate_synthetic_data.py --preset visual --count 100
```

**Presets**

- `--preset default`: broader engineering taxonomy (many category/subcategory buckets).
- `--preset visual`: showcase-style finished products so image search tends to return packshots rather than datasheet strips.

**Behavior (generator)**

- **Resumable**: existing rows in the output CSV are kept; the script only fills up to `--count`.
- **Checkpoints** every 10 rows on long runs.
- **`--seed`**: controls deterministic sampling of families/manufacturers; the LLM output is still stochastic.
- **Retries**: HTTP 429 / 5xx and network errors use backoff (default `--max-retries 10`); 429 honors `Retry-After` when present.
- **Rate limits**: prefer `--concurrency 1` (default) for NIM cloud; increase `--request-delay` if you hit 429.

Rows use `image` paths like `/images/<slug>.jpg` under `shared/` once files exist.

### 2. Fetch images (optional)

`fetch_synthetic_data_images.py` reads the CSV, searches DuckDuckGo (package `ddgs`, **no API key**), validates images with Pillow, and writes JPEGs under `--images-root` (default: repo `shared/` → `shared/images/...`).

```bash
cd synthetic-data
pip install -r requirements.txt   # includes ddgs + Pillow

# Defaults: CSV ../shared/data/electronic_components.csv, limit 100, concurrency 3
python3 fetch_synthetic_data_images.py

python3 fetch_synthetic_data_images.py --limit 50 --concurrency 4

python3 fetch_synthetic_data_images.py \
  --csv ../shared/data/electronic_components.csv \
  --images-root ../shared

# After --preset visual
python3 fetch_synthetic_data_images.py \
  --csv ../shared/data/electronic_components_visual.csv \
  --query-suffix "product photo" \
  --min-size 200 --max-aspect-ratio 5
```

**Fetcher notes**

- **Queries**: built from name, subcategory, and category; `--query-suffix` (e.g. `product photo`) helps retail-style hits on the visual preset.
- **Validation**: min dimension `--min-size` (default 160), max aspect ratio `--max-aspect-ratio` (default 12), corrupt/non-image rejected; tries up to `--max-candidates` results per row.
- **Output**: normalized JPEG (quality 88).
- **Resumable**: skips rows whose target file already exists.
- **Deduplicate**: by default avoids reusing the same JPEG bytes across rows (`--allow-duplicate-images` to disable); raise `--max-candidates` if dedup exhausts results.
- **Rate limits**: DDG throttles aggressive workers; keep `--concurrency` modest (default 3).
- **Ownership**: web images are **demo-only**; production should use licensed or first-party assets.

## Schema

Generated CSV columns align with the main catalog:

```text
category, subcategory, name, description, url, price, image
```

## Troubleshooting

- **No API key**: set `NGC_API_KEY` / `OPENAI_API_KEY` or `--api-key`.
- **Connection errors (local NIM)**: ensure NIM is up and port 8000 is reachable from the host; verify `shared/configs/chain_server/config.yaml` `llm_port` / `llm_name`.
- **HTTP 429 (cloud)**: `--concurrency 1`, higher `--request-delay`, re-run (resumable).
- **`ddgs` import error**: `pip install -r requirements.txt` from `synthetic-data/`.
