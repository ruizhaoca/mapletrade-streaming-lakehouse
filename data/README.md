# Data provenance

Only small, reproducible samples are committed. Generated events, downloaded raw files, and
processed local outputs are ignored by Git.

| Dataset | Origin | Notes |
|---|---|---|
| Trade events | Synthetic | Generated with a fixed seed; no real trades or customers |
| Instruments | TMX issuer directory | Symbol/name/exchange are source-derived; IDs, asset class, currencies and flags are project-generated |
| FX rates | Bank of Canada Valet API | Public indicative daily rates; CAD identity rows are project-generated |
| Portfolios | Synthetic | Fictional names and identifiers only |

The TMX site exposes the issuer download through its interactive page and may block automated
clients. Download the CSV manually, retain the untouched file under `data/raw/tmx/`, and run
`ingestion/prepare_tmx_instruments.py`.

