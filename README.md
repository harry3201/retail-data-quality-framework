# Retail Data Quality Framework

A PySpark data quality validation pipeline built against the **UCI Online
Retail dataset** — 541,909 real e-commerce transactions from a UK-based
online retailer (Dec 2010 – Dec 2011). This isn't a synthetic dataset with
injected faults; it's real transactional data with real, well-documented
messiness, so every number below comes from an actual pipeline run over
real data (see `output/quality_report.json` and `output/run_history.jsonl`).

## What it does

1. Loads the raw dataset with an explicit, enforced schema.
2. Runs a set of quality rules against every row.
3. **Quarantines** rows that fail a "hard" rule into a separate output,
   tagged with the exact rule(s) they failed — it does not silently drop
   them.
4. **Flags** rows that trip a "soft" rule but keeps them in the valid
   dataset, because the anomaly represents a real, legitimate business
   event rather than corrupted data.
5. Writes a JSON quality report per run and appends it to a run-history
   log, so quality metrics (e.g. quarantine rate) can be tracked and
   compared across runs.

## Design decision: hard vs. soft rules

The single most important choice in this project was **not** quarantining
everything that looked unusual. Three checks are deliberately soft:

| Check | Why it's soft, not hard |
|---|---|
| Missing `CustomerID` (24.9% of rows) | This is a guest checkout, not corrupted data. Dropping a quarter of the dataset to "clean" it would destroy real revenue records. |
| Negative `Quantity` (10,624 rows) | These are returns. A return is a real transaction, not a data error — it needs to stay in the dataset for accurate net-revenue reporting. |
| Non-product `StockCode` (POST, D, M, BANK CHARGES, etc.) | These are legitimate non-merchandise line items (postage, discounts, manual adjustments), not schema violations. |

By contrast, four checks are hard and *do* quarantine the row, because no
downstream aggregate should trust these values:

| Check | Why it's hard |
|---|---|
| Non-positive `UnitPrice` | A price of £0 or negative on a real sale line is not economically meaningful and skews revenue metrics. |
| Missing `InvoiceNo` / `StockCode` | Without a transaction or product identifier, the row can't be joined or aggregated correctly. |
| Unparseable `InvoiceDate` | Breaks any time-series or cohort analysis downstream. |
| Exact duplicate rows | Inflates counts and revenue if left in (first occurrence is kept, the rest are quarantined). |

## Actual results (from `output/quality_report.json`)

```
Total records:        541,909
Valid records:        534,129
Quarantined:            7,780  (1.44%)
  - duplicate rows:      5,268
  - non-positive price:  2,517
Flagged (kept):
  - missing CustomerID: 135,080  (24.9%)
  - negative quantity:   10,624
  - non-product line:     2,849
```

## Project structure

```
dq-project/
├── data/
│   └── online_retail_raw.csv       # UCI Online Retail, 541,909 rows
├── src/
│   ├── quality_pipeline.py         # main PySpark validation pipeline
│   └── run_history.py              # runs the pipeline + appends to history log
├── output/
│   ├── silver_valid_transactions/  # valid rows, with quality_flags column
│   ├── quarantined_records/        # quarantined rows, with quarantine_reasons column
│   ├── quality_report.json         # latest run's report
│   └── run_history.jsonl           # append-only log, one JSON line per run
└── README.md
```

## Running it

```bash
pip install -r requirements.txt
python src/run_history.py
```

## Dataset source

Chen, D. (2015). *Online Retail* [Dataset]. UCI Machine Learning Repository.
https://doi.org/10.24432/C5BW33 — licensed CC BY 4.0.
