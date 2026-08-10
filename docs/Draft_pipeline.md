# Draft & Prospect Pipeline Orchestration — `pl_prospect_history`

This document describes the Fabric Data Pipeline that orchestrates the draft-picks and prospect-history notebooks described in `Draft_setup_doc.md`. That document covers *what* each notebook does and *why* the data model looks the way it does; this one covers *how the whole chain is wired together, run, and kept safe to re-run year after year*.

---

## 1. Why a standalone pipeline, not part of `pl_ingestion`/`pl_bronze`/`pl_silver`

The project's other pipelines are organized **by layer**: `pl_ingestion` runs every Raw notebook, `pl_bronze` runs every Bronze notebook, `pl_silver` runs every Silver notebook, and `pl_main` chains those layer-pipelines in sequence (all of Raw, then all of Bronze, then all of Silver, then Gold).

The draft/prospect chain doesn't fit that shape. It's **two passes that each cross all four layers**, and the second pass depends on the first pass's *Silver* output, not just its Raw output:

```
Pass 0 (draft picks):     Raw → Bronze → Silver → Gold
Pass 1 (player ID resolution): Raw → Bronze → Silver          (needs Pass 0's Gold output)
Pass 2 (season history):       Raw → Bronze → Silver → Gold   (needs Pass 1's Silver output)
```

Slotting these notebooks into the per-layer pipelines would mean invoking `pl_ingestion`, then `pl_bronze`, then `pl_silver`, then back to `pl_ingestion` again for Pass 2's Raw step — awkward cross-pipeline choreography for what is really one linear story. `pl_prospect_history` keeps it as a single, self-contained, straight-line pipeline instead.

---

## 2. Parameters and variables

The pipeline exposes **one parameter**:

| Name | Type | Default | Purpose |
|---|---|---|---|
| `draft_years` | String | `2026` | Comma-separated list of draft year(s) to process this run (e.g. `2027` or `2026,2027`). Matches the existing convention in `raw_draft.Notebook`, which already split on commas. |

It does **not** expose `ingestion_date` as a parameter, even though most of the underlying notebooks accept one. Instead:

- A `Set variable` activity (`Set ingestion_date`) runs first, computing a pipeline variable `v_ingestion_date = @formatDateTime(utcNow(), 'yyyy-MM-dd')`.
- Every activity that needs a date is wired to `@variables('v_ingestion_date')`, not a pipeline parameter.

**Why:** this mirrors `pl_main`, which computes `v_ingestion_date` the same way rather than asking a caller to supply it. The reasoning: a Fabric pipeline parameter left blank at run time resolves to an **empty string**, not Python `None` — and every notebook's fallback logic (`if ingestion_date is None: ingestion_date = date.today()...`) only catches `None`. Relying on a human to type in today's date correctly on every manual run is a silent-failure risk; computing it automatically removes the risk entirely. `draft_years` stays a real parameter because, unlike the date, it's the one thing you actually want to consciously choose each run.

---

## 3. Full activity chain

Eleven activities, single linear chain, no branches:

| # | Activity (Notebook) | Base parameters | Depends on |
|---|---|---|---|
| 0 | `Set ingestion_date` *(Set variable)* | — | — |
| 1 | `raw_draft` | `ingestion_date` = `@variables('v_ingestion_date')`, `draft_years` = `@pipeline().parameters.draft_years` | 0 succeeded |
| 2 | `bronze_draft` | `ingestion_date` = `@variables('v_ingestion_date')` | 1 succeeded |
| 3 | `silver_draft_picks` | *(none)* | 2 succeeded |
| 4 | `gold_draft_picks` | *(none)* | 3 succeeded |
| 5 | `raw_prospect_search` | `ingestion_date`, `draft_years` (same expressions as #1) | 4 succeeded |
| 6 | `bronze_prospect_search` | `ingestion_date` | 5 succeeded |
| 7 | `silver_bridge_prospect_player` | *(none)* | 6 succeeded |
| 8 | `raw_prospect_landing` | `ingestion_date`, `draft_years` | 7 succeeded |
| 9 | `bronze_prospect_landing` | `ingestion_date` | 8 succeeded |
| 10 | `silver_fact_prospect_season_totals` | *(none)* | 9 succeeded |
| 11 | `gold_fact_prospect_season_total` | *(none)* | 10 succeeded |

Activities 3, 4, 7, 10, 11 take no parameters — they operate on whatever's already sitting in the table below them (see §4), not on this run's specific batch.

Two dependency edges matter more than they look:

- **#5 depends on #4 (`gold_draft_picks`), not #2 (`bronze_draft`).** `raw_prospect_search` filters `gold.dim_draft_picks` by `draft_years` — it needs the picks to have made it all the way to Gold first, not just landed in Bronze.
- **#6 depends on #5, and #9 depends on #8 — strictly, not in parallel.** `bronze_prospect_search`/`bronze_prospect_landing` read the exact files their Raw predecessor just wrote to `Files/raw/.../{ingestion_date}/`. If they ran concurrently off a shared upstream trigger instead of depending on each other directly, Bronze could start reading before Raw finished writing.

### Lineage summary

```
Set ingestion_date (variable)
  → raw_draft → bronze.dim_draft_picks → silver.dim_draft_picks → gold.dim_draft_picks
      → raw_prospect_search → bronze.dim_prospect_search → silver.bridge_prospect_player
          → raw_prospect_landing → bronze.fact_prospect_season_totals
              → silver.fact_prospect_season_totals → gold.fact_prospect_season_totals
```

---

## 4. Accumulate, don't overwrite — the Bronze-layer fix

Every notebook in this chain originally wrote its table with `mode("overwrite")`, and every Raw notebook only reads/lands files under **today's** `ingestion_date` folder. Combined, that meant: run the pipeline for `draft_years=2027`, and Bronze would read only 2027's freshly-landed files, then **replace the entire table** with just those rows — silently deleting every 2026 row that took multiple rounds of resolution-cascade debugging to get right (see `Draft_setup_doc.md` §2).

The fix was applied at exactly one layer:

| Notebook | Change | Merge key |
|---|---|---|
| `bronze_draft` | `overwrite` → `MERGE INTO` | `(draft_year, overall_pick)` — required a **one-time backfill**, since `overall_pick` wasn't previously extracted at Bronze (only later, in Silver, from the `raw_json` blob). Ran once via `util_scratchpad` using `get_json_object` before switching the notebook over. |
| `bronze_prospect_search` | `overwrite` → `MERGE INTO` | `(draft_year, overall_pick)` |
| `bronze_prospect_landing` | `overwrite` → `MERGE INTO` | `player_id` |

Each follows the same pattern: create the table if it doesn't exist yet; otherwise merge, matching on the key above, `UPDATE SET *` when matched (refreshes a row if this run re-fetched it) and `INSERT *` when not (adds a new row). Critically, there's no `WHEN NOT MATCHED BY SOURCE` clause — rows already in the table whose key isn't part of *this run's* batch (e.g. last year's picks, when this run is scoped to a new year) are left untouched. `spark.databricks.delta.schema.autoMerge.enabled` is set beforehand so the merge tolerates schema evolution.

**Silver and Gold notebooks needed no changes.** All five (`silver_draft_picks`, `gold_draft_picks`, `silver_bridge_prospect_player`, `silver_fact_prospect_season_totals`, `gold_fact_prospect_season_total`) already read the **entire** upstream table with no date/year filter, and fully recompute their own output from it. `overwrite` is only dangerous when it's fed a partial input; fed the complete, now-accumulating Bronze/Silver history, recomputing the full downstream table every run is correct and intentional — it's how the pipeline picks up newly-resolved prospects or corrected data without any special incremental logic.

---

## 5. Session tag

All 11 notebook activities share one `sessionTag` value (matching the convention already used in `pl_ingestion` (`"1"`), `pl_bronze` (`"2"`), and `pl_gold_dim` (`"4"`) — each of those pipelines tags all of its own activities identically). This lets Fabric reuse one warm Spark session across the whole chain instead of paying cold-start cost 11 times. All 11 notebooks here only depend on the default `lh_Main` lakehouse (none need the custom `env_nhl_api` environment that `raw_games`/`raw_edge_stats_*`/`raw_players` require), so there's no environment mismatch blocking session sharing. Requires **High Concurrency for pipelines** enabled at the workspace level (Settings → Data Engineering/Science → Spark Settings) — if it's off, the tag is simply inert and each activity runs its own session as before.

---

## 6. Running it for a new draft year

1. Open `pl_prospect_history` in the Fabric pipeline editor.
2. Click **Run** (manual trigger).
3. In the parameters dialog, set `draft_years` to the target year (e.g. `2027`, or `2025,2027` to backfill more than one year in a single run). Don't need to touch `ingestion_date` — it's no longer a parameter.
4. Confirm. The stored default (`2026`) is untouched by this — it's only used when a caller doesn't override it (e.g. an unparameterized future invocation from `pl_main`).

Because of the Bronze MERGE fix (§4), running this for a new year is additive — existing years' data in every downstream table is preserved.

---

## 7. Relationship to `pl_main`

`pl_prospect_history` is **not currently invoked by `pl_main`** — it's run standalone. It could be added as a new `InvokePipeline` activity in `pl_main` later, following the same pattern `pl_main` already uses for `pl_ingestion`/`pl_bronze`/`pl_silver`/`pl_gold_dim`/`pl_gold_fact`. Since the draft only happens once a year, this is a deliberate choice to keep it a manually-triggered, year-scoped action rather than something that runs on every regular daily/scheduled pipeline execution.

---

## Known limitations / future work

- `raw_players`, `raw_games`, and the edge-stats Raw notebooks still hardcode a single `SEASON` value with no equivalent accumulate-safe design — this pipeline and its Bronze-merge fix don't touch them. A similar audit would be needed before those could support multi-season backfills the way this chain now supports multi-draft-year backfills.
- The `Set ingestion_date` variable pattern solves the blank-parameter risk at the pipeline level for this specific pipeline. The underlying notebooks' own guard clauses (`if ingestion_date is None`) were **not** changed to `if not ingestion_date` — they'd still be vulnerable to the empty-string gotcha if run through a different pipeline, or manually triggered some other way, that leaves the parameter blank.
- No automated test/validation step (e.g. row-count sanity check, watermark comparison) runs at the end of the pipeline — success today just means every activity returned "Succeeded," not that the row counts look right. `util_check_gold_layer.Notebook` exists in the workspace but isn't wired into this pipeline.
