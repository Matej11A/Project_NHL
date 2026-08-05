# Draft & Prospect Data Architecture

This document describes the design and build of the NHL draft-pick and pre-NHL prospect-history data, added to the SportsIQ medallion lakehouse (Fabric / PySpark). It covers two related but independently-built sources:

1. **`dim_draft_picks`** — draft-day data for every pick in a given draft year.
2. **Prospect season history** — pre-NHL playing history (junior, college, international) for drafted players who haven't appeared in an NHL game yet, split into a player-ID resolution layer (`bridge_prospect_player`) and a stats layer (`fact_prospect_season_totals`).

Both follow the project's standard Bronze → Silver → Gold pattern: Bronze stores the raw API response untouched (as a `raw_json` string column plus lineage columns), Silver applies an explicit schema with drift checking, Gold selects final columns and computes surrogate keys.

---

## 1. `dim_draft_picks`

### Source

```
https://api-web.nhle.com/v1/draft/picks/{year}/all
```

Hit directly against the raw NHL API, bypassing the `nhl-api-py` wrapper. The wrapper's stats-based `DraftQuery` (`report_type='bios'`) was tested and rejected — it excludes players with zero NHL games, which is most of a draft class.

Returns every pick regardless of NHL experience: `round`, `pickInRound`, `overallPick`, `teamId`, `teamAbbrev`, `firstName`/`lastName` (nested `.default`), `positionCode`, `countryCode`, `height`, `weight`, `amateurLeague`, `amateurClubName`, `teamLogoDark`.

**Known payload gaps:**
- No NHL `playerId` — can't join directly to a player dimension.
- `teamId` is unreliable (confirmed incorrect in testing) — not used for a `team_sk` join.

### Pipeline

| Layer | Notebook | Table | Notes |
|---|---|---|---|
| Raw | `raw_draft` | `Files/raw/draft_picks/{ingestion_date}/{year}.json` | One file per draft year, parameterized by `draft_years` (comma-separated) |
| Bronze | `bronze_draft` | `bronze.dim_draft_picks` | One row per pick, `raw_json` + `draft_year` + lineage columns |
| Silver | `silver_draft_picks` | `silver.dim_draft_picks` | Explicit `StructType`, `check_schema_drift`, snake_case columns |
| Gold | `gold_draft_picks` | `gold.dim_draft_picks` | `draft_pick_sk = xxhash64(draft_year, overall_pick)` |

### Why `dim_`, not `fact_`

Originally scoped as `fact_draft_picks`. Reclassified to a dimension because a draft-pick row is descriptive (round, pick-in-round, player bio, team) with no additive measures — despite being one row per event, it doesn't aggregate the way a fact table does.

### Deliberate scope decisions

- **No `team_sk`** — `teamId` is untrustworthy; `team_abbrev` is kept as a plain string column, joined in Power BI directly against `dim_nhl_teams`, same treatment as `fact_season_stats.team_name`.
- **No `player_sk`** — no reliable player ID in the raw payload (see §2 for how this gap is closed for prospects specifically).
- **No `dim_date` join** — a draft is a single annual event, not a per-game date; the project's Oct-1 season-representative convention doesn't apply here.

---

## 2. Prospect player-ID resolution — `bridge_prospect_player`

### The problem

Most drafted players are 1-3+ years from an NHL game, so the existing player/roster pipeline (NHL-only, ANA-scoped) has nothing for them. The draft-pick payload has no `playerId` to look one up with.

### Endpoints evaluated

| Endpoint | Result |
|---|---|
| `/v1/draft/prospects/{year}/{category}` | 404 on all 4 categories — dead end |
| `/v1/prospects/{team}` | Real data, but a curated/slow-updating list — a player drafted weeks earlier was missing |
| `search.d3.nhle.com/api/v1/search/player?q={name}` | **Used.** Faster-updating than `/prospects/{team}`; resolves recently-drafted players with zero NHL games |
| `/v1/player/{id}/landing` | **Used.** League-agnostic — returns full history across youth/junior/college/international leagues once a `playerId` is known |

### The resolution cascade — design history

The search endpoint returns up to 20 candidates per name query, often spanning decades of players and unrelated namesakes (a query for "McKenna" returned 5 different people). Building a reliable automated match required several iterations, each one exposing a real bug caught by auditing actual output rather than assuming correctness:

1. **v1 — bio-only matching** (active + country + position + height/weight nearest-neighbor). Worked on hand-picked test cases, but an audit of the full 2026 class found **5 silently wrong matches** (e.g. "Thomas Bleyl" resolved to "Thomas Harley," a real, unrelated player) — the cascade never checked whether the candidate's *name* resembled the draft pick's name at all.
2. **v2 — added a last-name match gate.** Fixed the 5 wrong matches, but requiring `active: true` as a hard filter turned out to have false negatives: 6 real, correctly-named players were excluded because the search index hadn't flagged them active yet (a lag more common for later-round, lower-profile picks).
3. **v3 — demoted `active` to a confidence label instead of a filter.** Fixed those 6, but pulling inactive historical namesakes back into consideration introduced 3 new false ties (e.g. "Beckett Hamilton" tied against an unrelated "Jeff Hamilton" who merely shares a surname) — the last-name-only gate wasn't tight enough once `active` stopped doing that filtering implicitly.
4. **v4 — added a first-name substring requirement.** Fixed the 3 new ties, but broke on a player whose draft-table first name was stored as `"Jeffrey (JP)"` — the parenthetical nickname meant the field could never be a substring of the candidate's plain `"JP Hurlbert"`. It also surfaced a related risk: `"Huff"` is a literal substring of `"Huffman"`, an unrelated player, which had been silently over-matching on the last-name side too.
5. **v5 — final: token-based name matching.** Both names are split into word tokens (`"Jeffrey (JP)"` → `{"jeffrey", "jp"}`); the last name's tokens must be a **subset** of the candidate's tokens (fixes the Huff/Huffman overmatch), and the first name only needs **any overlap** (fixes the nickname case, since either "Jeffrey" or "JP" now counts).

### Final algorithm (`resolve_candidate`)

1. **Tier 0 — name gate.** Normalize both sides (lowercase, strip diacritics via `unicodedata`), tokenize on non-letter boundaries. Reject candidates that don't pass: last-name tokens ⊆ candidate tokens, and first-name tokens ∩ candidate tokens ≠ ∅.
2. **Tier 1 — bio filter.** Among name-matched candidates, filter to `birthCountry == country_code` and `positionCode == position_code` (mapped: draft data uses `LW`/`RW`, search uses `L`/`R`; `C`/`D`/`G` pass through unchanged). If exactly one survives, resolved — tagged `active` or `inactive` for confidence, not filtered on it.
3. **Tier 2 — height/weight tiebreak.** If 2+ candidates survive Tier 1, pick whichever is closest to the draft pick's `height`/`weight` (both already in the same units — inches/pounds, no conversion needed) — but only if the runner-up is at least `margin_threshold=10` combined inches+lbs farther away. Missing height/weight data (common for old historical-player records) is scored as infinite distance, so it can never win a comparison it can't actually be evaluated for.
4. **Unresolved** otherwise, with a specific reason recorded (`no_name_match`, `name_matched_no_country_position`, `ambiguous_gap_N`, `no_candidates`, `no_usable_height_weight_data`) rather than a generic failure.

### Results (2026 draft class)

**220 of 224 picks (98.2%) resolved**, all Tier 1 (the token-based name gate turned out to be discriminating enough on its own that Tier 2's tiebreak was never needed once it was in place). Verified via an identity audit (cross-checking the matched candidate's name against the draft pick's last name) — only 2 flags, both confirmed false alarms from accented characters (e.g. "Björck" vs. audit's plain-ASCII comparison), not real mismatches.

**4 unresolved, each independently confirmed as a genuine dead end rather than a matching-logic gap:**

| Pick | Player | Reason |
|---|---|---|
| #31 | Thomas Bleyl | Not found under any name variant in the search results |
| #63 | *(forfeited pick)* | Not a real player — a team lost the pick as a penalty |
| #64 | Benjamin MacBeath | Not found under any name variant in the search results |
| #148 | Luken Huff | Not found under any name variant in the search results |

### Pipeline

| Layer | Notebook | Table | Notes |
|---|---|---|---|
| Raw | `raw_prospect_search` | `Files/raw/prospect_search/{ingestion_date}/{draft_year}_{overall_pick}.json` | One file per pick, full 20-candidate array landed untouched |
| Bronze | `bronze_prospect_search` | `bronze.dim_prospect_search` | One row per pick, `raw_json` holds the untouched candidate **array** (not a single object, unlike other Bronze sources) |
| Silver | `silver_bridge_prospect_player` | `silver.bridge_prospect_player` | `from_json` with `ArrayType(StructType(...))`, joined to `silver.dim_draft_picks` for bio fields, cascade applied via a Python UDF |

The cascade is implemented as a plain Python function wrapped in a Spark UDF rather than native array expressions — the tiered branching and margin-based tiebreak don't map cleanly onto vectorized operations, and a UDF let the logic be reused verbatim from interactive testing. Row-by-row UDF execution is slower than vectorized Spark, but irrelevant at ~224 rows.

---

## 3. Prospect season history — `fact_prospect_season_totals`

### Source

```
https://api-web.nhle.com/v1/player/{id}/landing
```

Confirmed league-agnostic: for a single `playerId`, `seasonTotals` returns a complete history spanning youth hockey through junior/college through international play, each row keyed by `season`, `leagueAbbrev`, `teamName`, `gameTypeId`, plus stats.

### Key discovery: `gameTypeId`

Some season/league/team combinations appeared as duplicate-looking rows. Inspecting the full raw fields (not just the summary columns) showed the real, previously-invisible difference: `gameTypeId` — **2 = regular season, 3 = playoffs** — the standard NHL API convention. Silver's grain includes `game_type_id` specifically because of this; without it, a player's regular-season and playoff stats for the same team would silently collide on `mode("overwrite")`.

### Schema: skater and goalie stats, unified

`seasonTotals` rows carry different fields depending on position — inspecting a real goalie's and a real skater's raw rows (rather than assuming) surfaced two full, previously-uncaptured stat groups:

- **Skater extended stats:** `shots`, `shootingPctg`, `powerPlayGoals`, `shorthandedGoals`, `gameWinningGoals`, `avgToi` (string, `"MM:SS"` — per-game average)
- **Goalie stats:** `wins`, `losses`, `otLosses`, `goalsAgainst`, `goalsAgainstAvg`, `savePctg`, `shutouts`, `shotsAgainst`, `timeOnIce` (string — season **total**, not per-game, so it can exceed 59 in the minutes place)

Both groups live in one unified schema; each row is simply `null` for whichever side doesn't apply. `losses`/`wins`/`shutouts`/`otLosses` are further nullable *within* the goalie side too — some leagues omit them even for goalie rows.

`teamName` is nested (`{"default": "..."}`), same pattern as `firstName`/`lastName` elsewhere in the project. A sibling field, `teamCommonName` (same locale-variant shape), was found but deliberately not captured — `teamName.default` is sufficient and consistent with how team names are handled elsewhere.

### Pipeline

| Layer | Notebook | Table | Notes |
|---|---|---|---|
| Raw | `raw_prospect_landing` | `Files/raw/prospect_landing/{ingestion_date}/{player_id}.json` | One file per **resolved** player only (`bridge_prospect_player.player_id IS NOT NULL`), deduplicated |
| Bronze | `bronze_prospect_landing` | `bronze.fact_prospect_season_totals` | One row per player, `raw_json` holds the **entire** landing payload (bio, awards, `draftDetails`, `seasonTotals` all together) |
| Silver | *(prospect season totals)* | `silver.fact_prospect_season_totals` | `seasonTotals` extracted via `get_json_object` before parsing/drift-checking, so the check stays scoped and doesn't drown in unrelated payload-field noise; exploded to one row per season/league/team/game-type |
| Gold | `gold_fact_prospect_season_total` | `gold.fact_prospect_season_totals` | 3,172 rows |

### Gold relationship model

This was an explicit open question in the original project scoping (draft picks have no `player_id`; prospect stats have no natural draft-pick key). Resolved by computing the **same** surrogate key formula on both sides:

```
draft_pick_sk = xxhash64(draft_year, overall_pick)
```

`gold.fact_prospect_season_totals.draft_pick_sk` is computed by joining through `bridge_prospect_player` (which carries `draft_year`/`overall_pick`), giving a real foreign key into `gold.dim_draft_picks` — verified with a left-anti join (0 unmatched rows) rather than assumed. No separate bridge/junction table is needed in the Power BI star schema; the bridge table is a Silver-only implementation detail.

**No `player_sk`** — `gold.dim_player` is scoped to the ANA roster only, and would match essentially none of these prospects. `player_id` is kept as a plain informational string column, same treatment as `team_abbrev` in `dim_draft_picks`.

### Unresolved/forfeited picks are not represented in this fact table

Deliberate choice: the fact table only contains real player-seasons (3,172 rows, all from the 220 resolved players). The 4 unresolved/forfeited picks are **not** given placeholder rows. `gold.dim_draft_picks` already contains all 224 picks — a draft-board report should be built from that dimension table directly (all picks render unconditionally), with any stat measure from the fact table correctly showing blank for the 4 picks with no data, rather than the fact table needing to fake a row that represents no actual event.

---

## Summary: full lineage

```
Draft picks:
  raw_draft → bronze.dim_draft_picks → silver.dim_draft_picks → gold.dim_draft_picks

Prospect ID resolution (feeds the join below, not a standalone report table):
  raw_prospect_search → bronze.dim_prospect_search → silver.bridge_prospect_player

Prospect season history:
  raw_prospect_landing → bronze.fact_prospect_season_totals
    → silver.fact_prospect_season_totals → gold.fact_prospect_season_totals
                                              (draft_pick_sk → gold.dim_draft_picks)
```

## Known limitations / future work

- **4 picks require manual review** (see table in §2) — no automated path resolves them; a human would need to identify the correct `playerId` some other way and record it in a manual-override mechanism (discussed but not built — would be a small reference table LEFT JOINed in at Silver, taking precedence over the cascade so it survives re-runs).
- **`margin_threshold=10`** (Tier 2 tiebreak) and the token-matching approach were tuned against the 2026 draft class specifically. A future draft year should re-validate both against real data before trusting them, not assume they generalize.
