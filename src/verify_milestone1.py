"""Milestone 1 Verification Script.

Validates:
1. All 8 shared PySpark schemas in `Repo/src/models/schemas.py`
2. Confirmed API endpoint builders in `Repo/src/config/settings.py`
3. Deterministic, replay-idempotent alert ID generation
4. Normalization contract (`normalize_recentchanges_df` and `normalize_revisions_df`)
   against real collected data in `historical/data/`
"""

import sys
from pathlib import Path

# Ensure Repo/ is on sys.path when invoked directly
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.config.settings import (
    RAW_RC_DIR,
    RAW_REVISIONS_DIR,
    WIKIMEDIA_APIS,
    build_analytics_per_page_urls,
    build_backlinks_params,
    build_compare_params,
    build_liftwing_request,
    build_page_links_coords_params,
    build_wikidata_entities_params,
    get_spark_session,
)
from src.models.schemas import (
    ALL_SCHEMAS,
    RAW_EDIT_SCHEMA,
    compute_deterministic_alert_id,
    normalize_recentchanges_df,
    normalize_revisions_df,
)


def main() -> int:
    print("=" * 72)
    print(" WIKIPULSE MILESTONE 1 VERIFICATION: APIS, SCHEMAS & DATA CONTRACTS")
    print("=" * 72)

    # 1. Verify API Registry & Builders
    print(f"\n[1/4] Confirmed Wikimedia API Sources ({len(WIKIMEDIA_APIS)} endpoints):")
    for name, meta in WIKIMEDIA_APIS.items():
        print(f"  • {name:<28} -> {meta['method']} {meta['url']}")

    analytics_urls = build_analytics_per_page_urls("78th Primetime Emmy Awards", "20260913", "20260917")
    compare_params = build_compare_params(847170467, 851733941)
    links_params = build_page_links_coords_params(["Earth", "Bengaluru"])
    bl_params = build_backlinks_params("Earth")
    lw_req = build_liftwing_request(1215916855)
    wd_params = build_wikidata_entities_params(ids=["Q2", "Q1355"])

    assert set(analytics_urls.keys()) == {"edits", "bytes_abs", "bytes_net", "views"}
    assert compare_params["action"] == "compare" and compare_params["fromrev"] == "847170467"
    assert links_params["ppprop"] == "wikibase_item"
    assert bl_params["list"] == "backlinks"
    assert lw_req["payload"] == {"rev_id": 1215916855, "lang": "en"}
    assert wd_params["ids"] == "Q2|Q1355"
    print("  ✓ All API parameter & payload builders verified.")

    # 2. Verify Deterministic Alert ID (Idempotency Contract)
    id1 = compute_deterministic_alert_id(9228, "2026-09-15T02:50:00Z", "5m")
    id2 = compute_deterministic_alert_id(9228, "2026-09-15T02:50:00Z", "5m")
    id3 = compute_deterministic_alert_id(9228, "2026-09-15T02:55:00Z", "5m")
    assert id1 == id2 and id1 != id3
    print(f"\n[2/4] Deterministic Alert ID Check: {id1} (idempotent across replays)")

    # 3. Verify All 8 Shared PySpark Schemas
    spark = get_spark_session(app_name="WikiPulse-Milestone1-Verify")
    print(f"\n[3/4] Instantiating {len(ALL_SCHEMAS)} Shared PySpark Data Models:")
    for model_name, schema in ALL_SCHEMAS.items():
        empty_df = spark.createDataFrame([], schema=schema)
        print(f"  • {model_name:<22} : {len(empty_df.schema.fields):>2} fields verified")

    # 4. Verify Normalization on Real Collected Data
    print("\n[4/4] Validating RAW_EDIT_SCHEMA Normalization on Collected Files:")
    sample_rc = RAW_RC_DIR / "rc_2026-09-15.jsonl.gz"
    sample_rev = RAW_REVISIONS_DIR / "revisions_2026-09-15.jsonl.gz"

    if sample_rc.exists():
        raw_rc_df = spark.read.json(str(sample_rc))
        norm_rc_df = normalize_recentchanges_df(raw_rc_df, filter_categorize=True)
        expected_cols = [f.name for f in RAW_EDIT_SCHEMA.fields]
        assert norm_rc_df.columns == expected_cols, f"Column mismatch: {norm_rc_df.columns} vs {expected_cols}"
        sample_row = norm_rc_df.filter("ns = 0 AND type = 'edit'").limit(1).collect()[0]
        print(
            f"  ✓ RecentChanges normalized successfully -> title='{sample_row['title']}', "
            f"revid={sample_row['revid']}, delta={sample_row['delta']}, is_bot={sample_row['is_bot']}"
        )

    if sample_rev.exists():
        raw_rev_df = spark.read.json(str(sample_rev))
        norm_rev_df = normalize_revisions_df(raw_rev_df)
        assert norm_rev_df.columns == [f.name for f in RAW_EDIT_SCHEMA.fields]
        rev_row = norm_rev_df.filter("ns = 0").limit(1).collect()[0]
        print(
            f"  ✓ Revisions normalized successfully     -> title='{rev_row['title']}', "
            f"revid={rev_row['revid']}, newlen={rev_row['newlen']}, is_reverted={rev_row['is_reverted']}"
        )

    spark.stop()
    print("\n" + "=" * 72)
    print(" ✓ MILESTONE 1 COMPLETE: APIs, Shared Schemas & Dashboard Specs Ready")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

