"""Milestone 4.1: Historical Data Cleanup, Deduplication & Curated Parquet Ingestion.

What this script does:
1. Audits all `historical/data/enwiki_recentchanges/rc_*.jsonl.gz` files and checkpoints.
2. Optional (`--clean-raw-overlap`): Deduplicates overlapped raw `.jsonl.gz` files in-place
   (specifically `rc_2026-09-26.jsonl.gz` and `rc_2026-09-27.jsonl.gz`, or all days) by `rcid`.
3. Reads all daily `rc_*.jsonl.gz` files in PySpark, deduplicates by `rcid`, filters out
   `categorize` noise, normalizes to `RAW_EDIT_SCHEMA`, and writes partitioned Parquet to
   `historical/data/curated/rc_events/`.
4. Logs the Proposal performance metric: >= 99% of valid accepted events reach storage,
   and compares compressed JSONL.gz vs Parquet size on disk.

Usage:
    .venv/bin/python3 Repo/src/historical/ingest_curate.py --clean-raw-overlap
"""

import argparse
import gzip
import json
import sys
import time
import zlib
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import functions as F
from src.config.settings import CURATED_TABLES, RAW_RC_DIR, get_spark_session, safe_read_parquet
from src.models.schemas import normalize_recentchanges_df


def iter_recoverable_gz_lines(gz_path: Path) -> tuple[Iterator[str], bool]:
    """Yield all valid JSON lines from a .jsonl.gz file, seamlessly recovering multi-member
    appended streams even if an interrupted run left an unterminated gzip member."""
    try:
        # Fast path: standard single/multi-member clean gzip check
        lines: list[str] = []
        with gzip.open(gz_path, "rt", encoding="utf-8") as fin:
            for line in fin:
                if line.strip():
                    lines.append(line)
        return iter(lines), False
    except (zlib.error, EOFError, OSError):
        pass

    # Recovery path: scan for gzip member headers (\x1f\x8b\x08) and decompress each member
    raw = gz_path.read_bytes()
    header_offsets: list[int] = []
    pos = 0
    while pos < len(raw):
        idx = raw.find(b"\x1f\x8b\x08", pos)
        if idx == -1:
            break
        # Valid gzip header: method=8, flags <= 31, reserved=0
        if idx + 10 <= len(raw) and raw[idx + 3] < 32 and raw[idx + 8] in (0, 2, 4):
            header_offsets.append(idx)
        pos = idx + 1

    if not header_offsets:
        header_offsets = [0]

    recovered_lines: list[str] = []
    for i, start_idx in enumerate(header_offsets):
        end_idx = header_offsets[i + 1] if i + 1 < len(header_offsets) else len(raw)
        segment = raw[start_idx:end_idx]
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        out = bytearray()
        # Decompress in 1024-byte chunks so an unterminated member salvages up to the final line
        for c_pos in range(0, len(segment), 1024):
            try:
                out.extend(d.decompress(segment[c_pos : c_pos + 1024]))
            except zlib.error:
                break
        text = out.decode("utf-8", "replace")
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                json.loads(line)
                recovered_lines.append(line + "\n")
            except json.JSONDecodeError:
                continue

    return iter(recovered_lines), True


def deduplicate_raw_gz_file(gz_path: Path) -> tuple[int, int, str, str, bool]:
    """Deduplicate and repair a single `rc_YYYY-MM-DD.jsonl.gz` file in-place by `rcid`."""
    seen_rcids: set[int] = set()
    total_rows = 0
    kept_rows = 0
    min_ts = ""
    max_ts = ""
    tmp_path = gz_path.with_suffix(".jsonl.gz.tmp")

    line_iter, was_repaired = iter_recoverable_gz_lines(gz_path)

    with gzip.open(tmp_path, "wt", encoding="utf-8") as fout:
        for line in line_iter:
            if not line.strip():
                continue
            total_rows += 1
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                was_repaired = True
                continue
            rcid = row.get("rcid")
            if rcid is not None:
                if rcid in seen_rcids:
                    continue
                seen_rcids.add(rcid)
            ts = row.get("timestamp", "")
            if ts:
                if not min_ts or ts < min_ts:
                    min_ts = ts
                if not max_ts or ts > max_ts:
                    max_ts = ts
            fout.write(line if line.endswith("\n") else line + "\n")
            kept_rows += 1

    if total_rows != kept_rows or was_repaired:
        tmp_path.replace(gz_path)
    else:
        tmp_path.unlink(missing_ok=True)

    return total_rows, kept_rows, min_ts, max_ts, was_repaired


def main() -> int:
    ap = argparse.ArgumentParser(description="Clean up RecentChanges overlap and build curated Parquet lake.")
    ap.add_argument(
        "--clean-raw-overlap",
        action="store_true",
        help="Deduplicate raw rc_2026-09-26.jsonl.gz and rc_2026-09-27.jsonl.gz (and any overlapped files) in-place.",
    )
    ap.add_argument(
        "--days",
        default="2026-09-26,2026-09-27",
        help="Comma-separated YYYY-MM-DD dates to deduplicate in-place when --clean-raw-overlap is set (or 'all').",
    )
    ap.add_argument(
        "--skip-parquet",
        action="store_true",
        help="Only run raw file cleanup/audit without running the full Spark Parquet write.",
    )
    args = ap.parse_args()

    print("=" * 76)
    print(" WIKIPULSE MILESTONE 4.1: DATA CLEANUP & CURATED PARQUET INGESTION")
    print("=" * 76)

    if args.clean_raw_overlap:
        if args.days.strip().lower() == "all":
            target_files = sorted(RAW_RC_DIR.glob("rc_*.jsonl.gz"))
        else:
            target_files = [
                RAW_RC_DIR / f"rc_{d.strip()}.jsonl.gz"
                for d in args.days.split(",")
                if d.strip()
            ]
        print(f"\n[Step 1] In-place raw JSONL.gz deduplication ({len(target_files)} files):")
        for fpath in target_files:
            if not fpath.exists():
                print(f"  • {fpath.name}: not found, skipping")
                continue
            t0 = time.time()
            total, kept, min_ts, max_ts, repaired = deduplicate_raw_gz_file(fpath)
            removed = total - kept
            repair_tag = " [REPAIRED GZIP STREAM]" if repaired else ""
            print(
                f"  • {fpath.name}: {total:,} -> {kept:,} rows "
                f"(removed {removed:,} duplicates){repair_tag} | range [{min_ts} .. {max_ts}] ({time.time() - t0:.1f}s)"
            )

    if args.skip_parquet:
        print("\nRaw cleanup complete (--skip-parquet specified).")
        return 0

    # Step 2: Spark Ingestion -> Deduplicated, Partitioned Parquet Lake
    print("\n[Step 2] Running Spark ingestion across all historical/data/enwiki_recentchanges/rc_*.jsonl.gz ...")
    t_start = time.time()
    spark = get_spark_session(app_name="WikiPulse-Milestone4-IngestCurate")

    import shutil
    import tempfile

    raw_files = sorted(str(p) for p in RAW_RC_DIR.glob("rc_*.jsonl.gz"))
    raw_df = spark.read.json(raw_files)

    # Single-pass count of total vs valid (edit/new/log) events
    counts_row = raw_df.select(
        F.count("*").alias("total_raw"),
        F.sum(F.when(F.col("type").isin("edit", "new", "log"), 1).otherwise(0)).alias("valid_non_cat"),
    ).collect()[0]
    total_raw_events = int(counts_row["total_raw"] or 0)
    valid_non_cat_raw = int(counts_row["valid_non_cat"] or 0)

    # Filter out categorize noise, deduplicate by rcid, and normalize to RAW_EDIT_SCHEMA
    non_categorize_df = raw_df.filter(F.col("type").isin("edit", "new", "log"))
    dedup_df = non_categorize_df.dropDuplicates(["rcid"])
    norm_df = normalize_recentchanges_df(dedup_df, filter_categorize=False)

    out_path = CURATED_TABLES["rc_events"]
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Write to a clean local staging directory in /tmp first to avoid macOS Finder/Icon\r
    # interference with Hadoop's _temporary directory during commitJob, then move into place.
    stage_dir = Path(tempfile.gettempdir()) / "wikipulse_rc_events_stage"
    if stage_dir.exists():
        shutil.rmtree(stage_dir, ignore_errors=True)

    (
        norm_df.repartition(16, "day")
        .write.mode("overwrite")
        .partitionBy("day")
        .parquet(str(stage_dir))
    )

    if out_path.exists():
        shutil.rmtree(out_path, ignore_errors=True)
    shutil.move(str(stage_dir), str(out_path))

    # Remove any stray macOS Icon\r files inside the curated output
    for icon_file in out_path.rglob("Icon\r"):
        icon_file.unlink(missing_ok=True)

    # Verify written Parquet count & compute storage metrics
    curated_df = safe_read_parquet(spark, out_path)
    curated_count = curated_df.count()
    distinct_days = curated_df.select("day").distinct().count()

    raw_bytes = sum(p.stat().st_size for p in RAW_RC_DIR.glob("rc_*.jsonl.gz"))
    parquet_bytes = sum(p.stat().st_size for p in out_path.rglob("*.parquet"))
    elapsed = time.time() - t_start

    metrics = {
        "total_raw_events": total_raw_events,
        "valid_non_categorize_raw_events": valid_non_cat_raw,
        "curated_deduplicated_events": curated_count,
        "distinct_days": distinct_days,
        "raw_jsonl_gz_mb": round(raw_bytes / 1e6, 2),
        "curated_parquet_mb": round(parquet_bytes / 1e6, 2),
        "ingestion_runtime_sec": round(elapsed, 2),
    }

    bench_dir = CURATED_TABLES["benchmarks"]
    bench_dir.mkdir(parents=True, exist_ok=True)
    (bench_dir / "ingestion_summary.json").write_text(json.dumps(metrics, indent=2))

    print(f"  • Total raw events read (incl. categorize) : {total_raw_events:,}")
    print(f"  • Valid edit/new/log events (pre-dedup)    : {valid_non_cat_raw:,}")
    print(f"  • Curated deduplicated events in Parquet   : {curated_count:,} across {distinct_days} days")
    print(f"  • Raw JSONL.gz size vs Curated Parquet     : {metrics['raw_jsonl_gz_mb']} MB -> {metrics['curated_parquet_mb']} MB")
    print(f"  • Completed in                             : {elapsed:.1f}s")
    print(f"  • Output path                              : {out_path}")

    spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

