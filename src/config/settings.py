"""Milestone 1: Central configuration, paths, Spark session factory, and confirmed Wikimedia API contracts."""

import os
from pathlib import Path
from typing import Any, Dict

# ---------------------------------------------------------------------------
# 1. Directory & Storage Layout
# ---------------------------------------------------------------------------
# Repo/src/config/settings.py -> Repo/ -> Project-WikiPulse/
REPO_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = REPO_ROOT.parent

# `historical/data` serves as the shared raw & curated data lake
HISTORICAL_DATA_DIR = PROJECT_ROOT / "historical" / "data"
RAW_RC_DIR = HISTORICAL_DATA_DIR / "enwiki_recentchanges"
RAW_REVISIONS_DIR = HISTORICAL_DATA_DIR / "enwiki_revisions"
RAW_ANALYTICS_DIR = HISTORICAL_DATA_DIR / "analytics"
CURATED_DIR = HISTORICAL_DATA_DIR / "curated"

# Curated Parquet table paths (Milestones 4, 5, 6)
CURATED_TABLES = {
    "rc_events": CURATED_DIR / "rc_events",
    "page_baselines": CURATED_DIR / "page_baselines",
    "rankings_daily": CURATED_DIR / "rankings_daily",
    "churn_bot_summary": CURATED_DIR / "churn_bot_summary",
    "page_windows_5m": CURATED_DIR / "page_windows_5m",
    "page_windows_1h": CURATED_DIR / "page_windows_1h",
    "alerts": CURATED_DIR / "alerts",
    "graph_edges": CURATED_DIR / "graph_edges",
    "revision_diffs": CURATED_DIR / "revision_diffs",
    "map_points": CURATED_DIR / "map_points",
    "revert_risk_eval": CURATED_DIR / "revert_risk_eval",
    "benchmarks": CURATED_DIR / "benchmarks",
    "api_cache": CURATED_DIR / "cache",
}

# ---------------------------------------------------------------------------
# 2. Wikimedia API Policy & Confirmed Endpoints (Milestone 1)
# ---------------------------------------------------------------------------
USER_AGENT = os.environ.get(
    "WIKIPULSE_USER_AGENT",
    "WikiPulse/1.0 (Senthil Raj K; mailto:senthilrajk@iisc.ac.in)",
)

WIKI_DOMAIN = "en.wikipedia.org"
WIKI_DB = "enwiki"
LANGUAGE_CODE = "en"

WIKIMEDIA_APIS: Dict[str, Dict[str, Any]] = {
    "eventstreams_recentchange": {
        "url": "https://stream.wikimedia.org/v2/stream/recentchange",
        "method": "GET",
        "accept": "text/event-stream",
        "retention": "7 to 31 days (replay via ?since=ISO_TIMESTAMP; sparse on older offsets)",
        "purpose": "Live SSE ingestion and short-horizon Kafka replay",
    },
    "eventstreams_revert_risk": {
        "url": "https://stream.wikimedia.org/v2/stream/mediawiki.page_revert_risk_prediction_change.v1",
        "method": "GET",
        "accept": "text/event-stream",
        "retention": "Live stream (~7 days)",
        "purpose": "Live ML revert risk prediction stream",
    },
    "mediawiki_action_api": {
        "url": f"https://{WIKI_DOMAIN}/w/api.php",
        "method": "GET",
        "accept": "application/json",
        "rate_limit": "Sequential with maxlag=5 (~0.8-1.0 req/s polite rate)",
        "retention": "list=recentchanges retains ~30 days; list=allrevisions retains full permanent history",
        "purpose": "Historical recentchanges, allrevisions, compare diffs, links, backlinks, coordinates, and QIDs",
    },
    "wikimedia_analytics_api": {
        "url": "https://wikimedia.org/api/rest_v1/metrics",
        "method": "GET",
        "accept": "application/json",
        "retention": "Multi-year daily and monthly series (lags ~24h)",
        "purpose": "Project-wide and per-page daily edits, absolute/net byte differences, and daily pageviews",
    },
    "liftwing_revert_risk": {
        "url": "https://api.wikimedia.org/service/lw/inference/v1/models/revertrisk-language-agnostic:predict",
        "method": "POST",
        "accept": "application/json",
        "rate_limit": "Polite batch scoring with local disk cache; fallback to heuristic score on timeout/5xx",
        "purpose": "Historical & contextual revision Revert Risk probability scoring (0.0 to 1.0)",
    },
    "wikidata_action_api": {
        "url": "https://www.wikidata.org/w/api.php",
        "method": "GET",
        "accept": "application/json",
        "rate_limit": "Batch up to 50 titles/QIDs per request",
        "purpose": "Structured spatial fallback (P625 coordinates, P276 location, P17 country, P131 region)",
    },
    "wikimedia_dumps": {
        "url": f"https://dumps.wikimedia.org/{WIKI_DB}",
        "method": "GET",
        "accept": "application/json",
        "purpose": "Whole-wiki XML pages-meta-history dump manifest discovery",
    },
}

# ---------------------------------------------------------------------------
# 3. Domain Constants, Namespaces, Tags & Scoring Defaults
# ---------------------------------------------------------------------------
NAMESPACES: Dict[int, str] = {
    0: "Main Article",
    1: "Talk",
    2: "User",
    3: "User Talk",
    4: "Wikipedia Project",
    5: "Project Talk",
    6: "File",
    10: "Template",
    14: "Category",
    100: "Portal",
    118: "Draft",
}

# MediaWiki tags indicating a revision was reverted or performs a revert
REVERTED_TAG = "mw-reverted"
REVERT_ACTION_TAGS = ("mw-rollback", "mw-undo", "mw-manual-revert")
REVERT_TAGS = (REVERTED_TAG,) + REVERT_ACTION_TAGS

# Baseline vs Evaluation split for the Sept 15 - Oct 31 dataset
BASELINE_START_DATE = "2026-09-15"
BASELINE_END_DATE = "2026-10-05"       # 21-day initial baseline window
EVALUATION_START_DATE = "2026-10-06"
EVALUATION_END_DATE = "2026-10-30"
ROLLING_BASELINE_DAYS = 14

#Detector thresholds (Milestone 5)
DETECTOR_DEFAULTS = {
    "fixed_5m_min_human_edits": 6,
    "fixed_1h_min_human_edits": 15,
    "fixed_min_distinct_editors": 3,
    "historical_score_alert_threshold": 65.0,
    "edit_war_revert_ratio_threshold": 0.30,
    "pageview_confirmation_multiplier": 2.5,
    "revert_risk_default_threshold": 0.80,
    "revert_risk_calibrated_thresholds": [0.20, 0.35, 0.50, 0.65, 0.80, 0.90],
}


# ---------------------------------------------------------------------------
# 4. API Request Parameter Builders
# ---------------------------------------------------------------------------
def build_analytics_per_page_urls(
    page_title: str,
    start_yyyymmdd: str,
    end_yyyymmdd: str,
    project: str = WIKI_DOMAIN,
) -> Dict[str, str]:
    """Return the 4 Wikimedia Analytics REST URLs for a single article."""
    base = WIKIMEDIA_APIS["wikimedia_analytics_api"]["url"]
    safe_title = page_title.replace(" ", "_")
    return {
        "edits": f"{base}/edits/per-page/{project}/{safe_title}/all-editor-types/daily/{start_yyyymmdd}/{end_yyyymmdd}",
        "bytes_abs": f"{base}/bytes-difference/absolute/per-page/{project}/{safe_title}/all-editor-types/daily/{start_yyyymmdd}/{end_yyyymmdd}",
        "bytes_net": f"{base}/bytes-difference/net/per-page/{project}/{safe_title}/all-editor-types/daily/{start_yyyymmdd}/{end_yyyymmdd}",
        "views": f"{base}/pageviews/per-article/{project}/all-access/all-agents/{safe_title}/daily/{start_yyyymmdd}/{end_yyyymmdd}",
    }


def build_compare_params(from_revid: int, to_revid: int) -> Dict[str, str]:
    """Build query parameters for MediaWiki `action=compare`."""
    return {
        "action": "compare",
        "fromrev": str(from_revid),
        "torev": str(to_revid),
        "prop": "diff|ids|title|size|timestamp|user|comment",
        "format": "json",
        "formatversion": "2",
        "maxlag": "5",
    }


def build_page_links_coords_params(titles: list[str], limit: int = 500) -> Dict[str, str]:
    """Build query parameters for outgoing links, coordinates, and Wikidata QID."""
    return {
        "action": "query",
        "titles": "|".join(titles[:50]),
        "prop": "info|coordinates|pageprops|links|categories",
        "ppprop": "wikibase_item",
        "coprimary": "primary",
        "plnamespace": "0",
        "pllimit": str(limit),
        "cllimit": str(limit),
        "redirects": "1",
        "format": "json",
        "formatversion": "2",
        "maxlag": "5",
    }


def build_backlinks_params(page_title: str, limit: int = 500) -> Dict[str, str]:
    """Build query parameters for incoming article backlinks (`list=backlinks`)."""
    return {
        "action": "query",
        "list": "backlinks",
        "bltitle": page_title,
        "blnamespace": "0",
        "bllimit": str(limit),
        "format": "json",
        "formatversion": "2",
        "maxlag": "5",
    }


def build_liftwing_request(rev_id: int, lang: str = LANGUAGE_CODE) -> Dict[str, Any]:
    """Build URL, headers, and JSON body for Lift Wing Revert Risk inference."""
    return {
        "url": WIKIMEDIA_APIS["liftwing_revert_risk"]["url"],
        "headers": {
            "User-Agent": USER_AGENT,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        "payload": {"rev_id": int(rev_id), "lang": lang},
    }


def build_wikidata_entities_params(
    ids: list[str] | None = None,
    titles: list[str] | None = None,
    site: str = WIKI_DB,
    lang: str = LANGUAGE_CODE,
) -> Dict[str, str]:
    """Build query parameters for Wikidata `wbgetentities` (by QID list or page titles)."""
    params: Dict[str, str] = {
        "action": "wbgetentities",
        "props": "info|sitelinks|labels|claims",
        "languages": lang,
        "format": "json",
        "formatversion": "2",
    }
    if ids:
        params["ids"] = "|".join(ids[:50])
    elif titles:
        params["sites"] = site
        params["titles"] = "|".join(titles[:50])
    return params


# ---------------------------------------------------------------------------
# 5. Spark Session Factory
# ---------------------------------------------------------------------------
def get_spark_session(app_name: str = "WikiPulse", shuffle_partitions: int = 16, driver_memory: str = "4g"):
    """Initialize a standardized UTC, case-sensitive SparkSession with OpenJDK 17 support."""
    if "JAVA_HOME" not in os.environ or not os.path.exists(
        os.path.join(os.environ.get("JAVA_HOME", ""), "bin", "java")
    ):
        if os.path.exists("/opt/homebrew/opt/openjdk@17"):
            os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"
            os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ.get("PATH", "")

    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.files.ignoreCorruptFiles", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_macos_icon_files(root_dir: Path) -> int:
    """Remove stray 0-byte macOS Finder `Icon\\r` files that break Spark Parquet readers."""
    removed = 0
    if not root_dir.exists():
        return 0
    for p in root_dir.rglob("Icon*"):
        if p.name.startswith("Icon") and p.is_file() and p.stat().st_size == 0:
            try:
                p.unlink(missing_ok=True)
                removed += 1
            except OSError:
                pass
    return removed


def safe_read_parquet(spark, target_path: Path):
    """Read a Parquet directory while strictly filtering to `*.parquet` files so macOS
    0-byte `Icon\\r` files (which sort alphabetically before `part-*`) are never read."""
    clean_macos_icon_files(target_path)
    return spark.read.option("pathGlobFilter", "*.parquet").parquet(str(target_path))


def safe_write_parquet(
    df,
    target_path: Path,
    partition_by: str | list[str] | None = None,
    repartition_cols: list[Any] | None = None,
    repartition_num: int | None = None,
) -> Path:
    """Write a Spark DataFrame to a clean local /tmp staging path first and move into
    `target_path` to avoid macOS Finder / Icon\\r race conditions on `_temporary`."""
    import shutil
    import tempfile
    import uuid

    clean_macos_icon_files(target_path.parent)
    stage_dir = Path(tempfile.gettempdir()) / f"wikipulse_stage_{target_path.name}_{uuid.uuid4().hex[:8]}"
    if stage_dir.exists():
        shutil.rmtree(stage_dir, ignore_errors=True)

    out_df = df
    if repartition_num and repartition_cols:
        out_df = out_df.repartition(repartition_num, *repartition_cols)
    elif repartition_num:
        out_df = out_df.repartition(repartition_num)
    elif repartition_cols:
        out_df = out_df.repartition(*repartition_cols)

    writer = out_df.write.mode("overwrite")
    if partition_by:
        if isinstance(partition_by, str):
            writer = writer.partitionBy(partition_by)
        else:
            writer = writer.partitionBy(*partition_by)

    writer.parquet(str(stage_dir))

    target_path.parent.mkdir(parents=True, exist_ok=True)
    if target_path.exists():
        shutil.rmtree(target_path, ignore_errors=True)
    shutil.move(str(stage_dir), str(target_path))
    clean_macos_icon_files(target_path)
    return target_path

