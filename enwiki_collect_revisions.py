"""Collect English-Wikipedia edits (revisions) for a chosen UTC time window.

Source : MediaWiki Action API  https://en.wikipedia.org/w/api.php?action=query&list=allrevisions
Window : [--start, --end)  (start inclusive, end exclusive). Default = first batch:
         2026-09-15T00:00:00Z .. 2026-09-15T06:00:00Z

Everything lives in data/enwiki_revisions/ :
  revisions_YYYY-MM-DD.jsonl.gz     data, one file per UTC day (later windows append to it)
  checkpoints/<start>__<end>.json   resume state, one per window (status: in_progress | complete)
  collector.log                     human-readable log of every run (appended)
  runs.jsonl                        one JSON line per run: window, counts, status, timings

Properties: resumable, disk-capped (--max-gb), polite (sequential, maxlag=5, backoff). Stdlib only.

Examples:
  python3 enwiki_collect_revisions.py                       # first batch: 15-Sep 00:00-06:00
  python3 enwiki_collect_revisions.py --start 2026-09-15T06:00:00Z --end 2026-09-16T00:00:00Z
  python3 enwiki_collect_revisions.py --start 2026-09-15T00:00:00Z --end 2026-10-15T00:00:00Z
  python3 enwiki_collect_revisions.py --test                # 3 requests only
"""
import argparse
import collections
import gzip
import json
import logging
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

# ---- configuration ---------------------------------------------------------
USER_AGENT = "WikiPulse/0.1 (your-email@example.com)"   # <- put your real contact email
DEFAULT_START = "2026-09-15T00:00:00Z"
DEFAULT_END = "2026-09-15T06:00:00Z"
API = "https://en.wikipedia.org/w/api.php"
OUT = Path(__file__).parent / "data" / "enwiki_revisions"
CP_DIR = OUT / "checkpoints"
LOG_FILE = OUT / "collector.log"
RUNS_FILE = OUT / "runs.jsonl"
PROPS = "ids|timestamp|user|userid|size|flags|comment|tags"   # ids => revid + parentid
# ---------------------------------------------------------------------------

log = logging.getLogger("collector")


def setup_logging():
    OUT.mkdir(parents=True, exist_ok=True)
    CP_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)
    for h in (logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler(sys.stdout)):
        h.setFormatter(fmt)
        log.addHandler(h)


def parse(ts):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def dir_bytes():
    return sum(p.stat().st_size for p in OUT.glob("revisions_*.jsonl.gz"))


def api_get(params, retries=6):
    url = API + "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, "Accept-Encoding": "gzip"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip":
                    raw = gzip.decompress(raw)
                data = json.loads(raw)
            err = data.get("error")
            if err and err.get("code") == "maxlag":      # servers busy -> wait and retry
                wait = int(err.get("lag", 5)) + 2
                log.warning("maxlag, waiting %ss (attempt %d)", wait, attempt + 1)
                time.sleep(wait)
                continue
            if err:
                raise RuntimeError(f"API error: {err}")
            return data
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                wait = min(60, 2 ** attempt * 3)
                log.warning("HTTP %s, retrying in %ss (attempt %d)", e.code, wait, attempt + 1)
                time.sleep(wait)
                continue
            raise
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            wait = min(60, 2 ** attempt * 3)
            log.warning("network error %r, retrying in %ss (attempt %d)", e, wait, attempt + 1)
            time.sleep(wait)
    raise RuntimeError("too many retries")


def flatten(data):
    """allrevisions groups revisions by page; emit one flat row per revision."""
    for page in data.get("query", {}).get("allrevisions", []):
        for rev in page.get("revisions", []):
            yield {
                "pageid": page["pageid"], "ns": page["ns"], "title": page["title"],
                "revid": rev["revid"], "parentid": rev.get("parentid"),
                "ts": rev["timestamp"], "user": rev.get("user"), "userid": rev.get("userid"),
                "anon": rev.get("anon", False), "bot_flag": None,  # not in this API; join with user groups later
                "minor": rev.get("minor", False), "size": rev.get("size"),
                "comment": rev.get("comment"), "tags": rev.get("tags", []),
                "userhidden": rev.get("userhidden", False),
                "commenthidden": rev.get("commenthidden", False),
            }


def save_cp(path, cp):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cp, indent=1))
    tmp.replace(path)                                   # atomic: never leaves a half-written checkpoint


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=DEFAULT_START, help="window start, UTC ISO (inclusive)")
    ap.add_argument("--end", default=DEFAULT_END, help="window end, UTC ISO (exclusive)")
    ap.add_argument("--max-gb", type=float, default=8.0, help="stop when output folder reaches this size")
    ap.add_argument("--test", action="store_true", help="only 3 requests")
    a = ap.parse_args()

    setup_logging()
    cp_path = CP_DIR / f"{a.start.replace(':', '')}__{a.end.replace(':', '')}.json"
    cp = json.loads(cp_path.read_text()) if cp_path.exists() else {
        "start": a.start, "end": a.end, "status": "in_progress", "continue": None,
        "total": 0, "last_ts": a.start, "requests": 0, "created": datetime.now(timezone.utc).isoformat()}
    if cp["status"] == "complete":
        log.info("Window %s .. %s already complete (%s revisions). Nothing to do.", a.start, a.end, f"{cp['total']:,}")
        return
    log.info("=== run start: window %s .. %s | resume=%s | already have %s revs | cap %.1f GB",
             a.start, a.end, bool(cp["continue"]), f"{cp['total']:,}", a.max_gb)

    t0, req_count, new_rows, handle, handle_day = time.time(), 0, 0, None, None
    ns_counts, first_ts, last_seen, status = collections.Counter(), None, cp["last_ts"], "interrupted"
    span = (parse(a.end) - parse(a.start)).total_seconds()
    limit_bytes = a.max_gb * 1e9
    reached_end = False
    try:
        while True:
            params = {"action": "query", "list": "allrevisions", "arvdir": "newer",
                      "arvstart": a.start, "arvend": a.end, "arvlimit": "500",
                      "arvprop": PROPS, "maxlag": "5", "format": "json", "formatversion": "2"}
            if cp["continue"]:
                params.update(cp["continue"])           # arvcontinue + continue tokens
            data = api_get(params)
            req_count += 1

            for row in flatten(data):
                if row["ts"] >= a.end:                  # end is exclusive (API's arvend is inclusive)
                    reached_end = True
                    continue                            # rows inside a batch are grouped by page, not time-ordered
                day = row["ts"][:10]
                if day != handle_day:
                    if handle:
                        handle.close()
                    handle = gzip.open(OUT / f"revisions_{day}.jsonl.gz", "at", encoding="utf-8")
                    handle_day = day
                handle.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")
                cp["total"] += 1
                new_rows += 1
                ns_counts[row["ns"]] += 1
                first_ts = first_ts or row["ts"]
                last_seen = row["ts"]
            if handle:
                handle.flush()

            cp["last_ts"] = last_seen
            cp["continue"] = None if reached_end else data.get("continue")
            cp["requests"] += 1
            cp["status"] = "complete" if cp["continue"] is None else "in_progress"
            cp["updated"] = datetime.now(timezone.utc).isoformat()
            save_cp(cp_path, cp)                        # checkpoint only after data is flushed

            if req_count % 10 == 0 or cp["status"] == "complete":
                frac = min(1.0, (parse(last_seen) - parse(a.start)).total_seconds() / span) if span else 1
                el = time.time() - t0
                eta = el / max(frac, 1e-6) * (1 - frac) / 60 if cp["status"] != "complete" else 0
                log.info("progress: %s revs | at %s | %.1f%% | %.3f GB on disk | %.1f req/s | ETA ~%.0f min",
                         f"{cp['total']:,}", last_seen, frac * 100, dir_bytes() / 1e9, req_count / el, eta)
            if cp["status"] == "complete":
                status = "complete"
                break
            if req_count % 20 == 0 and dir_bytes() >= limit_bytes:
                status = "disk_budget_reached"
                log.warning("disk budget %.1f GB reached at %s; stopping (re-run with higher --max-gb to resume)",
                            a.max_gb, last_seen)
                break
            if a.test and req_count >= 3:
                status = "test"
                break
    except KeyboardInterrupt:
        log.warning("interrupted by user - progress saved, re-run the same command to resume")
    except Exception:
        status = "error"
        log.exception("run failed - progress saved up to the last checkpoint")
    finally:
        if handle:
            handle.close()
        summary = {
            "run_finished": datetime.now(timezone.utc).isoformat(), "window_start": a.start, "window_end": a.end,
            "status": status, "requests": req_count, "revisions_this_run": new_rows,
            "revisions_window_total": cp["total"], "first_ts": first_ts, "last_ts": last_seen,
            "namespace_counts": dict(sorted(ns_counts.items())), "seconds": round(time.time() - t0, 1),
            "output_gb_total": round(dir_bytes() / 1e9, 4),
        }
        with open(RUNS_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(summary) + "\n")
        log.info("=== run end: %s | %s new revs (%s total for window) | %s .. %s | ns=%s | %.3f GB",
                 status, f"{new_rows:,}", f"{cp['total']:,}", first_ts, last_seen,
                 dict(sorted(ns_counts.items())), dir_bytes() / 1e9)


if __name__ == "__main__":
    sys.exit(main())
