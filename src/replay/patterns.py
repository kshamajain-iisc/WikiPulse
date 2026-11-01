"""Small deterministic recentchanges fixtures with separate ground-truth labels.

Synthetic page/change IDs never claim to identify actual Wikimedia revisions.
Related pages are a controlled group only; Week 4 graph evidence is not invented.
"""

from datetime import datetime, timedelta, timezone

PATTERNS = (
    ("edit_burst", 24, 300, False, False, True),
    ("large_deletion", 18, -15000, False, False, True),
    ("human_heavy", 30, 500, False, False, True),
    ("bot_heavy", 40, 100, True, False, False),
    ("repeated_reverts", 24, 400, False, True, True),
    ("related_alpha", 20, 400, False, False, True),
    ("related_beta", 20, 400, False, False, True),
    ("normal_high_volume", 24, 20, False, False, False),
    ("normal_quiet", 2, 20, False, False, False),
)


def controlled_records():
    """One complete synthetic training day and a disjoint evaluation window."""
    start = datetime(2026, 10, 1, tzinfo=timezone.utc)
    training, evaluation, labels = [], [], []
    change_id = 9_000_000_000

    def edits(page, name, when, count, delta, bot, revert):
        nonlocal change_id
        records = []
        for index in range(count):
            change_id += 1
            old = 1_000_000
            records.append({
                "rcid": change_id, "revid": change_id, "old_revid": change_id - 1,
                "pageid": page, "ns": 0, "title": "Synthetic / " + name,
                "type": "edit", "timestamp": (when + timedelta(seconds=index % 240)).isoformat(),
                "user": f"Synthetic editor {index % 12}", "userid": index % 12 + 1,
                "bot": bot, "minor": index % 3 == 0, "oldlen": old, "newlen": old + delta,
                "tags": ["mw-undo"] if revert else [], "comment": "WikiPulse controlled fixture",
            })
        return records

    for page, (name, count, delta, bot, revert, expected) in enumerate(PATTERNS, 1):
        # All windows are represented. Small variation gives the ML scaler real
        # variance, and the high-volume control has a high-volume baseline.
        for index in range(288):
            normal_count = 22 + index % 5 if name == "normal_high_volume" else 1 + index % 3
            training.extend(edits(page, name, start + timedelta(minutes=index * 5), normal_count,
                                  10 + index % 5 * 10, bot, False))
        evaluation.extend(edits(page, name, start + timedelta(days=1), count, delta, bot, revert))
        labels.append({"title": "Synthetic / " + name, "pattern": name,
                       "expected_alert": expected, "related_group": "demo-group" if name.startswith("related_") else None})
    return training, evaluation, labels
