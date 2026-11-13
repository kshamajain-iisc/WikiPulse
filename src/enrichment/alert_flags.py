"""Compatibility expressions for alert flag columns across stored datasets."""

from pyspark.sql import DataFrame, functions as F

ALERT_FLAG_COLUMNS = (
    "historical_alert_flag", "historical_alert", "is_alert", "fixed_rule_alert",
)


def historical_alert_filter(alerts: DataFrame):
    """Return a boolean expression covering the alert flag names in this table."""
    flags = [
        F.coalesce(F.col(name).cast("boolean"), F.lit(False))
        for name in ALERT_FLAG_COLUMNS
        if name in alerts.columns
    ]
    if not flags:
        return F.lit(False)
    result = flags[0]
    for flag in flags[1:]:
        result = result | flag
    return result
