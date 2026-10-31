"""Stable internal page keys when EventStreams omits the MediaWiki page ID."""

from pyspark.sql import Column, functions as F


def title_page_id(title: Column) -> Column:
    """Return a negative internal key; positive IDs remain real MediaWiki IDs.

    Recent-change stream records can omit page_id. Do not call the API for every
    edit: use a deterministic title key until an actual page ID is available.
    A title rename changes this fallback key. This is not a MediaWiki page ID.
    """
    normalized_title = F.regexp_replace(F.trim(title), "_", " ")
    title_hash = F.xxhash64(F.lit("enwiki"), normalized_title)
    return -(F.pmod(title_hash, F.lit(9223372036854775807)) + F.lit(1))
