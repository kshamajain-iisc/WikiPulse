# Idea 4: Knowledge Churn and Bot Ratio

## 1. Simple idea

This feature measures how much Wikipedia content is changing and how much of that work is being done by people or bots.

It answers more than “Did the page become longer?” A page can be heavily rewritten while ending at almost the same size. The system therefore measures both final size change and total movement.

## 2. Main questions

- Which pages are changing the most?
- Are pages growing, shrinking, or being repeatedly rewritten?
- How many edits are made by people and how many by bots?
- Are a few editors doing most of the work?
- Which topics are stable and which are continuously changing?

## 3. Example

A page receives ten edits:

- 5,000 bytes are added
- 4,800 bytes are removed
- Final net change is only 200 bytes

If the dashboard reports only net change, the page appears almost unchanged. In reality, 9,800 bytes moved. The feature should report both values.

## 4. Data needed

### Live data

Use Wikimedia EventStreams fields:

- Wiki
- Page ID and title
- Event time
- Old and new revision IDs
- Old and new page lengths
- Editor identifier
- Bot flag
- Minor-edit flag
- Change type

### Historical aggregate data

Use the Wikimedia Analytics API for:

- Edits per page
- Net bytes changed
- Absolute bytes changed
- Editor totals
- Pageviews, when useful

Use MediaWiki History dumps when a larger historical dataset is required for Spark processing.

### Exact additions and deletions

For selected pages, use the MediaWiki Compare API. The stream's old and new lengths show the final size difference, but they do not provide every added and removed line.

## 5. Main measurements

### Net byte change

```text
net byte change = new length - old length
```

Examples:

```text
+500  page became larger
-500  page became smaller
0     final size did not change
```

### Total byte movement

For each edit:

```text
edit movement = absolute value of new length - old length
```

For a time window:

```text
total byte movement = sum of edit movement
```

This is an approximation of content churn. Exact line additions and deletions require revision comparison data.

### Edit throughput

```text
edit throughput = number of edits / window duration
```

Show edits per minute, hour, or day depending on the screen.

### Bot ratio

```text
bot ratio = bot edits / all edits
```

Also report:

- Human edit count
- Bot edit count
- Registered-account edit count
- Temporary or logged-out edit count, when the event provides this distinction

Do not assume that bot edits are harmful. Many bots perform useful maintenance.

### Editor concentration

Show whether activity is spread across many editors or controlled by a few.

A simple measure is:

```text
largest editor share = edits by most active editor / all edits
```

Example:

```text
20 edits by 15 editors -> low concentration
20 edits, 18 by one editor -> high concentration
```

### Revert ratio

```text
revert ratio = reverted edits / all edits
```

This can be calculated from historical data or later revision outcomes.

## 6. Processing flow

```text
Wikimedia EventStreams
        |
        v
Kafka topic: wiki.raw.edits
        |
        v
Spark Structured Streaming
        |
        +--> Page totals
        +--> Editor totals
        +--> Human/bot totals
        +--> Net byte change
        +--> Total byte movement
        +--> Edit throughput
        |
        v
Five-minute, hourly, and daily summary tables
        |
        +--> Live dashboard
        +--> Parquet history
        +--> Unusual-churn alerts
```

## 7. Time windows

Use several time views because each answers a different question:

| Window | Use |
|---|---|
| 5 minutes | Sudden activity |
| 30 minutes | Short event or dispute |
| 1 hour | Live dashboard summary |
| 1 day | Historical comparison |
| 7 or 30 days | Long-term stability |

## 8. Stored tables

### Page churn summary

```text
wiki
page_id
page_title
window_start
window_end
edit_count
net_byte_change
total_byte_movement
largest_addition
largest_deletion
different_editor_count
bot_edit_count
human_edit_count
bot_ratio
minor_edit_count
```

### Editor contribution summary

```text
wiki
window_start
window_end
editor_key
edit_count
page_count
net_byte_change
total_byte_movement
is_bot
```

Use an internal editor key when the real editor name is not required.

### Daily page summary

```text
wiki
page_id
date
edit_count
different_editor_count
net_byte_change
total_byte_movement
bot_ratio
revert_ratio
pageviews
```

## 9. Dashboard views

### Live overview

Show:

- Edits per minute
- Bytes added and removed
- Human and bot split
- Number of active pages
- Number of active editors
- Most changed pages

### Page detail

Show:

- Page size over time
- Net change and total movement
- Human/bot split
- Different-editor count
- Largest additions and deletions
- Revert activity
- Comparison with the page's normal behaviour

### Topic comparison

Compare categories such as:

- Politics
- Sports
- Science
- Entertainment
- Current events

Topic labels can come from Wikipedia categories or Wikidata. This should be optional because category cleaning can take significant time.

## 10. Useful derived labels

Create simple labels based on the measurements:

### Stable

- Few edits
- Low byte movement
- Low revert activity

### Growing

- Positive net change
- Several different editors
- Low deletion ratio

### Shrinking

- Negative net change
- Large removals

### Heavily rewritten

- High total byte movement
- Small final net change

### Bot-maintained

- High bot ratio
- Repeated small changes

### Disputed or unstable

- High churn
- High revert ratio
- Repeated activity from a small number of editors

These are activity descriptions, not judgments about quality.

## 11. Use of Spark RDD, SQL, and ML

### RDD work

Use pair RDDs for examples such as:

```text
(page_id, edit) -> combine page totals
(editor_key, edit) -> combine editor totals
```

Compare execution with and without useful partitioning.

### DataFrame and SQL work

Use them for:

- Time-window summaries
- Daily page totals
- Historical comparisons
- Joins with pageviews
- Ranking the most changed pages
- Comparing human and bot activity

### Spark ML work

Group pages by their activity pattern using values such as:

- Edit count
- Net byte change
- Total movement
- Bot ratio
- Revert ratio
- Different-editor count
- Editor concentration

The output can describe common groups such as stable, growing, bot-maintained, or heavily rewritten pages. Review the groups before assigning names to them.

## 12. Evaluation

Check calculation correctness using a small manually calculated sample.

For scale testing, increase:

- Number of events
- Number of pages
- Number of Kafka partitions
- Replay speed
- Number of Spark workers

Measure:

- Events processed per second
- Time required for each batch
- Kafka backlog
- Memory use
- Data written per hour
- Shuffle data
- Query time for daily reports

Compare:

- RDD and DataFrame versions of one aggregation
- Different partition counts
- Cached and uncached historical data
- Small files versus compacted Parquet files

## 13. Minimum version

- One Wikipedia edition
- Live edits through Kafka
- Five-minute and one-hour Spark summaries
- Net and absolute byte movement
- Human/bot split
- Different-editor count
- Parquet storage
- Live overview and page detail view
- Replay-based throughput test

## 14. Optional additions

- Revert ratio
- Pageview comparison
- Topic/category comparison
- Activity-pattern clustering
- Multiple languages
- Exact revision additions and deletions
- Long-term page stability score

## 15. Main risks

| Risk | Simple response |
|---|---|
| Length difference misses rewrites | Report it as an approximation and compare selected revisions |
| Bot flag is not perfect | Present it as the value reported by Wikimedia |
| Too many detailed API calls | Fetch exact differences only for selected pages |
| Large historical dataset | Use limited dates and selected wikis |
| Many small output files | Periodically combine them into larger Parquet files |

## 16. Success result

This idea is successful when the system can clearly show how much Wikipedia content is changing, distinguish net growth from heavy rewriting, show the human/bot split, and process the data fast enough for a live dashboard.
