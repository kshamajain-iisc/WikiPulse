# WikiPulse: Real-Time Detection and Explanation of Wikipedia Knowledge Shifts

## 1. Project in one sentence

WikiPulse watches live Wikipedia changes, finds pages whose current activity is very different from their normal activity, explains what changed, and shows whether related pages are also becoming active.

## 2. Main project question

```text
What is changing unusually in Wikipedia, what changed inside the pages,
and how is the activity spreading across related knowledge?
```

The project is not only a breaking-news detector, a vandalism detector, or a pageview dashboard. It studies Wikipedia as a knowledge system that changes over time.

## 3. What is a knowledge shift?

A knowledge shift is a clear change in the normal behaviour of one page or a group of related pages.

It may include:

- A sudden increase in edits
- Many new editors arriving together
- A large amount of text being added or removed
- Several revisions being reverted
- A sharp change in human or bot activity
- Several related pages becoming active close together
- A later increase in pageviews
- Similar activity appearing in another language edition

A single signal is normally not enough. WikiPulse combines several signals and explains which ones caused the alert.

## 4. Example

Assume that a Wikipedia page normally receives three edits per day. It suddenly receives 40 edits from 18 editors in 20 minutes. A large new section is added, two revisions have high edit-risk scores, and five linked pages also become active.

WikiPulse may show:

```text
Page: Example Event
Knowledge-shift score: 92/100

Why it was detected:
- Edit activity is 12 times higher than normal
- 18 different editors are active
- 24 KB of content moved
- A new section was added
- 5 related pages are also unusually active
- 2 revisions have high edit-risk scores

Possible activity type: developing public event
```

The activity type is a description, not a confirmed statement about the real-world cause.

## 5. Main system parts

WikiPulse has five connected parts.

### Part 1: Detect

Find pages with unusual live activity.

Use:

- Edit count
- Different-editor count
- Byte change
- Human/bot split
- Revert activity
- Comparison with historical behaviour

### Part 2: Explain

Explain why a page was flagged and what changed inside it.

Use:

- Revision comparison
- Added and removed text
- Sections with the largest changes
- Edit comments
- Edit-risk score

### Part 3: Connect

Find related pages and check whether they also become active.

Use:

- Links between Wikipedia pages
- Backlinks
- Shared Wikidata items
- Wikipedia categories, when useful
- Time difference between page alerts

### Part 4: Locate

Show where the article subject is located, when a meaningful location exists.

Use:

- Article coordinates
- Wikidata coordinates
- Country or region

Do not use editor IP addresses for the main map.

### Part 5: Present

Provide a dashboard where a user can move from a high-level alert to the exact evidence behind it.

## 6. How the four supporting ideas fit

| Supporting idea | Role inside WikiPulse |
|---|---|
| Breaking News Radar | Finds sudden page activity and creates the first candidate alert |
| Edit-Risk and Reversion Monitor | Adds evidence about unstable or likely reverted revisions |
| Global Geospatial Knowledge Map | Shows the location connected to active article subjects |
| Knowledge Churn and Bot Ratio | Explains how much content moved and who performed the edits |

These are not four separate applications. They are four views of the same knowledge-shift alert.

## 7. Data sources

### 7.1 Wikimedia EventStreams

Use the public recent-change stream:

```text
https://stream.wikimedia.org/v2/stream/recentchange
```

Purpose:

- Receive live edits
- Receive new-page events
- Read revision IDs
- Read old and new page lengths
- Identify bot and minor edits
- Record wiki, page, editor, time, and edit comment

This is the main live input.

### 7.2 MediaWiki History dumps

Purpose:

- Build historical page-activity baselines
- Study past edits and reverts
- Run large Spark batch jobs
- Train and test the unusual-activity model
- Replay past activity

Use only selected dates and Wikipedia editions. Processing the complete history of every Wikimedia project is outside the six-week scope.

### 7.3 Wikimedia Analytics API

Purpose:

- Daily edits per page
- Daily net and absolute bytes changed
- Daily pageviews
- Historical editor totals
- Most-viewed or most-edited pages

Pageviews are not part of the second-by-second edit stream. Use them as historical context or later confirmation.

### 7.4 MediaWiki Action or REST API

Purpose:

- Fetch revision information
- Compare two revisions
- Fetch page links and backlinks
- Fetch language links
- Read page coordinates
- Read the Wikidata item ID
- Fetch categories and page information

Call these APIs only after a page becomes a candidate. Do not request full revision details for every live event.

### 7.5 Wikimedia Lift Wing

Purpose:

- Request a Revert Risk score for selected revisions

This score is supporting evidence. It does not prove vandalism.

### 7.6 Wikidata

Purpose:

- Match the same subject across languages
- Obtain coordinates, country, or other structured facts
- Connect Wikipedia pages through a shared item ID

## 8. System flow

```text
Wikimedia EventStreams
        |
        v
Event reader
        |
        v
Kafka: raw edit events
        |
        +--------------------------+
        |                          |
        v                          v
Raw Parquet storage       Spark Structured Streaming
                                   |
                                   +--> Clean and validate events
                                   +--> Remove duplicates
                                   +--> Create time windows
                                   +--> Calculate page activity
                                   +--> Compare with page baseline
                                   |
                                   v
                         Knowledge-shift candidates
                                   |
                     +-------------+-------------+
                     |             |             |
                     v             v             v
              Revision diff   Edit-risk API   Page links/location
                     |             |             |
                     +-------------+-------------+
                                   |
                                   v
                           Enriched alert record
                                   |
                     +-------------+-------------+
                     |                           |
                     v                           v
              Historical storage          WikiPulse dashboard
```

## 9. Why Kafka is needed

EventStreams can be read directly, but Kafka gives the team several useful features:

- The live reader and Spark job can run separately
- Events can wait when Spark is temporarily slow
- More than one consumer can use the same events
- Recorded events can be replayed
- Consumer delay can be measured
- The pipeline can recover after a restart

Suggested topics:

```text
wiki.raw.edits
wiki.page.features
wiki.shift.candidates
wiki.enriched.alerts
wiki.failed.events
```

The first working version can combine some topics if five topics create unnecessary work.

## 10. Data cleaning

The streaming job should:

- Keep selected Wikipedia editions
- Keep article pages with namespace `0`
- Keep edit and new-page events
- Reject records without required identifiers
- Convert event time into one common format
- Remove duplicate event IDs
- Keep the original event in raw storage
- Send unreadable records to a failed-event topic

Start with English Wikipedia. Add Hindi or Kannada only after the English pipeline is stable.

## 11. Streaming measurements

For every page, create 5-minute, 10-minute, and 30-minute summaries.

Calculate:

| Measurement | Simple meaning |
|---|---|
| Edit count | Number of edits |
| Different editors | Number of separate contributors |
| Net byte change | Final growth or reduction |
| Total byte movement | Total size of changes without cancelling additions and deletions |
| Largest deletion | Biggest single reduction |
| Bot ratio | Share of edits marked as bot edits |
| Minor-edit ratio | Share of edits marked as minor |
| Editor concentration | Whether a few editors made most edits |
| Revert activity | Number or share of edits later undone |
| Related-page activity | Number of connected pages that are also unusual |

## 12. Historical baseline

The system needs to know what is normal for each page.

For each page, calculate values such as:

- Normal edits per hour and day
- Normal number of different editors
- Normal byte movement
- Normal bot ratio
- Normal revert ratio
- Variation by hour of day
- Variation by day of week

A simple comparison is:

```text
activity increase = current activity / normal activity
```

Example:

```text
Page A: current 20, normal 2  -> 10 times normal
Page B: current 20, normal 16 -> 1.25 times normal
```

Page A should normally receive the stronger signal.

For pages without enough history, compare them with pages of similar age and activity level, or mark the confidence as low.

## 13. Knowledge-shift score

Start with an understandable score:

```text
35% edit-activity increase
20% different-editor increase
20% content movement
10% revert or edit-risk evidence
10% related-page activity
 5% pageview increase, when available
```

Store every part of the score. The dashboard must show why the final value was produced.

Example record:

```text
Final score: 84
Edit activity: 31 points
Editor activity: 17 points
Content movement: 18 points
Edit risk: 8 points
Related pages: 10 points
Pageviews: not available yet
```

The exact weights should be tested and may change.

## 14. Unsupervised learning

The project should compare the simple score with an unsupervised Spark ML method.

Create one row for each page and time window with:

- Edit count
- Different-editor count
- Total byte movement
- Deletion percentage
- Bot ratio
- Minor-edit ratio
- Editor concentration
- Revert ratio
- Related active-page count
- Difference from historical behaviour

Use a simple grouping method such as K-means or Gaussian Mixture. Activity far from the normal groups receives a higher unusual-activity score.

The comparison should answer:

- Does the ML method find useful cases missed by the simple score?
- Does it create too many false alerts?
- Is its result harder to explain?
- How much extra processing time does it require?

The simple score remains valuable even if the ML result is better because users can understand it easily.

## 15. Explaining what changed

For a high-scoring page:

1. Read the old and new revision IDs from the event
2. Request their difference from MediaWiki
3. Extract added and removed lines
4. Find the section with the largest change when possible
5. Store a short structured result

Example:

```text
Largest changed section: Results
Lines added: 34
Lines removed: 7
Main change type: large addition
Edit comment: Updated after official announcement
```

The first version does not need advanced language generation. A clear revision difference and section name are enough.

## 16. Related-page propagation

Build a small graph around each candidate page.

Nodes:

- Wikipedia pages

Edges:

- Page A links to Page B
- Page B links back to Page A
- Two pages share a Wikidata or category relationship

A page becomes a propagation candidate when:

1. It is related to an already active page
2. It crosses its own historical activity limit
3. It does so within a chosen period, such as two hours

Example:

```text
10:00 Main event page becomes active
10:18 Person page becomes active
10:31 Organization page becomes active
10:47 Location page becomes active
```

This shows an activity sequence. It does not prove that one page caused another page to change.

## 17. Shared stored tables

### Raw events

```text
event_id
wiki
page_id
page_title
namespace
event_time
change_type
old_revision_id
new_revision_id
old_length
new_length
editor_key
is_bot
is_minor
comment
ingested_at
```

### Page-window features

```text
wiki
page_id
window_start
window_end
edit_count
different_editor_count
net_byte_change
total_byte_movement
largest_deletion
bot_ratio
minor_ratio
editor_concentration
```

### Historical baseline

```text
wiki
page_id
baseline_date
normal_edit_count
normal_editor_count
normal_byte_movement
normal_bot_ratio
normal_revert_ratio
history_quality
```

### Knowledge-shift alerts

```text
alert_id
wiki
page_id
page_title
detected_at
window_start
window_end
shift_score
statistical_score
ml_score
edit_increase
editor_increase
content_score
edit_risk_score
related_page_score
status
```

### Alert explanation

```text
alert_id
old_revision_id
new_revision_id
lines_added
lines_removed
largest_changed_section
revert_risk
was_later_reverted
summary_status
```

### Page relationships

```text
source_wiki
source_page_id
target_wiki
target_page_id
relationship_type
fetched_at
```

Store raw and processed data as Parquet. Divide large datasets by wiki and date.

## 18. Dashboard

### Screen 1: Live knowledge-shift feed

Show:

- Ranked page alerts
- Shift score
- Wikipedia language
- Detection time
- Current edits compared with normal edits
- Main reason for the alert
- Alert status

### Screen 2: Page investigation

Show:

- Activity timeline
- Historical comparison
- Human/bot split
- Net and total byte movement
- Different-editor count
- Revert and edit-risk evidence
- Important revision differences

### Screen 3: Related-page graph

Show:

- Starting page
- Related pages
- Time at which each page became active
- Strength of each page's alert

### Screen 4: Global map

Show:

- Location of article subjects
- Number of active topics by country
- Shift score and detection time

### Screen 5: System health

Show:

- Events received per second
- Events processed per second
- Kafka backlog
- Streaming batch time
- Failed-event count
- Last successful API enrichment

The system-health view helps demonstrate the data-engineering work, not only the application interface.

## 19. Historical replay and controlled tests

The final demonstration must not depend on a real event happening at that moment.

Build a replay tool that:

- Reads saved or historical events
- Sends them to Kafka in event-time order
- Supports normal, 10x, 50x, and 100x speed
- Can repeat the same experiment

Create controlled patterns:

- Sudden edit burst
- Large deletion
- Human-heavy activity
- Bot-heavy activity
- Repeated reverts
- Several related pages becoming active
- Normal high-volume page with no true change

The controlled patterns provide known expected results.

## 20. Evaluation

### Correctness

- Compare Spark totals with a small manually calculated file
- Check duplicate handling
- Check window calculations
- Check score components
- Check revision IDs and page links

### Detection quality

- Number of controlled anomalies detected
- Number of normal windows incorrectly flagged
- Time between event arrival and alert
- Comparison between fixed rule, historical score, and ML score

### Streaming performance

- Events processed per second
- Kafka backlog at different replay speeds
- Time used by each streaming batch
- Recovery after restart
- Effect of changing Kafka partitions

### Spark performance

- RDD versus DataFrame time for one selected job
- Shuffle size
- Memory use
- Effect of partitioning
- Cached versus uncached historical data
- Small files versus combined Parquet files

### API performance

- Number of candidate pages enriched
- Requests avoided by candidate filtering
- API response time
- Cache hit rate
- Failures and retry count

## 21. Minimum project scope

The required six-week version should contain:

- English Wikipedia article edits
- EventStreams ingestion
- Kafka storage and replay
- Spark Structured Streaming windows
- Raw and processed Parquet data
- Historical page baselines
- Explainable knowledge-shift score
- One Spark ML comparison
- Candidate-only revision comparison
- Small related-page graph
- Live alert and page-detail dashboard
- Controlled test-event injection
- Performance and correctness report

This is the main project. The map, multiple languages, external news, and rich text analysis are additions.

## 22. Optional additions

Add these only after the required system works:

- Hindi and Kannada Wikipedia
- Cross-language knowledge lag
- Lift Wing Revert Risk
- Global article-subject map
- Daily pageview confirmation
- External news matching
- More detailed section or entity extraction
- Larger page-link graph

Suggested removal order when time is short:

1. External news matching
2. Rich text analysis
3. Multiple languages
4. Map
5. Larger graph

Do not remove replay, historical baselines, explainable scoring, or performance measurement.

## 23. Work split for four engineers

### Engineer 1: Live ingestion and streaming

- EventStreams reader
- Kafka producer
- Event cleaning
- Structured Streaming
- Time windows
- Restart and failure handling

### Engineer 2: Storage and historical processing

- Raw and processed Parquet layout
- Historical data download
- Spark RDD and DataFrame jobs
- Page baselines
- Partition and performance tests

### Engineer 3: Detection and evaluation

- Knowledge-shift score
- Spark ML comparison
- Controlled event injector
- Detection-quality tests
- Final experiment results

### Engineer 4: Enrichment, graph, and dashboard

- Revision comparison
- Links and Wikidata enrichment
- Related-page graph
- Dashboard screens
- Optional map

Every engineer should help with integration, testing, documentation, and the final demonstration.

## 24. Six-week plan

The team has 40 person-hours per week and 240 person-hours in total. Some of this time will be used by meetings, integration, debugging, and presentation preparation.

### Week 1: Live data foundation

- Connect to EventStreams
- Create Kafka topics
- Store raw events
- Create shared event schema
- Set up Spark and dashboard skeleton

Result: live events reach Kafka and storage.

### Week 2: Historical and streaming measurements

- Create Spark time windows
- Calculate page features
- Download a limited historical dataset
- Create first page baselines
- Display live page activity

Result: current page activity can be compared with history.

### Week 3: Knowledge-shift detection

- Implement explainable score
- Create alert topic
- Add historical replay
- Add controlled test patterns
- Start Spark ML comparison

Result: repeatable alerts are detected.

### Week 4: Explanation and connections

- Fetch revision differences
- Build one-hop page graph
- Add edit-risk enrichment if time permits
- Build alert detail screen
- Show related-page activity

Result: an alert explains what changed and which pages are connected.

### Week 5: Testing and performance

- Run replay at several speeds
- Test recovery
- Compare RDD and DataFrame job
- Compare detection methods
- Measure API requests and caching
- Fix incorrect calculations

Result: evaluation tables and graphs are ready.

### Week 6: Final system and presentation

- Stabilize the pipeline
- Prepare one reliable replay scenario
- Complete dashboard
- Complete technical report
- Prepare slides and architecture diagram
- Rehearse live and backup demonstrations

Result: complete and repeatable project demonstration.

## 25. Suggested time budget

| Work area | Team hours |
|---|---:|
| EventStreams and Kafka | 38 |
| Spark streaming features | 42 |
| Historical data and baselines | 36 |
| Detection and Spark ML | 34 |
| Revision and graph enrichment | 24 |
| Dashboard | 24 |
| Testing and performance study | 22 |
| Documentation and presentation | 20 |
| Total | 240 |

This budget leaves little space for several large optional features. Finish the complete basic flow before expanding it.

## 26. Course connection

| Course area | WikiPulse work |
|---|---|
| Distributed storage | Raw and processed Parquet files, partitions, and optional HDFS |
| Spark RDD | Page, editor, and link aggregation; partition comparison |
| Spark SQL and DataFrames | Historical baselines, time calculations, joins, and reports |
| Spark ML | Unsupervised unusual-activity detection |
| Kafka | Event buffering, replay, separate consumers, and backlog measurement |
| Spark Streaming | Live time windows, duplicate removal, late events, and recovery |
| Graph | Page links, related-page search, and activity sequence |

Each technology contributes to the same project question.

## 27. Main risks

| Risk | Response |
|---|---|
| Project becomes four separate dashboards | Use one shared alert and one investigation flow |
| Too much data | Start with English article pages and limited history |
| Too many external API calls | Enrich only candidates and cache results |
| Nothing interesting happens during the demo | Use saved replay and controlled events |
| Fixed thresholds create weak alerts | Compare every page with its own history |
| ML result is difficult to explain | Keep the simple score and show both results |
| Pageviews are delayed | Use them as later confirmation |
| Graph becomes too large | Build one-hop candidate graphs first |
| Team spends too much time on the interface | Build simple tables and charts before advanced visuals |

## 28. Final deliverables

The project should finish with:

1. Source code for the complete data pipeline
2. Repeatable local or cluster setup
3. Saved historical replay data
4. Raw and processed example datasets
5. Live knowledge-shift dashboard
6. Detection and ML evaluation
7. Streaming and Spark performance results
8. Architecture and data-flow diagrams
9. Technical report
10. Final presentation and backup demo recording

## 29. Definition of success

WikiPulse is successful when the team can demonstrate this full sequence:

```text
A Wikipedia edit arrives live or through replay
        -> Kafka stores it
        -> Spark updates page activity
        -> Current activity is compared with history
        -> A knowledge-shift alert is created
        -> The alert explains its score
        -> The revision change is shown
        -> Related active pages are displayed
        -> System speed and correctness are reported
```

That complete path is more important than implementing every optional feature.

## 30. Detailed supporting documents

- [Breaking News Radar](01-breaking-news-radar.md)
- [Edit-Risk and Reversion Monitor](02-edit-risk-monitor.md)
- [Global Geospatial Knowledge Map](03-global-geospatial-map.md)
- [Knowledge Churn and Bot Ratio](04-knowledge-churn-and-bot-ratio.md)
