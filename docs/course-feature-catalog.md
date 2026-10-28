# WikiPulse Feature Catalogue for Data Engineering at Scale

## 1. Purpose

This document lists the features that WikiPulse can build using the Wikimedia interfaces described in `wikipedia-api-reference.md`.

It also connects every feature to the Data Engineering at Scale syllabus and suggests the charts, headline numbers, and important data to show in the final dashboards.

WikiPulse should be presented as one platform:

```text
Live Wikipedia activity
        +
Historical Wikipedia behaviour
        +
Revision content and page relationships
        =
Detection, explanation, and study of knowledge shifts
```

## 2. Syllabus understood from the attached screenshots

### Module 1: Distributed Storage Platforms

- Introduction to big data
- Distributed systems for parallel data processing
- Weak and strong scaling
- HDFS and GFS
- Ceph, cloud storage, and blob storage
- Map, combiner, partition, shuffle, and reduce

### Module 2: Large Volume Processing

- Spark basics
- RDD transformations and actions
- Key-value transformations
- Shuffle
- Accumulators and broadcast values
- Spark internals and tuning
- Spark DataFrames and SQL
- Catalyst query optimisation

### Module 3: Scalable ML

- Machine learning for big data
- Classic and deep learning
- ML pipelines
- Spark ML and MLlib
- Distributed DNN and GNN ideas
- Federated learning
- LLM training and inference topics

### Module 4: NoSQL Databases

- CAP theorem
- Key-value stores
- Dynamo-style systems
- Cassandra, CQL, and Redis
- MongoDB
- HBase, BigTable, and Spanner
- Neo4j and graph databases
- Data warehouses, data lakes, ETL, and cloud NoSQL

### Module 5: Fast and Graph Data Processing

- Low-latency ingestion, analysis, and querying
- Apache Kafka
- Kafka streaming
- Spark Streaming
- Complex event processing
- Pregel and Apache Giraph

## 3. Available Wikimedia data

| Data source | Best use |
|---|---|
| Wikimedia EventStreams | Live edits, new pages, logs, revision IDs, page lengths, bots, editors, and comments |
| Wikimedia Analytics API | Daily edits, pageviews, byte changes, editor totals, and ranked pages |
| MediaWiki History dumps | Large historical Spark jobs, revisions, editors, and reverts |
| XML content dumps | Whole-wiki content analysis when a large raw dataset is required |
| MediaWiki Action API | Revision details, comparisons, links, backlinks, language links, categories, and coordinates |
| MediaWiki REST API | Page details, revision HTML, and structured revision differences |
| Wikimedia Lift Wing | Revert Risk score for selected revisions |
| Wikidata API | Shared item IDs, language editions, coordinates, countries, labels, and structured facts |

## 4. Feasibility labels

| Label | Meaning |
|---|---|
| Core | Strongly recommended and directly supported by the data |
| Recommended | Valuable and realistic after the core pipeline works |
| Optional | Possible, but should be built only when the required system is stable |
| Research extension | Interesting, but likely too large for the six-week project |

## 5. Features grouped into five categories

This is the short feature list to use in the proposal and presentation. Feature names describe what the system does; technology names appear only in the implementation detail.

### Category 1: WikiPulse - Real-Time Detection and Explanation of Wikipedia Knowledge Shifts

| Feature | One-line detail |
|---|---|
| Knowledge-Shift Detection | Finds pages whose current edits, editors, content movement, or related-page activity are much higher than their normal behaviour; implemented with Kafka and Spark Structured Streaming. |
| Revision Change Explanation | Displays added, removed, and changed lines and the most affected section using the MediaWiki revision comparison API. |
| Change Evidence View | Places the alert score, edit comment, revision difference, page-size change, and later outcome in one investigation screen. |
| Data Completeness Indicator | Shows when a result lacks history, pageviews, coordinates, model output, or revision details so missing data is never mistaken for zero. |
| Historical Page Baseline | Learns normal edits, editors, content movement, bot ratio, and revert activity from Analytics APIs, history dumps, and Spark batch jobs. |
| Activity Pattern Groups | Groups pages into patterns such as stable, growing, heavily rewritten, bot-maintained, or event-driven using a Spark ML pipeline. |
| Method Comparison | Compares fixed rules, page-history scores, and ML anomaly scores using the same replay and historical test data. |
| Layered Data History | Keeps original, cleaned, and dashboard-ready records in Bronze, Silver, and Gold Parquet layers on HDFS or distributed storage. |
| Fast Dashboard Access | Serves recent page state and active alerts from a low-latency store such as Redis or Cassandra instead of scanning historical files. |
| Flexible Alert Records | Stores alerts with optional risk, location, graph, and pageview fields in a document store such as MongoDB. |
| Connected Knowledge Storage | Stores selected pages and relationships in Neo4j for neighbour, path, and active-subgraph queries. |
| Large-Scale Processing Comparison | Implements the same useful aggregation with Spark RDDs and DataFrames to compare runtime, shuffle, memory, and code complexity. |
| Work Distribution Study | Tests how page keys, Kafka partitions, Spark partitions, and file layout affect load balance and processing time. |
| Efficient Reference Data Join | Compares normal and broadcast joins when a large edit table is joined with a small page-information table. |
| Data Quality Monitoring | Counts invalid, duplicate, incomplete, and failed records with Spark counters and a failed-event stream. |
| System Scale Testing | Measures strong and weak scaling by changing worker count and input size while recording runtime, speed-up, and efficiency. |
| Stream Reliability Testing | Replays data at increasing rates, stops and restarts processing, and measures backlog, recovery time, delay, and duplicate results. |
| Storage Efficiency Testing | Compares JSON and Parquet size, read time, write time, compression, and small-file behaviour. |
| Dashboard Query Performance | Compares historical-file, key-value, document, and graph query response times for their correct dashboard workloads. |

### Category 2: Breaking News Radar

| Feature | One-line detail |
|---|---|
| Developing-Event Radar | Ranks sudden activity bursts that may represent a public event, dispute, or viral topic using short streaming windows and historical page baselines. |
| Emerging-Page Monitor | Finds newly created articles that quickly gain edits, editors, links, or readership using live creation events and later API enrichment. |
| Reader and Editor Attention | Compares daily pageviews with edit activity to separate widely followed topics from editor-only activity. |
| Historical Rankings | Lists the most edited, viewed, and rapidly growing pages using Wikimedia Analytics data and Spark SQL. |
| Time Pattern Analysis | Shows activity by hour, day, or week to separate regular cycles from unusual periods using historical timestamps. |
| Event Confirmation | Marks an activity alert as news-correlated only when delayed pageviews or an optional outside news source supports it. |
| Radar History | Stores earlier alerts, their peak score, duration, related pages, and later confirmation for the historical dashboard. |

### Category 3: Edit-Risk and Reversion Monitor

| Feature | One-line detail |
|---|---|
| Edit-Risk Review | Highlights selected revisions that may later be reverted using simple edit checks and Wikimedia Lift Wing Revert Risk. |
| Reversion Outcome Tracking | Connects an earlier risk result with its later reverted or not-reverted outcome using revision history and stored alert IDs. |
| Page Instability Monitor | Detects repeated reversions and rapid back-and-forth editing using ordered event sequences and short-term state. |
| Page Stability Score | Combines churn, revert rate, editor concentration, and repeated changes into an understandable historical stability measure. |
| Large-Change Warning | Flags unusually large additions, deletions, or replacements using page lengths and selected revision comparisons. |
| Risk Method Comparison | Compares simple rules, a local Spark ML model, and Wikimedia Revert Risk against later reversion outcomes. |
| Review Evidence | Shows the risk score, reasons, revision difference, edit comment, and later outcome without labelling an editor as a vandal. |

### Category 4: Global Geospatial Knowledge Map

| Feature | One-line detail |
|---|---|
| Global Topic Map | Maps the location of active article subjects using Wikipedia coordinates and Wikidata, without using editor IP addresses. |
| Regional Activity Summary | Groups located alerts by country or region and shows active topics, edit totals, and the highest shift scores. |
| Nearby Topic Discovery | Finds nearby Wikipedia subjects around a located alert using MediaWiki Geosearch and then checks their recent activity. |
| Related-Page Explorer | Shows active pages connected by links or backlinks using MediaWiki link data and a small graph store. |
| Knowledge-Shift Propagation | Displays the order and delay with which related pages become active using streaming alert times and graph relationships. |
| Important Connected Pages | Finds structurally important active pages using link counts or PageRank in Spark graph processing or Neo4j. |
| Topic Communities | Finds groups of strongly connected pages using connected-component or community-style graph analysis. |
| Cross-Language Knowledge Lag | Measures when the same Wikidata topic becomes active across English, Hindi, Kannada, or other language editions. |

### Category 5: Knowledge Churn and Bot Ratio

| Feature | One-line detail |
|---|---|
| Live Content Movement | Shows pages growing, shrinking, or being heavily rewritten by calculating net and total byte movement from recent-change events. |
| Contributor Mix | Shows the aggregate human, bot, registered, and temporary-account contribution split using Wikimedia event flags and Spark summaries. |
| Page Growth and Churn History | Shows daily net growth, total content movement, and page-size trends using Wikimedia historical metrics and stored revision data. |
| Largest Content Changes | Ranks pages and revisions by additions, deletions, and total movement using stream fields and selected revision comparisons. |
| Contributor Diversity | Measures the number of different editors and whether a small number of contributors produced most of the activity. |
| Bot Activity History | Shows bot edit counts and bot ratio over time without treating bot activity as harmful. |
| Churn Pattern Classification | Labels page windows as stable, growing, shrinking, heavily rewritten, or bot-maintained using clear rules or Spark ML groups. |

## 6. Brief feature groups

| Feature group | One-line detail | Feasibility |
|---|---|---|
| WikiPulse Knowledge-Shift Intelligence | Combines live edits, historical page behaviour, revision differences, ML scores, and page relationships to detect and explain unusual knowledge activity. | **High** - the required live events, revision data, historical sources, and page links are available. |
| Breaking News Radar | Finds sudden edit bursts and later checks pageviews or optional news data to distinguish developing public events from other unusual activity. | **High with limitation** - activity bursts are easy to detect, but they should not be called confirmed news without supporting evidence. |
| Edit-Risk and Reversion Monitor | Reviews selected revisions for large or repeated changes, adds Wikimedia Revert Risk, and records whether the revision was later reverted. | **Medium-high** - feasible for selected revisions, but external model calls and later revert checks add delay. |
| Global Geospatial Knowledge Map | Places active article subjects on a map, connects related pages, and shows how activity moves across locations and language editions. | **Medium** - coordinates and Wikidata are available, but many articles do not have a clear location. |
| Knowledge Churn and Bot Ratio | Measures page growth, rewriting, edit speed, contributor diversity, and the human-versus-bot contribution split in live and historical data. | **High** - page lengths, timestamps, editor identifiers, and bot flags are present in live events. |
| Historical Intelligence | Builds page baselines and long-term rankings from edits, pageviews, byte movement, editor activity, and revert history. | **High with delay** - the data is available, but some daily metrics may not be published immediately. |
| Scalable Data Platform | Uses distributed storage, Spark, Kafka, NoSQL stores, and graph processing to prepare reliable data for live and historical dashboards. | **High** - all parts can be demonstrated with a limited dataset and a small local or cloud cluster. |
| Course Evaluation and Scale | Measures processing speed, scaling, recovery, data quality, storage efficiency, and query response time to demonstrate the course concepts. | **High** - controlled replay makes the experiments repeatable without depending on live event volume. |

The concise list above is intended for the proposal and presentation. The older detailed notes are retained below only as an implementation reference.

<details>
<summary>Implementation reference for individual product features</summary>

### Individual product features

### Feature 1: Real-Time Knowledge-Shift Detection

**Priority:** Core

Find pages whose current activity is very different from their own normal activity.

Use:

- Edits in 5, 10, and 30 minutes
- Different editors
- Net byte change
- Total byte movement
- Bot ratio
- Revert activity
- Activity on related pages
- Historical page behaviour

Main result:

```text
Page: Example Event
Shift score: 91
Current edit rate: 8.4 times normal
Different editors: 17
Related active pages: 6
```

Syllabus value:

- Kafka ingestion
- Spark Streaming windows
- Spark SQL historical comparison
- Complex event processing
- Data lake storage

Dashboard:

- Ranked live alert table
- Alert-score gauge or bar
- Current versus normal edit-rate chart
- Five-minute activity line chart
- Top reasons for the score

### Feature 2: Live Activity or Breaking-Event Radar

**Priority:** Core

Show sudden edit bursts that may be connected to a developing public event.

The first result should be called an unusual activity alert. It becomes news-correlated only after pageviews or an outside source supports it.

Syllabus value:

- Streaming event windows
- Stateful processing
- Rule comparison
- Low-latency result serving

Dashboard:

- Live alert feed
- Edits-per-minute chart
- Current activity divided by normal activity
- Different-editor count
- Later pageview confirmation
- Alert category: public event, dispute, bot activity, revert activity, or unknown

### Feature 3: Knowledge Churn

**Priority:** Core

Measure how much page content is moving.

Calculate:

```text
net change = new length - old length
total movement = sum of absolute length changes
```

Keep these separate. A page can have high movement but little final size change.

Syllabus value:

- Key-value aggregation by page
- Spark RDD and DataFrame comparison
- Windowed streaming calculations
- Historical ETL

Dashboard:

- Net bytes added or removed
- Total byte movement
- Largest additions and deletions
- Most changed pages
- Page-size history
- Growing, shrinking, stable, or heavily rewritten label

### Feature 4: Human, Bot, and Temporary-Account Activity

**Priority:** Core

Show who is contributing at an aggregate level.

Calculate:

- Human edit count
- Bot edit count
- Bot ratio
- Registered and temporary/logged-out activity when available
- Different-editor count
- Most-active-editor share

Do not describe bot activity as bad. Many bots perform useful maintenance.

Syllabus value:

- Spark group-by operations
- SQL aggregation
- Key-value data model
- Historical trend calculation

Dashboard:

- Human versus bot stacked bar
- Bot-ratio timeline
- Different-editor headline number
- Editor-concentration chart
- Bot-heavy pages table

### Feature 5: Revision Change Explanation

**Priority:** Core

For an important alert, show what changed between its old and new revisions.

Display:

- Added lines
- Removed lines
- Changed lines
- Section with the largest change
- Edit comment
- Old and new page sizes

Syllabus value:

- Selective API enrichment
- ETL and caching
- Document storage
- Joining live and external data

Dashboard:

- Added/removed numbers
- Section-change table
- Revision difference viewer
- Size-before and size-after bars

### Feature 6: Edit-Risk and Reversion Monitor

**Priority:** Recommended

Use simple edit features and Wikimedia Revert Risk for selected revisions.

Show:

- Revert Risk probability
- Large deletion warning
- Missing-comment signal
- Repeated edits on the same page
- Later result: reverted or not reverted
- Time taken to revert

This is not proof of vandalism and should not label an editor as a vandal.

Syllabus value:

- External model inference
- Streaming feature calculation
- ML result evaluation
- NoSQL storage of current state and later outcome

Dashboard:

- Live risk candidates
- Risk-probability distribution
- Reverted versus not reverted
- Threshold comparison
- Time-to-revert histogram
- Local rule versus Wikimedia model

### Feature 7: Edit-War or Page-Instability Detection

**Priority:** Recommended

Find pages with repeated reversions or rapid back-and-forth changes.

Signals:

- Reverts in 30 or 60 minutes
- Alternating page size
- Same editors repeatedly changing the page
- High content movement with little final growth
- High editor concentration

Syllabus value:

- Complex event processing
- Stateful streaming
- Time-ordered event sequences

Dashboard:

- Unstable pages table
- Reverts per hour
- Revision sequence timeline
- Editor-concentration value
- Stability score over time

### Feature 8: New-Page and Emerging-Topic Monitor

**Priority:** Recommended

Track newly created articles and find those that quickly receive edits, editors, links, or pageviews.

Syllabus value:

- Stream filtering
- State from the time of page creation
- Growth calculations
- New-page historical comparisons

Dashboard:

- New articles per hour
- Fastest-growing new pages
- New pages by language
- New page size over time
- Time from creation to first alert

### Feature 9: Pageview and Edit Correlation

**Priority:** Recommended

Compare daily reader attention with editing activity.

Possible patterns:

| Edits | Pageviews | Possible meaning |
|---|---|---|
| High | High | Public event or widely followed topic |
| High | Low | Editorial dispute, maintenance, or vandalism |
| Low | High | Reader interest without much editing |
| Low | Low | Normal quiet period |

Pageviews are historical or delayed data, not part of the live edit stream.

Syllabus value:

- Time-series joins
- Spark SQL
- Missing-data handling
- Data warehouse queries

Dashboard:

- Dual-axis edits and views line chart
- Scatter plot of daily edits versus views
- Correlation value
- Four-pattern classification table
- Last available historical date

### Feature 10: Historical Page Baseline

**Priority:** Core

Create a normal activity profile for each page.

Include:

- Normal edits per day
- Normal different-editor count
- Normal byte movement
- Normal bot ratio
- Normal revert rate
- Day-of-week pattern
- Hour-of-day pattern when timestamp history is available

Syllabus value:

- Batch processing
- DataFrame windows
- SQL aggregation
- Partitioned historical data
- Broadcast join of small reference tables

Dashboard:

- Current versus baseline cards
- Day-of-week heatmap
- Historical range band with current value
- Baseline-data quality indicator

### Feature 11: Most Active, Most Viewed, and Most Changed Pages

**Priority:** Core

Build ranked historical lists from Analytics API data or Spark results.

Rank by:

- Edits
- Pageviews
- Net growth
- Absolute byte movement
- Different editors
- Reverts
- Bot ratio

Syllabus value:

- Large aggregation and sort
- Spark SQL ranking
- Shuffle study
- Warehouse-style dashboard query

Dashboard:

- Top-10 horizontal bars
- Sortable page table
- Rank-change indicator
- Date and language filters

</details>

## 7. Graph and relationship features

### Feature 12: Related-Page Activity Graph

**Priority:** Core in a small form

For each important page, fetch outgoing links and backlinks. Show which neighbours are also active.

Syllabus value:

- Graph data model
- Neo4j
- Spark graph processing
- Join between page graph and live features

Dashboard:

- One-hop network graph
- Node size by edit activity
- Node colour by shift score
- Edge direction
- Active-neighbour count

### Feature 13: Knowledge-Shift Propagation Timeline

**Priority:** Recommended

Show the order in which related pages became active.

Example:

```text
10:00 Event page
10:18 Person page
10:31 Organisation page
10:47 Location page
```

This shows a sequence, not proof of causation.

Syllabus value:

- Temporal graph processing
- Complex event processing
- Pregel-style message passing as an experiment

Dashboard:

- Propagation timeline
- Time-labelled network graph
- Delay from first page to each neighbour
- Number of reached pages

### Feature 14: Important Pages in the Knowledge Graph

**Priority:** Optional

Use PageRank or link counts to identify structurally important pages.

Compare structural importance with live activity.

Syllabus value:

- Pregel or Giraph concepts
- PageRank
- Distributed graph processing
- Neo4j queries

Dashboard:

- Pages with highest PageRank
- PageRank versus edit activity scatter plot
- In-link and out-link counts
- Important active pages

### Feature 15: Connected Components and Topic Groups

**Priority:** Optional

Find groups of strongly connected pages in a selected graph.

Syllabus value:

- Connected-components algorithm
- Distributed graph computation
- Graph storage

Dashboard:

- Component sizes
- Largest topic groups
- Selected component graph
- Activity by component

### Feature 16: Cross-Language Knowledge Lag

**Priority:** Recommended as a small demonstration

Use Wikidata IDs or language links to match the same topic across English, Hindi, Kannada, or another language.

Measure:

- First active language
- Time until another language becomes active
- Edits by language
- Byte growth by language
- Pageview differences when available

Syllabus value:

- Multi-source joins
- Graph edges between language editions
- Key-value processing by Wikidata ID
- Historical time comparison

Dashboard:

- Language activity timeline
- Per-language edit bars
- First-response and delay numbers
- Cross-language graph

## 8. Geographic features

### Feature 17: Global Article-Subject Map

**Priority:** Recommended with limited coverage

Map the location connected to the article subject using article coordinates or Wikidata.

Do not map editor IP addresses.

Syllabus value:

- API enrichment
- Cached key-value lookup
- Spatial grouping
- Historical and live joins

Dashboard:

- Map points for located alerts
- Point size by edits
- Point colour by shift score
- Country summary
- Exact, country-only, and unknown location counts

### Feature 18: Regional Knowledge-Activity Summary

**Priority:** Optional

Group located page alerts by country or region.

Dashboard:

- Active topics by country
- Total edits by country of article subject
- Highest shift score by country
- Daily regional timeline

This is article-subject geography, not editor geography.

### Feature 19: Nearby Active Topics

**Priority:** Optional

Use Geosearch to find pages close to the location of an active article, then check whether those pages are also active.

Dashboard:

- Nearby pages table
- Distance from the original article location
- Nearby-page activity score
- Local map view

## 9. Machine-learning features

### Feature 20: Unsupervised Activity Anomaly Detection

**Priority:** Core for Module 3

Create one feature row per page and time window.

Possible values:

- Edit count
- Different editors
- Total byte movement
- Deletion percentage
- Bot ratio
- Minor-edit ratio
- Editor concentration
- Revert ratio
- Active related pages
- Difference from historical behaviour

Use a simple Spark ML method such as K-means or Gaussian Mixture. Compare it with an understandable historical score.

Syllabus value:

- Spark ML pipeline
- Feature scaling
- Distributed model training
- Model evaluation

Dashboard:

- Rule score versus ML score
- Cluster sizes
- Two-dimensional feature plot
- Anomaly-score distribution
- Controlled anomaly detection result

### Feature 21: Page Activity Pattern Clustering

**Priority:** Recommended

Group pages into patterns such as:

- Stable
- Growing
- Heavily rewritten
- Bot-maintained
- Highly disputed
- Event-driven

Names should be assigned after inspecting the groups.

Dashboard:

- Cluster size bars
- Cluster feature averages
- Example pages from each group
- Cluster changes over time

### Feature 22: Local Revert Prediction Experiment

**Priority:** Optional

Train a simple Spark ML classifier using later reversion as the approximate result label.

Compare:

- Simple rules
- Local Spark model
- Wikimedia Revert Risk
- Combined score

Dashboard:

- Accuracy measurements
- Threshold results
- Confusion matrix
- Model-response time

### Feature 23: Activity Forecasting

**Priority:** Optional

Estimate the next day's edit level from recent historical activity.

This is useful as an ML experiment, but it is less important than anomaly detection.

Dashboard:

- Actual versus predicted edits
- Forecast error
- Pages with unexpected activity

### Feature 24: Rich Text or LLM Explanation

**Priority:** Research extension

Generate a short natural-language explanation from added and removed text.

This is not required. The structured revision difference already provides a strong result.

Use only if:

- The main data system is complete
- Model cost and response time are measured
- The original revision text remains visible for verification

## 10. Data serving and storage features

### Feature 25: Fast Dashboard Access

**Priority:** Core engineering feature

Use Redis, Cassandra, or another key-value store for the latest page state and current alerts.

Example keys:

```text
latest-page-window:{wiki}:{page_id}
active-alert:{alert_id}
page-baseline:{wiki}:{page_id}
revision-risk:{wiki}:{revision_id}
```

Syllabus value:

- Key-value model
- Fast reads
- Expiry of old live state
- CAP and consistency discussion

Dashboard proof:

- Current read time
- Current keys or records
- Data age
- Cache-hit percentage

### Feature 26: Flexible Alert Records

**Priority:** Recommended engineering feature

Use MongoDB or a similar document database for enriched alert records containing different optional fields.

An alert may contain:

- Revision explanation
- Risk score
- Location
- Related pages
- Pageview confirmation

Syllabus value:

- Flexible document schema
- NoSQL query design
- Live versus historical data separation

### Feature 27: Connected Knowledge Exploration

**Priority:** Recommended

Store selected pages and links in Neo4j.

Support queries such as:

- Find active pages within two hops
- Find the shortest path between two active pages
- Find high-degree active pages
- Find pages sharing the same active neighbours

Dashboard proof:

- Query result graph
- Graph-query response time
- Nodes and edges stored

### Feature 28: Layered Data History

**Priority:** Core engineering feature

Use three stored layers:

```text
Bronze: original events
Silver: cleaned and normalised edits
Gold: page windows, baselines, alerts, and dashboard tables
```

Syllabus value:

- HDFS or distributed storage
- Data lake design
- ETL
- Partitioning
- Schema evolution

Dashboard proof:

- Records in each layer
- Data size in each layer
- Last successful ETL time
- Failed-record count

## 11. Data engineering and scale experiments

These are important course outputs even though they are not end-user features.

### Feature 29: Large-Scale Processing Comparison

**Priority:** Core experiment

Implement one page or editor aggregation twice:

1. Pair RDD using `reduceByKey` or `combineByKey`
2. DataFrame using `groupBy` and SQL

Compare:

- Runtime
- Shuffle data
- Memory
- Code size
- Query plan

Dashboard:

- Runtime comparison bars
- Shuffle comparison bars
- Input-size table
- Spark plan summary

### Feature 30: Work Distribution Study

**Priority:** Core experiment

Test different:

- Kafka partition counts
- Spark partitions
- Page-key partitioning
- Date and wiki storage partitions
- Join strategies

Dashboard:

- Runtime versus partition count
- Shuffle read and write
- Task-duration distribution
- Slowest tasks

### Feature 31: Efficient Reference Data Join

**Priority:** Recommended experiment

Join a large edit table with a small page reference table with and without broadcast.

Dashboard:

- Runtime before and after broadcast
- Shuffle size
- Query plan difference

### Feature 32: Data Quality Monitoring

**Priority:** Recommended experiment

Use accumulators or streaming counters for:

- Invalid JSON
- Missing revision IDs
- Missing length values
- Unknown event types
- Duplicate events
- Failed enrichment calls

Dashboard:

- Data-quality counters
- Failure rate
- Missing-field trend
- Failed-event sample

### Feature 33: System Scale Testing

**Priority:** Core experiment for Module 1

Strong scaling:

- Keep the input size fixed
- Increase workers
- Measure runtime reduction

Weak scaling:

- Increase input data and workers together
- Check whether runtime stays similar

Dashboard:

- Workers versus runtime
- Speed-up
- Efficiency
- Data size and records processed

### Feature 34: Stream Reliability Testing

**Priority:** Core experiment

Replay events at:

```text
1x
10x
50x
100x
```

Stop and restart Spark during one run.

Dashboard:

- Input and processing rate
- Kafka backlog
- Batch duration
- Alert delay
- Recovery time
- Duplicate-alert count

### Feature 35: Storage Efficiency Testing

**Priority:** Recommended experiment

Compare:

- Raw JSON
- Compressed JSON
- Parquet
- Small Parquet files
- Combined Parquet files

Dashboard:

- Storage size
- Read time
- Write time
- File count
- Compression ratio

### Feature 36: Dashboard Query Performance

**Priority:** Optional experiment

Compare dashboard reads from:

- Parquet or Spark SQL
- Redis or Cassandra
- MongoDB
- Neo4j for graph queries

Discuss the consistency and availability choices using CAP ideas.

Dashboard:

- Read response time
- Write response time
- Freshness delay
- Records returned

## 12. Recommended dashboard structure

Do not put every chart on one page. Use five dashboard sections.

### Dashboard A: Live Pulse

Purpose: show what is happening now.

Headline numbers:

- Events per second
- Article edits per minute
- Active pages in the last five minutes
- Different editors
- Human/bot percentage
- Total bytes moved
- Active knowledge-shift alerts
- High-risk candidate revisions

Charts:

- Live events-per-second line
- Edits-per-minute line
- Human/bot stacked bar
- Top active pages bar
- Live alert table
- Net and total byte movement

### Dashboard B: Knowledge-Shift Investigation

Purpose: explain one selected alert.

Headline numbers:

- Shift score
- Current activity divided by normal activity
- Different editors
- Bytes moved
- Revert Risk
- Active related pages
- Pageview increase when available

Charts and views:

- Current versus historical activity
- Score-component bars
- Revision difference
- Human/bot split
- Related-page graph
- Propagation timeline
- Article-subject map point

### Dashboard C: Historical Intelligence

Purpose: show long-term behaviour.

Headline numbers:

- Total edits in selected period
- Total pageviews
- Net bytes
- Absolute bytes
- Active pages
- Different editors
- Bot ratio
- Revert ratio

Charts:

- Daily edits and pageviews
- Daily net and absolute bytes
- Day-of-week or hour heatmap
- Human/bot trend
- Most edited, viewed, and changed pages
- Page activity cluster summary

Always display:

- Selected date range
- Last available source date
- Missing-data warning

### Dashboard D: Graph and Geography

Purpose: show relationships and locations.

Headline numbers:

- Graph nodes
- Graph edges
- Active connected pages
- Located alerts
- Countries represented
- Average propagation delay

Charts:

- Related-page graph
- PageRank versus activity
- Propagation timeline
- World map
- Country summary
- Cross-language activity timeline

### Dashboard E: Engineering and Scale

Purpose: prove the Data Engineering at Scale work.

Headline numbers:

- Events processed per second
- Kafka backlog
- Spark batch time
- End-to-end alert delay
- Raw, clean, and result data sizes
- Failed records
- API cache-hit rate
- Worker count

Charts:

- Input rate versus processing rate
- Kafka backlog over time
- Replay-speed performance
- RDD versus DataFrame runtime
- Shuffle size
- Partition-count experiment
- Weak and strong scaling
- Storage-format comparison
- Restart and recovery timeline

This dashboard is essential for the course evaluation.

## 13. Important numbers to report in the final presentation

### Data size

- Total live events collected
- Total article edits retained
- Historical records processed
- Raw storage size
- Parquet storage size
- Number of pages and revisions

### Streaming

- Average and peak events per second
- Processing rate
- End-to-end delay
- Kafka backlog
- Recovery time
- Duplicate-event count

### Spark

- RDD runtime
- DataFrame runtime
- Shuffle read and write
- Best partition count
- Strong-scaling speed-up
- Weak-scaling efficiency

### Product results

- Knowledge-shift alerts
- Controlled anomalies detected
- False alerts in reviewed sample
- Candidate revisions scored
- Revisions later reverted
- Located-alert percentage
- Related pages found

### API use

- Revision comparisons requested
- Revert Risk requests
- Failed calls
- Average response time
- Cache-hit percentage
- Historical data's last available date

## 14. Syllabus coverage matrix

| Syllabus topic | WikiPulse evidence |
|---|---|
| HDFS and distributed storage | Bronze, Silver, and Gold datasets stored as partitioned Parquet |
| Weak and strong scaling | Fixed-data and growing-data Spark experiments |
| MapReduce stages | Page-key map, optional combiner, partition, shuffle, and reduce explanation |
| RDD transformations/actions | Page, editor, and graph aggregation |
| Key-value transformations | `(page_id, edit)` and `(editor_key, edit)` processing |
| Shuffle | Grouping, ranking, graph edge cleaning, and joins |
| Accumulators | Invalid-event and data-quality counters |
| Broadcast | Small page-reference table joined with large edit history |
| Spark tuning | Partition, cache, join, and file-size experiments |
| DataFrames and SQL | Historical baselines and dashboard tables |
| Catalyst | Query-plan comparison and broadcast selection |
| Spark ML | Anomaly pipeline and page clustering |
| MLOps pipeline | Saved feature steps, model version, and repeatable evaluation |
| Key-value NoSQL | Current page state and live alerts |
| Cassandra or Redis | Low-latency dashboard serving experiment |
| MongoDB | Enriched alert documents |
| HBase or BigTable ideas | Time-keyed page-window storage design discussion |
| Neo4j | Page-neighbour and path queries |
| Data lake and ETL | Raw, cleaned, and result layers |
| Kafka | Live ingestion and replay |
| Spark Streaming | Five-, ten-, and thirty-minute features |
| Complex event processing | Edit bursts, repeated reverts, and related-page sequences |
| Pregel or Giraph | PageRank or propagation experiment |

## 15. Recommended six-week scope

The complete catalogue is larger than a 240-person-hour project. Build the following set to cover all five modules without creating several unfinished systems.

### Required product features

1. Real-Time Knowledge-Shift Detection
2. Live Activity Radar
3. Knowledge Churn
4. Human/Bot Activity
5. Revision Change Explanation
6. Historical Page Baseline
7. Most Active/Viewed/Changed Pages
8. Small Related-Page Graph
9. Unsupervised Activity Anomaly Detection
10. Live and Historical Dashboards

### Recommended additions

11. Candidate-only Edit-Risk Monitor
12. Pageview and Edit Correlation
13. Small Propagation Timeline
14. Located-alert map
15. Small cross-language demonstration
16. Redis or Cassandra live-state store
17. Neo4j investigation graph

### Required engineering experiments

18. Bronze/Silver/Gold data lake
19. RDD versus DataFrame
20. Partition and shuffle tuning
21. Weak and strong scaling
22. Streaming replay and recovery
23. Data-quality counters

### Build only if time remains

- Full edit-war detection
- Large PageRank graph
- Connected components
- Local revert model
- Forecasting
- Rich LLM summaries
- External news feed matching
- Complete multi-language analysis

## 16. Final recommended project statement

```text
WikiPulse is a distributed platform that combines Wikipedia's live edit stream,
historical activity, revision content, and page relationships to detect unusual
knowledge shifts, explain what changed, and show how activity develops across
related pages. It demonstrates distributed storage, Spark RDD and SQL processing,
Spark ML, NoSQL serving, Kafka streaming, and graph analysis through live,
historical, relationship, and engineering dashboards.
```

## 17. Source documents

- [Simple Architecture Diagram](simple-architecture.md)
- [Wikipedia API Reference](wikipedia-api-reference.md)
- [WikiPulse Main Design](ideas/00-wikipulse-knowledge-shifts.md)
- [Feature Feasibility and Validation Plan](feasibility/feature-validation-plan.md)
