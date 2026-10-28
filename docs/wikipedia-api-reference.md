# WikiPulse: Wikipedia APIs and cURLs

This document lists every Wikipedia or Wikimedia data source named in the WikiPulse design docs.

The matching import-ready collection is available at [WikiPulse Wikimedia APIs Postman collection](postman/wikipulse-wikimedia-apis.postman_collection.json).

The examples use English Wikipedia and the page `Earth`. Replace the page, dates, and revision IDs as needed. Sample responses are shortened examples; live values will change.

Use a real contact address in the `User-Agent` header before running these commands.

```bash
User-Agent: WikiPulse/0.1 (your-email@example.com)
```

## Quick table

| Category | API or data source | What it does | Main result |
|---|---|---|---|
| Live changes | Wikimedia EventStreams | Sends live edits and supports limited recent replay | Edit, page, user, time, and size-change fields |
| Historical activity | Wikimedia Analytics API | Returns daily edits, page views, bytes changed, and editor counts | Time-series or ranked-page JSON |
| Historical activity | Wikipedia XML history dumps | Provides whole-wiki files containing past revisions | Compressed XML shards |
| Page and revision data | MediaWiki Action API | Reads revisions, compares edits, and reads page details | JSON page and revision data |
| Page and revision data | MediaWiki REST API | Reads a page or revision in a simpler format | Page metadata and HTML |
| Related pages and facts | MediaWiki Action API | Reads links, backlinks, language links, coordinates, categories, and Wikidata IDs | JSON page facts and related pages |
| Location data | MediaWiki Geosearch | Finds pages near a latitude and longitude | Nearby pages and distances |
| Edit-risk score | Wikimedia Lift Wing | Scores the chance that a revision may be reverted | Risk score and model details |
| Shared knowledge | Wikidata API | Connects language versions and returns structured facts | Item ID, sitelinks, labels, and claims |

## 1. Live changes

### Wikimedia EventStreams: recent changes

| What it does | Sample response |
|---|---|
| Keeps a live feed of edits, new pages, page IDs, revision IDs, old and new page lengths, bot flags, minor-edit flags, users, and comments. | One event arrives as `data: {"type":"edit","title":"Earth","namespace":0,"revision":{"old":123,"new":124},"length":{"old":1000,"new":1050},"bot":false}`. |

**cURL**

```bash
curl -N \
  -H 'Accept: text/event-stream' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://stream.wikimedia.org/v2/stream/recentchange'
```

The response stays open and sends one event at a time. Keep only namespace `0` for normal article pages.

### Replay recent EventStreams data

| What it does | Sample response |
|---|---|
| Starts near a recent timestamp and sends retained events until the request catches up with live data. | The response has the same event format as the live stream. |

**cURL**

```bash
curl -N \
  -H 'Accept: text/event-stream' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://stream.wikimedia.org/v2/stream/recentchange?since=2026-09-13T00:00:00Z'
```

Replay rules:

- History is normally available for only about 7 to 31 days, depending on the stream.
- Replay runs as fast as Wikimedia, the network, and the client allow. There is no `2x` or `5x` option.
- `Last-Event-ID` can resume a disconnected client from its last received position.
- For permanent or controlled-speed replay, save events to Kafka and replay them from there.

## 2. Historical activity

The Analytics API uses this base URL:

```text
https://wikimedia.org/api/rest_v1/metrics/
```

### Total edits for the whole Wikipedia project

This request does not need a page title. It returns monthly edit totals for the entire selected Wikipedia project.

| What it does | Sample response |
|---|---|
| Counts every edit in the project for each month in the date range. | `{"items":[{"project":"en.wikipedia.org","results":[{"timestamp":"2026-03-01T00:00:00.000Z","edits":4200000}]}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/edits/aggregate/en.wikipedia.org/all-editor-types/all-page-types/monthly/20260301/20260831'
```

Add the `edits` values in `items[0].results` to calculate one total for the full six-month period.

### Daily edits for one page

| What it does | Sample response |
|---|---|
| Counts edits to one page for each day. Use it to build a normal activity level. | `{"items":[{"project":"en.wikipedia.org","page-title":"Earth","results":[{"timestamp":"2026010100","edits":12}]}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/edits/per-page/en.wikipedia.org/Earth/all-editor-types/daily/20260101/20260131'
```

### Daily bytes changed for one page

| What it does | Sample response |
|---|---|
| Shows how many bytes a page gained or lost. The `absolute` form counts movement even when the final size is close to the starting size. | `{"items":[{"project":"en.wikipedia.org","page-title":"Earth","results":[{"timestamp":"2026010100","bytes_diff":250,"abs_bytes_diff":430}]}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/bytes-difference/absolute/per-page/en.wikipedia.org/Earth/all-editor-types/daily/20260101/20260131'
```

### Daily net bytes changed for one page

| What it does | Sample response |
|---|---|
| Shows only the final size change for each day. A value of `0` does not mean that no text moved. | `{"items":[{"project":"en.wikipedia.org","page-title":"Earth","results":[{"timestamp":"2026010100","bytes_diff":250}]}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/bytes-difference/net/per-page/en.wikipedia.org/Earth/all-editor-types/daily/20260101/20260131'
```

### Daily page views for one page

| What it does | Sample response |
|---|---|
| Shows how many times a page was viewed. Use it as historical context or later confirmation of an alert. | `{"items":[{"project":"en.wikipedia.org","article":"Earth","timestamp":"2026010100","views":12500}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/en.wikipedia.org/all-access/all-agents/Earth/daily/20260101/20260131'
```

### Most-viewed pages

| What it does | Sample response |
|---|---|
| Lists the most-viewed pages for a month. | `{"items":[{"articles":[{"article":"Main_Page","rank":1,"views":5000000}]}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/pageviews/top/en.wikipedia.org/all-access/2026/01/all-days'
```

### Most-edited pages

| What it does | Sample response |
|---|---|
| Lists the pages with the most edits in a month. | `{"items":[{"results":[{"timestamp":"2026010100","top":[{"page_title":"Earth","rank":1,"edits":120}]}]}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/edited-pages/top-by-edits/en.wikipedia.org/all-editor-types/content/2026/01/all-days'
```

### Editor totals

| What it does | Sample response |
|---|---|
| Counts editors in a chosen edit-count range over time. | `{"items":[{"project":"en.wikipedia.org","results":[{"timestamp":"2026010100","editors":420}]}]}` |

**cURL**

```bash
curl -G \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://wikimedia.org/api/rest_v1/metrics/editors/aggregate/en.wikipedia.org/all-editor-types/all-page-types/1..4-edits/monthly/20260101/20260901'
```

### Wikipedia XML history dumps: whole-wiki data

The XML dump does not require a page title. It contains an entire Wikipedia edition. Large wikis such as English Wikipedia are divided into multiple files by page-ID range; each file is a shard of the complete dump, not a dump for one page.

| Dump type | What it contains | Best use |
|---|---|---|
| `stub-meta-history` | All pages and revisions without article text | Counts, timestamps, editors, sizes, and comments |
| `pages-meta-history` | All pages, revisions, and raw article text | Detailed historical content analysis |
| `pages-meta-current` | Latest revision of every page | Current-state analysis |
| `pages-articles` | Current article content | Article analysis without full history |
| `pages-logging` | Moves, deletions, protections, blocks, and other logs | Administrative-event analysis |

#### Step 1: read the dump manifest

The manifest lists the exact filenames available in the latest dump.

```bash
curl \
  -H 'Accept: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://dumps.wikimedia.org/enwiki/latest/dumpstatus.json'
```

#### Step 2: list full-history XML shards

```bash
curl -s \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://dumps.wikimedia.org/enwiki/latest/dumpstatus.json' \
  | jq -r '.jobs[].files // {} | keys[] | select(test("pages-meta-history.*\\.bz2$"))'
```

#### Step 3: download a selected shard

```bash
curl -L -O \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://dumps.wikimedia.org/enwiki/latest/FILENAME_FROM_MANIFEST'
```

Download every `pages-meta-history` shard to process the complete English Wikipedia history. These files are very large.

For a simple value such as total edits during the last six months, use the aggregate Analytics API above. XML history is raw data, so calculating a six-month total requires reading the dump, filtering revision timestamps, and counting the matching revisions. For custom Spark analysis without article text, the monthly [MediaWiki History TSV dump](https://dumps.wikimedia.org/other/mediawiki_history/) is usually smaller and easier to process.

## 3. Page and revision data

### MediaWiki Action API: revision details

| What it does | Sample response |
|---|---|
| Reads the latest revision ID, time, editor, edit comment, and page size. | `{"query":{"pages":[{"title":"Earth","revisions":[{"revid":124,"parentid":123,"timestamp":"2026-01-01T10:00:00Z","size":1050}]}]}}` |

**cURL**

```bash
curl -G \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://en.wikipedia.org/w/api.php' \
  --data-urlencode 'action=query' \
  --data-urlencode 'prop=revisions' \
  --data-urlencode 'titles=Earth' \
  --data-urlencode 'rvprop=ids|timestamp|user|comment|size|flags' \
  --data-urlencode 'rvlimit=1' \
  --data-urlencode 'format=json' \
  --data-urlencode 'formatversion=2'
```

### MediaWiki Action API: compare two revisions

| What it does | Sample response |
|---|---|
| Shows the actual difference between an older revision and a newer revision. | `{"compare":{"fromrevid":123,"torevid":124,"fromtitle":"Earth","totitle":"Earth","*":"<table class=\"diff\">...</table>"}}` |

**cURL**

```bash
curl -G \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://en.wikipedia.org/w/api.php' \
  --data-urlencode 'action=compare' \
  --data-urlencode 'fromrev=123' \
  --data-urlencode 'torev=124' \
  --data-urlencode 'prop=diff|ids|title|size|timestamp' \
  --data-urlencode 'format=json' \
  --data-urlencode 'formatversion=2'
```

### MediaWiki REST API: page details

| What it does | Sample response |
|---|---|
| Returns simple page information, the latest revision, license, and a URL for the page HTML. | `{"id":736,"key":"Earth","title":"Earth","latest":{"id":124,"timestamp":"2026-01-01T10:00:00Z"},"html_url":"https://en.wikipedia.org/w/rest.php/v1/page/Earth/html"}` |

**cURL**

```bash
curl \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://en.wikipedia.org/w/rest.php/v1/page/Earth/bare'
```

### MediaWiki REST API: revision HTML

| What it does | Sample response |
|---|---|
| Returns the rendered HTML for one revision. | `{"id":124,"timestamp":"2026-01-01T10:00:00Z","html":"<p>Earth...</p>"}` |

**cURL**

```bash
curl \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://en.wikipedia.org/w/rest.php/v1/revision/124/with_html'
```

## 4. Related pages and page facts

### Page details, links, language links, location, categories, and Wikidata ID

| What it does | Sample response |
|---|---|
| Reads the facts needed for page alerts and the map: page size, article links, language versions, coordinates, categories, and the Wikidata item ID. | `{"query":{"pages":[{"title":"Earth","pageprops":{"wikibase_item":"Q2"},"coordinates":[{"lat":0,"lon":0}],"langlinks":[{"lang":"fr","title":"Terre"}],"categories":[{"title":"Category:Earth"}],"links":[{"title":"Atmosphere"}]}]}}` |

**cURL**

```bash
curl -G \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://en.wikipedia.org/w/api.php' \
  --data-urlencode 'action=query' \
  --data-urlencode 'titles=Earth' \
  --data-urlencode 'prop=info|coordinates|pageprops|categories|langlinks|links' \
  --data-urlencode 'inprop=url' \
  --data-urlencode 'ppprop=wikibase_item' \
  --data-urlencode 'coprimary=1' \
  --data-urlencode 'cllimit=10' \
  --data-urlencode 'lllimit=10' \
  --data-urlencode 'pllimit=10' \
  --data-urlencode 'redirects=1' \
  --data-urlencode 'format=json' \
  --data-urlencode 'formatversion=2'
```

The `links` result is the list of pages linked from the article. The `langlinks` result connects the same article across languages. The `pageprops.wikibase_item` value connects the article to Wikidata.

### Backlinks

| What it does | Sample response |
|---|---|
| Finds pages that link to a chosen page. | `{"query":{"backlinks":[{"pageid":10,"title":"Moon"},{"pageid":11,"title":"Solar System"}]}}` |

**cURL**

```bash
curl -G \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://en.wikipedia.org/w/api.php' \
  --data-urlencode 'action=query' \
  --data-urlencode 'list=backlinks' \
  --data-urlencode 'bltitle=Earth' \
  --data-urlencode 'blnamespace=0' \
  --data-urlencode 'bllimit=10' \
  --data-urlencode 'format=json' \
  --data-urlencode 'formatversion=2'
```

### Geosearch

| What it does | Sample response |
|---|---|
| Finds Wikipedia pages near a latitude and longitude. Use it for nearby topics on the map. | `{"query":{"geosearch":[{"pageid":736,"title":"Earth","dist":250,"primary":true}]}}` |

**cURL**

```bash
curl -G \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://en.wikipedia.org/w/api.php' \
  --data-urlencode 'action=query' \
  --data-urlencode 'list=geosearch' \
  --data-urlencode 'gscoord=48.8584|2.2945' \
  --data-urlencode 'gsradius=1000' \
  --data-urlencode 'gslimit=10' \
  --data-urlencode 'gsnamespace=0' \
  --data-urlencode 'format=json' \
  --data-urlencode 'formatversion=2'
```

## 5. Edit-risk score

### Wikimedia Lift Wing: Revert Risk

| What it does | Sample response |
|---|---|
| Gives a probability that a selected revision may later be reverted. Use it only after simple checks select a revision. It is supporting evidence, not proof that an edit is bad. | `{"model_name":"revertrisk-language-agnostic","wiki_db":"enwiki","revision_id":124,"output":{"prediction":true,"probabilities":{"true":0.69,"false":0.31}}}` |

**cURL**

```bash
curl -X POST \
  -H 'Content-Type: application/json' \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  --data '{"rev_id":124,"lang":"en"}' \
  'https://api.wikimedia.org/service/lw/inference/v1/models/revertrisk-language-agnostic:predict'
```

## 6. Wikidata

### Wikidata API: item, language links, and structured facts

| What it does | Sample response |
|---|---|
| Reads the Wikidata item connected to a Wikipedia article. Use it to join language versions and read facts such as coordinates or country. | `{"entities":{"Q2":{"id":"Q2","sitelinks":{"enwiki":{"title":"Earth"}},"claims":{"P625":[{"mainsnak":{"datavalue":{"value":{"latitude":0,"longitude":0}}}}]}}}}` |

**cURL**

```bash
curl -G \
  -H 'User-Agent: WikiPulse/0.1 (your-email@example.com)' \
  'https://www.wikidata.org/w/api.php' \
  --data-urlencode 'action=wbgetentities' \
  --data-urlencode 'sites=enwiki' \
  --data-urlencode 'titles=Earth' \
  --data-urlencode 'props=info|sitelinks|labels|claims' \
  --data-urlencode 'languages=en' \
  --data-urlencode 'format=json' \
  --data-urlencode 'formatversion=2'
```

## 7. Recommended call order

1. Read edits from EventStreams.
2. Keep article pages and calculate cheap checks locally.
3. Use Analytics for the page's normal history and page views.
4. For selected alerts, read revision details and compare revisions.
5. Read links, backlinks, page facts, coordinates, and Wikidata only for selected alerts.
6. Ask Lift Wing for a risk score only for selected revisions.
7. Cache results so the same page or revision is not requested repeatedly.

## Official references

- [Wikimedia EventStreams](https://stream.wikimedia.org/)
- [Wikimedia Analytics API](https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/)
- [MediaWiki Action API](https://www.mediawiki.org/wiki/API:Main_page)
- [MediaWiki Compare API](https://www.mediawiki.org/wiki/API:Compare)
- [MediaWiki REST API](https://www.mediawiki.org/wiki/API:REST_API/Reference)
- [Wikimedia API Portal](https://api.wikimedia.org/wiki/Main_Page)
- [Wikidata API](https://www.wikidata.org/wiki/Wikidata:Data_access)
- [Wikimedia history dumps](https://dumps.wikimedia.org/)
