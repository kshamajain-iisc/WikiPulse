# Idea 1: Breaking News Radar

## 1. Simple idea

Wikipedia pages often receive many edits when an important event happens. This feature watches live Wikipedia edits and finds pages whose activity suddenly becomes much higher than usual.

The result is a live list of pages that may be connected to a breaking event.

This feature does not claim that every edit burst is breaking news. A burst can also be caused by an edit dispute, vandalism, a sports match, a popular television programme, or routine maintenance. The dashboard should therefore call it an **unusual activity alert** until more evidence is available.

## 2. Main question

Which Wikipedia pages are receiving much more editing activity than they normally receive?

## 3. Example

A page normally receives two edits in one hour. It then receives 25 edits from 12 different people in ten minutes. Three related pages also become active.

WikiPulse creates an alert and explains:

- The current number of edits
- The normal number of edits
- The number of different editors
- The amount of text added or removed
- The related pages that also became active
- The reason the page received a high score

## 4. Data needed

### Live data

Use Wikimedia EventStreams:

```text
https://stream.wikimedia.org/v2/stream/recentchange
```

Useful fields include:

- Event ID
- Wiki name, such as `enwiki`
- Page ID
- Page title
- Page namespace
- Event time
- Change type
- Old revision ID
- New revision ID
- Old page length
- New page length
- Editor name or temporary account
- Bot flag
- Minor-edit flag
- Edit comment

Only keep normal article pages at first. In most Wikipedias, these have namespace value `0`.

### Historical data

Use one or both of these sources:

1. Wikimedia MediaWiki History dump for past revision activity
2. Wikimedia Analytics API for daily edits, pageviews, and bytes changed

The historical data is used to learn what is normal for each page.

### Extra data for an alert

After a page is flagged, use:

- MediaWiki Compare API to see what changed
- MediaWiki Links API to get related pages
- Wikimedia Analytics API to obtain past pageviews

Do not call these extra APIs for every edit. Call them only for pages that have already passed the first alert check.

## 5. Processing flow

```text
Wikimedia EventStreams
        |
        v
Small event reader
        |
        v
Kafka topic: wiki.raw.edits
        |
        v
Spark Structured Streaming
        |
        +--> Count edits in 5, 10, and 30 minute windows
        +--> Count different editors
        +--> Calculate text-size change
        +--> Calculate bot ratio
        |
        v
Compare with historical values
        |
        v
Kafka topic: wiki.breaking.candidates
        |
        v
Fetch revision difference and related pages
        |
        v
Dashboard alert
```

## 6. Measurements to calculate

For every page and time window, calculate:

| Measurement | Meaning |
|---|---|
| Edit count | Number of edits in the window |
| Different editors | Number of separate editors in the window |
| Net byte change | New page length minus old page length |
| Total byte movement | Sum of the absolute size of every edit |
| Largest deletion | Biggest single reduction in page size |
| Bot ratio | Bot edits divided by all edits |
| Minor-edit ratio | Minor edits divided by all edits |
| Related active pages | Linked pages that are also unusually active |

### Why a fixed rule is not enough

A rule such as “more than 15 edits in five minutes” treats every page in the same way. That is not fair because popular pages and quiet pages have very different normal behaviour.

Use the page's own history. A simple score can be:

```text
edit increase = current edit count / normal edit count
```

Examples:

```text
Current: 20 edits, normal: 2 edits   -> 10 times normal
Current: 20 edits, normal: 15 edits  -> 1.3 times normal
```

The first page should receive the stronger alert.

## 7. Simple alert score

Start with an easy score that the team can explain:

```text
40% edit-rate increase
20% increase in different editors
20% amount of text changed
10% activity on related pages
10% pageview increase, when available
```

The weights can be changed after testing. Always show the individual measurements along with the final score.

## 8. Use of Spark ML

Create one record for each page and time window. The record can contain:

- Edit count
- Different-editor count
- Total byte movement
- Deletion ratio
- Bot ratio
- Minor-edit ratio
- Number of active related pages
- Increase compared with normal activity

Use Spark ML to group normal activity patterns. A new window that is far from the normal groups can receive a higher unusual-activity score.

Compare this result with the simple historical rule. The goal is to show whether the ML method improves the result, not merely to add ML to the project.

## 9. Stored tables

### Raw edit table

```text
event_id
wiki
page_id
page_title
event_time
change_type
old_revision_id
new_revision_id
old_length
new_length
editor
is_bot
is_minor
comment
```

### Page-window table

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
```

### Alert table

```text
alert_id
wiki
page_id
page_title
detected_at
alert_score
edit_increase
editor_increase
byte_change_score
related_active_pages
status
```

Store the tables as Parquet files. Divide the raw data by wiki, date, and hour so Spark does not need to read every file for every job.

## 10. Dashboard view

The main screen should show:

- Live ranked alert list
- Page title and Wikipedia language
- Alert score
- Current edits compared with normal edits
- Number of different editors
- Text added or removed
- Small activity timeline
- Link to the actual Wikipedia page

The page-detail screen should show:

- Why the alert was created
- Activity over time
- Important revision changes
- Related active pages
- Available pageview history

## 11. Testing

Do not depend on a real breaking event during the final demonstration.

Build a replay tool that sends old events through Kafka. It should support different speeds, such as:

- Normal speed
- 10 times normal speed
- 50 times normal speed

Also create test patterns:

- Sudden burst of edits
- Many editors arriving together
- Large deletion
- Bot-heavy burst
- Several related pages becoming active

Check:

- Was the event detected?
- How long did detection take?
- How many normal pages were incorrectly flagged?
- How many events per second could the system process?
- Did the system recover after Kafka or Spark was restarted?

## 12. Minimum version

The first working version should include:

- English Wikipedia article edits
- EventStreams to Kafka
- Five-minute and thirty-minute Spark windows
- Historical edit baseline
- Simple explainable alert score
- Historical replay
- Live alert table

## 13. Optional additions

Add these only after the minimum version is stable:

- Hindi and Kannada Wikipedia
- Revision-change explanation
- Related-page graph
- Pageview confirmation
- External news matching
- Article location map

## 14. Main risks

| Risk | Simple response |
|---|---|
| Too many events | Start with one wiki and article pages only |
| API becomes slow | Enrich only high-scoring pages and cache results |
| No live event during demo | Use historical replay |
| Popular pages create false alerts | Compare each page with its own history |
| Pageviews are delayed | Use them as later confirmation, not as a live signal |

## 15. Success result

This idea is successful when the system can consume live edits, find unusual page activity, explain why it was flagged, and reproduce the same result using a saved historical replay.
