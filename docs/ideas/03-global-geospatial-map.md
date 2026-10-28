# Idea 3: Global Geospatial Knowledge Map

## 1. Simple idea

This feature displays where unusually active Wikipedia topics are located in the world.

The map should show the location connected to an article's subject. It should not try to show the location of an editor.

Examples:

- An earthquake is shown at the affected location
- An election is shown in the relevant country
- A sports event is shown at the host city
- A company event is shown near its main location when that information exists

## 2. Main question

Where in the world are Wikipedia topics experiencing unusual knowledge activity?

## 3. Why editor-IP mapping should not be the main design

Older Wikipedia systems could publicly show an IP address for some logged-out edits. Wikimedia is moving toward temporary accounts that hide these IP addresses from the public.

IP addresses are also sensitive and do not reliably identify a person's true location. Shared networks, mobile networks, company gateways, and VPNs can all give misleading results.

For these reasons, WikiPulse should map article subjects, not individual editors.

## 4. Data needed

### Live activity

Use the recent-change EventStream to obtain:

- Wiki
- Page ID and title
- Edit time
- Revision IDs
- Page-length change
- Bot flag

### Page location

Use one or more of these sources:

1. MediaWiki `prop=coordinates` for an article's coordinates
2. MediaWiki `prop=pageprops` to obtain its Wikidata item ID
3. Wikidata statements for coordinates or country
4. MediaWiki Geosearch for articles around a location

Not every article has coordinates. The system must allow an unknown location.

### Historical activity

Use the same page-activity baseline as the Breaking News Radar. The map should display only a small number of important active topics, not every edit in the stream.

## 5. Location levels

Use the most accurate available level:

```text
Exact coordinates
      else
City or region
      else
Country
      else
Unknown
```

Store how the location was found so users can understand its quality.

Example:

```text
Article: 2026 example earthquake
Latitude: 35.2
Longitude: 72.1
Source: article coordinates
Location quality: exact
```

Another example:

```text
Article: Example national election
Country: Exampleland
Source: Wikidata country statement
Location quality: country only
```

## 6. Processing flow

```text
Knowledge-shift alert
        |
        v
Read page ID, title, and wiki
        |
        v
Check location cache
        |
        +--> Location found: use cached result
        |
        +--> Location missing: request page coordinates/Wikidata ID
                                  |
                                  v
                           Request Wikidata location
        |
        v
Save location and its source
        |
        v
Add the alert to the map
```

The location request happens only after an unusual page has been detected. This keeps the number of API calls small.

## 7. Map measurements

Each map point can contain:

- Page title
- Wikipedia language
- Latitude and longitude, when known
- Country or region
- Knowledge-shift score
- Number of recent edits
- Number of different editors
- Amount of text changed
- Bot ratio
- First detection time
- Number of related active pages
- Location source
- Location quality

## 8. Map display

Use simple visual rules:

- Point size: number of recent edits
- Point colour: knowledge-shift score
- Point border: event type or confidence
- Time slider: when the topic became active

When several points are close together, group them at low zoom and separate them when the user zooms in.

Clicking a point should show:

```text
Page title
Location
Why it is active
Current edits versus normal edits
Text change
First detection time
Link to page details
```

## 9. Country summary

Besides map points, show a country table:

| Country | Active topics | Total recent edits | Highest score |
|---|---:|---:|---:|
| Country A | 7 | 143 | 92 |
| Country B | 4 | 61 | 81 |

This table represents the location of article subjects. It does not represent the location of editors or readers.

## 10. Optional language view

A second view can group activity by Wikipedia language rather than physical location:

```text
English Wikipedia
Hindi Wikipedia
Kannada Wikipedia
French Wikipedia
```

This is useful for articles that have no physical coordinates.

If pages share the same Wikidata item, display when each language version became active. This creates a simple cross-language timeline.

## 11. Stored tables

### Page location table

```text
wiki
page_id
page_title
wikidata_id
latitude
longitude
country_code
region
location_source
location_quality
fetched_at
```

### Map event table

```text
alert_id
wiki
page_id
detected_at
shift_score
recent_edits
different_editors
byte_movement
bot_ratio
latitude
longitude
country_code
```

The location table should be cached because an article location normally changes much less often than its edit activity.

## 12. Use of Spark

Spark can:

- Join alerts with cached locations
- Group active topics by country or region
- Calculate activity totals for map tiles or country summaries
- Join language editions through a Wikidata ID
- Build hourly and daily geographic summaries

The external APIs should resolve locations. Spark should perform the large joins and summaries.

## 13. Testing

Create a test set with different page types:

- Article with exact coordinates
- Article with only a country
- Article with several possible locations
- Article with no location
- Article about a person
- Article about a worldwide topic
- Same subject in several languages

Check:

- Was the correct location returned?
- Was the source stored?
- Was an unknown result handled safely?
- Were API calls cached?
- Did map points update when alert values changed?
- Did country totals match the underlying data?

## 14. Minimum version

- Receive alerts from the Breaking News Radar
- Resolve article coordinates using MediaWiki
- Cache location results
- Plot active articles on a world map
- Show a country summary
- Handle articles without a location

## 15. Optional additions

- Wikidata country fallback
- Cross-language activity timeline
- Time animation
- Topic categories
- Related-page paths on the map
- Daily geographic report

## 16. Main risks

| Risk | Simple response |
|---|---|
| Many articles have no exact location | Fall back to region, country, or unknown |
| A topic concerns several locations | Show the primary location and explain the limitation |
| Too many API calls | Resolve only alerts and cache results |
| Users confuse subject and editor location | Clearly label the map as article-subject location |
| Map becomes crowded | Show only high-scoring alerts and group nearby points |

## 17. Success result

This idea is successful when an unusual Wikipedia page can be connected to a clear article location, shown on a live map, and grouped into correct country or regional summaries without using editor IP addresses.
