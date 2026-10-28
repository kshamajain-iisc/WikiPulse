# Idea 2: Edit-Risk and Reversion Monitor

## 1. Simple idea

Some Wikipedia edits are quickly undone because they are incorrect, harmful, accidental, or part of an edit dispute. This feature watches live edits and identifies revisions that may need attention.

The feature should be called an **Edit-Risk and Reversion Monitor**, not a vandalism detector. A high score does not prove that an editor acted badly.

## 2. Main question

Which recent edits have signs that they may be reverted or may need human review?

## 3. Example

A new edit removes most of a page, has an empty edit comment, and is made by a new or temporary account. Wikimedia's Revert Risk model also gives the edit a high score.

The dashboard displays:

```text
Edit risk: High

- 74% of page text removed
- No edit comment
- Temporary account
- Wikimedia revert-risk score: 0.87
- Revision was reverted six minutes later
```

The dashboard reports the evidence. It does not call the editor a vandal.

## 4. Data needed

### Live edit data

Use the Wikimedia recent-change EventStream. Useful fields include:

- Wiki
- Page ID and title
- Event time
- Old and new revision IDs
- Old and new page lengths
- Editor identifier
- Bot flag
- Minor-edit flag
- Edit comment
- Tags, when present

### Wikimedia Revert Risk

Use the Lift Wing Revert Risk API for selected revisions. A request needs the revision ID and language.

Do not call the model for every edit in the global stream. First apply simple checks and send only selected revisions for scoring.

### Revision result

Use later recent-change events or revision history to check whether the edit was reverted. This result can be used for evaluation.

### Revision difference

Use the MediaWiki Compare API to retrieve the actual added and deleted text for a high-risk candidate.

## 5. Processing flow

```text
Recent edit event
      |
      v
Kafka topic: wiki.raw.edits
      |
      v
Spark applies simple low-cost checks
      |
      +--> Very large deletion?
      +--> Many edits by the same editor?
      +--> Repeated back-and-forth changes?
      +--> Missing edit comment?
      +--> Bot, registered, or temporary account?
      |
      v
Candidate edit
      |
      +--> Fetch revision difference
      +--> Request Wikimedia Revert Risk score
      |
      v
Combined edit-risk result
      |
      v
Dashboard and stored alert
      |
      v
Later check: was the revision reverted?
```

## 6. Simple first checks

Calculate these values before calling an outside API:

### Byte change

```text
byte change = new page length - old page length
```

A negative value means that the page became smaller.

### Deletion percentage

```text
deletion percentage = bytes removed / old page length
```

Example:

```text
Old page length: 20,000 bytes
New page length: 5,000 bytes
Deletion: 15,000 bytes, or 75%
```

### Editor activity

For a short time window, calculate:

- Edits by the same editor
- Different pages edited by the editor
- Number of edits that were later reverted
- Average size of the editor's changes

Do not keep unnecessary personal data. Use an internal hashed editor key in analytical tables if the editor name is not needed in the user interface.

### Back-and-forth editing

Look for a sequence such as:

```text
Revision A -> Revision B -> Revision A-like content -> Revision B-like content
```

A simpler first version can identify repeated reverts on the same page within 30 minutes.

## 7. Risk score

Begin with a clear points system:

| Evidence | Example points |
|---|---:|
| More than half of the page removed | 25 |
| Very large change compared with normal page edits | 15 |
| High Wikimedia Revert Risk score | 30 |
| Repeated edits on the same page | 10 |
| Empty edit comment | 5 |
| Several recent edits by the same account were reverted | 15 |

Convert the total to a label:

```text
0-29   Low
30-59  Medium
60-100 High
```

These values are starting points. Test different values using historical revisions.

## 8. Use of Spark ML

The project can create a model that learns patterns from past edits. Use historical reverts as an approximate result label:

```text
reverted later = 1
not reverted in the chosen period = 0
```

Possible input values include:

- Size of change
- Deletion percentage
- Page length
- Bot flag
- Minor-edit flag
- Whether an edit comment exists
- Recent edits by the editor
- Recent edits on the page
- Time since the previous edit
- Number of recent reversions on the page

Use a simple Spark ML classification method and compare it with:

1. The rule-based score
2. Wikimedia's Revert Risk score
3. A combined score

This comparison is more useful than building many models.

## 9. Stored tables

### Edit feature table

```text
wiki
page_id
revision_id
parent_revision_id
event_time
byte_change
deletion_percentage
is_bot
is_minor
has_comment
recent_page_edits
recent_editor_edits
```

### Risk result table

```text
wiki
revision_id
rule_score
wikimedia_revert_risk
combined_score
risk_level
score_time
```

### Outcome table

```text
wiki
revision_id
was_reverted
reverting_revision_id
time_to_revert_seconds
checked_at
```

Keep the risk score and later result in separate tables. The later result may arrive minutes or hours after the first score.

## 10. Dashboard view

The live table should show:

- Page title
- Revision ID
- Edit time
- Risk level
- Main reasons for the score
- Amount of text added or removed
- Current result: waiting, reverted, or not reverted
- Link to the Wikipedia revision difference

The detail screen should show:

- Old and new page sizes
- Added and deleted lines
- Rule score
- Wikimedia model score
- Later revert result
- Recent page activity

Avoid displaying an editor leaderboard of “bad users.” The goal is to study edit patterns, not publicly judge people.

## 11. Evaluation

Use a historical set of edits for which enough time has passed to observe reverts.

Measure:

- How many flagged edits were later reverted?
- How many reverted edits were detected?
- How many normal edits were incorrectly flagged?
- How quickly was the first score produced?
- How much time was added by the external model request?
- How many model requests were avoided by the first filter?

Also evaluate different thresholds, such as:

```text
revert risk > 0.60
revert risk > 0.70
revert risk > 0.80
revert risk > 0.90
```

Do not assume that `0.80` is automatically the best threshold.

## 12. Minimum version

- English Wikipedia article edits
- Live stream to Kafka
- Large-change and deletion checks
- Candidate-only Lift Wing requests
- Stored risk result
- Later revert check
- Simple live table
- Historical evaluation

## 13. Optional additions

- Repeated-reversion detection
- Spark ML model trained on historical outcomes
- Revision difference viewer
- Page-level edit-war warning
- Multiple Wikipedia languages
- Comparison between rule, local model, and Wikimedia model

## 14. Main risks

| Risk | Simple response |
|---|---|
| High risk is treated as proof of vandalism | Use neutral wording and show evidence |
| Too many model requests | Score only candidates and cache each revision result |
| Model API is unavailable | Keep the local rule-based system working |
| Revert label is imperfect | State that a revert is only an approximate result |
| Personal-data concerns | Store only the minimum editor information needed |

## 15. Success result

This idea is successful when the system can score a live revision, explain the score, later determine whether the revision was reverted, and report the accuracy and response time of the process.
