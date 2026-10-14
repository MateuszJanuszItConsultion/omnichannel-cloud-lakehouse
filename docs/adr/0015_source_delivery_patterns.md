# ADR-0015: Source delivery patterns

**Status:** Accepted
**Date:** 2026-10-12

## Context
Real source systems deliver data in different ways. Legacy CRM systems often
export a full snapshot every day; transactional systems deliver increments.
The ingestion pattern decides how changes and deletions are detected.

## Decision
- **Loyalty program:** full daily snapshot of all current members (CSV).
- **Webshop:** increments of events (orders as JSON).
- **POS:** increments of transactions (Parquet).
- All files land in the `landing.inbound` volume, one directory per channel.
- **Bronze** stays append-only for every channel: each loyalty snapshot is
  appended with its snapshot date. Retention of old snapshots is decided later.
- **Silver** turns snapshots into changes: snapshot D is compared with D-1 by
  member key and attribute hash, producing inserts, updates and deletes.
  A member missing from today's snapshot is a delete event (membership ended),
  not a silent disappearance: the source record gets an end date, the golden
  record keeps the person.
- **Truncated snapshot guard:** if the record count of a snapshot drops by more
  than a configured threshold versus the previous one, the job stops instead of
  closing records en masse.

## Consequences
- Two ingestion patterns in one project: snapshot-to-changes without source CDC,
  and append-only increments.
- An empty or partial loyalty file is the most dangerous failure mode; the guard
  is mandatory before Silver processes snapshots.
- The generator will later produce a truncated snapshot on purpose (fire drill).
- Open question for stage 3: how much trust the data of former loyalty members
  keeps in survivorship.

## Alternatives considered
- Loyalty as increments: simpler, but it would remove the snapshot-to-changes
  pattern, which is common in real projects.
