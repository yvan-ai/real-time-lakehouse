# ADR-0001 — Record architecture decisions

**Status**: Accepted · **Date**: 2026-07-04

## Context

The project combines many interchangeable technologies (catalogs, brokers, table formats,
deployment models). Six months from now — or for any reviewer — the *why* behind each
choice matters as much as the choice itself.

## Decision

Record every significant architecture decision as a short ADR in `docs/decisions/`,
numbered sequentially, using this format: Context → Decision → Consequences.
An ADR is never edited to change its meaning; superseding decisions get a new ADR that
links back.

## Consequences

- Reviewers can audit trade-offs without reading the full git history.
- Decisions can be revisited explicitly (new ADR) instead of silently drifting.
