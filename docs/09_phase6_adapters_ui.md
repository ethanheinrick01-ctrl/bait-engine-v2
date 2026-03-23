# Phase 6 — Adapters and UI

Phase 6 is where the engine stops being a self-licking skull and becomes something a real surface can consume.

## Doctrine

Do not let platform glue leak backward into:
- analysis
- planning
- generation
- storage

The core produces decisions and candidate replies.
Adapters translate that into surface-specific envelopes.

## First seam

The first adapter layer should be a neutral outbound contract:
- target platform
- thread / reply identifiers
- selected candidate body
- run metadata for auditability

That gives us a stable handoff point before building any real posting integrations.

## Initial modules

### `adapters/contracts.py`
Canonical transport-neutral envelope types.

### `adapters/compiler.py`
Compiler that turns a stored run + selected candidate into an outbound reply envelope.

### `cli adapter-preview`
Dry-run inspection tool for the adapter handoff.
It should never post. It only shows what a real adapter would receive.

## Why this order

If we jump straight to provider APIs or UI widgets, the boundary gets muddy and the project turns to sludge.
A previewable envelope keeps the seam inspectable and testable.

## Registry layer

The second seam is a registry of named adapters with capability flags.
Initial targets:
- reddit
- x
- discord
- web

This lets the rest of the system ask:
- what surfaces exist?
- can this surface create threads?
- can it edit/delete?
- does it support media?

## Inbound context layer

The third seam is inbound thread context.
That contract should capture:
- platform
- thread id
- subject
- root author handle
- recent messages
- extra metadata

This allows reply compilation to carry lightweight thread awareness without infecting the core analysis/planning model.

## Target normalization layer

Different surfaces encode target references differently.
So the adapter seam needs a normalization step for:
- thread identifiers
- reply identifiers
- author handles

Examples:
- reddit strips `u/` prefixes from handles
- x strips `@`
- discord lowercases handles for stable comparison
- reply ids may backfill thread ids on surfaces where replies imply thread context

## Candidate selection layer

Preview and compile flows should not be forced to pick by ordinal alone.
Selection helpers now allow:
- rank-based selection
- highest rank score
- highest bite score
- highest audience score
- lowest critic penalty
- optional filtering by tactic or objective

This keeps adapter-facing selection explicit and inspectable.

## Capability enforcement layer

Registry data is not decorative.
Compile/preview flows should reject targets that violate surface capabilities.
Examples:
- surfaces without thread lookup cannot infer thread context from a loose reply id
- surfaces without thread creation should demand explicit reply targets when needed
- reply compilation should fail fast instead of emitting invalid envelopes

## Surface-specific selection presets

Selection should also respect the social geometry of a surface.
The adapter registry now carries named presets per platform.
Examples:
- Reddit defaults toward engagement/bite
- X defaults toward audience-facing replies
- Discord defaults toward lower-penalty conversational replies
- generic web stays balanced unless told otherwise

Presets are explicit registry data, not hidden heuristics, so preview tooling can inspect and override them.

## Context-aware preset recommendation

Static per-surface presets are useful, but threads vary.
A recommendation layer can inspect inbound context and choose a preset based on:
- thread length
- recent hostility markers
- participant counts / unique authors
- audience or engagement metadata
- root-author presence and other thread-shape hints
- surface norms

This recommendation should remain explicit and inspectable, not hidden inside generation.
The preview payload should expose the metrics that drove the recommendation so the heuristic basis can be audited.

## Preview panel

A minimal local panel can sit entirely on top of existing contracts.
It only needs to assemble:
- adapter descriptor
- recommended preset
- context summary
- compiled reply envelope
- transport-specific emit request
- inspectable control metadata (available presets, strategies, tactic/objective filters)
- precomputed local variants so a static HTML export can still feel interactive
- variant-generation settings so CLI callers can decide how broad or narrow the preview set should be

An HTML export is enough for static inspection scaffolding.
A little client-side JavaScript can swap among precomputed variants without introducing a backend.
A lightweight localhost server can then upgrade the same panel into a direct-review surface, letting operator judgments post straight into storage without shell-copy handoff.
That served mode should also be able to launch itself in the default browser so the operator enters the review loop with one command.
It should default to the latest run when no explicit run id is provided, and it should expose a lightweight recent-runs dashboard so the operator can switch targets without returning to the shell.
That dashboard should also be able to create and save a fresh run directly from pasted source text, then jump into the resulting panel without any command-line round trip.
Within the served panel, the current variant should expose one-click browser-native export actions (copy/download envelope and emit payloads) so operators can move artifacts without touching the shell.
The dashboard should also maintain a local outbox for staged emit requests, with explicit state transitions like staged, approved, and archived so execution can be gated later without losing operator intent.
Outbox entries should also support direct note editing in-browser, not just append-only status annotations, so operator intent can be corrected instead of accreting into sludge.
To stop the cockpit from being a fragile one-command toy, the UI layer should also be able to surface LaunchAgent/daemon persistence metadata and manage install/remove flows for a local login daemon without forcing the operator to handwrite plist files.
CLI flags should let the user widen the variant set (more presets/strategies) or clamp it down for compact review.
The panel should also be able to pull recent historical outcomes so each variant can show whether similar tactic/objective combinations actually produced bites.
Those overlays should expose confidence/coverage fields and drive variant ordering so the first surfaced envelope is not merely the heuristic recommendation but the strongest historically-supported option.
Equivalent envelopes should collapse together, and the UI should expose a short rationale plus recent comparable outcomes so the operator can see why the top variant won.
It should also compare the winner against the runner-up with explicit metric deltas, dominant advantages, and per-metric winner/runner-up values, while surfacing compact win/loss/pending badges for faster human scanning.
Operator review decisions should persist alongside runs so future panels can bias toward shapes a human actually promoted, favored, or rejected.
Each surfaced variant should also carry explicit review-action payload templates so a UI layer can submit `promote`, `favorite`, or `avoid` without reverse-engineering the current selection state.
For static local HTML preview, that bridge can be expressed as executable CLI request payloads with note binding.
For served local preview, the bridge should upgrade to direct HTTP submission so review actions mutate storage immediately and the refreshed panel can re-rank with the new operator signal.

## Transport-specific emitters

Emitters are the last seam before real posting.
They convert a validated neutral reply envelope into a dry-run transport request per surface.
That keeps posting syntax out of the cognitive core while making downstream integrations obvious.

## Closure status

Phase 6 is complete.

Closure record, smoke path, and handoff notes live in `docs/10_phase6_completion.md`.

## Near-term next steps

1. Add richer per-platform normalization rules as real adapters land.
2. Add transport-specific delivery adapters that consume emit requests.
3. Add richer recommendation heuristics from author/thread metadata.
4. Add execution-layer handling for staged outbox items.
