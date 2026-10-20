# Codex epoch-5 backfill selection

Epoch 5 adds text only when the epoch-4 whole-output source resolver refused a
Codex tool event. An unchanged, already typed epoch-4 document with complete
native coverage and zero resolver refusals therefore has no new extraction work.
Both existing backfill seed owners use the same scheduling exception for it.

The exception requires current revision, source hash, manifest hash, policy and
requested/applied typed mode. The checkpoint must contain matching coverage and
the complete current record count; pending logical work prevents exemption. Only
the exact known coverage fields, valid integer counters and known dispositions
are accepted. In that shape, an absent `source_unit_refused` key is the native
zero-counter convention. Null, malformed, unknown or positive-refusal coverage
remains eligible for ordinary rebuilding. Other dispositions can be nonzero;
this does not declare all raw source content interpretable.

The stored checkpoint remains version 4. No append verifier, source reader,
authority predicate or publication transaction changes. A real source update
still queues ordinary work and the unchanged append verifier refuses the old
checkpoint, causing the normal full rebuild. The scheduling exception applies
only to the epoch-5 transition and cannot suppress a future epoch's migration.
Existing queued work is not cancelled.

Native controls compare old4/new5 full-preparation output and coverage for visible,
tool-only and empty output. Both seed owners skip unchanged complete cases,
retain unknown/stale/affected obligations, and still rebuild actual mixed outputs
through the original source/publication tests. The skipped native fixtures make
zero archive-part reads. These measurements do not establish corpus savings or
provider billing: unchanged passage IDs already avoid unnecessary replacement
through the native diff.

Legacy-mode documents remain a separate typed-enrollment obligation. This change
does not mark them complete, alter source selection or finish the corpus campaign.
