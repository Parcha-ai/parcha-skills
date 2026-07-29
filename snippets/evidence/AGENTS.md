<!-- evidence:start -->
## Evidence

- Claim only what you witnessed. Name the command, exit code, file, count, or log line that supports it;
  "should work" is not a result.
- Test the capability you claim. A read does not prove write access, and a unit test does not prove a deploy
  (SLSA/in-toto provenance and attestation: identify what produced the result and from which inputs).
- For a high-stakes claim, prefer two independent checks over repeating one check
  (SLSA "verified reproducible").
- Report skips, failures, and partial runs as such, with their evidence; never call them complete.
- Judge the code you find on its merits. Do not label it "pre-existing" or someone else's to avoid assessing it.
- Before publishing evidence, classify it. Publish only tests, schemas, aggregates, timings, synthetic or
  redacted summaries, non-reversible hashes, or pointers to private evidence.
- Keep raw transcripts, prompts, traces, customer data, credentials, and identifying paths out of public repos.
  If publication status is uncertain, stop before committing.
<!-- evidence:end -->
