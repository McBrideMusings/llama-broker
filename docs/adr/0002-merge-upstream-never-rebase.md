# ADR-0002: Upstream llama-swap is merged, never rebased

llama-broker is a regular fork of mostlygeek/llama-swap. New upstream releases are merged into `main`; `main` is never rebased or force-pushed. Fork additions go in new files, and edits to upstream files stay limited to the hook points ADR-0001 names.
