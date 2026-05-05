# StreamKernel License History

This file records the release history and module boundary for the
StreamKernel licensing transition.

## Releases Through `v0.1.0`

Releases up to and including `v0.1.0` were published under the
Apache License, Version 2.0.

Recipients who obtained those releases under Apache 2.0 retain
their Apache 2.0 rights for those specific versions. Those rights
are not revoked retroactively.

## Current Source Tree And Post-`v0.1.0` Releases

The current source tree uses a mixed-license model:

- Apache 2.0 SDK modules:
  - `streamkernel-api`
  - `streamkernel-spi`
  - `streamkernel-metrics/metrics-api`
- SSAL v1.0 runtime and first-party modules:
  - all remaining modules, runtime integrations, deployment files,
    and operational tooling in this repository

The first release cut from the mixed-license source tree after
`v0.1.0` will carry this boundary unless superseded by a later
written notice in the repository.

## Practical Meaning

- Build custom plugins against the Apache SDK modules:
  allowed under Apache 2.0. Independently authored plugins remain
  author-owned unless a separate written agreement says otherwise.
- Use the StreamKernel runtime internally:
  allowed under SSAL v1.0.
- Redistribute, embed, resell, or offer the SSAL-governed runtime
  to third parties:
  requires a commercial license.

## Patent Rights

The SSAL-governed runtime does not grant patent rights. Some StreamKernel
runtime mechanisms may be covered by pending or future patent applications
owned by Steven Lopez or StreamKernel. Commercial license agreements may
include explicit patent grants as negotiated terms.

Use "patent pending" language only with the confirmed filing details recorded
in [`PATENT-NOTICE.md`](PATENT-NOTICE.md).

## Third-Party Software

Third-party tools and runtimes used by optional profiles remain under their
own upstream licenses. See [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md).

## Related Files

- [`LICENSE`](LICENSE)
- [`LICENSE-APACHE-2.0.txt`](LICENSE-APACHE-2.0.txt)
- [`PATENT-NOTICE.md`](PATENT-NOTICE.md)
- [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md)
- [`TRADEMARK-POLICY.md`](TRADEMARK-POLICY.md)
