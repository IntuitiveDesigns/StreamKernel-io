# Changelog

All notable changes to the **StreamKernel** project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added buyer-facing commercial, comparison, demo, benchmark-suite, and SPI
  plugin-example documentation.

### Changed
- Reworked README into a tighter category, pain, benchmark, comparison, SPI,
  and licensing overview.
- Updated benchmark runner documentation to use the public `tests.csv` and
  `tests_oidc.csv` matrices.

## [0.2.0] - 2026-04-24

### Added
- Added `PATENT-NOTICE.md` for filing-status and public-release hygiene.
- Added `THIRD-PARTY-NOTICES.md` for public third-party project hygiene.
- Added `PLAYBOOKS.md` for public build, run, benchmark, and release hygiene.

### Changed
- Clarified the mixed Apache SDK / SSAL runtime license boundary in README,
  NOTICE, LICENSE-HISTORY, and CONTRIBUTING.
- Disabled the legacy `fix_docs.ps1` helper because it generated obsolete
  Apache-only documentation.
- Rewrote `ARCHITECTURE.md` and `MODULES.md` around the transport-agnostic
  kernel and explicit SDK/runtime/plugin boundaries.

### Removed
- Removed tracked secrets, stale temporary files, backup files, and private
  patent/legal work product from the public release tree.

## [0.1.0] - 2026-01-03
### 🚀 Major Architecture Overhaul
This release marks the transition from a monolithic prototype to an Enterprise Multi-Module Architecture under **Apache License 2.0**.

### Added
- **Multi-Module Structure**: Separation of `spi`, `core`, `plugins`, and `app`.
- **High-Performance Source**: `source-synthetic` plugin with Lock-Free Ring Buffer (>13M EPS).
- **Enterprise Security**: `security-opa` plugin with Fail-Closed logic and caching.
- **Configuration**: Profile-based configuration via `SK_CONFIG_PATH`.

### Changed
- **License**: Switched from MIT to **Apache License 2.0**.
- **Identity**: Repository ownership transferred to **IntuitiveDesigns**.

## [0.0.1] - 2025-10-15
### Added
- Initial proof-of-concept prototype.
