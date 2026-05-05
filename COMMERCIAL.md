# Commercial Licensing

StreamKernel uses a mixed model: a source-available core runtime, Apache 2.0 SDK contracts, and author-owned custom plugins.

## Clear Boundary

- Core runtime and first-party modules are source-available under the StreamKernel Source Available License.
- `streamkernel-api`, `streamkernel-spi`, and `streamkernel-metrics/metrics-api` are Apache 2.0 SDK modules.
- Plugins written by customers, partners, or independent authors against the Apache SDK remain author-owned.
- StreamKernel does not claim ownership of independently authored plugins merely because they implement the SPI.

## Commercial License Required For

- External redistribution of the SSAL-governed runtime.
- OEM embedding inside a product shipped to third parties.
- Managed service, hosted service, or platform-as-a-service use.
- Resale, sublicensing, or bundled enterprise distribution.
- Support, SLA, private builds, air-gapped delivery, or negotiated patent rights.

## Common Commercial Packages

| Package | Fit |
|---|---|
| OEM Runtime | Embed StreamKernel in a shipped product or appliance. |
| Managed Service | Operate StreamKernel as part of a hosted platform. |
| Enterprise Internal | Internal production use with support, private builds, and deployment guidance. |
| Government / Defense | Air-gapped delivery, security review, and negotiated terms. |
| Partner Plugin | Partner-owned plugin distributed beside or through StreamKernel commercial channels. |

## Plugin Ownership

The SPI is intended to create an ecosystem boundary. A plugin author can implement `SourcePlugin`, `TransformerPlugin`, `SinkPlugin`, cache, security, DLQ, or metrics contracts and keep the plugin under their own license, subject to whatever separate agreement they choose.

Commercial agreements may cover joint distribution, certification, support, listing, or bundling of a plugin, but the default position is simple: author-owned plugins remain author-owned.

## Contact

Commercial licensing and enterprise inquiries:

- [linkedin.com/in/steve-lopez-b9941](https://www.linkedin.com/in/steve-lopez-b9941/)

For public project questions, use [GitHub Issues](https://github.com/IntuitiveDesigns/StreamKernel-io/issues).
