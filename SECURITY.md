# Security

## StreamKernel Security Threat Model

Scope: in-process data orchestration runtime

Audience: enterprise security, architecture, and acquisition review

### Executive Summary

StreamKernel's core security thesis is simple: minimize moving parts in the
critical data path. Traditional streaming pipelines accumulate sidecars, policy
services, sink clients, schema transforms, and ad hoc retry logic. Each extra
component becomes another place to configure credentials, expose ports, and lose
observability.

StreamKernel keeps orchestration, transformation, policy checks, metrics, and
sink execution inside one controlled runtime boundary unless a configured source
or sink explicitly sends data elsewhere.

### Architectural Security Principles

#### 1. In-process execution reduces attack surface

Typical streaming architectures often require sidecar containers, service-mesh
routing, and internal HTTP or gRPC traffic. Each additional service introduces
credentials, certificates, network paths, and attack vectors.

StreamKernel executes orchestration, transformation, policy enforcement, and
sink delivery inside a single runtime boundary.

Security outcome:

- No lateral movement path between helper services by default
- Less service-to-service credential sprawl
- Reduced blast radius
- Fewer components requiring hardening

#### 2. Optional sidecars stay explicit

Operational helper services expose REST endpoints, health probes, and metrics
endpoints. These can become port-scanning targets, misconfiguration risks, and
credential leakage vectors.

StreamKernel's core pipeline execution does not require hidden companion
services. Optional external systems are configured explicitly and can be reviewed
per deployment.

Security outcome:

- Fewer implicit ports to scan
- Clearer service inventory
- Reduced credential sprawl

#### 3. Fewer HTTP hops reduce exfiltration paths

Sidecar-heavy pipelines require serialization, network transmission, TLS
termination, and deserialization. Every hop becomes a potential data leak vector,
MITM opportunity, or logging exposure point.

StreamKernel keeps data in memory from ingestion through transformation to egress
unless a configured connector explicitly sends it elsewhere.

Security outcome:

- Fewer network-level data exfiltration paths
- No unnecessary intermediate serialization exposure
- No hidden intermediate persistence of sensitive data

This is particularly important for PII, financial data, defense workloads, and
healthcare data.

#### 4. Plugin sandboxing model

Untrusted or poorly written plugins could access sensitive runtime state,
exfiltrate data, or crash the runtime.

StreamKernel plugin controls include strict lifecycle management, explicit SPI
boundaries, configuration-driven enablement, and controlled execution context.
Plugins operate within the JVM boundary, orchestrator lifecycle, and policy
enforcement layer.

Security outcome:

- No arbitrary runtime injection
- Deterministic startup validation
- Controlled extension surface

#### 5. mTLS support

Data entering or leaving the runtime could be intercepted, spoofed, or tampered
with.

StreamKernel supports mutual TLS for Kafka and external connectors,
certificate-based authentication, and encrypted transport.

Security outcome:

- Authenticated producers and consumers
- Encrypted ingress and egress
- Prevention of rogue client injection

#### 6. OPA policy enforcement

StreamKernel integrates Open Policy Agent for batch-level authorization,
policy-driven access control, and real-time enforcement. The security model is
fail-closed: if the policy engine fails, processing is denied.

Security outcome:

- Externalized governance
- Auditable authorization decisions
- Zero-trust data pipeline enforcement

#### 7. Deterministic startup gate

Pipelines should not start in a partially initialized or unsafe state.
StreamKernel performs a deterministic readiness check before accepting data:
components are initialized, security providers are validated, policies are
reachable, and sinks are available.

Security outcome:

- Prevents partial-startup data loss
- Prevents unsafe runtime states
- Guarantees integrity from the first record processed

## Summary

StreamKernel reduces enterprise risk by reducing unnecessary service surfaces,
removing avoidable inter-service network hops, enforcing policy at runtime,
securing transport with mTLS, validating deterministic startup state, and
containing execution within a single trusted boundary.
