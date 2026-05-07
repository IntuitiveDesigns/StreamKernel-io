# StreamKernel AI: DJL, ONNX, and MLflow Integration

StreamKernel's AI capability is designed for companies that need model output inside the event pipeline, with governance and audit evidence attached to the records that leave the pipeline. This document describes the capability and benchmark evidence while intentionally withholding protected implementation mechanics, model artifacts, tokenizer artifacts, private configs, and proprietary adapter code.

The business point is simple: many teams can call a model. Fewer can run AI enrichment, policy, provenance, rollback, DLQ handling, metrics, and delivery inside one controlled runtime that can be licensed, embedded, benchmarked, and supported.

---

## Why This Matters to Buyers

| Buyer problem | StreamKernel AI answer |
|---|---|
| Sidecar inference adds a network hop and another service to operate | DJL/ONNX enrichment can run inside the JVM pipeline boundary. |
| Model changes are risky in production event flows | MLflow registry integration provides a control point for bootstrap, promotion evidence, and health-gated rollback behavior. |
| AI records need auditability | Model alias/version/run labels can travel with enriched records into downstream systems. |
| Teams need vector/lakehouse/event outputs from one flow | The same enrichment stage can feed Kafka, MongoDB vector collections, Delta Lake, Snowflake, DevNull, or custom sinks. |
| Procurement needs proof, not architecture claims | The evidence below gives records processed, throughput, latency, sink behavior, and rollback timing from dated runs. |
| OEMs and enterprise platforms need a clean IP boundary | Public docs show capability and evidence; commercial builds and negotiated license terms protect the implementation. |

---

## Public and Protected Boundary

| Public in this repo | Withheld for IP protection and commercial licensing |
|---|---|
| Capability descriptions for DJL, ONNX Runtime, MLflow, provenance, sinks, and benchmarks | Source code for protected model lifecycle coordination, hot-swap mechanics, rollback evaluator internals, and proprietary adapters |
| Benchmark summaries and selected evidence from May 4, 2026 runs | Raw private logs unless intentionally published, local configs, secrets, model cache contents, and generated model artifacts |
| Public SDK/plugin boundary and deterministic enrichment profiles | Private model artifacts, tokenizer artifacts, customer adapters, deployment-specific tuning, and privileged patent work product |
| Patent notice and commercial licensing entry points | Any license grant beyond the public license unless negotiated in a commercial agreement |

This framing lets companies understand why StreamKernel is commercially valuable without giving away the implementation playbook.

---

## Capability Map

**DJL + ONNX in-process enrichment.** StreamKernel can place embedding or classification-style model work directly in the transformer chain. The public claim is the operational capability and benchmark evidence, not the private implementation.

**MLflow model registry integration.** The protected AI build uses MLflow registry metadata as an operational control point for model selection, promotion evidence, and rollback provenance.

**Health-gated model rollback.** Benchmark evidence shows a running pipeline detecting an unhealthy promoted model and returning to a prior active model without a JVM restart.

**Transport-agnostic AI path.** The AI enrichment stage is independent of source transport. Evidence covers Kafka, Apache Pulsar, synthetic sources, MongoDB vector output, Delta Lake output, and Kafka output.

**Record-level provenance.** Enriched records can carry model metadata so downstream consumers know which model produced the output they are reading.

---

## Evidence Summary: May 4, 2026

Evidence environment: ONNX Runtime 1.20.0, DJL 0.32.0, MiniLM-L6-v2, CPU-only, Intel i9-8950HK (6 cores / 12 threads / 32GB RAM), local Docker, single JAR.

| Run | Profile | Throughput / records | Outcome |
|---|---|---:|---|
| 1 | ONNX embedding to MongoDB vector | 331 eps / 198,560 records | Zero record loss; enriched records persisted to vector collection |
| 2 | MLflow bootstrap to Delta Lake | 38 eps / 11,072 records | Champion model artifact resolved at startup; enriched records landed in Delta |
| 3 | Lineage audit with DJL chain | 36 eps / 21,504 records | Model provenance labels carried through Kafka output |
| 4 | Live model swap and rollback | 10,496 records | Promotion observed; health guard rolled back in about 4 seconds; zero restarts |
| 5 | Pulsar to ONNX to Delta Lake | 9,600 records | Same AI path worked from Apache Pulsar source into Delta Lake |
| 6 | ONNX in-process inference to Kafka | 204 eps / 122,272 records | Sustained CPU-only inference baseline with stable latency |

These numbers are intentionally presented as evidence of operating behavior, not as a disclosure of the protected model lifecycle implementation.

---

## Live Model Governance Evidence

The landmark run demonstrates the buyer-facing outcome companies care about: a model registry change happened while the pipeline was running, the pipeline promoted the new model, health checks detected an unhealthy promotion, and the runtime returned to the previous model without restarting the JVM or dropping records.

| Observed step | Public evidence |
|---|---|
| Startup | Pipeline resolved the active MLflow model alias and began processing with the initial model version. |
| Promotion | Registry alias changed from version 4 to version 10; the running pipeline observed the change and activated the promoted model. |
| Health gate | The promoted model breached configured latency health criteria during the stabilization window. |
| Rollback | The active model returned to version 4 in about 4 seconds. |
| Continuity | The run completed with 10,496 records, zero JVM restarts, zero dropped records, and zero DLQ events. |

The public documentation does not publish the internal transition algorithm, concurrency coordination, rollback evaluator details, or deployment-specific tuning.

---

## Commercial Use Cases

- Regulated event enrichment where every AI output needs model provenance.
- Product-owned AI pipelines where an OEM or SaaS vendor needs a small embeddable runtime instead of a separate model-serving stack.
- Vector search and retrieval pipelines that enrich live events before writing to MongoDB vector collections or lakehouse destinations.
- Customer-support, claims, risk, fraud, telemetry, and operational-event flows where latency, auditability, and rollback behavior matter.
- Enterprise environments that need private builds, air-gapped delivery, support, and negotiated patent rights.

---

## What a Commercial Evaluation Can Include

- Private demo of the protected DJL/ONNX/MLflow path.
- Benchmark replay using a customer-relevant source, model shape, and sink.
- Architecture review of runtime boundaries, plugin ownership, deployment topology, and support model.
- Licensing discussion for internal production use, OEM embedding, managed service use, redistribution, or negotiated patent rights.

---

## Patent and Licensing Boundary

StreamKernel US Provisional Patent Application 64/057,035 was filed May 4, 2026. The public repository may describe StreamKernel as patent pending, but it does not publish privileged patent work product or protected implementation mechanics.

The evidence above is proof of capability, not an implementation disclosure. See [PATENT-NOTICE.md](PATENT-NOTICE.md) and [COMMERCIAL.md](COMMERCIAL.md) for the public notice and licensing boundary.

---

## Contact

Commercial licenses covering redistribution, OEM embedding, managed services, private builds, support, and negotiated patent rights are available.

Contact: [LinkedIn](https://www.linkedin.com/in/steve-lopez-b9941/) or [GitHub Issues](https://github.com/IntuitiveDesigns/StreamKernel-io/issues).