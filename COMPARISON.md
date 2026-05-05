# Why Not Kafka, Flink, Spark, Or Databricks Alone?

StreamKernel is not a replacement for Kafka, Flink, Spark, or Databricks. It is a small operational runtime for the pipeline logic that usually gets scattered around those platforms.

## Positioning

Kafka moves events. Flink processes streams. Spark and Databricks are excellent for analytics, lakehouse workflows, and managed data/ML platforms. StreamKernel handles the low-latency operational path where a product team needs policy, inference, cache, transformation, DLQ, metrics, and destination writes in one controlled process.

## Comparison

| Choice | Best Use | Pain When Used Alone | StreamKernel Role |
|---|---|---|---|
| Kafka | Durable event log and pub/sub backbone. | You still build transforms, model calls, policy checks, cache logic, DLQs, sink writers, metrics, retries, and benchmark harnesses. | Uses Kafka as a source/sink while owning the operational pipeline kernel. |
| Flink | Stateful stream processing at cluster scale. | More moving parts than many inference, policy, enrichment, and delivery workflows need. Operational tuning can dominate the project. | Gives teams a lighter single-JVM runtime when cluster-stateful processing is not the job. |
| Spark | Batch analytics, large transforms, lake jobs. | Micro-batch/cluster model is often too heavy for product-owned low-latency event decisions. | Feeds or complements Spark/Delta paths without making Spark own every operational decision. |
| Databricks | Managed lakehouse, notebooks, jobs, ML workflows, governance. | Not designed as a tiny redistributable runtime inside another product or controlled customer environment. | Writes to lakehouse paths while keeping edge/product pipeline logic local and portable. |

## When StreamKernel Is A Fit

- You need to run inference or policy before events leave the process.
- You need portable source/sink profiles without changing orchestration code.
- You need benchmark evidence that includes JVM and runtime settings.
- You want customers or internal teams to author plugins without forking the runtime.
- You want a commercial runtime boundary around first-party orchestration and enterprise modules.

## When It Is Not The Fit

- Kafka is enough because you only need durable transport.
- Flink is already required for complex stateful windowing and cluster-scale stream joins.
- Spark or Databricks is already the correct control plane for large analytical jobs.
- The workload does not need policy, inference, cache, DLQ, or destination-specific runtime behavior.
