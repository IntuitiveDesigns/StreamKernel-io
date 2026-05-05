# Benchmark Suite

StreamKernel benchmark results are meant to be replayed, not guessed from prose. The public benchmark suite combines a pipeline file with a CSV row and runner-captured artifacts.

## Suite Files

- `benchmark-runs/tests.csv`: primary CPU suite.
- `benchmark-runs/tests_oidc.csv`: OIDC/security suite.
- `benchmark-runs/tests_lineage.csv`: provenance/audit suite.
- `benchmark-runs/tests_pulsar.csv`: Pulsar seeded-backlog source suite.
- `benchmark-runs/tests_pulsar_live.csv`: Pulsar live-producer source suite.
- `benchmark-runs/tests_snowflake.csv`: Snowflake Snowpipe Streaming sink suite.

Each matrix row includes the profile, run length, topic settings, heap size, GC mode, executor mode, cache toggle, sink-copy setting, and run ID.

## Reproduce One Row

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv -SingleTest streamkernel_kafka_at_least_once_baseline_10m
```

## Reproduce A Suite

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests.csv
```

## Evidence Contract

For a published number, keep these together:

1. The row in `benchmark-runs/*.csv`.
2. The pipeline file under `config/pipelines/`.
3. Runner output containing logs, GC output, Prometheus snapshot, effective settings, and `meta.json`.

The CSV row is the public envelope. The pipeline file is the wiring. `meta.json` is the audit trail for the actual JVM command and runtime properties.

## Useful Starting Rows

| Goal | Matrix | Row |
|---|---|---|
| Kafka at-least-once baseline | `benchmark-runs/tests.csv` | `streamkernel_kafka_at_least_once_baseline_10m` |
| Kafka exactly-once baseline | `benchmark-runs/tests.csv` | `streamkernel_kafka_exactly_once_baseline_10m` |
| MongoDB insert baseline | `benchmark-runs/tests.csv` | `streamkernel_mongodb_insert_baseline_10m` |
| Delta/Spark local lakehouse | `benchmark-runs/tests.csv` | `streamkernel_delta_spark_local_5m` |
| Lineage audit headers | `benchmark-runs/tests_lineage.csv` | `streamkernel_lineage_audit_10m` |
| Pulsar source drain | `benchmark-runs/tests_pulsar.csv` | `streamkernel_pulsar_source_kafka_10m` |
| Pulsar live producer | `benchmark-runs/tests_pulsar_live.csv` | `streamkernel_pulsar_live_kafka_10m` |
| Snowflake Snowpipe Streaming | `benchmark-runs/tests_snowflake.csv` | `streamkernel_snowflake_snowpipe_streaming_5m` |
| Source uncorked ceiling | `benchmark-runs/tests.csv` | `streamkernel_source_baseline_uncorked_5m` |
| OIDC/security profile | `benchmark-runs/tests_oidc.csv` | see matrix rows |

## Notes

Results vary by CPU, storage, Docker settings, JVM, and destination services. Compare runs by row name and environment, not by isolated screenshots.
