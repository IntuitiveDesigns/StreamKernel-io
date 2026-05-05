# Profiles

The active profile set lives under `config/pipelines/`, not `config/profiles/`.

Use these references as the current entry points:

- [../PROFILES.md](../PROFILES.md)
- [18_benchmark_runner.md](18_benchmark_runner.md)
- [19_configuration_reference.md](19_configuration_reference.md)

If you are reproducing a published result, prefer `test-java-runner.ps1` over an ad hoc Java launch so the JVM and runtime overrides are captured in the benchmark artifacts.
