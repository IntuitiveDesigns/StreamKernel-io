# 14 - Snowflake Snowpipe Streaming

This public profile lands deterministic enriched records into Snowflake through the JVM Snowpipe Streaming sink. It is included as a runnable use case, but publish results only after running it against the intended Snowflake account.

## Required Environment

```powershell
$env:SNOWFLAKE_ACCOUNT = "<account>"
$env:SNOWFLAKE_USER = "<user>"
$env:SNOWFLAKE_DATABASE = "STREAMKERNEL"
$env:SNOWFLAKE_SCHEMA = "PUBLIC"
$env:SNOWFLAKE_PIPE = "STREAMKERNEL_PIPE"
$env:SNOWFLAKE_ROLE = "STREAMKERNEL_ROLE"
$env:SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
$env:SNOWFLAKE_PRIVATE_KEY_FILE = "C:\path\to\rsa_key.p8"
```

## Run

```powershell
.\gradlew.bat --no-daemon :streamkernel-app:shadowJar
.\test-java-runner.ps1 -MatrixFile .\benchmark-runs\tests_snowflake.csv
```

Profile: `config/pipelines/streamkernel_snowflake_snowpipe_streaming.properties`.

Schema helper SQL lives in `tools/snowflake/`.
