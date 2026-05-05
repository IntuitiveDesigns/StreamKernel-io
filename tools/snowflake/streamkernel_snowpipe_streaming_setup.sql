-- StreamKernel Snowpipe Streaming smoke setup
--
-- Run this in Snowflake before using a Snowpipe Streaming sink profile.

CREATE DATABASE IF NOT EXISTS STREAMKERNEL;
CREATE SCHEMA IF NOT EXISTS STREAMKERNEL.PUBLIC;

CREATE OR REPLACE TABLE STREAMKERNEL.PUBLIC.ENRICHED_EVENTS (
    EVENT_ID STRING,
    EVENT_KEY STRING,
    EVENT_TS TIMESTAMP_NTZ,
    PAYLOAD_TYPE STRING,
    PAYLOAD_TEXT STRING,
    SOURCE_TEXT STRING,
    SENTIMENT STRING,
    EMBEDDING ARRAY,
    METADATA VARIANT,
    PIPELINE_ID STRING,
    RUN_ID STRING,
    SOURCE_TYPE STRING,
    SINK_TYPE STRING,
    SOURCE_AUTH STRING,
    SINK_AUTH STRING,
    SECURITY_TYPE STRING,
    TRANSFORM_CHAIN STRING,
    TRANSFORM_VERSION STRING,
    FEATURE_VERSION STRING,
    PROMPT_VERSION STRING,
    MODEL_NAME STRING,
    MODEL_ALIAS STRING,
    MODEL_VERSION STRING,
    MODEL_RUN_ID STRING,
    MODEL_EXPERIMENT_ID STRING,
    MODEL_STAGE STRING,
    INFERENCE_TIMESTAMP TIMESTAMP_NTZ,
    CONFIG_SHA256 STRING,
    POLICY_SHA256 STRING
);

CREATE OR REPLACE PIPE STREAMKERNEL.PUBLIC.STREAMKERNEL_PIPE
AS
COPY INTO STREAMKERNEL.PUBLIC.ENRICHED_EVENTS (
    EVENT_ID,
    EVENT_KEY,
    EVENT_TS,
    PAYLOAD_TYPE,
    PAYLOAD_TEXT,
    SOURCE_TEXT,
    SENTIMENT,
    EMBEDDING,
    METADATA,
    PIPELINE_ID,
    RUN_ID,
    SOURCE_TYPE,
    SINK_TYPE,
    SOURCE_AUTH,
    SINK_AUTH,
    SECURITY_TYPE,
    TRANSFORM_CHAIN,
    TRANSFORM_VERSION,
    FEATURE_VERSION,
    PROMPT_VERSION,
    MODEL_NAME,
    MODEL_ALIAS,
    MODEL_VERSION,
    MODEL_RUN_ID,
    MODEL_EXPERIMENT_ID,
    MODEL_STAGE,
    INFERENCE_TIMESTAMP,
    CONFIG_SHA256,
    POLICY_SHA256
)
FROM (
    SELECT
        $1:event_id::STRING,
        $1:event_key::STRING,
        $1:event_ts::TIMESTAMP_NTZ,
        $1:payload_type::STRING,
        $1:payload_text::STRING,
        $1:source_text::STRING,
        $1:sentiment::STRING,
        $1:embedding,
        $1:metadata,
        $1:pipeline_id::STRING,
        $1:run_id::STRING,
        $1:source_type::STRING,
        $1:sink_type::STRING,
        $1:source_auth::STRING,
        $1:sink_auth::STRING,
        $1:security_type::STRING,
        $1:transform_chain::STRING,
        $1:transform_version::STRING,
        $1:feature_version::STRING,
        $1:prompt_version::STRING,
        $1:model_name::STRING,
        $1:model_alias::STRING,
        $1:model_version::STRING,
        $1:model_run_id::STRING,
        $1:model_experiment_id::STRING,
        $1:model_stage::STRING,
        $1:inference_timestamp::TIMESTAMP_NTZ,
        $1:config_sha256::STRING,
        $1:policy_sha256::STRING
    FROM TABLE(DATA_SOURCE(TYPE => 'STREAMING'))
);
