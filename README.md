# mc_to_synq
Built from a production migration of 200+ monitors across a Snowflake data platform. Handles the full lifecycle: extract monitors from MC's GraphQL API, convert them to SYNQ-compatible formats, deploy via SYNQ's REST API, and clean up when you need to start over.
