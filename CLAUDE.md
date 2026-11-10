# CLAUDE.md: Meridian build guide

This file tells Claude (Claude Code) how to build this project. Read **README.md** first: it is the design spec. This file is the build plan.

## Goal

Real-time + batch lakehouse on NYC TLC taxi data: Kafka -> Spark Structured Streaming -> Delta Lake (bronze/silver/gold) -> Snowflake + dbt, orchestrated by Airflow, with Great Expectations quality gates and Terraform infrastructure.

## Tech stack

Python 3.11, PySpark 3.5, Delta Lake, Apache Kafka (Docker: Bitnami/Redpanda), Apache Airflow 2.x, dbt-snowflake (or dbt-duckdb for a free local mode), Great Expectations, Terraform (AWS S3/IAM), Docker Compose, GitHub Actions.

## Build order (phases)

1. Repo scaffold: folders from README, `docker-compose.yml` with Kafka, Spark, Airflow and Postgres; Makefile targets (`make up`, `make test`).
2. Data download script for 1-3 months of TLC yellow-taxi Parquet + taxi zone lookup; EDA notebook.
3. Kafka producer that replays trips as JSON events at a configurable rate.
4. Spark Structured Streaming job: Kafka -> Delta bronze with checkpoints and exactly-once writes.
5. Batch jobs: bronze -> silver (schema enforcement, dedup, invalid-record filtering) -> gold (star schema + KPI aggregates) using Delta MERGE.
6. Airflow DAGs for batch ingestion and transforms, with retries, SLAs and backfill support.
7. dbt project (staging -> marts, tests, snapshots for SCD2). Support Snowflake, with a DuckDB fallback for a free local demo.
8. Great Expectations suites between layers; a failed check stops promotion.
9. Terraform for S3 bucket + IAM; document how to run in the cloud vs. locally.
10. Benchmarks: events/sec, end-to-end latency, rows processed. Save to docs/results and update the README.

## Working rules for Claude

- Build **one phase at a time**, in the order below. Each phase must run end to end before the next starts.
- After each phase, update the **Roadmap checklist in README.md** and commit with a clear message.
- **Never invent results.** Every number in README.md (accuracy, latency, throughput, cost) must come from a run in this repo. Put benchmark output in `docs/results/`.
- Keep secrets out of git: use `.env` (gitignored) plus a committed `.env.example`.
- Never commit raw datasets or large model files. Write download scripts and add the data folders to `.gitignore`.
- Python: 3.11+, `uv` or `pip-tools` for dependencies, `ruff` + `black` for formatting, type hints, `pytest` for tests.
- Every service gets a `Dockerfile`. Local development runs with `docker compose up`.
- Add a GitHub Actions workflow early (lint + tests) and keep it green.
- Prefer small, readable modules over clever abstractions. Add docstrings where intent is not obvious.
- **Screenshots:** when a phase has something visible (UI, dashboard, Grafana, CLI output, test report), capture real screenshots into `docs/screenshots/` and embed them in README.md. Never mock them up.
- When a phase is done, add a short "How it works" note or diagram to `docs/`. This is what interviewers will read.

## Definition of done (whole project)

- `docker compose up` (or the documented command) brings up a working demo in one step
- README has a real architecture diagram, setup steps, screenshots or GIFs, and measured results
- CI is green, tests cover the core logic
- A 2-3 minute demo video or GIF is linked in the README

## Status

Only `README.md` and this file exist so far. Start at **Phase 1**.
