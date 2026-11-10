# CLAUDE.md: TaxiStream Lakehouse build guide

This file tells Claude (Claude Code) how to build this project. Read **README.md** first: it is the design spec. **INFO.md** is the interview prep companion. This file is the build plan.

## Goal

A lakehouse on public NYC TLC yellow taxi trip data that runs fully locally for free with Docker Compose.

- **Batch path:** download script for a few months of TLC Parquet + taxi zone lookup into MinIO (S3 API). PySpark jobs move data bronze -> silver (schema enforcement, deterministic `trip_key`, dedup, invalid-record filtering into `silver.trips_rejected`) -> gold (star schema + KPI tables) in Delta Lake, using `replaceWhere` and `MERGE` so every rerun is idempotent.
- **Streaming path (light):** a Python producer replays trips as JSON events into Redpanda (Kafka API). Spark Structured Streaming writes them to `bronze.trip_events` with checkpoints. Silver picks up new stream rows incrementally (`availableNow` trigger).
- **Quality:** Great Expectations gates after bronze, silver and gold. A failed gate fails the task and stops promotion. dbt tests on the marts.
- **Orchestration:** Airflow DAGs for the monthly batch (retries, catchup, backfills) and an hourly stream-to-silver refresh.
- **Serving:** dbt-duckdb models on gold with tests and docs, and a small Streamlit dashboard (trip KPIs + pipeline health).
- **CI:** GitHub Actions for lint, tests, DAG import, dbt build on fixtures.

## Tech stack

Python 3.11 (uv, ruff, pytest), PySpark 3.5 (standalone master + 1 worker), Delta Lake (`delta-spark` 3.x matching Spark 3.5), Redpanda (Kafka API), Spark Structured Streaming, MinIO, Apache Airflow with Postgres metadata and LocalExecutor, dbt-duckdb + DuckDB (`httpfs` and `delta` extensions), Great Expectations (GX Core), Streamlit, Docker Compose (profiles: `core`, `batch`, `stream`), GitHub Actions.

Build notes:
- Pin jar versions together: `delta-spark`, `hadoop-aws` matching Spark's bundled Hadoop, and `spark-sql-kafka-0-10` matching the Spark version. Record them in one place (Dockerfile build args).
- Each Delta table has exactly one writer job. Batch and stream bronze stay separate tables (single-driver LogStore on S3-style storage).
- Confirm at Phase 1 that a pinnable MinIO image is available. If not, use another S3-compatible store and update the README.
- If DuckDB's `delta_scan` against MinIO is unreliable, have the gold job also write a Parquet snapshot for dbt to read, and document it.
- Spark session time zone is UTC. TLC timestamps are NYC local wall-clock and are kept as-is.

## Build order (phases)

1. Scaffold: folders from README, `docker-compose.yml` with MinIO, Postgres, Airflow, Spark master + worker; Makefile (`make up`, `make test`); `.env.example`; CI skeleton (ruff + pytest).
2. Download script for a month range of TLC yellow taxi Parquet + zone lookup into MinIO `raw/` (skip unchanged files); EDA notebook on dirty-data patterns.
3. Bronze ingest (metadata columns, `replaceWhere` per month) and silver clean (fixed schema, `trip_key`, in-batch dedup, YAML rejection rules, `MERGE`); `ops.run_metrics`; unit tests on fixtures.
4. Gold: `fact_trips` + dimensions and KPI tables (`agg_zone_hourly_demand`, `agg_daily_revenue`, `agg_tip_rate`); tests.
5. Great Expectations suites for bronze, silver and gold; a failed check raises and stops promotion.
6. Airflow: `tlc_monthly_batch` DAG with retries, catchup and backfills, gates wired in; DAG import test in CI.
7. Streaming: Redpanda in Compose, replay producer, Structured Streaming job to `bronze.trip_events` with checkpoints; hourly `stream_to_silver` DAG.
8. dbt-duckdb project on gold: staging, marts, tests, docs; `dbt build` in the DAG and in CI on fixture Parquet.
9. Streamlit dashboard on the marts (trip KPIs + pipeline health); unslop pass; real screenshots.
10. Benchmarks: rows processed per run, batch runtime per layer, streaming events/sec, end-to-end latency to bronze (p50/p95), % records rejected. Save raw output to `docs/results/` and fill the README Results table.

## Stretch goals

Only after Phase 10 is done:
- Snowflake as a second dbt target.
- Terraform for a real S3 bucket + least-privilege IAM, with a documented cloud run.
- SCD Type 2 dbt snapshots for the zone dimension.
- Data contracts for the event schema, checked by the producer and in CI.
- `OPTIMIZE` / Z-ordering with measured before and after query times.

## Working rules for Claude

- Build **one phase at a time**, in the order above. Each phase must run end to end before the next starts.
- After each phase, update the **Roadmap checklist in README.md** and commit with a clear message.
- **Never invent results.** Every number in README.md (accuracy, latency, throughput, cost) must come from a run in this repo. Put benchmark output in `docs/results/`.
- Keep secrets out of git: use `.env` (gitignored) plus a committed `.env.example`.
- Never commit raw datasets or large model files. Write download scripts and add the data folders to `.gitignore`.
- Python: 3.11+, `uv` or `pip-tools` for dependencies, `ruff` + `black` for formatting, type hints, `pytest` for tests.
- Every service gets a `Dockerfile`. Local development runs with `docker compose up`.
- Add a GitHub Actions workflow early (lint + tests) and keep it green.
- Prefer small, readable modules over clever abstractions. Add docstrings where intent is not obvious.
- The UI follows the unslop skill (`~/.claude/skills/unslop`): run `/unslop` on the main screens before calling the UI done.
- **Screenshots:** when a phase has something visible (UI, dashboard, Grafana, CLI output, test report), capture real screenshots into `docs/screenshots/` and embed them in README.md. Never mock them up.
- When a phase is done, add a short "How it works" note or diagram to `docs/`. This is what interviewers will read.

## Definition of done (whole project)

- `docker compose up` (or the documented command) brings up a working demo in one step
- README has a real architecture diagram, setup steps, screenshots or GIFs, and measured results
- CI is green, tests cover the core logic
- A 2-3 minute demo video or GIF is linked in the README

## Status

Only `README.md`, `INFO.md` and this file exist so far. Start at **Phase 1**.
