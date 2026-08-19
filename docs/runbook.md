# Cloud execution runbook

The repository code and local evidence are prepared. The following steps require your accounts,
credentials, and Databricks workspace permissions.

## 1. Local preparation

```powershell
python -m pip install -e ".[producer,dev]"
python scripts/prepare_mvp_data.py --num-trades 10000 --use-sample-fx
pytest -q
```

For an updated TMX extract, download the issuer CSV from the TMX directory and run:

```powershell
python scripts/prepare_mvp_data.py `
  --num-trades 10000 `
  --event-date 2026-08-11 `
  --tmx-input C:\path\to\tmx-issuers.csv
```

Keep the untouched TMX download under `data/raw/tmx/`. The project-generated instrument ID,
asset class, quote currency, effective date, and active flag must remain identified as synthetic.

## 2. Confluent setup

You must perform these account actions:

1. Create a basic Kafka cluster.
2. Create topic `mapletrade.trade-events.v1` with three partitions and 3–7 day retention.
3. Create a Kafka API key and secret scoped to the cluster/topic.
4. Create a Schema Registry API key and secret.
5. Set value-subject compatibility to `BACKWARD`.
6. Export the six credentials/endpoints shown in `.env.example`.
7. Publish the generated file with `producer/kafka_producer.py`.

Record the final producer message count. Do not put keys, secrets, or screenshots containing them
in Git.

## 3. Databricks setup

1. Add the Git repository as a Databricks Git folder.
2. Select a Unity Catalog-enabled workspace and compute that can reach Confluent.
3. Confirm permission to create the `mapletrade_dev` catalog. If not, ask an administrator to
   create it and grant `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, and `CREATE VOLUME` as needed.
4. Run `databricks/00_setup_unity_catalog.py` once.
5. Upload the contents of `data/upload/reference/` to:

   ```text
   /Volumes/mapletrade_dev/reference/raw_files/
   ```

6. Run `databricks/01_load_reference_data.py` and verify all three manifest rows are `SUCCESS`.

## 4. Secrets

Create secret scope `mapletrade` and add:

```text
kafka-api-key
kafka-api-secret
```

The transformation notebook uses the checked-in Avro schema, so Schema Registry credentials are
required by the producer but not exposed to the Databricks consumer.

## 5. Choose an execution mode

### Full Kafka mode

Use `source_mode=kafka`, provide the Confluent bootstrap server, and publish the events before or
during the Workflow run.

### Replay fallback

Upload `data/upload/replay/trade_events.jsonl` into:

```text
/Volumes/mapletrade_dev/ops/pipeline_state/replay/
```

Use `source_mode=replay`. Use a new subdirectory for a newly generated replay because the file
source checkpoint intentionally does not process the same file twice.

The README and résumé evidence must disclose replay mode if Kafka-to-Databricks was not executed.

## 6. Workflow

Replace these placeholders in `databricks/workflow.template.json`:

- `<YOUR_CLUSTER_ID>`
- `<REPO_WORKSPACE_PATH>`
- `<CONFLUENT_BOOTSTRAP_SERVERS>`

Create the job through the Databricks UI/API. Confirm the dependency:

```text
ingest_and_transform → build_gold_and_reconcile
```

Run the Workflow and retain the run ID.

## 7. Acceptance checks

The run is acceptable only when:

- Bronze contains the expected number of source messages;
- every outcome category expected from failure injection is present;
- Current contains both `BOOKED` and `CANCELLED` states;
- all reconciliation rows for the run are `PASS`;
- rerunning with the same checkpoints does not increase outcome counts; and
- no secrets appear in the repository or notebook output.

## 8. Restart test

1. Record Bronze, outcome, and Current counts.
2. Rerun Task 1 with the same checkpoint and no new source data.
3. Confirm all three counts remain unchanged.
4. Publish a small new event batch and rerun.
5. Confirm only the new messages and valid higher versions are reflected.

## Troubleshooting

| Symptom | Check |
|---|---|
| Kafka connection timeout | Workspace outbound access, bootstrap hostname/port, SASL_SSL options |
| Authentication failure | Secret scope names, Kafka API key scope, accidental whitespace |
| Avro deserialization failures | Producer schema, Confluent five-byte wire header, checked-in schema version |
| Missing FX rates | Event date exists in normalized calendar file; inspect carry-forward rows |
| `MERGE` duplicate-match error | Confirm one candidate per `trade_id` after micro-batch sequencing |
| Reconciliation failure | Query the named outcome view and compare the failed check's source keys |

