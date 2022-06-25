# bigquery-backup-snapshot-automation

Automate the backup process using BigQuery Snapshot

## Scan

The [scan/main.py](scan/main.py) uses BigQuery INFORMATION_SCHEMA metadata tables to find all tables (base tables)
requires to be backed up.
You can use one or multiple projects depending on where your source data are stored, and it will automatically find all
tables belongs to a single region (or multi regional region). 
The convention of the backup dataset name will include the source project name to avoid conflict.

All tables will be sent in the form of a single message to a Pub/Sub topic containing information of the table to be
backed up.

### Creating the pubsub topic for backup subscriber

```
gcloud pubsub topics create tables_to_be_backedup
```

### Creating the pubsub topic for the scan on a schedule

```
gcloud pubsub topics create scheduled_scan_for_backup
```

### Deploy the Scan

> Run this under the `scan` folder

```
export PROJECT_ID=YOUR_GCP_PROJECT_ID_TO_RUN_CLOUD_FUNCTION
export BACKUP_PROJECT_ID=YOUR_PROJECT_ID_WHERE_THE_BACKUPS_GO
export PROJECTS_TO_SCAN=PROJECTS_YOUR_WANT_TO_SCAN
export TOPIC_ID=YOUR_PUBSUB_TOPIC_TO_SEND_TABLES_TO_BE_BACKED_UP

gcloud functions deploy scan_and_send_to_pubsub \
--runtime python38 \
--region europe-west2 \
--trigger-topic scheduled_scan_for_backup \
--set-env-vars PROJECT_ID=${PROJECT_ID},BACKUP_PROJECT_ID=${BACKUP_PROJECT_ID},PROJECTS_TO_SCAN=${PROJECTS_TO_SCAN},TOPIC_ID=${TOPIC_ID}
```

## Trigger

The [trigger/main.py](trigger/main.py) python component is the subscriber part used to do the back of individual
BigQuery
tables passed from the scan. This is very powerful because backups of all tables can be handled concurrently and greatly
speeds up the backup process.

Error handling in this part is important because failing for a bad reason could result in retrying for a very long
period of time and could lead to unexpected behaviours.

### Deploy the cloud function

> Run this under the `trigger` folder

```
export PROJECT_ID=YOUR_GCP_PROJECT_ID_TO_RUN_CLOUD_FUNCTION

gcloud functions deploy backup_table \
--runtime python38 \
--region europe-west2 \
--trigger-topic tables_to_be_backedup \
--set-env-vars PROJECT_ID=${PROJECT_ID} \
--retry
```