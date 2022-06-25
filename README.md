# bigquery-backup-snapshot-automation
Automate the backup process using BigQuery Snapshot

## Scan

### Creating the pubsub topic
```
gcloud pubsub topics create tables_to_be_backedup
```

## Trigger
### Deploy the cloud function
> Run this under the `trigger` folder
```
export PROJECT_ID=rocketech-de-pgcp-sandbox
gcloud functions deploy backup_table \
--runtime python38 \
--region europe-west2 \
--trigger-topic tables_to_be_backedup \
--set-env-vars PROJECT_ID=${PROJECT_ID}
```