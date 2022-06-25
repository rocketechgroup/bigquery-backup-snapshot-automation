import json
import logging
import os
import base64

from google.cloud import bigquery
from google.api_core import exceptions as api_core_exceptions

PROJECT_ID = os.environ.get('PROJECT_ID')
LOCATION = os.environ.get('LOCATION', 'EU')

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)


def get_client():
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def dataset_exists(project_id, dataset_id):
    try:
        get_client().get_dataset(f"{project_id}.{dataset_id}")
    except api_core_exceptions.NotFound:
        return False
    return True


def create_dataset(project_id, dataset_id):
    client = get_client()
    dataset = bigquery.Dataset(dataset_ref=f"{project_id}.{dataset_id}")
    dataset.location = LOCATION
    dataset = client.create_dataset(dataset=dataset, exists_ok=True, timeout=30)
    logging.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))


def build_backup_table_query(
        source_project_id,
        source_dataset_id,
        source_table_id,
        target_project_id,
        target_dataset_id,
        target_table_id
):
    back_table_query = f"""
                    -- Declare variables
                    DECLARE snapshot_name STRING;
                    DECLARE expiration TIMESTAMP;
                    DECLARE query STRING;

                    -- Set variables
                    SET expiration = DATE_ADD(current_timestamp(), INTERVAL 1 DAY);
                    SET snapshot_name = CONCAT(
                                          "{target_project_id}.{target_dataset_id}.{target_table_id}_",
                                          FORMAT_DATETIME('%Y%m%d', current_date()));

                    -- Construct the query to create the snapshot
                    SET query = CONCAT(
                                  "CREATE SNAPSHOT TABLE ",
                                  snapshot_name,
                                  " CLONE {source_project_id}.{source_dataset_id}.{source_table_id} OPTIONS(expiration_timestamp = TIMESTAMP '",
                                  expiration,
                                  "');");

                    -- Run the query
                    EXECUTE IMMEDIATE query;
                    """
    return back_table_query


def backup_table(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
        Args:
             event (dict):  The dictionary with data specific to this type of
                            event. The `@type` field maps to
                             `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
                            The `data` field maps to the PubsubMessage data
                            in a base64-encoded string. The `attributes` field maps
                            to the PubsubMessage attributes if any is present.
             context (google.cloud.functions.Context): Metadata of triggering event
                            including `event_id` which maps to the PubsubMessage
                            messageId, `timestamp` which maps to the PubsubMessage
                            publishTime, `event_type` which maps to
                            `google.pubsub.topic.publish`, and `resource` which is
                            a dictionary that describes the service API endpoint
                            pubsub.googleapis.com, the triggering topic's name, and
                            the triggering event type
                            `type.googleapis.com/google.pubsub.v1.PubsubMessage`.
        Returns:
            None. The output is written to Cloud Logging.
        """

    if 'data' in event:
        event = base64.b64decode(event['data']).decode('utf-8')
    else:
        raise RuntimeError('Message contains no data')

    message = json.loads(event)

    if not dataset_exists(project_id=message['target_project_id'], dataset_id=message['target_dataset_id']):
        create_dataset(project_id=message['target_project_id'], dataset_id=message['target_dataset_id'])

    backup = build_backup_table_query(
        source_project_id=message['source_project_id'],
        source_dataset_id=message['source_dataset_id'],
        source_table_id=message['source_table_id'],
        target_project_id=message['target_project_id'],
        target_dataset_id=message['target_dataset_id'],
        target_table_id=message['target_table_id']
    )

    try:
        query_job_backup = get_client().query(backup)
        if query_job_backup.errors:
            raise RuntimeError(query_job_backup.errors)
        query_job_backup.result()
        logging.info(
            f"Backed up table {message['source_project_id']}.{message['source_dataset_id']}.{message['source_table_id']} "
            f"to {message['target_project_id']}.{message['target_project_id']}.{message['target_table_id']}"
        )
    except api_core_exceptions.BadRequest as e:

        if 'Already Exists' in e.message:
            """
            Target backup snapshot already exists
            """
            logging.warning(e.message)
        elif 'Access Denied' in e.message:
            """
            The overwrite behaviour will try to remove the snapshot, 
            but permission isn't given because it's dangerous, so ignore is the correct behaviour
            """
            logging.warning(e.message)
        else:
            raise e
