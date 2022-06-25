import json
import logging
import os
from concurrent import futures

from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.api_core import exceptions as api_core_exceptions

PROJECT_ID = os.environ.get('PROJECT_ID')
LOCATION = os.environ.get('LOCATION', 'EU')
PROJECTS_TO_SCAN = os.environ.get('PROJECTS_TO_SCAN', [])
BACKUP_PROJECT_ID = os.environ.get('BACKUP_PROJECT_ID')
TOPIC_ID = os.environ.get('TOPIC_ID')

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)

projects_to_scan = [] if PROJECTS_TO_SCAN == [] else PROJECTS_TO_SCAN.split(',')


# Resolve the published future in a separate thread.
def callback(future) -> None:
    message_id = future.result()
    logging.info(f"Message ID: {message_id}")


batch_settings = pubsub_v1.types.BatchSettings(
    max_bytes=2048000,  # One kilobyte
    max_latency=5,  # One second
)

publisher_options = pubsub_v1.types.PublisherOptions(
    flow_control=pubsub_v1.types.PublishFlowControl(
        message_limit=500,
        byte_limit=2 * 1024 * 1024,
        limit_exceeded_behavior=pubsub_v1.types.LimitExceededBehavior.BLOCK,
    ),
)

publisher = pubsub_v1.PublisherClient(batch_settings=batch_settings, publisher_options=publisher_options)
topic_name = 'projects/{project_id}/topics/{topic}'.format(
    project_id=PROJECT_ID,
    topic=TOPIC_ID,
)


def get_client():
    return bigquery.Client(project=PROJECT_ID, location=LOCATION)


def scan_and_send_to_pubsub():
    client = get_client()
    publish_futures = []
    for project_to_scan in projects_to_scan:
        find_all_tables = f"""
            SELECT table_catalog as project_id, table_schema as dataset_id, table_name as table_id
            FROM `{project_to_scan}.region-eu.INFORMATION_SCHEMA.TABLES`
            WHERE table_type = 'BASE TABLE'
        """
        query_job_find_all_tables = client.query(find_all_tables)
        if query_job_find_all_tables.errors:
            raise RuntimeError(query_job_find_all_tables.errors)

        project_id_as_part_of_dataset_id = project_to_scan.replace('-', '_')
        for table_info in query_job_find_all_tables:
            target_dataset_id = f"{project_id_as_part_of_dataset_id}_{table_info['dataset_id']}"
            message_to_send = json.dumps({
                "source_project_id": project_to_scan,
                "source_dataset_id": table_info['dataset_id'],
                "source_table_id": table_info['table_id'],
                "target_project_id": BACKUP_PROJECT_ID,
                "target_dataset_id": target_dataset_id,
                "target_table_id": table_info['table_id']
            })

            publish_future = publisher.publish(topic_name, message_to_send.encode('utf-8'))
            logging.info(f"Sending message: {message_to_send}")
            publish_future.add_done_callback(callback)
            publish_futures.append(publish_future)

    futures.wait(fs=publish_futures, return_when=futures.ALL_COMPLETED)


if __name__ == '__main__':
    scan_and_send_to_pubsub()
