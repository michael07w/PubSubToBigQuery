# PubSubToBigQuery

Sets up a pipeline run by Google's Dataflow to stream messages from Google's Pub/Sub service and append messages
to a BigQuery table.

Google dataset_id and table_id must be provided. The table can be an existing BQ table, or not yet exist. 
  If the table exists, data will be appended.
  If table does not exist, a new table will be generated with the table_id provided.

GCS account must have an existing Pub/Sub subscription to bind to the pipeline, as well as GCS buckets to store staging/temp files
generated by the pipeline.

Package dependecies include:
  apache_beam[gcp] -- can be installed with following command: 
  pip install apache_beam[gcp]

Example Command (all cmd-line args are required):
python driver.py --project={YOUR_PROJECT_ID} \
--subscription=projects/{YOUR_PROJECT_ID}/subscriptions/{SUBSCRIPTION_NAME} \
--dataset_id={DATASET_ID} \
--table_id={TABLE_ID} \
--job_name=dataflowjob \
--temp_location=gs://{GCS_BUCKET_NAME}/ \
--staging_location=gs://{GCS_BUCKET_NAME}/ \
--runner=DataflowRunner
