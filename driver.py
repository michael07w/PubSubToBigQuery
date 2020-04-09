import argparse
import datetime
import sys

import apache_beam as beam
from apache_beam import window
from apache_beam.options.pipeline_options import PipelineOptions

# Minor modifications made to this class
# Originally written as GroupWindowsIntoBatches here:
# https://cloud.google.com/pubsub/docs/pubsub-dataflow
class GroupWindows(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """

    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub mesage based on its
            # publish timestamp
            | "Window into Fixed Intervals"
            >> beam.WindowInto(window.FixedWindows(self.window_size))
            | "Add timestamps to messages" >> beam.ParDo(AddTimestamps())
        )


# Minor modifications made to this class
# Originally written as AddTimestamps here:
# https://cloud.google.com/pubsub/docs/pubsub-dataflow
class AddTimestamps(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server.
        """

        yield {
            "message_body": element.decode("utf-8"),
            "publish_time": datetime.datetime.utcfromtimestamp(
                float(publish_time)
            ).strftime("%Y-%m-%d %H:%M:%S.%f")
        }


def run(known_args, pipeline_args, window_size=1.0):
    # User must specify following options as command-line args:
    # job_name, project, subscription, runner, staging_location, temp_location

    # specify BQ Dataset and Table to write to; [dataset_id].[table_id]
    table_spec = '{dataset}.{table}'.format(
        dataset=known_args.dataset_id,
        table=known_args.table_id
    )

    # specify BQ Table Schema
    table_schema = 'message_body:STRING, publish_time:STRING'

    # Build Pipeline with passed args
    with beam.Pipeline(options=PipelineOptions(pipeline_args, region='us-west1', streaming=True, save_main_session=True)) as p:
        (
            p
            | "Read PubSub Message" >> beam.io.ReadFromPubSub(subscription=known_args.subscription)
            | "Window into" >> GroupWindows(window_size)
            # write the PCollection generated above into BigQuery
            | "Write to BQ" >> beam.io.WriteToBigQuery(
                table_spec,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        )


if __name__ == '__main__':
    # Get Pub/Sub Subscription to read from as cmd-line arg
    # Using a Sub allows us to retrieve msgs published while pipeline was offline
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subscription",
        help="Cloud Pub/Sub Subscription to use.\n"
        '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION>".'
    )
    parser.add_argument(
        "--dataset_id",
        help="Dataset that holds BQ Table to write to.\n"
    )
    parser.add_argument(
        "--table_id",
        help="Name of BQ Table to write to.\n"
    )
    known_args, pipeline_args = parser.parse_known_args()

    run(known_args, pipeline_args)
