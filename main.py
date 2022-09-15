from beam_utils.beam_functions import ReadPostgres, WritePostgres
import apache_beam as beam
from config import (
    ARROW_DATABASE_CREDS,
    NUM_DIRECT_WORKERS,
    TARGET_DATABASE_CREDS,
)


if __name__ == "__main__":

    query = """
    select * from
    arrow_table
    where
    id >= %(lower_bound)s
    and id < %(upper_bound)s
    """
    poptions = beam.pipeline.PipelineOptions(
        [
            "--direct_running_mode=multi_processing",
            f"--direct_num_workers={NUM_DIRECT_WORKERS}",
        ]
    )

    with beam.Pipeline(options=poptions) as pipeline:
        numbers = (
            pipeline
            | "Read"
            >> beam.io.Read(
                ReadPostgres(
                    query,
                    "arrow_table",
                    "id",
                    ARROW_DATABASE_CREDS,
                )
            )
            | beam.ParDo(
                WritePostgres(
                    TARGET_DATABASE_CREDS,
                    """insert into target_table values(%s, %s)""",
                )
            )
        )
