import apache_beam as beam

from beam_utils.beam_functions import ReadPostgres, WritePostgres
from config import (ARROW_DATABASE_CREDS, NUM_DIRECT_WORKERS,
                    TARGET_DATABASE_CREDS)


def main():
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
        (
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


if __name__ == "__main__":
    main()
