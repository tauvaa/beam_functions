import apache_beam as beam

import beam_utils.example_function
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
        data = pipeline | "Read" >> beam.io.Read(
            ReadPostgres(
                query,
                "arrow_table",
                "id",
                ARROW_DATABASE_CREDS,
            )
        )
        data | beam.ParDo(
            WritePostgres(
                TARGET_DATABASE_CREDS,
                """insert into target_table values(%s, %s)""",
            )
        )
        data | beam_utils.example_function.extra_pipe()

if __name__ == "__main__":
    main()
