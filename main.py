from beam_utils.beam_functions import ReadPostgres, WritePostgres
import apache_beam as beam
from config import ARROW_DATABASE_CREDS, NUM_DIRECT_WORKERS


if __name__ == "__main__":

    query = """
    select * from
    test_table
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
            | "ProduceNumbers"
            >> beam.io.Read(
                ReadPostgres(
                    query,
                    "test_table",
                    "id",
                    ARROW_DATABASE_CREDS,
                )
            )
            # | beam.GroupBy(lambda s: s[1][0:2])
            # | beam.Map(lambda x: (x[0], len(x[1])))
            # | beam.Filter(lambda x: x[0] == "Gc")
            | beam.ParDo(WritePostgres(ARROW_DATABASE_CREDS, """insert into test_table1 values(%s, %s)"""))
        )
