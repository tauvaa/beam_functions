import random
import string

import apache_beam as beam

from beam_utils.beam_functions import WritePostgres
from config import ARROW_DATABASE_CREDS, NUM_DIRECT_WORKERS


def get_random_string(num):
    return "".join([random.choice(string.ascii_letters) for _ in range(num)])


def write_data(num):

    write_query = """insert into arrow_table(first_name) values (%s)"""
    poptions = beam.pipeline.PipelineOptions(
        [
            "--direct_running_mode=multi_processing",
            f"--direct_num_workers={NUM_DIRECT_WORKERS}",
        ]
    )
    to_write = [get_random_string(5) for _ in range(num)]

    with beam.pipeline.Pipeline(options=poptions) as pipe:
        p = pipe | beam.Create(to_write) | beam.Map(lambda x: (x,))
        p | beam.ParDo(WritePostgres(ARROW_DATABASE_CREDS, write_query))


if __name__ == "__main__":
    write_data(100000)
