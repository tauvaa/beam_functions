import apache_beam as beam

from apache_beam.io import iobase
from apache_beam.io.range_trackers import (
    OffsetRangeTracker,
    OrderedPositionRangeTracker,
)

from config import ARROW_DATABASE_CREDS, TARGET_DATABASE_CREDS
from connections import Connector


class ReadPostgres(beam.PTransform):
    def __init__(self, query, table_name, column_name, creds, params=None):
        super().__init__()

        self.bounded_source = _ReadPostgres(
            query, table_name, column_name, creds, params
        )

    def expand(self, pcoll):
        return pcoll | iobase.Read(self.bounded_source)


class _ReadPostgres(iobase.BoundedSource):
    def __init__(self, query, table_name, column_name, creds, params):
        super().__init__()
        self.query = query
        self.creds = creds
        self.column_name = column_name
        self.table_name = table_name
        self.params = params

    def estimate_size(self):
        with Connector(self.creds) as connector:
            table_size_query = f"""
            select pg_relation_size('{self.table_name}')
            """
            to_ret = connector.run_read_query(table_size_query)
            to_ret = list(to_ret)[0][0]

        return to_ret

    def get_range_tracker(self, start_position, stop_position):
        start_position = start_position or 0
        stop_position = stop_position or 0

        return OffsetRangeTracker(start_position, stop_position)

    def split(
        self, desired_bundle_size, start_position=None, stop_position=None
    ):
        with Connector(self.creds) as connector:
            query = f"""
            select max({self.column_name}), min({self.column_name})
            from {self.table_name}
            """
            table_size_query = f"""
            select pg_relation_size('{self.table_name}')
            """
            max_id, min_id = list(connector.run_read_query(query))[0]
            table_size = list(connector.run_read_query(table_size_query))[0][0]

        total_rows = max_id - min_id
        rows_per_bundle = (desired_bundle_size / table_size) * total_rows
        rows_per_bundle = int(rows_per_bundle)
        print("rows per bundle", rows_per_bundle)
        start_position = min_id
        stop_position = min_id + rows_per_bundle
        while stop_position <= max_id + rows_per_bundle:
            yield iobase.SourceBundle(
                weight=1,
                source=self,
                start_position=start_position,
                stop_position=stop_position,
            )
            start_position = stop_position
            stop_position += rows_per_bundle

    def read(self, range_tracker):
        lower_bound = range_tracker.start_position()

        upper_bound = range_tracker.stop_position()

        params = {"lower_bound": lower_bound, "upper_bound": upper_bound}
        if self.params is not None:
            params = {**params, **self.params}

        with Connector(self.creds) as connector:
            for d in connector.run_read_query(self.query, params):
                yield d


class WritePostgres(beam.DoFn):
    def __init__(self, db_creds, query, *unused_args, **unused_kwargs):
        self.db_creds = db_creds
        self.batch = []
        self.batch_size = 10000
        self.query = query

    def process(self, element, *args, **kwargs):
        self.batch.append(element)
        if len(self.batch) > self.batch_size:
            self._flush()

    def finish_bundle(self):
        self._flush()

    def _flush(self):
        with Connector(self.db_creds) as connect:
            connect.run_commit_query(self.query, self.batch, multi_params=True)
        print("batch ran")

        self.batch = []


if __name__ == "__main__":
    query = """
    select * from
    arrow_table
    where
    id >= %(lower_bound)s
    and id < %(upper_bound)s
    and id > %(fake_id)s
    """
    poptions = beam.pipeline.PipelineOptions([])

    with beam.Pipeline(options=poptions) as pipeline:
        numbers = (
            pipeline
            | "read_stuff"
            >> ReadPostgres(
                query,
                "arrow_table",
                "id",
                ARROW_DATABASE_CREDS,
                params={"fake_id": 299000},
            )
            | beam.ParDo(
                WritePostgres(
                    TARGET_DATABASE_CREDS,
                    query="insert into target_table values(%s, %s)",
                )
            )
        )
