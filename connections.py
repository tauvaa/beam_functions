from config import ARROW_DATABASE_CREDS
import psycopg2 as psy


class Connector:
    """
    base connector class
        creds: dict of database connection creds
    """

    def __init__(self, creds):
        self.creds = creds
        self.connection: psy.extensions.connection

    def run_read_query(self, query, params=None, with_cols=False):
        """
        Method for running read queries, returns a generator.
        params:
            query:
                query to run
            params:
                params to provide to query
            with_cols:
                if true will return gen or dicts with keys as columns of
                query, and values as the column values.  Otherwise will return
                a list of tuples of values from query
        """
        params = params or ()
        cur: psy.extensions.cursor = self.connection.cursor()
        cur.execute(query, params)
        cols = [x.name for x in cur.description]
        to_yield = cur.fetchone()
        while to_yield:
            if with_cols:
                to_yield = dict(zip(cols, to_yield))
            yield to_yield
            to_yield = cur.fetchone()

        data = cur.fetchall()
        if with_cols:
            data = map(lambda x: dict(zip(cols, x)), data)

    def run_commit_query(self, query, params=None, multi_params=False):
        """Method for running commit queries against the database."""
        params = params or ()
        cur = self.connection.cursor()
        if multi_params:
            cur.executemany(query, params)
        else:
            cur.execute(query, params)
        self.connection.commit()

    def __enter__(self):
        self.connection: psy.extensions.connection = psy.connect(
            **ARROW_DATABASE_CREDS
        )
        return self

    def __exit__(self, *args, **kwargs):
        self.connection.close()


def run_read_query(query, params=None, with_cols=False, creds=None):
    params = params or ()
    creds = creds or ARROW_DATABASE_CREDS
    con = psy.connect(**ARROW_DATABASE_CREDS)
    try:
        cur = con.cursor()
        cur.execute(query, params)
        cols = [x.name for x in cur.description]

        data = cur.fetchall()
        if with_cols:
            data = map(lambda x: dict(zip(cols, x)), data)
        return list(data)

    finally:
        con.close()


if __name__ == "__main__":
    with Connector(ARROW_DATABASE_CREDS) as conn:
        create_query = """create table if not exists 
        test_table(
            id serial primary key,
            first_name varchar(100)
            )
        """
        conn.run_commit_query(create_query)
        query = """select count(*) from test_table"""
        gen = conn.run_read_query(query, with_cols=True)
        for g in gen:
            print(g)
