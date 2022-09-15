import os
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def get_db_creds(prefix):
    """function to get basic database creds"""
    return {
        x: os.getenv(f"{prefix}_{x}")
        for x in "database user host port password".split()
    }


ARROW_DATABASE_CREDS = get_db_creds("arrow")
TARGET_DATABASE_CREDS = get_db_creds("target")
NUM_DIRECT_WORKERS = 4


if __name__ == "__main__":
    print(ARROW_DATABASE_CREDS)
