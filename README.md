# Description
This repo can be used to deminstate and test apache beam functions for data extract transformation and loading.  In particular it makes use of
data extract and loading from and to a postgresql database by implementing data sync and source classes. You can use docker to set up
an arrow and target database to read from and write to, generate data and then write your beam functions to be implemented

# Environment
Clone the repo and navigate to root.

create a python virtual environment `python3 -m venv/bin/activate;source venv/bin/activate`

Run `docker compuser up --build` to spin up the databases

add a .env file to the root with the creds for connecting to the arrow and target databases from your local machine.
Should be similar to the following:

```
arrow_database=arrow_db
arrow_password=postgres
arrow_user=postgres
arrow_port=5433
arrow_host=localhost

target_database=target_db
target_password=postgres
target_user=postgres
target_port=5434
target_host=localhost
```
