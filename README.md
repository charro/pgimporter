# PostgreSQL Data Importer

## Interactive tool to import one or more tables from a source DB to a target DB

Command line tool intended mainly for developers that need to import some data from a source DB (usually a production or development DB) to a target DB (usually your own local development DB).

Please note that this isn't a migration tool to be used to move data between production Databases, if you need something reliable to make backups or migrations between production DBs you should look for a different kind of tool (most likely PostgreSQL official tools).

## HOW TO BUILD

The tool is developed in Rust, so you just need to [intall Rust in your machine](https://www.rust-lang.org/tools/install)

Once you've done it, just use cargo to make your own executable:

```
    cargo build --release
```

## HOW TO USE IT

This is an interactive tool, so just run the executable and it will guide you through the import process.
By default, the tool is configured to connect only to local source and target DBs in ports `5432` and `5555`, but you can override all basic connection properties (host/port/user/pass) via environment variables to connect to any source and target DBs you need. Run the executable with any parameter to see the available env vars:

```
    pgimporter help
```

Note that the schemas should be the same in source and target DBs (or at least all columns in source DB imported tables must exist in target DB tables), or the import will fail.

### Example

Start source and target DBs, using docker, we'll use docker images for this example (You need to [install Docker](https://docs.docker.com/get-docker/) first to do the same):

```
docker run -d -p 5432:5432 postgres:10.9
docker run -d -p 5555:5432 postgres:10.9
```

We create two tables (table1 and table2) in source and target DB:
```
CREATE TABLE table1(
	id serial,
	some_text TEXT,
	a_number INT4
);

CREATE TABLE table2(
	id serial,
	more_text TEXT,
	float_number DECIMAL
);
```

And then we populate both tables (only in source DB) with 20.000 rows of mock data.

Then you can run the pgimporter and choose to import both tables to target DB as easy and quick as this:

```
> ./pgimporter
Postgres Data Importer - v0.1.0

Checking DB connections...
Checking Postgres server localhost:5432...     OK
Checking Postgres server localhost:5555...     OK
Choose an Schema: public
Choose tables to import:
  [x] table2
> [x] table1

WHERE: [Optional]: 
Importing table public.table2 ...
20000 rows to insert in total
Finished importing 20000 rows from table public.table2 in 0 secs
Importing table public.table1 ...
20000 rows to insert in total
Finished importing 20000 rows from table public.table1 in 0 secs

```
As you see, you can choose only one schema at a time and then all the tables you need to import from it.

### Import only a subset of the table's rows
In case you only need to import part of the table(s), you can optionally use a custom WHERE clause like:

```
Postgres Data Importer - v0.1.0

Checking DB connections...
Checking Postgres server localhost:5432...     OK
Checking Postgres server localhost:5555...     OK
Choose an Schema: public
Choose tables to import: table2
WHERE: [Optional]: other_number > 500000
Importing table public.table2 ...
19003 rows to insert in total
Finished importing 19003 rows from table public.table2 in 0 secs

```

Note that as you can only specify one WHERE each time, it will be used to all tables you've selected, so make sure it will apply on each of those tables columns.