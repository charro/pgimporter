# PostgreSQL Data Importer

## Tool to import one or more tables from a source DB to a target DB

Command line tool intended mainly for developers that need to import some data from a source DB (usually a production or development DB) to a target DB (usually your own local development DB).

Please note that **this isn't a migration tool to be used to move data between production Databases**, if you need something reliable to make backups or migrations between production DBs you should look for a different kind of tool (most likely PostgreSQL official tools).

The tool is intended to give you quick access to your source DB, and let you explore all schemas and tables (in the interactive mode), selecting those that you want to import to the target DB, so you don't need to know the exact names of what you're looking for beforehand.

It also has a very low use of resources:

- 0 bytes of disk space, no files are created, all work is done directly in memory

- Low memory footprint. It grows with the number of columns of the table you're importing, but for tables with a regular number of columns (less than 100 columns), the total amount of memory for the whole table (even if there are millions of rows) could be as low as 5-20 MB

- Fast paralell import: Rows to import are divided by the number of Threads you decide (8 by default) and imported concurrently.
  Again, it depends a lot of how many columns you have in your table, but for small tables you can expect performances of around 1M rows/min fetching data from a remote DB.

## HOW TO BUILD

The tool is developed in Rust, so you just need to [intall Rust in your machine](https://www.rust-lang.org/tools/install)

Once you've done it, just use cargo to make your own executable:

```bash
    cargo build --release
```

## INTERACTIVE MODE

In interactive mode, you just run the executable without any parameter and the tool will guide you through the import process, checking first all available schemas in the default DB for the specified user (support to connect to any database of that user will be added in the future).

By default, the tool is configured to connect only to local source and target DBs in ports `5432` and `5555`, but you can override all basic connection properties (host/port/user/pass) via environment variables to connect to any source and target DBs you need. 

Run the executable with help params (--help | -h) to see the available env vars and the current configuration:

```bash
    pgimporter --help
```

```bash
    pgimporter -h
```

Note that the schemas should be the same in source and target DBs (or at least all columns in source DB imported tables must exist in target DB tables with the same name and type), or the import will fail.

### Example

Start source and target DBs, using docker, we'll use docker images for this example (You need to [install Docker](https://docs.docker.com/get-docker/) first to do the same):

```bash
docker run -d -p 5432:5432 postgres:10.9
docker run -d -p 5555:5432 postgres:10.9
```

We create two tables (table1 and table2) in source and target DB:
```sql
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

```bash
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

```bash
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

## BATCH MODE

You can also define your own import batch job, including as many schemas and tables you want, as well as the rest of values for all parameters supported in the interactive mode (WHERE, truncate...)

To run the importer in batch mode, you only need to pass as a parameter a YAML file with all the imports you need, following the structure of the example below.

Let's see an example, importing the same tables we created previoulsy to test the interactive mode:

### Example
The connection parameters will be read from the env vars, same as in interactive mode.

Let's see an example, importing the same tables we created previoulsy for the interactive mode into a file named `test.yml`:

```yaml
imports:
    - schema: public
      tables:
        - table1
        - table2
      where_clause:
      truncate: false
```

We could instead split the work in two jobs if we wanted for instance to treat each table in a different way:

```yaml
imports:
    - schema: public
      tables:
        - table1
      where_clause:
      truncate: false
      
    - schema: public
      tables:
        - table2
      where_clause:  some_text  = 'MY TEXT'
      truncate: true
```

And then run

```
	pgimporter test.yml
```