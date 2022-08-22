# PostgreSQL Data Importer

## Tool to import one or more tables from a source DB to a target DB

Command line tool intended mainly for developers that need to import some data from a source DB (usually a production or development DB) to a target DB (usually your own local development DB).

Please note that **this isn't a migration tool to be used to move data between production Databases**, if you need something reliable to make backups or migrations between production DBs you should look for a different kind of tool (most likely PostgreSQL official tools).

The tool is intended to give you quick access to your source DB (maybe your production DB, your pre-production DB...), and let you explore all schemas and tables, selecting those that you want to import to the target DB, so you don't need to know beforehand the exact names of what you're looking for.

It also has a very low use of resources:

- 0 bytes of disk space, no files are created, all work is done directly into a memory buffer, so data goes from source DB to buffer
 and then from buffer to Target DB.

- Tiny executable size. Thanks to the awesomeness of the Rust compiler, everything fits in around 5MB  

- Low memory footprint. It grows with the number and type sizes of the columns of the table you're importing, but for tables with a regular number of columns (less than 100 columns), the total amount of memory while importing each table (independentlyof the number of rows) could be as low as 5-20 MB

- Very Fast paralell import: Rows to import are divided by the number of Threads you decide (8 by default) and imported concurrently.
  Again, it depends a lot of how many columns you have in your table, but for not really wide tables you can expect performances of millions of rows/min.

## HOW TO BUILD

The tool is developed in Rust, so you just need to [intall Rust in your machine](https://www.rust-lang.org/tools/install)

Once you've done that, just use cargo to make your own executable:

```bash
    cargo build --release
```

## INTERACTIVE MODE

In interactive mode, just run the executable providing the source and target DB urls and the tool will guide you through the import process, checking first all available schemas in the default DB for the specified user (support to connect to any database of that user will be added in the future).

By default, the tool is configured to connect only to local source and target DBs in ports `5432` (source) and (target) `5555`, but you can override all basic connection properties (host/port/user/pass/dbname) via command line params to connect to any source and target DBs you need. 

Run the executable with help params (--help | -h) to see the available params (you could use also env vars) and the current configuration:

```bash
    pgimporter --help  |  pgimporter -h
```

A typical usage would be something like this, providing the source and target connection URLs:

```bash
    pgimporter -s user:secret@host:port/dbname -t user:secret@host:port/dbname
```

Note that the same schemas and tables that you're importing must exist previously in both source and target DBs (or at least all columns in source DB imported tables must exist in target DB tables and to have the same name and type). Otherwise the import will fail.

### Example

Let's make an example creating both source and target DBs, using docker, as we'll use docker images for this example (You need to [install Docker](https://docs.docker.com/get-docker/) in case you didn't):

```bash
docker run -d -p 5432:5432 postgres:10.9
docker run -d -p 5555:5432 postgres:10.9
```

Then access both source and target DB and create two tables (table1 and table2):
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

And then populate both tables (only in source DB) with, let's say 20.000 rows of mock data 
(or with whatever amount you want for testing).

Then you can run the pgimporter and choose to import both tables to target DB as easy and quick as this:

```bash
> ./pgimporter

Checking DB connections...
Checking Postgres server localhost:5432...     OK
Checking Postgres server localhost:5555...     OK
✔ Choose an Schema · public
✔ Choose tables to import · table1, table2
✔ WHERE: [Optional] · 
✔ Do you want to TRUNCATE selected tables in target DB [localhost:5555] ? (WARNING: ALL DATA WILL BE LOST!) · yes

Importing table public.table2 ...
20000 rows to insert in total
Finished importing 20000 rows from table public.table2 in 0 secs
Importing table public.table1 ...
20000 rows to insert in total
Finished importing 20000 rows from table public.table1 in 0 secs

```
As you see, you can choose only one schema at a time and then all the tables you need to import from it.

### Import only a subset of the table's rows
In case you only need to import part of the table(s), you can optionally use a custom WHERE clause like this:
We're fetching only those rows that have `other_number` column with a value greater than 500.000: 

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

Note that as you can only specify one WHERE each time, it will be used to all tables you've selected, so make sure that will apply on each of those tables columns.

## BATCH MODE

You can also define your own import batch job, including as many schemas and tables you want, as well as the rest of values for all parameters supported in the interactive mode (WHERE, truncate...)

To run the importer in batch mode, you only need to pass as a parameter (check the --help to know how) a YAML file with all the imports you need, following the structure of the example below.

Let's see an example, importing the same tables we created previoulsy to test the interactive mode:

### Example
The connection parameters will be read from the command line params or env vars, same as in interactive mode.

Let's see an example, importing the same tables we created previoulsy for the interactive mode into a file named `test.yml`:

```yaml
imports:
    - schema: public
      tables:
        - table1
        - table2
```

We could instead split the work in two jobs if we wanted for instance to treat each table in a different way:

```yaml
imports:
    - schema: public
      tables:
        - table1
      
    - schema: public
      tables:
        - table2
      where_clause:  some_text  = 'MY TEXT'
      truncate: true
      cascade: true
```

And then run (using default local DBs as in previous examples)

```
	pgimporter --batch-filename test.yml
```
