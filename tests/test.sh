# Create schema in both DBs (source and target)
PGPASSWORD=postgres psql -h localhost -U postgres -f create.sql
PGPASSWORD=postgres psql -h localhost -U postgres -p 5555 -f create.sql
# Insert data
for i in {1..1000}
do
   PGPASSWORD=postgres psql -h localhost -U postgres -f insert.sql -o /dev/null
done