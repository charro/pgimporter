# Create schema in both DBs (source and target)
psql -h localhost -U postgres -f create.sql
psql -h localhost -U postgres -p 5555 -f create.sql
# Insert data
for i in {1..1000}
do
   psql -h localhost -U postgres -f insert.sql
done