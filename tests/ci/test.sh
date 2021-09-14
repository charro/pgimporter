# Create schema in both DBs (source and target)
echo "Creating schema in source DB $1:$2"
PGPASSWORD=postgres psql -h $1 -p $2 -U postgres -f create.sql
echo "Creating schema in target DB $3:$4"
PGPASSWORD=postgres psql -h $3 -p $4 -U postgres -f create.sql
# Insert data
echo "Inserting data in source DB..."
PGPASSWORD=postgres psql -h $1 -p $2 -U postgres -f insert.sql -o /dev/null
