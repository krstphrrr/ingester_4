## INGESTER v4: leverages mdbtools to extract mdb/accdb

### TODO:
- [x] alpine configuration
- [ ] basic table class
- [ ] mdb-table instead of pyodbc table list 
- [ ] single table function with switch case for each
- [ ] directory management function

### to run the application

1. Build the docker image using the docker-compose engine
```
docker compose build
```
2. Run the interactive python container
```
docker compose run --rm ingester-local
```