#!/bin/bash

# into container
# docker exec -it 0765 bash
# login postgres
# psql -h 10.39.6.78 -p 5432 -d postgres -U yuzhe --password

CREATE DATABASE suppliers;
CREATE DATABASE tse;

\c suppliers
\dt

SELECT * FROM vendors;
