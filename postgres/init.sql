CREATE USER postgres;
ALTER ROLE postgres WITH PASSWORD 'password';
CREATE DATABASE permits;
GRANT ALL PRIVELEGES ON DATABASE permits to postgres;

CREATE TABLE permits_raw (
    "Assessor Book" INT PRIMARY KEY
    "Assessor Page" INT
    "Zip Code" INT
)