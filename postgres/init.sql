CREATE USER postgres;
ALTER ROLE postgres WITH PASSWORD 'password';
CREATE DATABASE permits;
GRANT ALL PRIVELEGES ON DATABASE permits to postgres;