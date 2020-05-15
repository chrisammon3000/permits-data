# docker build -t permits-data:postgres_db ./postgres \
# && docker run --rm --name postgres_db -d -p 5432:5432 \
# -v $PWD/postgres/pgdata:/var/lib/postgresql/data permits-data:postgres_db

FROM postgres:11

# Avoid conflicts with SQL statements in init files
ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD password
ENV POSTGRES_DB permits

COPY *.sql /docker-entrypoint-initdb.d/

RUN chmod a+r /docker-entrypoint-initdb.d/*

EXPOSE 5432