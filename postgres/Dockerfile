# docker build -t permits-data:postgres_db ./postgres \
# && docker run --rm --name postgres_db -d -p 5432:5432 \
# -v $PWD/postgres/data:/var/lib/postgresql/data permits-data:postgres_db

FROM postgres:11

ENV POSTGRES_USER postgres
ENV POSTGRES_PASSWORD password
ENV POSTGRES_DB permits

COPY *.sql /docker-entrypoint-initdb.d/

EXPOSE 5432