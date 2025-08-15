FROM python:3.8-slim

# Install PostgreSQL 17 client to match your server version
RUN apt-get update && apt-get install -y wget gnupg \
    && echo "deb http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
    && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && apt-get update \
    && apt-get install -y postgresql-client-17 \
    && rm -rf /var/lib/apt/lists/*

COPY elt_script.py .

CMD ["python", "elt_script.py" ]
