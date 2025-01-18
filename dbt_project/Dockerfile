FROM --platform=linux/x86_64 ghcr.io/dbt-labs/dbt-bigquery:1.9.latest

COPY profiles.yml dbt_project.yml packages.yml ./

RUN dbt deps

COPY src/ src/
