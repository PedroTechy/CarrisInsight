# edit-de-project-dbt

This repo should serve as a starting point for the dbt project that is part of
the final project of the Data Engineering course at EDIT.

## Setup local development

Create a virtual environment and activate it.

```
$ python3 -m venv venv
$ source venv/bin/activate
```

Install dbt and dependencies.

```
$ pip install -r requirements.txt
```

Install dbt packages.

```
$ dbt deps
```

Add a `edit_de_project` profile to your `profiles.yml` file. You can use the
contents of of `profiles.yml` as a starting point. For local development, you
should create a new target called `dev` using a personal dataset as the target
for your models. For example:

```
edit_de_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: data-eng-dev-437916
      dataset: de_project_teachers_gn
      job_execution_timeout_seconds: 300
      job_retries: 1
      location: EU
      priority: interactive
      threads: 4 # Must be a value of 1 or greater 
    staging:
      type: bigquery
      method: oauth
      project: data-eng-dev-437916
      dataset: de_project_teachers
      job_execution_timeout_seconds: 300
      job_retries: 1
      location: EU
      priority: interactive
      threads: 4 # Must be a value of 1 or greater 
```

Point dbt to your local `profiles.yml` by exporting the environment variable
`DBT_PROFILES_DIR`.

```
$ export DBT_PROFILES_DIR=$HOME/.dbt/
```

To authenticate with Google Cloud Platform, you need to install `gcloud` and
create local credentials with `gcloud auth application-default login`. Follow
[these steps](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup#local-oauth-gcloud-setup).

Run `dbt debug` and check the connection.

```
$ dbt debug
```

Check if dbt is using the dataset that you intend it to use.

Run the test model.

```
$ dbt run --select test_model
```

## Running with Docker

After installing Docker, you can use the Dockerfile in this repo to build an
image that will run dbt.

Build the image

```
$ docker build -t edit-de-project-dbt .
```

The image built from the previous command will contain your `profiles.yml`,
`dbt_project.yml` and `packages.yml` files. It will install dbt packages with
`dbt deps` before running any command. The contents of the `src/` folder will
also be copied to the container. You can run it with the following command:

```
$ docker run edit-de-project-dbt debug
```

The entrypoint of the container is the command `dbt`. This means that any
arguments at the end of the `docker run` command will be appended to the `dbt`
command. The previous command effectively runs `dbt debug` inside the container.

This image can then be pushed to Docker Hub and be used in a Cloud Run
job. It will automatically find credentials to authenticate with BigQuery in
that environment. However, to test locally, you need to provide it your local
credentials. You can do this by mounting your local credentials to the container
and pointing the environment variable `GOOGLE_APPLICATION_CREDENTIALS` to it.

```
$ docker run \
    -v $HOME/.config/gcloud/application_default_credentials.json:/gcloud/application_default_credentials.json \
    -e GOOGLE_APPLICATION_CREDENTIALS=/gcloud/application_default_credentials.json \
    edit-de-project-dbt debug
```

## Push Docker image to Docker Hub

To upload the built image to Docker Hub, you first need to create an account
with Docker Hub. After that, run this command to authenticate using a browser
window.

```
$ docker login
```

Then, you need to build the image with the proper tag.

```
$ docker build -t <your-docker-hub-username>/edit-de-project-dbt:latest .
```

Finally, you can push the image to Docker Hub.

```
$ docker push <your-docker-hub-username>/edit-de-project-dbt:latest
```
