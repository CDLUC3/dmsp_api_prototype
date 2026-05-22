# DMPTool Apache Airflow Workflows
Astronomer.io based Apache Airflow workflows for matching academic works to DMPTool DMPs using the Academic Observatory 
BigQuery datasets.

## Dependencies
Install the Astro CLI: https://www.astronomer.io/docs/astro/cli/install-cli

Clone the project and enter the `dmptool-workflows` directory:
```bash
git clone --branch feature/dmp-works-matching git@github.com:CDLUC3/dmsp_api_prototype.git
cd dmsp_api_prototype/queries/dmptool-workflows
```

Setup a Python virtual environment:
```bash
python3.10 -m venv venv
source venv/bin/activate 
```

Install Python dependencies:
```bash
pip install git+https://github.com/The-Academic-Observatory/observatory-platform.git@feature/astro_kubernetes --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.7.3/constraints-no-providers-3.10.txt
```

## Local Development Setup
Add the following to your `.env` file:
```bash
GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/gcloud/application_default_credentials.json
```

Add `docker-compose.override.yml` to the root of this project and customise the path to the Google Cloud credentials file:
```commandline
version: "3.1"
services:
  scheduler:
    volumes:
      - /path/to/host/google-application-credentials.json:/usr/local/airflow/gcloud/application_default_credentials.json:ro
  webserver:
    volumes:
      - /path/to/host/google-application-credentials.json:/usr/local/airflow/gcloud/application_default_credentials.json:ro
  triggerer:
    volumes:
      - /path/to/host/google-application-credentials.json:/usr/local/airflow/gcloud/application_default_credentials.json:ro
```

Customise the `workflow-config.yaml` file:
```yaml
cloud_workspaces:
  - workspace: &dmptool_dev
      project_id: academic-observatory-project-id
      output_project_id: my-project-id
      download_bucket: my-download-bucket
      transform_bucket: my-transform-bucket
      data_location: us

workflows:
  - dag_id: "dmp_match_workflow"
    name: "DMP Match Workflow"
    class_name: "dmptool_workflows.dmp_match_workflow.workflow"
    cloud_workspace: *dmptool_dev
```

Convert `workflow-config.yaml` to JSON:
```bash
yq -o=json '.workflows' workflows-config.yaml | jq -c .
```

Create or add the following to `airflow_settings.yaml`, making sure to paste the JSON output from above into the 
WORKFLOWS variable_value:
```yaml
airflow:
  variables:
    - variable_name: DATA_PATH
      variable_value: /home/astro/data

    - variable_name: WORKFLOWS
      variable_value: REPLACE WITH WORKFLOWS JSON
```

## Running Airflow locally
Run the following command:
```bash
astro dev start
```

Then open the Airflow UI and run the workflow at: http://localhost:8080

## Running the Queries
You may also run or generate the queries. Customise the project IDs and the shard date (the shard date of the dmps_raw
table). Add `--dry-run` to just generate the SQL queries and not run them.
```bash
cd bin
export PYTHONPATH=/path/to/dmptool-workflows/dags:$PYTHONPATH
python3 run_queries.py ao-project-id my-project-id YYYY-MM-DD
```

## Deploy
TODO
