# DMPTool Apache Airflow Workflows
Astronomer.io based Apache Airflow workflows for matching academic works to DMPTool DMPs using the Academic Observatory 
BigQuery datasets.

## Requirements
* Astro CLI: https://www.astronomer.io/docs/astro/cli/install-cli
* gcloud CLI: https://cloud.google.com/sdk/docs/install

## Installation
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

## Config
The workflow is configured via a config file stored as an Apache Airflow variable. It is often easier to work in YAML 
and then convert it to JSON.

`workflow-config.yaml` file:
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

## Local Development
The following instructions show how to run the workflow locally.

### Setup
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

### Running Airflow locally
Run the following command:
```bash
astro dev start
```

Then open the Airflow UI and run the workflow at: http://localhost:8080

### Running the Queries
You may also run or generate the queries. Customise the project IDs and the shard date (the shard date of the dmps_raw
table). Add `--dry-run` to just generate the SQL queries and not run them.
```bash
cd bin
export PYTHONPATH=/path/to/dmptool-workflows/dags:$PYTHONPATH
python3 run_queries.py ao-project-id my-project-id YYYY-MM-DD
```

## Deployment
Switch to the Astro workspace that you want to work in:
```bash
astro workspace switch
```

Create your Astro deployment. Note that you may need to customise some of the variables, such as workspace_name and
alert_emails.
```bash
astro deployment create --deployment-file ./bin/deployment.yaml
```

In the Astronomer.io WebUI, click on your deployment, click Details and copy the Workload Identity for the next step.

Setup your Google Cloud Project:
```bash
cd bin && ./setup-gcloud-project.sh my-project-name my-bucket-name my-astro-workload-identity@my-astro-workload-identity.iam.gserviceaccount.com
```

Create Apache Airflow Variables, customising the value for the WORKFLOWS key:
```bash
astro deployment airflow-variable create --key DATA_PATH --value /home/astro/data
astro deployment airflow-variable create --key WORKFLOWS --value '[{"dag_id":"dmp_match_workflow","name":"DMP Match Workflow","class_name":"dmptool_workflows.dmp_match_workflow.workflow","cloud_workspace":{...}}]'
```

Deploy workflows:
```bash
astro deploy
```
