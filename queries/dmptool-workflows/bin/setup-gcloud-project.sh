#!/bin/bash

# Positional arguments
PROJECT_ID=$1
BUCKET_NAME=$2

# Optional arguments with default values
BQ_REGION="us"
GCS_REGION="us-central1"
CONNECTION_ID="vertex_ai"
PER_USER_PER_DAY=$((2 * 1024 * 1024)) # 2 TiB in MiB
PER_PROJECT_PER_DAY=$((2 * 1024 * 1024)) # 2 TiB in MiB
ACADEMIC_OBSERVATORY_PROJECT_ID="academic-observatory"

# Parse optional arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --bq-region) BQ_REGION="$2"; shift ;;
        --gcs-region) GCS_REGION="$2"; shift ;;
        --connection-id) CONNECTION_ID="$2"; shift ;;
        --per-user-per-day) PER_USER_PER_DAY="$2"; shift ;;
        --per-project-per-day) PER_PROJECT_PER_DAY="$2"; shift ;;
        --academic-observatory-project-id) ACADEMIC_OBSERVATORY_PROJECT_ID="$2"; shift ;;
        *) break ;;
    esac
    shift
done

# Check if required positional arguments are provided and if not print usage
if [[ -z "$PROJECT_ID" || -z "$BUCKET_NAME" ]]; then
    echo "Usage: $0 <PROJECT_ID> <BUCKET_NAME> [optional arguments]"
    echo "Optional arguments:"
    echo "  --bq-region <value>                       Default: us"
    echo "  --gcs-region <value>                      Default: us-central1"
    echo "  --connection-id <value>                   Default: vertex_ai"
    echo "  --per-user-per-day <value>                Default: $((1 * 1024 * 1024)) (1 TiB in MiB)"
    echo "  --per-project-per-day <value>             Default: $((1 * 1024 * 1024)) (1 TiB in MiB)"
    echo "  --academic-observatory-project-id <value> Default: academic-observatory"
    exit 1
fi

echo "Configuration:"
echo "  PROJECT_ID: $PROJECT_ID"
echo "  BUCKET_NAME: $BUCKET_NAME"
echo "  BQ_REGION: $BQ_REGION"
echo "  GCS_REGION: $GCS_REGION"
echo "  CONNECTION_ID: $CONNECTION_ID"
echo "  PER_USER_PER_DAY: $PER_USER_PER_DAY"
echo "  PER_PROJECT_PER_DAY: $PER_PROJECT_PER_DAY"
echo "  ACADEMIC_OBSERVATORY_PROJECT_ID: $ACADEMIC_OBSERVATORY_PROJECT_ID"
echo ""

echo "Enable Google Cloud APIs"
gcloud services enable storage.googleapis.com \
bigquery.googleapis.com \
bigqueryconnection.googleapis.com \
aiplatform.googleapis.com --project=$PROJECT_ID

echo "Set BigQuery Quota"
gcloud alpha services quota update \
  --consumer=projects/$PROJECT_ID \
  --service bigquery.googleapis.com \
  --metric bigquery.googleapis.com/quota/query/usage \
  --value $PER_USER_PER_DAY --unit 1/d/{project}/{user} --force

gcloud alpha services quota update \
  --consumer=projects/$PROJECT_ID \
  --service bigquery.googleapis.com \
  --metric bigquery.googleapis.com/quota/query/usage \
  --value $PER_PROJECT_PER_DAY --unit 1/d/{project} --force

echo "Create DMP Airflow IAM role"
DMP_ROLE_NAME="DMPAirflowRole"
gcloud iam roles create $DMP_ROLE_NAME --project=$PROJECT_ID \
 --title="DMP Airflow Role" \
 --description="Gives Astro permissions to specific Google Cloud resources" \
 --permissions=bigquery.datasets.create,\
bigquery.datasets.get,\
bigquery.datasets.update,\
bigquery.jobs.create,\
bigquery.tables.create,\
bigquery.tables.update,\
bigquery.tables.updateData,\
bigquery.tables.get,\
bigquery.tables.getData,\
bigquery.tables.list,\
bigquery.tables.export,\
bigquery.connections.use,\
bigquery.models.create,\
bigquery.models.updateData,\
bigquery.models.updateMetadata,\
bigquery.routines.create,\
bigquery.routines.update,\
bigquery.routines.get

echo "Create DMP Airflow Service Account"
DMP_SERVICE_ACCOUNT_NAME="dmp-airflow"
DMP_SERVICE_ACCOUNT="$DMP_SERVICE_ACCOUNT_NAME@$PROJECT_ID.iam.gserviceaccount.com"
gcloud iam service-accounts create $DMP_SERVICE_ACCOUNT_NAME \
  --project=$PROJECT_ID \
  --description="The DMP Tool Apache Airflow Service Account" \
  --display-name="DMP Airflow Service Account"

echo "Add $DMP_ROLE_NAME Role to DMP Airflow Service Account"
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:$DMP_SERVICE_ACCOUNT" \
  --role=projects/$PROJECT_ID/roles/$DMP_ROLE_NAME

echo "Create GCS bucket and add lifecycle rules"
gcloud storage buckets create gs://$BUCKET_NAME --location=$GCS_REGION --project=$PROJECT_ID
gcloud storage buckets update gs://$BUCKET_NAME --lifecycle-file=lifecycle.json --project=$PROJECT_ID

echo "Give DMP Service Account permission to access bucket"
gsutil iam ch \
serviceAccount:$DMP_SERVICE_ACCOUNT:roles/storage.legacyBucketReader \
serviceAccount:$DMP_SERVICE_ACCOUNT:roles/storage.objectCreator \
serviceAccount:$DMP_SERVICE_ACCOUNT:roles/storage.objectViewer \
gs://$BUCKET_NAME

echo "Grant DMP Service Account access to Academic Observatory BigQuery"
gcloud projects add-iam-policy-binding $ACADEMIC_OBSERVATORY_PROJECT_ID \
    --member="serviceAccount:$DMP_SERVICE_ACCOUNT" \
    --role="roles/bigquery.dataViewer"

echo "Create a Cloud Resource Connection"
bq mk --connection --location=$BQ_REGION --project_id=$PROJECT_ID --connection_type=CLOUD_RESOURCE $CONNECTION_ID
CRC_SERVICE_ACCOUNT=$(bq show --connection $PROJECT_ID.$BQ_REGION.$CONNECTION_ID | grep -oP '(?<="serviceAccountId": ")[^"]+')

echo "Grant Cloud Resource Connection Service Account access to Vertex AI"
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$CRC_SERVICE_ACCOUNT" \
    --role="roles/aiplatform.user"

echo ""
echo "DMP Airflow Service Account: $DMP_SERVICE_ACCOUNT"