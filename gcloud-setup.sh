#set config
gcloud config set project evictions-analysis-nyc
PROJECT_ID=evictions-analysis-nyc
LOCATION=us-central1

#enable required APIs for the project
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable composer.googleapis.com

#create BQ dataset
bq --location=$LOCATION mk \
 --dataset eviction_analysis

#create GCS buckets
gsutil mb -l $LOCATION gs://eviction_analysis

#create cloud composer instance
gcloud composer environments create evictions-analysis \
    --location $LOCATION

#add env variables to cloud composer
gcloud composer environments update \
  evictions-analysis \
  --location $LOCATION \
  --update-env-variables=AIRFLOW_VAR_OUTPUT_BUCKET=eviction_analysis

gcloud composer environments update \
  evictions-analysis \
  --location $LOCATION \
  --update-env-variables=AIRFLOW_VAR_PROJECT=$PROJECT_ID