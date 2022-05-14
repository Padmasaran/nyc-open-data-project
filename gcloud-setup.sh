#set config
gcloud config set project evictions-analysis-nyc
PROJECT_ID=evictions-analysis-nyc
LOCATION=us-central1
NAME=eviction_analysis

#enable required APIs for the project
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable composer.googleapis.com

#create BQ dataset
bq --location=$LOCATION mk \
 --dataset $NAME

#create GCS buckets
gsutil mb -l $LOCATION gs://$NAME

#create cloud composer instance
gcloud composer environments create $NAME \
    --location $LOCATION

#add env variables to cloud composer
gcloud composer environments update \
  $NAME \
  --location $LOCATION \
  --update-env-variables=AIRFLOW_VAR_OUTPUT_BUCKET=$NAME

gcloud composer environments update \
  $NAME \
  --location $LOCATION \
  --update-env-variables=AIRFLOW_VAR_PROJECT=$PROJECT_ID

#copy the dags folder from repo to gcs bucket
wget --no-parent -r 'https://github.com/Padmasaran/nyc-open-data-project/tree/main/dags'
gsutil 