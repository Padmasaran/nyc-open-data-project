#set config
UUID=$(cat /proc/sys/kernel/random/uuid | head -c 5)

PROJECT_ID=evictions-analysis-nyc-1234
LOCATION=us-central1
NAME=eviction-analysis
DATASET_NAME=eviction_analysis

gcloud config set project $PROJECT_ID

#enable required APIs for the project
gcloud services enable bigquery.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable composer.googleapis.com

#create BQ dataset
bq --location=$LOCATION mk \
 --dataset $DATASET_NAME

#create GCS buckets
gsutil mb -l $LOCATION gs://$NAME-$UUID

#create cloud composer instance with env variables
gcloud composer environments create $NAME \
    --location $LOCATION
    --env_variables AIRFLOW_VAR_OUTPUT_BUCKET=$NAME-$UUID,AIRFLOW_VAR_PROJECT=$PROJECT_ID

#copy the dags folder from repo to gcs bucket
wget --no-parent -r 'https://github.com/Padmasaran/nyc-open-data-project/archive/main.zip'
unzip main.zip
DAGS_FOLDER=$(gcloud composer environments describe --location=$LOCATION $NAME | grep dagGcsPrefix: | cut -d ":" -f2- | xargs)
gsutil cp -r nyc-open-data-project-main/dags/ ${DAGS_FOLDER::-4}