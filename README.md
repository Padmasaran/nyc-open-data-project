### NYC Open data project - Evictions

Goal is to setup a data pipeline to pull data from NYC Socrata Open Data API (SODA) and present the analytics via a presentation layer. This architecture is hosted on Google Cloud Platform (GCP) based on open-source frameworks like Apache Airflow and Apache Beam.

For all questions and suggestions, please reach out to me at: padmasaransathiakumar@gmail.com

#### Architecture
![NYC Open - Architecture](https://user-images.githubusercontent.com/56570539/168430409-e17469bc-e723-492c-a058-b9eeac41d8d5.png)

#### Process Flow
##### Ingestion (Extract): 
Data is fetched from SODA API using Apache Beam pipelines on dataflow runner. This setup allows for parallelization of the API pull with a separate thread for every 100000 records.
Data ingestion allows for both full-load and incremental-load with the same beam script via parameters.

##### Load:
Data is loaded into the BigQuery staging layer as-is without any transformations. This is a truncate and load operation.
In the full-load pipeline, the staging and DW tables are created before loading.

##### Transformation:
In this implementation, there are two sources - eviction data and census data. Both the data is combined into the same table as the final layer for analysis, using BQ scripts.

##### Orchestration:
Cloud composer, managed version of Apache Airflow is used to orchestrate the above steps. There are two DAGs as below:

###### Full-load data pipeline:

![Legend](https://user-images.githubusercontent.com/56570539/168415239-9e58612b-e53d-4d04-8d2d-a2fc84f95d68.png)
![Full-load pipeline DAG](https://user-images.githubusercontent.com/56570539/168415223-091ec8ed-5fe7-4018-bfd3-d6fd4bdffd36.png)

###### Delta-load pipeline DAG:

![Delta-load pipeline DAG](https://user-images.githubusercontent.com/56570539/168415435-fcbd1ba1-03f2-44ab-9b40-0da1cb2c5e8a.png)

Delta load pipeline does not fetch the census data, as it is not altered in a daily basis.

#### Setup
GCP setup can be performed by executing the gcloud-setup.sh script in GCP CLI. Script performs the following steps:
- Setup BigQuery and create required dataset
- Setup Cloud Storage and create required buckets
- Setup Google Data Studio
- Setup Cloud composer (managed Apache Airflow) and required environmental variables
- Copy the scripts from this repo to the Cloud Composer dags folder

Replace the PROJECT_ID parameter with the your GCP project ID. Once script is executed, GCP environment should be ready to analyse the evictions data ingested and extend this model to other SODA API based data.

#### Infrastructure
This solution uses serverless and managed offerings of Google Cloud Platform as below:
- Cloud Dataflow - serverless data processing framework used for data ingestion
- Cloud Composer - managed Apache Airflow instance used for data orchestration
- Cloud Storage - cloud object storage
- BigQuery - serverless data warehousing solution with pay-per-use compute
- Google Data Studio - self service BI tool for data visualization
