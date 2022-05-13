CREATE OR REPLACE TABLE {{ var.value.project }}.eviction_analysis.stg_evictions
(
  bbl INT64,
  census_tract INT64,
  council_district INT64,
  latitude FLOAT64,
  eviction_possession STRING,
  nta STRING,
  docket_number INT64,
  ejectment STRING,
  longitude FLOAT64,
  court_index_number STRING,
  borough STRING,
  residential_commercial_ind STRING,
  marshal_last_name STRING,
  bin INT64,
  community_board INT64,
  marshal_first_name STRING,
  executed_date TIMESTAMP OPTIONS(description="bq-datetime"),
  eviction_zip INT64,
  eviction_apt_num STRING,
  eviction_address STRING
);

CREATE OR REPLACE TABLE {{ var.value.project }}.eviction_analysis.stg_census_data
(
  total_population_change_2000_2010_number INT64,
  total_population_2010_number INT64,
  total_population_2000_number INT64,
  geographic_area_neighborhood_tabulation_area_nta_name STRING,
  geographic_area_2010_census_fips_county_code INT64,
  geographic_area_neighborhood_tabulation_area_nta_code STRING,
  total_population_change_2000_2010_percent FLOAT64,
  geographic_area_borough STRING
);

CREATE OR REPLACE TABLE {{ var.value.project }}.eviction_analysis.evictions
(
  bbl INT64,
  census_tract INT64,
  council_district INT64,
  latitude FLOAT64,
  eviction_possession STRING,
  nta STRUCT <
    nta STRING,
    population INT64
    >,
  docket_number INT64,
  ejectment STRING,
  longitude FLOAT64,
  court_index_number STRING,
  borough STRING,
  residential_commercial_ind STRING,
  marshal_last_name STRING,
  bin INT64,
  community_board INT64,
  marshal_first_name STRING,
  executed_date TIMESTAMP,
  eviction_zip INT64,
  eviction_apt_num STRING,
  eviction_address STRING
);