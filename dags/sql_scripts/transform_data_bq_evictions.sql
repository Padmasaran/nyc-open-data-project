merge into {{ var.value.project }}.eviction_analysis.evictions target1
using (
  
  with census_data as (
    select distinct 
      geographic_area_neighborhood_tabulation_area_nta_name as nta,
      total_population_2010_number as population
  from {{ var.value.project }}.eviction_analysis.stg_census_data
  ),
  evictions as (
    select distinct
      *
    from {{ var.value.project }}.eviction_analysis.stg_evictions
  )

  select
    bbl,
    census_tract,
    council_district,
    latitude,
    eviction_possession,
    STRUCT(evictions.nta as nta, census_data.population as population) as nta,
    docket_number,
    ejectment,
    longitude,
    court_index_number,
    borough,
    residential_commercial_ind,
    marshal_last_name,
    bin,
    community_board,
    marshal_first_name,
    executed_date,
    eviction_zip,
    eviction_apt_num,
    eviction_address
  from evictions
  left join census_data
    on census_data.nta = evictions.nta
) source1
on target1.executed_date = source1.executed_date
and target1.docket_number = source1.docket_number
and target1.court_index_number = source1.court_index_number
when matched then update set 
  target1.bbl = source1.bbl,
  target1.census_tract = source1.census_tract,
  target1.council_district = source1.council_district,
  target1.latitude = source1.latitude,
  target1.eviction_possession = target1.eviction_possession,
  target1.nta = source1.nta,
  target1.docket_number = source1.docket_number,
  target1.ejectment = source1.ejectment,
  target1.longitude = source1.longitude,
  target1.court_index_number = source1.court_index_number,
  target1.borough = source1.borough,
  target1.residential_commercial_ind = source1.residential_commercial_ind,
  target1.marshal_last_name = source1.marshal_last_name,
  target1.bin = source1.bin,
  target1.community_board = source1.community_board,
  target1.marshal_first_name = source1.marshal_first_name,
  target1.executed_date = source1.executed_date,
  target1.eviction_zip = source1.eviction_zip,
  target1.eviction_apt_num = source1.eviction_apt_num,
  target1.eviction_address = source1.eviction_address
when not matched then insert(
  bbl,
  census_tract,
  council_district,
  latitude,
  eviction_possession,
  nta,
  docket_number,
  ejectment,
  longitude,
  court_index_number,
  borough,
  residential_commercial_ind,
  marshal_last_name,
  bin,
  community_board,
  marshal_first_name,
  executed_date,
  eviction_zip,
  eviction_apt_num,
  eviction_address
)
values(
  source1.bbl,
  source1.census_tract,
  source1.council_district,
  source1.latitude,
  source1.eviction_possession,
  source1.nta,
  source1.docket_number,
  source1.ejectment,
  source1.longitude,
  source1.court_index_number,
  source1.borough,
  source1.residential_commercial_ind,
  source1.marshal_last_name,
  source1.bin,
  source1.community_board,
  source1.marshal_first_name,
  source1.executed_date,
  source1.eviction_zip,
  source1.eviction_apt_num,
  source1.eviction_address
);