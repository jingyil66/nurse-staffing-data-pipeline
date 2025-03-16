select 
    distinct
    PROVNUM,
    PROVNAME,
    CITY,
    STATE,
    COUNTY_NAME,
    COUNTY_FIPS
from {{ source('staff_project', 'all_data') }}
