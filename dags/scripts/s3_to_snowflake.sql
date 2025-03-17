create database raw;
create database analytics;
create schema raw.staff;
create table raw.staff.all_data 
( 
    PROVNUM VARCHAR(6),
    PROVNAME TEXT,
    CITY TEXT,
    STATE VARCHAR(2),
    COUNTY_NAME TEXT,
    COUNTY_FIPS BIGINT,
    CY_Qtr VARCHAR(6),
    WorkDate TIMESTAMP,
    MDScensus BIGINT,
    Hrs_RNDON FLOAT8,
    Hrs_RNDON_emp FLOAT8,
    Hrs_RNDON_ctr FLOAT8,
    Hrs_RNadmin FLOAT8,
    Hrs_RNadmin_emp FLOAT8,
    Hrs_RNadmin_ctr FLOAT8,
    Hrs_RN FLOAT8,
    Hrs_RN_emp FLOAT8,
    Hrs_RN_ctr FLOAT8,
    Hrs_LPNadmin FLOAT8,
    Hrs_LPNadmin_emp FLOAT8,
    Hrs_LPNadmin_ctr FLOAT8,
    Hrs_LPN FLOAT8,
    Hrs_LPN_emp FLOAT8,
    Hrs_LPN_ctr FLOAT8,
    Hrs_CNA FLOAT8,
    Hrs_CNA_emp FLOAT8,
    Hrs_CNA_ctr FLOAT8,
    Hrs_NAtrn FLOAT8,
    Hrs_NAtrn_emp FLOAT8,
    Hrs_NAtrn_ctr FLOAT8,
    Hrs_MedAide FLOAT8,
    Hrs_MedAide_emp FLOAT8,
    Hrs_MedAide_ctr FLOAT8,
    Hrs_RNDON_MDS_Ratio FLOAT8,
    Hrs_RNDON_emp_MDS_Ratio FLOAT8,
    Hrs_RNDON_ctr_MDS_Ratio FLOAT8,
    Hrs_RNadmin_MDS_Ratio FLOAT8,
    Hrs_RNadmin_emp_MDS_Ratio FLOAT8,
    Hrs_RNadmin_ctr_MDS_Ratio FLOAT8,
    Hrs_RN_MDS_Ratio FLOAT8,
    Hrs_RN_emp_MDS_Ratio FLOAT8,
    Hrs_RN_ctr_MDS_Ratio FLOAT8,
    Hrs_LPNadmin_MDS_Ratio FLOAT8,
    Hrs_LPNadmin_emp_MDS_Ratio FLOAT8,
    Hrs_LPNadmin_ctr_MDS_Ratio FLOAT8,
    Hrs_LPN_MDS_Ratio FLOAT8,
    Hrs_LPN_emp_MDS_Ratio FLOAT8,
    Hrs_LPN_ctr_MDS_Ratio FLOAT8,
    Hrs_CNA_MDS_Ratio FLOAT8,
    Hrs_CNA_emp_MDS_Ratio FLOAT8,
    Hrs_CNA_ctr_MDS_Ratio FLOAT8,
    Hrs_NAtrn_MDS_Ratio FLOAT8,
    Hrs_NAtrn_emp_MDS_Ratio FLOAT8,
    Hrs_NAtrn_ctr_MDS_Ratio FLOAT8,
    Hrs_MedAide_MDS_Ratio FLOAT8,
    Hrs_MedAide_emp_MDS_Ratio FLOAT8,
    Hrs_MedAide_ctr_MDS_Ratio FLOAT8
);

CREATE STORAGE INTEGRATION storage_itg_staff
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = ... -- Change with your STORAGE_AWS_ROLE_ARN
  STORAGE_ALLOWED_LOCATIONS = ('*');

CREATE FILE FORMAT my_parquet_format
  TYPE = PARQUET
  COMPRESSION = AUTO;
  
CREATE STAGE stage_staff
  STORAGE_INTEGRATION = storage_itg_staff
  URL = ... -- Change with your bucket url
  FILE_FORMAT = my_parquet_format;
  
copy into raw.staff.all_data
from @stage_staff
file_format = (type = 'parquet')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select * from raw.STAFF.all_data
LIMIT 5;