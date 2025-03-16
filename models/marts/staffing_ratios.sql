with renamed_data as (
    select
        PROVNUM,
        WorkDate,
        Hrs_RNDON_MDS_Ratio AS RNDON_all,
        Hrs_RNDON_emp_MDS_Ratio AS RNDON_emp, 
        Hrs_RNDON_ctr_MDS_Ratio AS RNDON_ctr,
        Hrs_RNadmin_MDS_Ratio AS RNadmin_all,
        Hrs_RNadmin_emp_MDS_Ratio AS RNadmin_emp,
        Hrs_RNadmin_ctr_MDS_Ratio AS RNadmin_ctr,
        Hrs_RN_MDS_Ratio AS RN_all,
        Hrs_RN_emp_MDS_Ratio AS RN_emp,
        Hrs_RN_ctr_MDS_Ratio AS RN_ctr,
        Hrs_LPNadmin_MDS_Ratio AS LPNadmin_all,
        Hrs_LPNadmin_emp_MDS_Ratio AS LPNadmin_emp,
        Hrs_LPNadmin_ctr_MDS_Ratio AS LPNadmin_ctr,
        Hrs_LPN_MDS_Ratio AS LPN_all,
        Hrs_LPN_emp_MDS_Ratio AS LPN_emp,
        Hrs_LPN_ctr_MDS_Ratio AS LPN_ctr,
        Hrs_CNA_MDS_Ratio AS CNA_all,
        Hrs_CNA_emp_MDS_Ratio AS CNA_emp,
        Hrs_CNA_ctr_MDS_Ratio AS CNA_ctr,
        Hrs_NAtrn_MDS_Ratio AS NAtrn_all,
        Hrs_NAtrn_emp_MDS_Ratio AS NAtrn_emp,
        Hrs_NAtrn_ctr_MDS_Ratio AS NAtrn_ctr,
        Hrs_MedAide_MDS_Ratio AS MedAide_all,
        Hrs_MedAide_emp_MDS_Ratio AS MedAide_emp,
        Hrs_MedAide_ctr_MDS_Ratio AS MedAide_ctr
    from {{ ref('stg_staffing_ratios') }}
),
unpivoted_data AS (
    SELECT 
        PROVNUM,
        WorkDate,
        Role_Type,
        hour_MDS_Ratio
    FROM renamed_data
    UNPIVOT(
        hour_MDS_Ratio FOR Role_Type IN (
            RNDON_all, RNDON_emp, RNDON_ctr,
            RNadmin_all, RNadmin_emp, RNadmin_ctr,
            RN_all, RN_emp, RN_ctr,
            LPNadmin_all, LPNadmin_emp, LPNadmin_ctr,
            LPN_all, LPN_emp, LPN_ctr,
            CNA_all, CNA_emp, CNA_ctr,
            NAtrn_all, NAtrn_emp, NAtrn_ctr,
            MedAide_all, MedAide_emp, MedAide_ctr
        )
    )
)

select
    PROVNUM,
    WorkDate,
    SPLIT_PART(role_type, '_', 1) AS Nurse_Role,
    SPLIT_PART(role_type, '_', 2) AS Employment_Type,
    hour_MDS_Ratio
FROM unpivoted_data