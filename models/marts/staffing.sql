with renamed_data as (
    select
        PROVNUM,
        WorkDate,
        MDScensus,
        Hrs_RNDON AS RNDON_all,
        Hrs_RNDON_emp AS RNDON_emp,
        Hrs_RNDON_ctr AS RNDON_ctr,
        Hrs_RNadmin AS RNadmin_all,
        Hrs_RNadmin_emp AS RNadmin_emp,
        Hrs_RNadmin_ctr AS RNadmin_ctr,
        Hrs_RN AS RN_all,
        Hrs_RN_emp AS RN_emp,
        Hrs_RN_ctr AS RN_ctr,
        Hrs_LPNadmin AS LPNadmin_all,
        Hrs_LPNadmin_emp AS LPNadmin_emp,
        Hrs_LPNadmin_ctr AS LPNadmin_ctr,
        Hrs_LPN AS LPN_all,
        Hrs_LPN_emp AS LPN_emp,
        Hrs_LPN_ctr AS LPN_ctr,
        Hrs_CNA AS CNA_all,
        Hrs_CNA_emp AS CNA_emp,
        Hrs_CNA_ctr AS CNA_ctr,
        Hrs_NAtrn AS NAtrn_all,
        Hrs_NAtrn_emp AS NAtrn_emp,
        Hrs_NAtrn_ctr AS NAtrn_ctr,
        Hrs_MedAide AS MedAide_all,
        Hrs_MedAide_emp AS MedAide_emp,
        Hrs_MedAide_ctr AS MedAide_ctr
    from {{ ref('stg_staffing') }}
),
unpivoted_data AS (
    SELECT 
        PROVNUM,
        WorkDate,
        MDScensus,
        Role_Type,
        Hours
    FROM renamed_data
    UNPIVOT(
        Hours FOR Role_Type IN (
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
    MDScensus,
    SPLIT_PART(role_type, '_', 1) AS Nurse_Role,
    SPLIT_PART(role_type, '_', 2) AS Employment_Type,
    Hours
FROM unpivoted_data