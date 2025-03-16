select
    distinct
    WorkDate,
    CY_Qtr
from {{ source('staff_project', 'all_data') }}
