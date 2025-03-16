select
    WorkDate,
    year(WorkDate) as year,
    month(WorkDate) as month,
    day(WorkDate) as day,
    CY_Qtr
from  {{ ref('stg_date') }}