-- Staffing ratio by state
WITH Provider_Daily_Staffing AS (
    SELECT 
        s.PROVNUM,
        s.WorkDate,
        SUM(s.Hours) AS total_staffing_hours,
        MAX(s.MDScensus) AS MDScensus
    FROM staffing s
    WHERE s.Employment_Type = 'ALL'
    GROUP BY s.PROVNUM, s.WorkDate
)
SELECT 
    p.STATE,
    AVG(CASE 
            WHEN pds.MDScensus > 0 THEN pds.total_staffing_hours * 1.0 / pds.MDScensus 
            ELSE NULL 
        END) AS avg_staffing_ratio
FROM Provider_Daily_Staffing pds
JOIN providers p ON pds.PROVNUM = p.PROVNUM
GROUP BY p.STATE
ORDER BY avg_staffing_ratio;

-- Contractor staffing percentage by state
SELECT 
    p.STATE,
    SUM(CASE WHEN s.Employment_Type = 'CTR' THEN s.Hours ELSE 0 END) * 100.0 / SUM(CASE WHEN s.Employment_Type = 'ALL' THEN s.Hours
    ELSE 0 END) AS contractor_percentage
FROM staffing s
JOIN providers p ON s.PROVNUM = p.PROVNUM
GROUP BY p.STATE
ORDER BY contractor_percentage DESC;

-- Contractor staffing percentage by role
SELECT 
    s.Nurse_Role,
    SUM(CASE WHEN s.Employment_Type = 'CTR' THEN s.Hours ELSE 0 END) * 100.0 / SUM(CASE WHEN s.Employment_Type = 'ALL' THEN s.Hours ELSE 0 END) AS contractor_percentage
FROM staffing s
GROUP BY s.Nurse_Role
ORDER BY contractor_percentage DESC;

-- Month with the highest MDScensus
SELECT 
    d.year,
    d.month,
    SUM(distinct_counts.MDScensus) AS total_MDScensus
FROM (
    SELECT DISTINCT WorkDate, PROVNUM, MDScensus
    FROM staffing
) AS distinct_counts
JOIN DATE d ON distinct_counts.WorkDate = d.WorkDate
GROUP BY d.year, d.month
ORDER BY total_MDScensus DESC;

-- Weekday with the highest MDScensus
SELECT 
    DAYNAME(d.WorkDate) AS weekday,
    SUM(distinct_counts.MDScensus) AS total_MDScensus
FROM (
    SELECT DISTINCT WorkDate, PROVNUM, MDScensus
    FROM staffing
) AS distinct_counts
JOIN DATE d ON distinct_counts.WorkDate = d.WorkDate
GROUP BY weekday
ORDER BY total_MDScensus DESC;

-- Top month of nursing staffing shortage (lowest staffing_ratio)
WITH Provider_Daily_Staffing AS (
    SELECT 
        s.PROVNUM,
        s.WorkDate,
        SUM(s.Hours) AS total_staffing_hours,
        MAX(s.MDScensus) AS MDScensus
    FROM staffing s
    WHERE s.Employment_Type = 'ALL'
    GROUP BY s.PROVNUM, s.WorkDate
),
Monthly_Staffing AS (
    SELECT 
        d.year,
        d.month,
        pds.PROVNUM,
        AVG(CASE 
                WHEN pds.MDScensus > 0 THEN pds.total_staffing_hours * 1.0 / pds.MDScensus 
                ELSE NULL 
            END) AS avg_staffing_ratio
    FROM Provider_Daily_Staffing pds
    JOIN DATE d ON pds.WorkDate = d.WorkDate
    GROUP BY d.year, d.month, pds.PROVNUM
)
SELECT 
    year,
    month,
    AVG(avg_staffing_ratio) AS monthly_avg_staffing_ratio
FROM Monthly_Staffing
GROUP BY year, month
ORDER BY monthly_avg_staffing_ratio;

-- Top weekday of nursing staffing shortage (lowest staffing_ratio)
WITH Provider_Daily_Staffing AS (
    SELECT 
        s.PROVNUM,
        s.WorkDate,
        SUM(s.Hours) AS total_staffing_hours,
        MAX(s.MDScensus) AS MDScensus
    FROM staffing s
    WHERE s.Employment_Type = 'ALL'
    GROUP BY s.PROVNUM, s.WorkDate
)
SELECT 
    EXTRACT(DOW FROM pds.WorkDate) AS weekday,
    AVG(CASE 
            WHEN pds.MDScensus > 0 THEN pds.total_staffing_hours * 1.0 / pds.MDScensus 
            ELSE NULL 
        END) AS avg_staffing_ratio
FROM Provider_Daily_Staffing pds
JOIN DATE d ON pds.WorkDate = d.WorkDate
GROUP BY weekday
ORDER BY avg_staffing_ratio;

