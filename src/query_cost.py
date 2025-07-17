def get_bq_cost_usage():
    return '''
    WITH gigs_per_day AS (
        SELECT
            DATE(creation_time) as date,
            SUM(total_bytes_billed/1000000000) AS gigs_billed
        FROM `region-US`.INFORMATION_SCHEMA.JOBS
        WHERE DATE(creation_time) BETWEEN DATE_TRUNC(CURRENT_DATE(), MONTH) AND CURRENT_DATE()
        GROUP BY 1
    )
    SELECT 
        date,
        gigs_billed,
        SUM(gigs_billed) OVER (
            PARTITION BY EXTRACT(MONTH FROM date), EXTRACT(YEAR FROM date)
            ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS month_to_date_gigs_billed_sum
    FROM gigs_per_day
    '''
