-- ============================================================
-- VIEW: vw_fact_movies_unique
-- Description: Cleaned, deduplicated fact view with Data Quality Gate
-- ============================================================

CREATE OR REPLACE VIEW vw_fact_movies_unique AS
WITH RankedMovies AS (
    SELECT 
        movie_id,
        title,
        original_language,
        release_date,
        budget,
        revenue,
        (revenue - budget) AS profit,
        CASE 
            WHEN budget > 0 THEN ROUND(((revenue - budget) / budget) * 100, 2)
            ELSE 0 
        END AS roi_percent,
        vote_average,
        vote_count,
        popularity,
        runtime,
        ROW_NUMBER() OVER (
            PARTITION BY movie_id 
            ORDER BY updated_at DESC, popularity DESC
        ) AS rn
    FROM fact_movies
    -- DATA QUALITY GATE: Strictly filter out invalid financial records
    WHERE budget > 0 
      AND revenue > 0
      AND release_date IS NOT NULL
)
SELECT 
    movie_id,
    title,
    original_language,
    release_date,
    EXTRACT(YEAR FROM release_date) AS release_year,
    budget,
    revenue,
    profit,
    roi_percent,
    vote_average,
    vote_count,
    popularity,
    runtime,
    -- Budget Segmentation Logic
    CASE 
        WHEN budget >= 100000000 THEN 'High Budget ($100M+)'
        WHEN budget >= 20000000  THEN 'Mid Budget ($20M-100M)'
        ELSE 'Low Budget (<$20M)'
    END AS budget_category,
    -- Budget Category Sort Order (For Power BI Sort by Column)
    CASE 
        WHEN budget >= 100000000 THEN 3
        WHEN budget >= 20000000  THEN 2
        ELSE 1
    END AS budget_category_sort
FROM RankedMovies
WHERE rn = 1;