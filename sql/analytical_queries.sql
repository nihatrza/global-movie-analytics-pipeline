-- ============================================================
-- GLOBAL MOVIE ANALYTICS - TOP 7 ENTERPRISE ANALYTICAL SQL QUERIES
-- Dataset: tmdb-movie-analytics.tmdb_analytics
-- Target Layer: vw_fact_movies_unique (Data Quality Gate Layer)
-- ============================================================

-- ------------------------------------------------------------
-- 1. Top Grossing & Most Profitable Directors
-- Description: Ranks directors by cumulative box office revenue, total net profit, 
--              and average profit per movie (minimum 3 movies directed).
-- ------------------------------------------------------------
SELECT 
    d.director_name,
    COUNT(DISTINCT f.movie_id) AS total_movies_directed,
    SUM(f.revenue) AS total_revenue,
    SUM(f.profit) AS total_profit,
    ROUND(AVG(f.profit), 2) AS avg_profit_per_movie
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique` f
JOIN `tmdb-movie-analytics.tmdb_analytics.bridge_movie_directors` bd ON f.movie_id = bd.movie_id
JOIN `tmdb-movie-analytics.tmdb_analytics.dim_directors` d ON bd.director_id = d.director_id
GROUP BY d.director_name
HAVING COUNT(DISTINCT f.movie_id) >= 3
ORDER BY total_profit DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 2. Year-over-Year Box Office Performance & Trend (1977-2026)
-- Description: Analyzes annual box office growth, total budget allocation, 
--              net profit, and average ROI percentage per release year.
-- ------------------------------------------------------------
SELECT 
    EXTRACT(YEAR FROM SAFE_CAST(release_date AS DATE)) AS release_year,
    COUNT(movie_id) AS total_movies_released,
    SUM(budget) AS total_budget,
    SUM(revenue) AS total_revenue,
    SUM(profit) AS total_profit,
    ROUND(AVG(SAFE_DIVIDE(profit, budget) * 100), 2) AS avg_roi_percentage
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique`
WHERE release_date IS NOT NULL AND release_date != ''
GROUP BY release_year
ORDER BY release_year DESC;


-- ------------------------------------------------------------
-- 3. Budget Category Performance (Low, Mid, High Budget Breakdown)
-- Description: Segments movies into budget tiers to evaluate financial margin 
--              and capital efficiency across production scales.
-- ------------------------------------------------------------
SELECT 
    CASE 
        WHEN budget >= 100000000 THEN 'High Budget ($100M+)'
        WHEN budget >= 20000000  THEN 'Mid Budget ($20M-100M)'
        ELSE 'Low Budget (<$20M)'
    END AS budget_category,
    COUNT(movie_id) AS movie_count,
    SUM(budget) AS category_total_budget,
    SUM(revenue) AS category_total_revenue,
    SUM(profit) AS category_total_profit,
    ROUND((SUM(profit) / SUM(revenue)) * 100, 2) AS profit_margin_percent,
    ROUND(AVG(SAFE_DIVIDE(profit, budget) * 100), 2) AS avg_roi_percent
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique`
GROUP BY budget_category
ORDER BY category_total_budget DESC;


-- ------------------------------------------------------------
-- 4. Top 3 Most Profitable Movies per Genre (DENSE_RANK Window Function)
-- Description: Evaluates the top 3 highest-earning unique films within each movie genre 
--              using window partitioning and deduplicated genre mapping.
-- ------------------------------------------------------------
WITH RankedGenreMovies AS (
    SELECT DISTINCT
        g.genre_name,
        f.movie_id,
        f.title,
        EXTRACT(YEAR FROM SAFE_CAST(f.release_date AS DATE)) AS release_year,
        f.budget,
        f.revenue,
        f.profit,
        DENSE_RANK() OVER (
            PARTITION BY g.genre_id 
            ORDER BY f.profit DESC
        ) AS rank_within_genre
    FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique` f
    JOIN `tmdb-movie-analytics.tmdb_analytics.bridge_movie_genres` bg ON f.movie_id = bg.movie_id
    JOIN `tmdb-movie-analytics.tmdb_analytics.dim_genres` g ON bg.genre_id = g.genre_id
    WHERE f.release_date IS NOT NULL AND f.release_date != ''
)
SELECT 
    genre_name,
    title,
    release_year,
    budget,
    revenue,
    profit
FROM RankedGenreMovies
WHERE rank_within_genre <= 3
ORDER BY genre_name, rank_within_genre;


-- ------------------------------------------------------------
-- 5. Top Performing Actors by Box Office Profit
-- Description: Identifies top-tier actors (minimum 5 films) generating the highest 
--              cumulative commercial profit and average audience ratings.
-- ------------------------------------------------------------
SELECT 
    c.actor_name,
    COUNT(DISTINCT f.movie_id) AS total_movies_starred,
    SUM(f.revenue) AS cumulative_box_office,
    SUM(f.profit) AS total_actor_profit,
    ROUND(AVG(f.vote_average), 2) AS avg_audience_rating
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique` f
JOIN `tmdb-movie-analytics.tmdb_analytics.bridge_movie_cast` bc ON f.movie_id = bc.movie_id
JOIN `tmdb-movie-analytics.tmdb_analytics.dim_cast` c ON bc.actor_id = c.actor_id
GROUP BY c.actor_name
HAVING COUNT(DISTINCT f.movie_id) >= 5
ORDER BY total_actor_profit DESC
LIMIT 10;


-- ------------------------------------------------------------
-- 6. Portfolio Profit vs. Loss Ratio Analysis
-- Description: Determines overall enterprise portfolio health by calculating 
--              the proportion and net cash flow of profitable vs. loss-making films.
-- ------------------------------------------------------------
SELECT 
    CASE 
        WHEN profit > 0 THEN 'Profitable'
        ELSE 'Loss-making'
    END AS financial_outcome,
    COUNT(movie_id) AS movie_count,
    ROUND(COUNT(movie_id) * 100.0 / SUM(COUNT(movie_id)) OVER (), 2) AS percentage_share,
    SUM(budget) AS total_budget_allocated,
    SUM(profit) AS total_net_outcome
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique`
GROUP BY 
    CASE 
        WHEN profit > 0 THEN 'Profitable'
        ELSE 'Loss-making'
    END;


-- ------------------------------------------------------------
-- 7. Studio Box Office Market Share & ROI Performance
-- Description: Ranks major production companies by total revenue contribution, 
--              net profit generated, and overall studio ROI.
-- ------------------------------------------------------------
SELECT 
    co.company_name,
    COUNT(DISTINCT f.movie_id) AS total_produced_movies,
    SUM(f.revenue) AS total_studio_revenue,
    SUM(f.profit) AS total_studio_profit,
    ROUND(AVG(SAFE_DIVIDE(f.profit, f.budget) * 100), 2) AS avg_studio_roi
FROM `tmdb-movie-analytics.tmdb_analytics.vw_fact_movies_unique` f
JOIN `tmdb-movie-analytics.tmdb_analytics.bridge_movie_companies` bco ON f.movie_id = bco.movie_id
JOIN `tmdb-movie-analytics.tmdb_analytics.dim_companies` co ON bco.company_id = co.company_id
GROUP BY co.company_name
ORDER BY total_studio_revenue DESC
LIMIT 10;