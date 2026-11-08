-- ============================================================
-- GLOBAL MOVIE ANALYTICS - STAR SCHEMA CREATION (POSTGRESQL / STANDARD SQL)
-- ============================================================

-- 1. DIMENSION TABLES
CREATE TABLE IF NOT EXISTS dim_genres (
    genre_id INT PRIMARY KEY,
    genre_name VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_companies (
    company_id INT PRIMARY KEY,
    company_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_cast (
    actor_id INT PRIMARY KEY,
    actor_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_directors (
    director_id INT PRIMARY KEY,
    director_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key DATE PRIMARY KEY,
    year INT,
    quarter INT,
    month INT,
    day INT,
    day_name VARCHAR(20)
);

-- 2. FACT TABLE
CREATE TABLE IF NOT EXISTS fact_movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    original_language VARCHAR(10),
    release_date DATE,
    budget NUMERIC(15, 2),
    revenue NUMERIC(15, 2),
    profit NUMERIC(15, 2),
    roi_percent NUMERIC(12, 2),
    vote_average NUMERIC(3, 2),
    vote_count INT,
    popularity NUMERIC(10, 2),
    runtime INT,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 3. BRIDGE TABLES (Many-to-Many Relationships)
CREATE TABLE IF NOT EXISTS bridge_movie_genres (
    movie_id INT REFERENCES fact_movies(movie_id),
    genre_id INT REFERENCES dim_genres(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS bridge_movie_companies (
    movie_id INT REFERENCES fact_movies(movie_id),
    company_id INT REFERENCES dim_companies(company_id),
    PRIMARY KEY (movie_id, company_id)
);

CREATE TABLE IF NOT EXISTS bridge_movie_cast (
    movie_id INT REFERENCES fact_movies(movie_id),
    actor_id INT REFERENCES dim_cast(actor_id),
    character_name VARCHAR(255),
    cast_order INT,
    PRIMARY KEY (movie_id, actor_id)
);

CREATE TABLE IF NOT EXISTS bridge_movie_directors (
    movie_id INT REFERENCES fact_movies(movie_id),
    director_id INT REFERENCES dim_directors(director_id),
    PRIMARY KEY (movie_id, director_id)
);