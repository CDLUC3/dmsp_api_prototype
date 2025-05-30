-- Create Variables
SET VARIABLE DATA_PATH = '/path/to/parquets/';

-- Create Macros
CREATE OR REPLACE MACRO ARRAY_AGG_DISTINCT(col) AS COALESCE(ARRAY_AGG(DISTINCT col), []);

-- DuckDB settings
SET memory_limit = '80GB';
SET enable_progress_bar = true;
SET threads = 4;
SET preserve_insertion_order = false;