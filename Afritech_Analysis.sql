CREATE TABLE afritech_data (
  customer_id INT,
  customer_name VARCHAR(100),
  region VARCHAR(100),
  age INT,
  income NUMERIC(10,2),
  customer_type VARCHAR(50),
  transaction_year INT,
  transaction_date TEXT,
  product_purchased VARCHAR(100),
  purchase_amount NUMERIC(10,2),
  product_recalled BOOLEAN,
  competitor_x VARCHAR(100),
  interaction_date TEXT,
  platform VARCHAR(50),
  post_type VARCHAR(50),
  engagement_likes INT,
  engagement_shares INT,
  engagement_comments INT,
  user_followers INT,
  influencer_score NUMERIC(6,3),
  brand_mention BOOLEAN,
  competitor_mention BOOLEAN,
  sentiment VARCHAR(50),
  crisis_event_time TEXT,
  first_response_time TEXT,
  resolution_status BOOLEAN,
  nps_response INT
);

SELECT * FROM afritech_data_deduped
--- Step 1 
-- converting and cleaning transactiondate column for data consistency 
SELECT transaction_date,
  CASE
    WHEN transaction_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(transaction_date, 'MM/DD/YYYY')
    WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN transaction_date::DATE
    WHEN transaction_date ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(transaction_date, 'YYYY/MM/DD')
    WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(transaction_date, 'DD-MM-YYYY')
    WHEN transaction_date ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(transaction_date, 'DD/MM/YYYY')
    WHEN transaction_date ~ '^[A-Za-z]+ \d{1,2}, \d{4}$' THEN TO_DATE(transaction_date, 'Month DD, YYYY')
    WHEN transaction_date ~ '^\d{5}(\.0)?$' THEN TO_DATE('1899-12-30', 'YYYY-MM-DD') + transaction_date::FLOAT::INT
    ELSE NULL
  END AS cleaned_date
FROM afritech_data
LIMIT 200;

--Adding this as a new column and naming it transaction_date_cleaned date
ALTER TABLE afritech_data ADD COLUMN transaction_date_cleaned DATE;
--updating my new column with the cleaned date
UPDATE afritech_data_deduped
SET transaction_date_cleaned = 
  CASE
    WHEN transaction_date ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(transaction_date, 'MM/DD/YYYY')
    WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$' THEN transaction_date::DATE
    WHEN transaction_date ~ '^\d{4}/\d{2}/\d{2}$' THEN TO_DATE(transaction_date, 'YYYY/MM/DD')
    WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4}$' THEN TO_DATE(transaction_date, 'DD-MM-YYYY')
    WHEN transaction_date ~ '^\d{2}/\d{2}/\d{4}$' THEN TO_DATE(transaction_date, 'DD/MM/YYYY')
    WHEN transaction_date ~ '^[A-Za-z]+ \d{1,2}, \d{4}$' THEN TO_DATE(transaction_date, 'Month DD, YYYY')
    WHEN transaction_date ~ '^\d{5}(\.0)?$' THEN TO_DATE('1899-12-30', 'YYYY-MM-DD') + transaction_date::FLOAT::INT
    ELSE NULL
  END;
 -- Verifying my update 
 SELECT transaction_date
FROM afritech_data_deduped
WHERE transaction_date_cleaned IS NULL;

-- step 2
-- updating my transaction year by etracting it from the cleaned transaction date 
UPDATE afritech_data_deduped
SET transaction_year = EXTRACT(YEAR FROM transaction_date_cleaned);

 -- Verifying my update 
 SELECT transaction_date_cleaned, transaction_year
FROM afritech_data_deduped
ORDER BY transaction_date_cleaned
LIMIT 200;
-- checking for accuracy on the data sets 
SELECT transaction_date_cleaned, transaction_year
FROM afritech_data_deduped
WHERE EXTRACT(YEAR FROM transaction_date_cleaned) != transaction_year;

--step 3 
--checking for blanks on interaction date 
SELECT interaction_date
FROM afritech_data_deduped
WHERE interaction_date IS  NULL
LIMIT 100;
--- syntax to let you see the cleaned version without updating
SELECT interaction_date,
  CASE
    WHEN interaction_date::TEXT ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(interaction_date::TEXT, 'MM/DD/YYYY')
    WHEN interaction_date::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN interaction_date::DATE
    WHEN interaction_date::TEXT ~ '^\d{5}(\.0)?$' THEN TO_DATE('1899-12-30', 'YYYY-MM-DD') + interaction_date::TEXT::FLOAT::INT
    ELSE NULL
  END AS cleaned_date
FROM afritech_data_deduped
LIMIT 200;

---Updating, cleaning and converting the interaction date  data type to date 
UPDATE afritech_data_deduped
SET interaction_date = 
  CASE
    WHEN interaction_date::TEXT ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(interaction_date::TEXT, 'MM/DD/YYYY')
    WHEN interaction_date::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN interaction_date::DATE
    WHEN interaction_date::TEXT ~ '^\d{5}(\.0)?$' THEN TO_DATE('1899-12-30', 'YYYY-MM-DD') + interaction_date::TEXT::FLOAT::INT
    ELSE NULL
  END;
-- changing the data type for interaction date to date
ALTER TABLE afritech_data_deduped
ALTER COLUMN interaction_date TYPE DATE
USING
  CASE
    WHEN interaction_date::TEXT ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(interaction_date::TEXT, 'MM/DD/YYYY')
    WHEN interaction_date::TEXT ~ '^\d{4}-\d{2}-\d{2}$' THEN interaction_date::DATE
    WHEN interaction_date::TEXT ~ '^\d{5}(\.0)?$' THEN TO_DATE('1899-12-30', 'YYYY-MM-DD') + interaction_date::TEXT::FLOAT::INT
    ELSE NULL
  END;
--- cleaning and convert crisiseventTime and firsttimeresponse and changing data type to date 
UPDATE afritech_data_deduped
SET crisis_event_time = CASE
  -- ISO format: 2023-01-15
  WHEN TRIM(crisis_event_time) ~ '^\d{4}-\d{2}-\d{2}$' THEN crisis_event_time::DATE

  -- U.S. format: 1/15/2023 or 01/15/2023
  WHEN TRIM(crisis_event_time) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(crisis_event_time), 'MM/DD/YYYY')

  -- Excel serials: 44235 or 44235.0
  WHEN TRIM(crisis_event_time) ~ '^\d{5}(\.0)?$' THEN TO_DATE('1899-12-30', 'YYYY-MM-DD') + TRIM(crisis_event_time)::FLOAT::INT

  -- Year-only values: 2023 → assume January 1st
  WHEN TRIM(crisis_event_time) ~ '^\d{4}$' THEN TO_DATE(TRIM(crisis_event_time) || '-01-01', 'YYYY-MM-DD')

  -- Anything else: treat as NULL
  ELSE NULL
END;

UPDATE afritech_data_deduped
SET first_response_time = CASE
  -- ISO format: 2023-01-15
  WHEN TRIM(first_response_time) ~ '^\d{4}-\d{2}-\d{2}$' THEN first_response_time::DATE

  -- U.S. format: 1/15/2023 or 01/15/2023
  WHEN TRIM(first_response_time) ~ '^\d{1,2}/\d{1,2}/\d{4}$' THEN TO_DATE(TRIM(first_response_time), 'MM/DD/YYYY')

  -- Excel serials: 44235 or 44235.0
  WHEN TRIM(first_response_time) ~ '^\d{5}(\.0)?$' THEN TO_DATE('1899-12-30', 'YYYY-MM-DD') + TRIM(first_response_time)::FLOAT::INT

  -- Year-only values: 2023 → assume January 1st
  WHEN TRIM(first_response_time) ~ '^\d{4}$' THEN TO_DATE(TRIM(first_response_time) || '-01-01', 'YYYY-MM-DD')

  -- Anything else: treat as NULL
  ELSE NULL
END;

-- changing the data type of crisiseventtime and firstresponsetime from text to date 
ALTER TABLE afritech_data_deduped
ALTER COLUMN first_response_time TYPE DATE
USING first_response_time::DATE;

ALTER TABLE afritech_data_deduped
ALTER COLUMN crisis_event_time TYPE DATE
USING crisis_event_time::DATE;
SELECT COUNT(*) FROM afritech_data;
SELECT COUNT(*) AS total_rows
FROM afritech_data_deduped;

-- verifying the update done on crisiseventtime and firstresponsetime
SELECT first_response_time, crisis_event_time
FROM afritech_data_deduped
ORDER BY first_response_time
LIMIT 100;
-- validating data types and ranges 
SELECT * FROM afritech_data_deduped WHERE Age < 0 OR Income < 0;
---Step 5

--Handling missing/null values for competitor_x
UPDATE afritech_data_deduped
SET Competitor_x = NULL
WHERE Competitor_x IS NULL OR TRIM(Competitor_x) = '';


-- checking data for duplicate 
SELECT COUNT(*) AS total_rows
FROM afritech_data_deduped;

SELECT COUNT(*) AS distinct_rows
FROM (
  SELECT DISTINCT *
  FROM afritech_data_deduped
) AS deduped;

--checking indidvidual customers and how many time and how many times they appear
-- on the data sets 
SELECT customer_id, transaction_date, product_purchased, COUNT(*) AS occurrences,
       COUNT(DISTINCT interaction_date) AS unique_interactions,
       COUNT(DISTINCT platform) AS unique_platforms,
       COUNT(DISTINCT product_recalled) AS recall_variants
FROM afritech_data_deduped
GROUP BY customer_id, transaction_date, product_purchased
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
--Total number of unique transactions, where a transaction is defined as a specific customer buying product on a specific date 
SELECT COUNT(*) AS unique_transactions
FROM (
  SELECT DISTINCT customer_id, transaction_date, product_purchased
  FROM afritech_data_deduped
) AS unique_tx;

--- Query to give one row per transactions, with a clear view of purchase details, interaction count and paltform and 
-- post type diversity, recall status consistency

SELECT customer_id, transaction_date, product_purchased,
       MAX(purchase_amount) AS purchase_amount,
       BOOL_OR(product_recalled) AS any_recalled,
       COUNT(*) AS interaction_count,
       ARRAY_AGG(DISTINCT platform) AS platforms,
       ARRAY_AGG(DISTINCT post_type) AS post_types
FROM afritech_data_deduped
GROUP BY customer_id, transaction_date, product_purchased;

--Spliting data sets table into Customer table, Transaction table and engagement tables
-- Table 1 (customer dimension table)
CREATE TABLE afritech_customer AS
SELECT DISTINCT customer_id, customer_name, region, age, income, customer_type
FROM afritech_data_deduped;

-- Table 2 (Transactional fact table )
CREATE TABLE afritech_transaction AS
SELECT DISTINCT customer_id,
       TO_DATE(transaction_date, 'MM/DD/YYYY') AS transaction_date,
       transaction_year, product_purchased, purchase_amount,
       product_recalled, competitor_x, sentiment,
       crisis_event_time, first_response_time,
       resolution_status, nps_response
FROM afritech_data_deduped;

--Table 3 (engagement dimensional table)
CREATE TABLE afritech_engagement AS
SELECT customer_id, TO_DATE(transaction_date, 'MM/DD/YYYY') AS transaction_date,
       product_purchased, interaction_date,
       platform, post_type, engagement_likes, engagement_shares,
       engagement_comments, user_followers, influencer_score,
       brand_mention, competitor_mention
FROM afritech_data_deduped;

--- joining all three tables
SELECT 
  c.customer_id, c.customer_name, c.region, c.age, c.income, c.customer_type,
  t.transaction_date, t.transaction_year, t.product_purchased, t.purchase_amount,
  t.product_recalled, t.sentiment, t.nps_response, t.resolution_status,
  e.interaction_date, e.platform, e.post_type, e.engagement_likes,
  e.engagement_shares, e.engagement_comments, e.user_followers,
  e.influencer_score, e.brand_mention, e.competitor_mention
FROM afritech_engagement e
JOIN afritech_transaction t
  ON e.customer_id = t.customer_id
  AND e.transaction_date = t.transaction_date
  AND e.product_purchased = t.product_purchased
JOIN afritech_customer c
  ON e.customer_id = c.customer_id;

--uncovering relationship trends with aggregations & joins doing exploratory analysis 
--•	Engagement vs. Revenue
SELECT platform, TO_CHAR(sum(purchase_amount), 'FM$999,999,999.00') AS Total_revenue FROM afritech_data_deduped 
GROUP BY platform ORDER by Total_revenue DESC; 

--•	Sentiment vs. NPS
SELECT sentiment, ROUND(AVG(nps_response), 3) AS avg_nps
FROM afritech_data_deduped
GROUP BY sentiment;

-- • Influencer Score Impact
SELECT influencer_score, TO_CHAR(AVG(purchase_amount), 'FM$999,999,999.00') AS avg_revenue FROM afritech_data_deduped 
GROUP BY influencer_score
ORDER by influencer_score DESC; 

-- Revenue by Product
SELECT product_purchased, TO_CHAR(SUM(purchase_amount), 'FM$999,999,999.00') AS total_revenue
FROM afritech_transaction
GROUP BY product_purchased
ORDER BY SUM(purchase_amount) DESC; 

SELECT COUNT(*) FROM afritech_transaction; --48,062 rows
SELECT COUNT(*) FROM afritech_data_deduped; --- 73,586 rows

-- Engagement by Platform
SELECT platform, COUNT(*) AS total_interactions, round(AVG(influencer_score),3) AS 
avg_influencer_score FROM afritech_engagement GROUP BY platform ORDER BY 
total_interactions DESC; 

-- Customer Type vs. Revenue
SELECT c.customer_type, TO_CHAR(SUM(t.purchase_amount), 'FM$999,999,999.00') AS total_revenue, FROM 
afritech_transaction t JOIN afritech_customer c ON t.customer_id = c.customer_id 
GROUP BY c.customer_type; 

-- Analyzing Customer Behavior & Performance Metrics
--NPS by Region
SELECT c.region, round(AVG(t.nps_response),2) AS avg_nps FROM afritech_transaction t JOIN 
afritech_customer c ON t.customer_id = c.customer_id GROUP BY c.region order by avg_nps DESC; 

--Sentiment vs. Purchase Amount
SELECT sentiment, round(AVG(purchase_amount),2) AS avg_spend FROM afritech_transaction
GROUP BY sentiment; 

--Crisis Impact on Resolution
SELECT resolution_status, COUNT(*) AS cases,
ROUND(AVG(EXTRACT(EPOCH FROM (first_response_time::timestamp - crisis_event_time::timestamp)) / 86400), 2) AS avg_response_days
FROM afritech_transaction
WHERE crisis_event_time IS NOT NULL 
  AND first_response_time IS NOT NULL
GROUP BY resolution_status;

--Exploratory Data Analysis based on business objectives 
-- Brand Reputation & Market Position
-- Focus: Sentiment, competitor mentions, influencer score
SELECT t.sentiment,e.platform,round(AVG(e.influencer_score),3) AS avg_influencer_score, COUNT(*) AS mentions, 
  SUM(CASE WHEN e.competitor_mention THEN 1 ELSE 0 END) AS competitor_mentions
FROM afritech_transaction t
JOIN afritech_engagement e ON t.customer_id = e.customer_id 
GROUP BY t.sentiment, e.platform
ORDER BY mentions DESC; 

--Customer Retention & Satisfaction
-- Focus: NPS, resolution status, repeat purchases
SELECT c.customer_type, ROUND(AVG(t.nps_response), 2) AS avg_nps, 
  ROUND(AVG(CASE WHEN t.resolution_status THEN 1 ELSE 0 END), 2) AS resolution_rate
FROM afritech_transaction t
JOIN afritech_customer c ON t.customer_id = c.customer_id
GROUP BY c.customer_type
ORDER BY avg_nps DESC; 

--Crisis Mitigation
--Focus: Response time, resolution status, crisis frequency
SELECT ROUND(AVG(EXTRACT(EPOCH FROM (first_response_time::timestamp - crisis_event_time::timestamp)) / 86400), 2) 
AS avg_response_days, COUNT(*) FILTER (WHERE resolution_status = FALSE) AS unresolved_cases 
FROM afritech_transaction WHERE crisis_event_time IS NOT NULL; 

--Data-Driven Decision-Making
--Focus: Sentiment vs. revenue, engagement vs. NPS
SELECT sentiment, round(AVG(purchase_amount),2) AS avg_spend, round(AVG(nps_response),2) AS 
avg_nps FROM afritech_transaction GROUP BY sentiment ORDER BY avg_spend DESC; 

--Enhanced Marketing Effectiveness
--Focus: Platform performance, post type impact, engagement metrics
SELECT platform, post_type, 
  ROUND(AVG(engagement_likes + engagement_shares + engagement_comments), 2) AS avg_engagement
FROM afritech_engagement
GROUP BY platform, post_type
ORDER BY avg_engagement DESC;

-- Monthly revenue
SELECT 
  TO_CHAR(month, 'Mon YYYY') AS month_name,
  TO_CHAR(monthly_revenue, 'FM$999,999,999.00') AS monthly_revenue
FROM (
  SELECT DATE_TRUNC('month', transaction_date) AS month,
         SUM(purchase_amount)::NUMERIC AS monthly_revenue
  FROM afritech_transaction
  GROUP BY DATE_TRUNC('month', transaction_date)
) AS monthly_summary
ORDER BY month;

-- Monthly sentiment trend
SELECT 
  TO_CHAR(month, 'Mon YYYY') AS month_name,
  sentiment,
  sentiment_count
FROM (
  SELECT 
    DATE_TRUNC('month', transaction_date) AS month,
    sentiment,
    COUNT(*) AS sentiment_count
  FROM afritech_transaction
  GROUP BY DATE_TRUNC('month', transaction_date), sentiment
) AS sentiment_summary
ORDER BY month;

select sum(purchase_amount) from afritech_transaction




