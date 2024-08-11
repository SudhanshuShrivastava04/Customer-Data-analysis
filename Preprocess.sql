CREATE SCHEMA starbucks_customer_data;

-- portfolio_proc: after data cleanning

CREATE TABLE PORTFOLIO_PROC AS
SELECT
    reward,
    difficulty,
    duration,
    TRIM(UPPER(offer_type)) AS offer_type,
    id AS offer_id,
    CASE
        WHEN JSON_CONTAINS(channels, '["web"]') THEN 1
        ELSE 0
    END AS channel_web,
    CASE
        WHEN JSON_CONTAINS(channels, '["email"]') THEN 1
        ELSE 0
    END AS channel_email,
    CASE
        WHEN JSON_CONTAINS(channels, '["social"]') THEN 1
        ELSE 0
    END AS channel_social,
    CASE
        WHEN JSON_CONTAINS(channels, '["mobile"]') THEN 1
        ELSE 0
    END AS channel_mobile
FROM portfolio;

-- profile: convert DATETIME to DATE

ALTER TABLE profile MODIFY COLUMN became_member_on DATE;

-- Change mode

SHOW VARIABLES LIKE 'sql_mode';

SET sql_mode = 'NO_ENGINE_SUBSTITUTION';

-- profile_proc: after data cleaning

DROP TABLE PROFILE_PROC;

CREATE TABLE PROFILE_PROC AS
SELECT
    MyUnknownColumn,
    CASE
        WHEN GENDER IN ('F', 'M', 'O') THEN TRIM(UPPER(GENDER))
        ELSE 'U'
    END AS gender,
    age,
    id AS customer_id,
    became_member_on,
    YEAR(became_member_on) AS became_member_year,
    CAST(income AS SIGNED) AS income_zero,
    CAST(NULLIF(income, '') AS SIGNED) AS income_null
FROM profile
WHERE age != 118;

-- transcript_proc: after data cleaning

DROP TABLE transcript_proc;

CREATE TABLE TRANSCRIPT_PROC AS
SELECT
    `Unnamed: 0` AS MyUnknownColumn,
    person,
    event,
    COALESCE(
        REPLACE (value -> '$."offer id"', '"', ''),
        REPLACE (value -> '$."offer_id"', '"', '')
    ) AS offer_id,
    value -> '$."amount"' AS amount,
    value -> '$."reward"' AS reward,
    time
FROM transcript
WHERE person in (
        SELECT customer_id
        FROM profile_proc
    );

-- convert time (hours since become member) of transaction to DATETIME

DROP VIEW transcript_proc_view;

CREATE VIEW TRANSCRIPT_PROC_VIEW AS 
	SELECT
	    trans.*,
	    DATE_ADD(
	        prof.became_member_on,
	        INTERVAL trans.time HOUR
	    ) AS time_date
	FROM transcript_proc AS trans
	    LEFT JOIN profile_proc AS prof ON trans.person = prof.customer_id
; 