-- date range: from 2013-07-29 to 2018-07-26

SELECT
    MIN(became_member_on),
    MAX(became_member_on)
FROM profile_proc;

-- percentage of each gender

SELECT
    DISTINCT gender,
    COUNT(gender) OVER() AS total,
    COUNT(gender) OVER(PARTITION BY gender) AS each_gender,
    COUNT(gender) OVER(PARTITION BY gender) / COUNT(gender) OVER() AS gender_percentage
FROM profile_proc;

-- percentage of each gender above certain age

DELIMITER //

CREATE PROCEDURE GENDER_PERCENTAGE_BY_AGE(IN INPUT_AGE 
INT) BEGIN 
	SELECT
	    DISTINCT gender,
	    COUNT(gender) OVER() AS total,
	    COUNT(gender) OVER(PARTITION BY gender) AS each_gender,
	    COUNT(gender) OVER(PARTITION BY gender) / (
	        SELECT COUNT(*)
	        FROM
	            profile_proc
	    ) AS gender_percentage
	FROM profile_proc
	WHERE age >= input_age;
	END// 


DELIMITER;

CALL `GENDER_PERCENTAGE_BY_AGE`(60);

-- age from 18 to 118 (abnormal)

SELECT age, COUNT(age)
FROM profile_proc
GROUP BY age
ORDER BY age ASC;

-- 2175 customers have age 118, gender U, and income NULL

-- Delete abnormal data, total 2175 (in Preprocess)

SELECT gender, COUNT(*)
FROM profile_proc
WHERE age = 118
GROUP BY gender;

SELECT COUNT(*) FROM profile_proc WHERE income_null is NULL;

-- age distribution

WITH group_age AS (
        SELECT
            *,
            CASE
                WHEN age < 30 THEN 'lese than 30'
                WHEN age >= 30
                AND age < 40 THEN '30-40'
                WHEN age >= 40
                AND age < 50 THEN '40-50'
                WHEN age >= 50
                AND age < 60 THEN '50-60'
                WHEN age >= 60
                AND age < 70 THEN '60-70'
                WHEN age >= 70 THEN 'greater than 70'
                ELSE NULL
            END AS age_group
        FROM profile_proc
    )
SELECT
    DISTINCT age_group,
    COUNT(*) OVER() AS total_count,
    COUNT(*) OVER(PARTITION BY age_group) / COUNT(*) OVER() AS age_group_percentage
FROM group_age;

-- became_member_on YEAR_MONTH distribution

WITH `year_month` AS (
        SELECT
            *,
            EXTRACT(
                YEAR_MONTH
                FROM
                    became_member_on
            ) AS date_v1,
            DATE_FORMAT(became_member_on, "%Y-%m") AS date_v2
        FROM profile_proc
    )
SELECT
    DISTINCT date_v1,
    date_v2,
    COUNT(*) OVER() AS total,
    COUNT(*) OVER(PARTITION BY date_v2) / COUNT(*) OVER() AS percentage
FROM `year_month`;

-- 10 different offers in portfolio table

SELECT COUNT(DISTINCT offer_id) AS num_id FROM portfolio_proc;

-- BOGO & DISCOUNT each has 4; INFORMATIONAL has 2

SELECT
    offer_type,
    COUNT(offer_id) AS num_per_type
FROM portfolio_proc
GROUP BY offer_type;

-- Extract each offer_type, the most difficult offer_id and it's reward and duration

SELECT *
FROM (
        SELECT
            reward,
            difficulty,
            duration,
            offer_type,
            offer_id,
            DENSE_RANK() OVER(
                PARTITION BY offer_type
                ORDER BY
                    difficulty DESC
            ) AS difficulty_rank
        FROM
            portfolio_proc
    ) AS temp
WHERE difficulty_rank = 1;

-- Extract duration rank 1 and 2

SELECT
    offer_type,
    MAX(
        CASE
            WHEN difficulty_rank = 1 THEN duration
        END
    ) AS `rank1_duration`,
    MAX(
        CASE
            WHEN difficulty_rank = 2 THEN duration
        END
    ) AS `rank2_duration`
FROM (
        SELECT
            *,
            ROW_NUMBER() OVER(
                PARTITION BY offer_type
                ORDER BY
                    difficulty DESC
            ) AS difficulty_rank
        FROM
            portfolio_proc
    ) AS temp
GROUP BY offer_type;

-- Number of customers completed/not completed all processes: offer received -> offer view -> offer complete

CREATE VIEW PERSON_EVENT AS 
	SELECT
	    person,
	    JSON_ARRAYAGG(event) AS combined_event
	FROM (
	        SELECT
	            DISTINCT person,
	            event
	        FROM
	            transcript_proc
	    ) AS temp
	GROUP BY
PERSON; 

DROP PROCEDURE COUNT_PROCESS_COMPLETE_STATUS;

DELIMITER //

CREATE PROCEDURE COUNT_PROCESS_COMPLETE_STATUS(IN COMPLETED 
INT, OUT OUTPUT_COUNT INT) BEGIN 
	SELECT
	    COUNT(*) INTO output_count
	FROM person_event
	WHERE
	    JSON_CONTAINS(
	        combined_event,
	        JSON_ARRAY(
	            'offer received',
	            'offer viewed',
	            'offer completed'
	        )
	    ) = completed;
	END// 


DELIMITER;

-- num completed

CALL `COUNT_PROCESS_COMPLETE_STATUS`(1, @num_completed);

SELECT @num_completed;

-- num not completed

CALL `COUNT_PROCESS_COMPLETE_STATUS`(0, @num_not_complete);

SELECT @num_not_complete;

DROP PROCEDURE DISTRB_PROCESS_COMPLETE;

-- Procedure for calculating distribution

DELIMITER //

CREATE PROCEDURE DISTRB_PROCESS_COMPLETE(IN COMPLETED 
INT, IN LEFT_TABLE_NAME VARCHAR(255), IN RIGHT_TABLE_NAME 
VARCHAR(255), IN COL_NAME VARCHAR(255)) BEGIN 
	SET
	    @sql = CONCAT(
	        "SELECT DISTINCT ",
	        COL_NAME,
	        ", COUNT(",
	        COL_NAME,
	        ") OVER(PARTITION BY ",
	        COL_NAME,
	        ") AS each_num, COUNT(",
	        COL_NAME,
	        ") OVER() AS total, COUNT(",
	        COL_NAME,
	        ") OVER(PARTITION BY ",
	        COL_NAME,
	        ") / COUNT(",
	        COL_NAME,
	        ") OVER() AS percentage FROM ",
	        LEFT_TABLE_NAME,
	        " AS pe LEFT JOIN ",
	        RIGHT_TABLE_NAME,
	        " AS pp ON pe.person = pp.customer_id WHERE JSON_CONTAINS( combined_event, JSON_ARRAY( 'offer received', 'offer viewed', 'offer completed' ) ) = ",
	        COMPLETED,
	        ";"
	    );
	PREPARE stmt FROM @sql;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	END// 


DELIMITER;

-- gender distribution of completed

CALL
    `DISTRB_PROCESS_COMPLETE`(
        1,
        'person_event',
        'profile_proc',
        'gender'
    );

-- gender distribution of not completed

CALL`DISTRB_PROCESS_COMPLETE`(0, 'person_event', 'gender');

-- income distribution of those completed

DROP VIEW income_distrib;

CREATE VIEW INCOME_DISTRIB AS 
	SELECT
	    *,
	    CASE
	        WHEN income_null < 50000 THEN 'low income'
	        WHEN income_null >= 50000
	        AND income_null < 90000 THEN 'medium income'
	        WHEN income_null >= 90000 THEN 'high income'
	        ELSE NULL
	    END AS income_group
	FROM
PROFILE_PROC; 

CALL
    `DISTRB_PROCESS_COMPLETE`(
        1,
        'person_event',
        'income_distrib',
        'income_group'
    );

-- income distribution of those not completed

CALL
    `DISTRB_PROCESS_COMPLETE`(
        0,
        'person_event',
        'income_distrib',
        'income_group'
    );

-- count number of offer received, offer view, and offer complete for each customer

WITH num_offer AS (
        SELECT
            person,
            SUM(
                CASE
                    WHEN event = 'offer received' THEN 1
                    ELSE 0
                END
            ) AS num_offer_received,
            SUM(
                CASE
                    WHEN event = 'offer viewed' THEN 1
                    ELSE 0
                END
            ) AS num_offer_viewed,
            SUM(
                CASE
                    WHEN event = 'offer completed' THEN 1
                    ELSE 0
                END
            ) AS num_offer_completed
        FROM transcript_proc
        GROUP BY person;

-- count number each reward/difficulty/duration for each offer type

DELIMITER //

CREATE PROCEDURE COUNT_EACH_OFFER(IN COL VARCHAR(255
)) BEGIN 
	SELECT
	    *,
	    COUNT(offer_id) OVER(PARTITION BY difficulty) AS difficulty_range,
	    COUNT(offer_id) OVER(PARTITION BY reward) AS reward_range,
	    COUNT(offer_id) OVER(PARTITION BY duration) AS duration_range
	FROM portfolio_proc
	WHERE offer_type = COL;
	END// 


DELIMITER;

CALL `COUNT_EACH_OFFER`("DISCOUNT");

SELECT
    *,
    COUNT(offer_id) OVER(PARTITION BY difficulty) AS difficulty_range,
    COUNT(offer_id) OVER(PARTITION BY reward) AS reward_range,
    COUNT(offer_id) OVER(PARTITION BY duration) AS duration_range
FROM portfolio_proc
WHERE offer_type = "DISCOUNT"
UNION ALL
SELECT
    *,
    COUNT(offer_id) OVER(PARTITION BY difficulty) AS difficulty_range,
    COUNT(offer_id) OVER(PARTITION BY reward) AS reward_range,
    COUNT(offer_id) OVER(PARTITION BY duration) AS duration_range
FROM portfolio_proc
WHERE offer_type = "BOGO"
UNION ALL
SELECT
    *,
    COUNT(offer_id) OVER(PARTITION BY difficulty) AS difficulty_range,
    COUNT(offer_id) OVER(PARTITION BY reward) AS reward_range,
    COUNT(offer_id) OVER(PARTITION BY duration) AS duration_range
FROM portfolio_proc
WHERE
    offer_type = "INFORMATIONAL";

SELECT
    gender,
    COUNT(gender) OVER(PARTITION BY gender) AS each_gender_num,
    COUNT(gender) OVER() AS total_gender,
    COUNT(gender) OVER(PARTITION BY gender) / COUNT(gender) OVER() AS `gender percentage`
FROM person_event AS pe
    LEFT JOIN profile_proc AS pp ON pe.person = pp.customer_id
WHERE
    JSON_CONTAINS(
        combined_event,
        JSON_ARRAY(
            'offer received',
            'offer viewed',
            'offer completed'
        )
    ) = 1;

SELECT
    *,
    CASE
        WHEN JSON_CONTAINS(
            combined_event,
            JSON_ARRAY(
                'offer received',
                'offer viewed',
                'offer completed'
            )
        ) = 1 THEN 1
        ELSE 0
    END AS complete_offer
FROM person_event AS pe
    LEFT JOIN profile_proc AS pp ON pe.person = pp.customer_id;