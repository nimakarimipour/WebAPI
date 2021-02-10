--------------------------------------------------------------------------------
-- Create cohort table
--------------------------------------------------------------------------------

create table omop_orc_results_280.temp_cohort_y2f4nuwx1
(
  COHORT_DEFINITION_ID INT,
  SUBJECT_ID BIGINT,
  cohort_start_date TIMESTAMP,
  cohort_end_date TIMESTAMP
);

--------------------------------------------------------------------------------
-- Generate cohort
--------------------------------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS Codesets  (codeset_id INT,
  concept_id BIGINT
)
;

INSERT INTO Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from omop_orc.CONCEPT where concept_id in (1177480)
UNION  select c.concept_id
  from omop_orc.CONCEPT c
  join omop_orc.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (1177480)
  and c.invalid_reason is null

) I
) C
;

DROP TABLE IF EXISTS primary_events  ;

CREATE TEMPORARY TABLE primary_events  AS (
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.DRUG_EXPOSURE_END_DATE, DATE_ADD(CAST(DRUG_EXPOSURE_START_DATE AS TIMESTAMP), C.DAYS_SUPPLY), DATE_ADD(CAST(C.DRUG_EXPOSURE_START_DATE AS TIMESTAMP), 1)) as end_date,
       C.visit_occurrence_id,C.drug_exposure_start_date as sort_date
from 
(
  select de.* 
  FROM omop_orc.DRUG_EXPOSURE de
JOIN Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Drug Exposure Criteria

  ) E
  JOIN omop_orc.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATE_ADD(CAST(OP.OBSERVATION_PERIOD_START_DATE AS TIMESTAMP), 0) <= E.START_DATE AND DATE_ADD(CAST(E.START_DATE AS TIMESTAMP), 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P

-- End Primary Events

)
;
CREATE TEMPORARY TABLE qualified_events
 AS SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
 FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

CREATE TEMPORARY TABLE IF NOT EXISTS Inclusion_0
 AS
SELECT
0 as inclusion_rule_id, person_id, event_id

FROM
(
  select pe.person_id, pe.event_id
  FROM qualified_events pe
  
JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id 
  FROM qualified_events E
  INNER JOIN
  (
    -- Begin Demographic Criteria
SELECT 0 as index_id, e.person_id, e.event_id
FROM qualified_events E
JOIN omop_orc.PERSON P ON P.PERSON_ID = E.PERSON_ID
WHERE P.gender_concept_id in (8507)
GROUP BY e.person_id, e.event_id
-- End Demographic Criteria

UNION ALL
-- Begin Demographic Criteria
SELECT 1 as index_id, e.person_id, e.event_id
FROM qualified_events E
JOIN omop_orc.PERSON P ON P.PERSON_ID = E.PERSON_ID
WHERE YEAR(E.start_date) - P.year_of_birth > 17
GROUP BY e.person_id, e.event_id
-- End Demographic Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 2
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

CREATE TEMPORARY TABLE IF NOT EXISTS inclusion_events
 AS
SELECT
inclusion_rule_id, person_id, event_id

FROM
(select inclusion_rule_id, person_id, event_id from Inclusion_0) I;
TRUNCATE TABLE Inclusion_0;
DROP TABLE Inclusion_0;


DROP TABLE IF EXISTS cteIncludedEvents ;

CREATE TEMPORARY TABLE cteIncludedEvents AS (
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from qualified_events Q
    LEFT JOIN inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),1)-1)

)
;
CREATE TEMPORARY TABLE included_events
 AS SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;



-- generate cohort periods into #final_cohort
DROP TABLE IF EXISTS cohort_ends  ;

DROP TABLE IF EXISTS cohort_ends   ; DROP TABLE IF EXISTS first_ends ; CREATE TEMPORARY TABLE cohort_ends  AS (select event_id, person_id, op_end_date as end_date from included_events
)
;
CREATE TEMPORARY TABLE first_ends  AS (select F.person_id, F.start_date, F.end_date
  FROM (
    select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
    from included_events I
    join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
  ) F
  WHERE F.ordinal = 1
)
;
CREATE TEMPORARY TABLE cohort_rows
 AS SELECT person_id, start_date, end_date
 FROM first_ends;

DROP TABLE IF EXISTS cteEndDates  ;

DROP TABLE IF EXISTS cteEndDates   ; DROP TABLE IF EXISTS cteEnds ; CREATE TEMPORARY TABLE cteEndDates  AS (SELECT
    person_id
    , DATE_ADD(CAST(event_date AS TIMESTAMP), -1 * 0)  as end_date
  FROM
  (
    SELECT
      person_id
      , event_date
      , event_type
      , MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
      , ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
    FROM
    (
      SELECT
        person_id
        , start_date AS event_date
        , -1 AS event_type
        , ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
      FROM cohort_rows
    
      UNION ALL
    

      SELECT
        person_id
        , DATE_ADD(CAST(end_date AS TIMESTAMP), 0) as end_date
        , 1 AS event_type
        , NULL
      FROM cohort_rows
    ) RAWDATA
  ) e
  WHERE (2 * e.start_ordinal) - e.overall_ord = 0
)
;
CREATE TEMPORARY TABLE cteEnds  AS (SELECT
     c.person_id
    , c.start_date
    , MIN(e.end_date) AS end_date
  FROM cohort_rows c
  JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
  GROUP BY c.person_id, c.start_date
)
;
CREATE TEMPORARY TABLE final_cohort
 AS SELECT person_id, min(start_date) as start_date, end_date
 FROM cteEnds
group by person_id, end_date
;

DELETE FROM omop_orc_results_280.cohort_cache where design_hash = -1914464508;
INSERT INTO omop_orc_results_280.cohort_cache (design_hash, subject_id, cohort_start_date, cohort_end_date)
select -1914464508 as design_hash, person_id, start_date, end_date 
FROM final_cohort CO
;

-- BEGIN: Censored Stats

delete from omop_orc_results_280.cohort_censor_stats_cache where design_hash = -1914464508;

-- END: Censored Stats



-- Create a temp table of inclusion rule rows for joining in the inclusion rule impact analysis

CREATE TEMPORARY TABLE IF NOT EXISTS inclusion_rules
 AS
SELECT
cast(rule_sequence as int) as rule_sequence

FROM
(
  SELECT CAST(0 as int) as rule_sequence
) IR;


-- Find the event that is the 'best match' per person.  
-- the 'best match' is defined as the event that satisfies the most inclusion rules.
-- ties are solved by choosing the event that matches the earliest inclusion rule, and then earliest.

CREATE TEMPORARY TABLE IF NOT EXISTS best_events
 AS
SELECT
q.person_id, q.event_id

FROM
qualified_events Q
join (
  SELECT R.person_id, R.event_id, ROW_NUMBER() OVER (PARTITION BY R.person_id ORDER BY R.rule_count DESC,R.min_rule_id ASC, R.start_date ASC) AS rank_value
  FROM (
    SELECT Q.person_id, Q.event_id, COALESCE(COUNT(DISTINCT I.inclusion_rule_id), 0) AS rule_count, COALESCE(MIN(I.inclusion_rule_id), 0) AS min_rule_id, Q.start_date
    FROM qualified_events Q
    LEFT JOIN inclusion_events I ON q.person_id = i.person_id AND q.event_id = i.event_id
    GROUP BY Q.person_id, Q.event_id, Q.start_date
  ) R
) ranked on Q.person_id = ranked.person_id and Q.event_id = ranked.event_id
WHERE ranked.rank_value = 1
;

-- modes of generation: (the same tables store the results for the different modes, identified by the mode_id column)
-- 0: all events
-- 1: best event


-- BEGIN: Inclusion Impact Analysis - event
-- calculte matching group counts
delete from omop_orc_results_280.cohort_inclusion_result_cache where design_hash = -1914464508 and mode_id = 0;
insert into omop_orc_results_280.cohort_inclusion_result_cache (design_hash, inclusion_rule_mask, person_count, mode_id)
select -1914464508 as design_hash, inclusion_rule_mask, COUNT(*) as person_count, 0 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from qualified_events Q
  LEFT JOIN inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from omop_orc_results_280.cohort_inclusion_stats_cache where design_hash = -1914464508 and mode_id = 0;
insert into omop_orc_results_280.cohort_inclusion_stats_cache (design_hash, rule_sequence, person_count, gain_count, person_total, mode_id)
select -1914464508 as design_hash, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 0 as mode_id
from inclusion_rules ir
left join
(
  select i.inclusion_rule_id, COUNT(i.event_id) as person_count
  from qualified_events Q
  JOIN inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
CROSS JOIN (select COUNT(event_id) as total from qualified_events) EventTotal
LEFT JOIN omop_orc_results_280.cohort_inclusion_result_cache SR on SR.mode_id = 0 AND SR.design_hash = -1914464508 AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from omop_orc_results_280.cohort_summary_stats_cache where design_hash = -1914464508 and mode_id = 0;
insert into omop_orc_results_280.cohort_summary_stats_cache (design_hash, base_count, final_count, mode_id)
select -1914464508 as design_hash, PC.total as person_count, coalesce(FC.total, 0) as final_count, 0 as mode_id
FROM
(select COUNT(event_id) as total from qualified_events) PC,
(select sum(sr.person_count) as total
  from omop_orc_results_280.cohort_inclusion_result_cache sr
  CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
  where sr.mode_id = 0 and sr.design_hash = -1914464508 and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - event

-- BEGIN: Inclusion Impact Analysis - person
-- calculte matching group counts
delete from omop_orc_results_280.cohort_inclusion_result_cache where design_hash = -1914464508 and mode_id = 1;
insert into omop_orc_results_280.cohort_inclusion_result_cache (design_hash, inclusion_rule_mask, person_count, mode_id)
select -1914464508 as design_hash, inclusion_rule_mask, COUNT(*) as person_count, 1 as mode_id
from
(
  select Q.person_id, Q.event_id, CAST(SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) AS bigint) as inclusion_rule_mask
  from best_events Q
  LEFT JOIN inclusion_events I on q.person_id = i.person_id and q.event_id = i.event_id
  GROUP BY Q.person_id, Q.event_id
) MG -- matching groups
group by inclusion_rule_mask
;

-- calculate gain counts 
delete from omop_orc_results_280.cohort_inclusion_stats_cache where design_hash = -1914464508 and mode_id = 1;
insert into omop_orc_results_280.cohort_inclusion_stats_cache (design_hash, rule_sequence, person_count, gain_count, person_total, mode_id)
select -1914464508 as design_hash, ir.rule_sequence, coalesce(T.person_count, 0) as person_count, coalesce(SR.person_count, 0) gain_count, EventTotal.total, 1 as mode_id
from inclusion_rules ir
left join
(
  select i.inclusion_rule_id, COUNT(i.event_id) as person_count
  from best_events Q
  JOIN inclusion_events i on Q.person_id = I.person_id and Q.event_id = i.event_id
  group by i.inclusion_rule_id
) T on ir.rule_sequence = T.inclusion_rule_id
CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
CROSS JOIN (select COUNT(event_id) as total from best_events) EventTotal
LEFT JOIN omop_orc_results_280.cohort_inclusion_result_cache SR on SR.mode_id = 1 AND SR.design_hash = -1914464508 AND (POWER(cast(2 as bigint),RuleTotal.total_rules) - POWER(cast(2 as bigint),ir.rule_sequence) - 1) = SR.inclusion_rule_mask -- POWER(2,rule count) - POWER(2,rule sequence) - 1 is the mask for 'all except this rule'
;

-- calculate totals
delete from omop_orc_results_280.cohort_summary_stats_cache where design_hash = -1914464508 and mode_id = 1;
insert into omop_orc_results_280.cohort_summary_stats_cache (design_hash, base_count, final_count, mode_id)
select -1914464508 as design_hash, PC.total as person_count, coalesce(FC.total, 0) as final_count, 1 as mode_id
FROM
(select COUNT(event_id) as total from best_events) PC,
(select sum(sr.person_count) as total
  from omop_orc_results_280.cohort_inclusion_result_cache sr
  CROSS JOIN (select count(*) as total_rules from inclusion_rules) RuleTotal
  where sr.mode_id = 1 and sr.design_hash = -1914464508 and sr.inclusion_rule_mask = POWER(cast(2 as bigint),RuleTotal.total_rules)-1
) FC
;

-- END: Inclusion Impact Analysis - person

TRUNCATE TABLE best_events;
DROP TABLE best_events;

TRUNCATE TABLE inclusion_rules;
DROP TABLE inclusion_rules;




TRUNCATE TABLE cohort_rows;
DROP TABLE cohort_rows;

TRUNCATE TABLE final_cohort;
DROP TABLE final_cohort;

TRUNCATE TABLE inclusion_events;
DROP TABLE inclusion_events;

TRUNCATE TABLE qualified_events;
DROP TABLE qualified_events;

TRUNCATE TABLE included_events;
DROP TABLE included_events;

TRUNCATE TABLE Codesets;
DROP TABLE Codesets;

--------------------------------------------------------------------------------
-- Generate cohort
--------------------------------------------------------------------------------

CREATE TEMPORARY TABLE IF NOT EXISTS Codesets  (codeset_id INT,
  concept_id BIGINT
)
;

INSERT INTO Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from omop_orc.CONCEPT where concept_id in (21500148,21600712)
UNION  select c.concept_id
  from omop_orc.CONCEPT c
  join omop_orc.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (21500148,21600712)
  and c.invalid_reason is null

) I
) C UNION ALL 
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
  select concept_id from omop_orc.CONCEPT where concept_id in (201820)
UNION  select c.concept_id
  from omop_orc.CONCEPT c
  join omop_orc.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (201820)
  and c.invalid_reason is null

) I
) C
;

DROP TABLE IF EXISTS primary_events  ;

CREATE TEMPORARY TABLE primary_events  AS (
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM 
  (
  -- Begin Condition Era Criteria
select C.person_id, C.condition_era_id as event_id, C.condition_era_start_date as start_date,
       C.condition_era_end_date as end_date, CAST(NULL as bigint) as visit_occurrence_id,
       C.condition_era_start_date as sort_date
from 
(
  select ce.* 
  FROM omop_orc.CONDITION_ERA ce
where ce.condition_concept_id in (SELECT concept_id from  Codesets where codeset_id = 2)
) C


-- End Condition Era Criteria

  ) E
  JOIN omop_orc.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATE_ADD(CAST(OP.OBSERVATION_PERIOD_START_DATE AS TIMESTAMP), 0) <= E.START_DATE AND DATE_ADD(CAST(E.START_DATE AS TIMESTAMP), 0) <= OP.OBSERVATION_PERIOD_END_DATE
) P
WHERE P.ordinal = 1
-- End Primary Events

)
;
CREATE TEMPORARY TABLE qualified_events
 AS SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
 FROM 
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe
  
) QE

;

--- Inclusion Rule Inserts

CREATE TEMPORARY TABLE IF NOT EXISTS inclusion_events  (inclusion_rule_id bigint,
  person_id bigint,
  event_id bigint
);

DROP TABLE IF EXISTS cteIncludedEvents ;

CREATE TEMPORARY TABLE cteIncludedEvents AS (
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from qualified_events Q
    LEFT JOIN inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

)
;
CREATE TEMPORARY TABLE included_events
 AS SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date
 FROM cteIncludedEvents Results
WHERE Results.ordinal = 1
;

-- date offset strategy

CREATE TEMPORARY TABLE IF NOT EXISTS strategy_ends
 AS
SELECT
event_id, person_id, 
  case when DATE_ADD(CAST(start_date AS TIMESTAMP), 1095) > op_end_date then op_end_date else DATE_ADD(CAST(start_date AS TIMESTAMP), 1095) end as end_date

FROM
included_events;


-- generate cohort periods into #final_cohort
DROP TABLE IF EXISTS cohort_ends  ;

DROP TABLE IF EXISTS cohort_ends   ; DROP TABLE IF EXISTS first_ends ; CREATE TEMPORARY TABLE cohort_ends  AS (SELECT event_id, person_id, end_date from strategy_ends

)
;
CREATE TEMPORARY TABLE first_ends  AS (select F.person_id, F.start_date, F.end_date
  FROM (
    select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal 
    from included_events I
    join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
  ) F
  WHERE F.ordinal = 1
)
;
CREATE TEMPORARY TABLE cohort_rows
 AS SELECT person_id, start_date, end_date
 FROM first_ends;

DROP TABLE IF EXISTS cteEndDates  ;

DROP TABLE IF EXISTS cteEndDates   ; DROP TABLE IF EXISTS cteEnds ; CREATE TEMPORARY TABLE cteEndDates  AS (SELECT
    person_id
    , DATE_ADD(CAST(event_date AS TIMESTAMP), -1 * 0)  as end_date
  FROM
  (
    SELECT
      person_id
      , event_date
      , event_type
      , MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal 
      , ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
    FROM
    (
      SELECT
        person_id
        , start_date AS event_date
        , -1 AS event_type
        , ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
      FROM cohort_rows
    
      UNION ALL
    

      SELECT
        person_id
        , DATE_ADD(CAST(end_date AS TIMESTAMP), 0) as end_date
        , 1 AS event_type
        , NULL
      FROM cohort_rows
    ) RAWDATA
  ) e
  WHERE (2 * e.start_ordinal) - e.overall_ord = 0
)
;
CREATE TEMPORARY TABLE cteEnds  AS (SELECT
     c.person_id
    , c.start_date
    , MIN(e.end_date) AS end_date
  FROM cohort_rows c
  JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
  GROUP BY c.person_id, c.start_date
)
;
CREATE TEMPORARY TABLE final_cohort
 AS SELECT person_id, min(start_date) as start_date, end_date
 FROM cteEnds
group by person_id, end_date
;

DELETE FROM omop_orc_results_280.cohort_cache where design_hash = -1202406987;
INSERT INTO omop_orc_results_280.cohort_cache (design_hash, subject_id, cohort_start_date, cohort_end_date)
select -1202406987 as design_hash, person_id, start_date, end_date 
FROM final_cohort CO
;

-- BEGIN: Censored Stats

delete from omop_orc_results_280.cohort_censor_stats_cache where design_hash = -1202406987;

-- END: Censored Stats



TRUNCATE TABLE strategy_ends;
DROP TABLE strategy_ends;


TRUNCATE TABLE cohort_rows;
DROP TABLE cohort_rows;

TRUNCATE TABLE final_cohort;
DROP TABLE final_cohort;

TRUNCATE TABLE inclusion_events;
DROP TABLE inclusion_events;

TRUNCATE TABLE qualified_events;
DROP TABLE qualified_events;

TRUNCATE TABLE included_events;
DROP TABLE included_events;

TRUNCATE TABLE Codesets;
DROP TABLE Codesets;

--------------------------------------------------------------------------------
-- Copy data from cache
--------------------------------------------------------------------------------

INSERT INTO omop_orc_results_280.temp_cohort_y2f4nuwx1 (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) SELECT 77054 as cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM (SELECT * FROM omop_orc_results_280.cohort_cache WHERE design_hash = -1202406987) r;
INSERT INTO omop_orc_results_280.temp_cohort_y2f4nuwx1 (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date) SELECT 77054 as cohort_definition_id, subject_id, cohort_start_date, cohort_end_date FROM (SELECT * FROM omop_orc_results_280.cohort_cache WHERE design_hash = -1914464508) r;


--------------------------------------------------------------------------------
-- Generate CC
--------------------------------------------------------------------------------


DROP TABLE IF EXISTS cov_ref;
DROP TABLE IF EXISTS analysis_ref;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_ref  (covariate_id BIGINT,
  covariate_name VARCHAR(512),
  analysis_id INT,
  concept_id INT
  );
CREATE TEMPORARY TABLE IF NOT EXISTS analysis_ref  (analysis_id BIGINT,
  analysis_name VARCHAR(512),
  domain_id VARCHAR(20),
  
  start_day INT,
  end_day INT,

  is_binary VARCHAR(1),
  missing_means_zero VARCHAR(1)
  );
CREATE TEMPORARY TABLE IF NOT EXISTS cov_1
 AS
SELECT
(CAST(measurement_concept_id AS BIGINT) * 10000) + (range_group * 1000) + 712 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT measurement_concept_id,
    CASE 
      WHEN value_as_number < range_low THEN 1
      WHEN value_as_number > range_high THEN 3
      ELSE 2
    END AS range_group,   
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.measurement
    ON cohort.subject_id = measurement.person_id

  WHERE measurement_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
        AND measurement_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -365)
    AND measurement_concept_id != 0

    AND range_low IS NOT NULL
    AND range_high IS NOT NULL


    AND cohort.cohort_definition_id IN (77049)
) by_row_id

GROUP BY measurement_concept_id,
  range_group
    
  ,cohort_definition_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('measurement ', range_name, ' during day -365 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,


  712 AS analysis_id,
  CAST(FLOOR(covariate_id / 10000.0) AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id,
     CASE 
      WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 1 THEN 'below normal range'
      WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 2 THEN 'within normal range'
      WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 3 THEN 'above normal range'
    END AS range_name
  FROM cov_1
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = FLOOR(covariate_id / 10000.0);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 712 AS analysis_id,
  CAST('MeasurementRangeGroupShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('' AS VARCHAR(20)) AS domain_id,


  -365 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS meas_cov;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_cov
 AS
SELECT
DISTINCT measurement_concept_id,
  unit_concept_id,
  CAST((CAST(measurement_concept_id AS BIGINT) * 1000000) + ((unit_concept_id - (FLOOR(unit_concept_id / 1000) * 1000)) * 1000) + 708 AS BIGINT) AS covariate_id

FROM
omop_orc.measurement
WHERE value_as_number IS NOT NULL



;
DROP TABLE IF EXISTS meas_val_data;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_data
 AS
SELECT
cohort_definition_id,
    subject_id,
    cohort_start_date,

  
  covariate_id,
  value_as_number

FROM
(
  SELECT 

    cohort_definition_id,
    subject_id,
    cohort_start_date,

    ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id, cohort_start_date, measurement.measurement_concept_id ORDER BY measurement_date DESC, measurement.unit_concept_id, value_as_number) AS rn,


    covariate_id,
    value_as_number
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.measurement
    ON cohort.subject_id = measurement.person_id
  INNER JOIN meas_cov meas_cov
    ON meas_cov.measurement_concept_id = measurement.measurement_concept_id 
      AND meas_cov.unit_concept_id = measurement.unit_concept_id 

  WHERE measurement_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
        AND measurement_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND measurement.measurement_concept_id != 0
  
    AND value_as_number IS NOT NULL       
    AND cohort.cohort_definition_id IN (77049)
) temp
WHERE rn = 1;
DROP TABLE IF EXISTS meas_val_stats;
DROP TABLE IF EXISTS meas_val_prep;
DROP TABLE IF EXISTS meas_val_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_stats
 AS
SELECT
cohort_definition_id,
  covariate_id,

  MIN(value_as_number) AS min_value,
  MAX(value_as_number) AS max_value,
  CAST(AVG(value_as_number) AS FLOAT) AS average_value,
  CAST(STDDEV_POP(value_as_number) AS FLOAT) AS standard_deviation,
  COUNT(*) AS count_value

FROM
meas_val_data
GROUP BY cohort_definition_id,

  covariate_id;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_prep
 AS
SELECT
cohort_definition_id,
  covariate_id,
  
  value_as_number,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, covariate_id ORDER BY value_as_number) AS rn

FROM
meas_val_data
GROUP BY cohort_definition_id,
  value_as_number,
  
  covariate_id;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_prep2 
 AS
SELECT
s.cohort_definition_id,
  s.covariate_id,
  
  s.value_as_number,
  SUM(p.total) AS accumulated

FROM
meas_val_prep s
INNER JOIN meas_val_prep p
  ON p.rn <= s.rn
    AND p.covariate_id = s.covariate_id
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.cohort_definition_id,
  s.covariate_id,
      
  s.value_as_number;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_2
 AS
SELECT
o.cohort_definition_id,
  o.covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  MIN(CASE WHEN p.accumulated >= 0.50  *  o.count_value THEN value_as_number END) AS median_value,
  MIN(CASE WHEN p.accumulated >= 0.10  *  o.count_value THEN value_as_number END) AS p10_value,
  MIN(CASE WHEN p.accumulated >= 0.25  *  o.count_value THEN value_as_number END) AS p25_value,
  MIN(CASE WHEN p.accumulated >= 0.75  *  o.count_value THEN value_as_number END) AS p75_value,
  MIN(CASE WHEN p.accumulated >= 0.90  *  o.count_value THEN value_as_number END) AS p90_value  

FROM
meas_val_prep2 p
INNER JOIN meas_val_stats o
  ON o.covariate_id = p.covariate_id
    AND o.cohort_definition_id = p.cohort_definition_id
    
GROUP BY o.covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.cohort_definition_id;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT temp.covariate_id,


  CAST(CASE WHEN unit_concept.concept_id = 0 THEN
    CONCAT('measurement value during day -30 through 0 days relative to index: ', measurement_concept.concept_name, ' (Unknown unit)')
  ELSE  
    CONCAT('measurement value during day -30 through 0 days relative to index: ', measurement_concept.concept_name, ' (', unit_concept.concept_name, ')')
  END AS VARCHAR(512)) AS covariate_name,


  708 AS analysis_id,
  covariate_ids.measurement_concept_id AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_2
  ) temp
INNER JOIN meas_cov covariate_ids
  ON covariate_ids.covariate_id = temp.covariate_id
LEFT JOIN omop_orc.concept measurement_concept
  ON covariate_ids.measurement_concept_id = measurement_concept.concept_id
LEFT JOIN omop_orc.concept unit_concept
  ON covariate_ids.unit_concept_id = unit_concept.concept_id;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 708 AS analysis_id,
  CAST('MeasurementValueShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('N' AS VARCHAR(1)) AS missing_means_zero;
TRUNCATE TABLE meas_cov;
DROP TABLE meas_cov;
DROP TABLE IF EXISTS charlson_concepts;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_concepts  (diag_category_id INT,
  concept_id INT
  );
DROP TABLE IF EXISTS charlson_scoring;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_scoring  (diag_category_id INT,
  diag_category_name VARCHAR(255),
  weight INT
  );
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  1,
  'Myocardial infarction',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 1,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4329847);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  2,
  'Congestive heart failure',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 2,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (316139);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  3,
  'Peripheral vascular disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 3,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (321052);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  4,
  'Cerebrovascular disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 4,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  5,
  'Dementia',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 5,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4182210);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  6,
  'Chronic pulmonary disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 6,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4063381);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  7,
  'Rheumatologic disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 7,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  8,
  'Peptic ulcer disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 8,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4247120);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  9,
  'Mild liver disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 9,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4064161, 4212540);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  10,
  'Diabetes (mild to moderate)',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 10,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (201820);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  11,
  'Diabetes with chronic complications',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 11,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4192279, 443767, 442793);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  12,
  'Hemoplegia or paralegia',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 12,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (192606, 374022);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  13,
  'Renal disease',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 13,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4030518);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  14,
  'Any malignancy',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 14,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (443392);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  15,
  'Moderate to severe liver disease',
  3
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 15,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4245975, 4029488, 192680, 24966);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  16,
  'Metastatic solid tumor',
  6
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 16,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (432851);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  17,
  'AIDS',
  6
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 17,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (439727);
DROP TABLE IF EXISTS charlson_data;
DROP TABLE IF EXISTS charlson_stats;
DROP TABLE IF EXISTS charlson_prep;
DROP TABLE IF EXISTS charlson_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_data

 AS
SELECT
cohort_definition_id,
  subject_id,
  cohort_start_date,
  SUM(weight) AS score

FROM
(
  SELECT DISTINCT charlson_scoring.diag_category_id,
    charlson_scoring.weight,

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date
      
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era condition_era
    ON cohort.subject_id = condition_era.person_id
  INNER JOIN charlson_concepts charlson_concepts
    ON condition_era.condition_concept_id = charlson_concepts.concept_id
  INNER JOIN charlson_scoring charlson_scoring
    ON charlson_concepts.diag_category_id = charlson_scoring.diag_category_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)

    AND cohort.cohort_definition_id IN (77049)
  ) temp

GROUP BY cohort_definition_id,
  subject_id,
  cohort_start_date
  
;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77049)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
    MIN(score) AS min_score, 
    MAX(score) AS max_score, 
    SUM(score) AS sum_score,
    SUM(score * score) as squared_score
  FROM charlson_data
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE charlson_stats
 AS SELECT t1.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
  t2.max_score AS max_value,
  CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_prep
 AS
SELECT
cohort_definition_id,
  score,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY score) AS rn

FROM
charlson_data
GROUP BY cohort_definition_id,
  score;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_prep2 
 AS
SELECT
s.cohort_definition_id,
  s.score,
  SUM(p.total) AS accumulated

FROM
charlson_prep s
INNER JOIN charlson_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.cohort_definition_id,
  s.score;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_3
 AS
SELECT
o.cohort_definition_id,
  CAST(1000 + 901 AS BIGINT) AS covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN score  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN score  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN score  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN score  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN score  END) 
    END AS p90_value    

FROM
charlson_prep2 p
INNER JOIN charlson_stats o
  ON p.cohort_definition_id = o.cohort_definition_id

GROUP BY o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.population_size,
  o.cohort_definition_id;
TRUNCATE TABLE charlson_data;
DROP TABLE charlson_data;
TRUNCATE TABLE charlson_stats;
DROP TABLE charlson_stats;
TRUNCATE TABLE charlson_prep;
DROP TABLE charlson_prep;
TRUNCATE TABLE charlson_prep2;
DROP TABLE charlson_prep2;
TRUNCATE TABLE charlson_concepts;
DROP TABLE charlson_concepts;
TRUNCATE TABLE charlson_scoring;
DROP TABLE charlson_scoring;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST('Charlson index - Romano adaptation' AS VARCHAR(512)) AS covariate_name,
  901 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_3
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 901 AS analysis_id,
  CAST('CharlsonIndex' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  0 AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_4
 AS
SELECT
CAST(FLOOR((YEAR(cohort_start_date) - year_of_birth) / 5) * 1000 + 3 AS BIGINT) AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
INNER JOIN omop_orc.person
  ON cohort.subject_id = person.person_id


  WHERE cohort.cohort_definition_id IN (77049)

    
GROUP BY cohort_definition_id,
  FLOOR((YEAR(cohort_start_date) - year_of_birth) / 5)

;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST(CONCAT (
    'age group: ',
    SUBSTR(CONCAT('   ', CAST(5 * (covariate_id - 3) / 1000  AS VARCHAR(1000))),-3),
    ' - ',
    SUBSTR(CONCAT('   ', CAST((5 * (covariate_id - 3) / 1000) + 4  AS VARCHAR(1000))),-3)
    ) AS VARCHAR(512)) AS covariate_name,
  3 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_4
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 3 AS analysis_id,
  CAST('DemographicsAgeGroup' AS VARCHAR(512)) AS analysis_name,
  CAST('Demographics' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_5
 AS
SELECT
CAST(condition_concept_id AS BIGINT) * 1000 + 204 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT DISTINCT condition_concept_id,
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND condition_era_end_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND condition_concept_id != 0





    AND cohort.cohort_definition_id IN (77049)
) by_row_id
    
GROUP BY cohort_definition_id,
  condition_concept_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('condition_era during day -30 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END ) AS VARCHAR(512)) AS covariate_name,


  204 AS analysis_id,
  CAST((covariate_id - 204) / 1000 AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_5
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = CAST((covariate_id - 204) / 1000 AS INT);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 204 AS analysis_id,
  CAST('ConditionEraShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,


  -30 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS dem_age_data;
DROP TABLE IF EXISTS dem_age_stats;
DROP TABLE IF EXISTS dem_age_prep;
DROP TABLE IF EXISTS dem_age_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS dem_age_data

 AS
SELECT
subject_id,

  cohort_definition_id,

  cohort_start_date,
  age

FROM
(
  SELECT 

    subject_id,
    cohort_definition_id,
    cohort_start_date,  

    YEAR(cohort_start_date) - year_of_birth AS age
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.person
    ON cohort.subject_id = person.person_id
  WHERE cohort.cohort_definition_id IN (77049)
  ) raw_data;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77049)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
    MIN(age) AS min_age, 
    MAX(age) AS max_age, 
    SUM(CAST(age AS BIGINT)) AS sum_age, 
    SUM(CAST(age AS BIGINT) * CAST(age AS BIGINT)) AS squared_age 
  FROM dem_age_data
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE dem_age_stats
 AS SELECT t2.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_age ELSE 0 END AS min_value,
  t2.max_age AS max_value,
  CAST(t2.sum_age / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_age - 1.0 * t2.sum_age*t2.sum_age) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS dem_age_prep
 AS
SELECT
cohort_definition_id,
  age,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY age) AS rn

FROM
dem_age_data
GROUP BY cohort_definition_id,
  age;
CREATE TEMPORARY TABLE IF NOT EXISTS dem_age_prep2  
 AS
SELECT
s.cohort_definition_id,
  s.age,
  SUM(p.total) AS accumulated

FROM
dem_age_prep s
INNER JOIN dem_age_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.age,
  s.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_6
 AS
SELECT
o.cohort_definition_id,
  CAST(1000 + 2 AS BIGINT) AS covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN age  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN age  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN age  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN age  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN age  END) 
    END AS p90_value    

FROM
dem_age_prep2 p
INNER JOIN dem_age_stats o
  ON p.cohort_definition_id = o.cohort_definition_id

GROUP BY o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.population_size,
  o.cohort_definition_id;
TRUNCATE TABLE dem_age_data;
DROP TABLE dem_age_data;
TRUNCATE TABLE dem_age_stats;
DROP TABLE dem_age_stats;
TRUNCATE TABLE dem_age_prep;
DROP TABLE dem_age_prep;
TRUNCATE TABLE dem_age_prep2;
DROP TABLE dem_age_prep2;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST('age in years' AS VARCHAR(512)) AS covariate_name,
  2 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_6
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 2 AS analysis_id,
  CAST('DemographicsAge' AS VARCHAR(512)) AS analysis_name,
  CAST('Demographics' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_7
 AS
SELECT
CAST(measurement_concept_id AS BIGINT) * 1000 + 704 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT DISTINCT measurement_concept_id,
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.measurement
    ON cohort.subject_id = measurement.person_id

  WHERE measurement_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND measurement_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND measurement_concept_id != 0





    AND cohort.cohort_definition_id IN (77049)
) by_row_id
    
GROUP BY cohort_definition_id,
  measurement_concept_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('measurement during day -30 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END ) AS VARCHAR(512)) AS covariate_name,


  704 AS analysis_id,
  CAST((covariate_id - 704) / 1000 AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_7
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = CAST((covariate_id - 704) / 1000 AS INT);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 704 AS analysis_id,
  CAST('MeasurementShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Measurement' AS VARCHAR(20)) AS domain_id,


  -30 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_8
 AS
SELECT
CAST(condition_concept_id AS BIGINT) * 1000 + 202 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT DISTINCT condition_concept_id,
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND condition_era_end_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -365)
    AND condition_concept_id != 0





    AND cohort.cohort_definition_id IN (77049)
) by_row_id
    
GROUP BY cohort_definition_id,
  condition_concept_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('condition_era during day -365 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END ) AS VARCHAR(512)) AS covariate_name,


  202 AS analysis_id,
  CAST((covariate_id - 202) / 1000 AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_8
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = CAST((covariate_id - 202) / 1000 AS INT);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 202 AS analysis_id,
  CAST('ConditionEraLongTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,


  -365 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS chads2_concepts;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_concepts  (diag_category_id INT,
  concept_id INT
  );
DROP TABLE IF EXISTS chads2_scoring;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_scoring  (diag_category_id INT,
  diag_category_name VARCHAR(255),
  weight INT
  );
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  1,
  'Congestive heart failure',
  1
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 1,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (316139);
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  2,
  'Hypertension',
  1
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 2,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (316866);
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  4,
  'Diabetes',
  1
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 4,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (201820);
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  5,
  'Stroke',
  2
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 5,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);
DROP TABLE IF EXISTS chads2_data;
DROP TABLE IF EXISTS chads2_stats;
DROP TABLE IF EXISTS chads2_prep;
DROP TABLE IF EXISTS chads2_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_data

 AS
SELECT
cohort_definition_id,
  subject_id,
  cohort_start_date,
  SUM(weight) AS score

FROM
(
  SELECT DISTINCT chads2_scoring.diag_category_id,
    chads2_scoring.weight,

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date
      
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id
  INNER JOIN chads2_concepts chads2_concepts
    ON condition_era.condition_concept_id = chads2_concepts.concept_id
  INNER JOIN chads2_scoring chads2_scoring
    ON chads2_concepts.diag_category_id = chads2_scoring.diag_category_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)

    AND cohort.cohort_definition_id IN (77049)

  UNION
  
  SELECT 3 AS diag_category_id,
    CASE WHEN (YEAR(cohort_start_date) - year_of_birth) >= 75 THEN 1 ELSE 0 END AS weight,

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date
    
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.person
    ON cohort.subject_id = person.person_id
  WHERE cohort.cohort_definition_id IN (77049)
  ) temp

GROUP BY cohort_definition_id,
  subject_id,
  cohort_start_date
  
;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77049)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
    MIN(score) AS min_score, 
    MAX(score) AS max_score, 
    SUM(score) AS sum_score, 
    SUM(score*score) AS squared_score 
  FROM chads2_data
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE chads2_stats
 AS SELECT t1.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
  t2.max_score AS max_value,
  CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_prep
 AS
SELECT
cohort_definition_id,
  score,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY score) AS rn

FROM
chads2_data
GROUP BY cohort_definition_id,
  score;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_prep2 
 AS
SELECT
s.cohort_definition_id,
  s.score,
  SUM(p.total) AS accumulated

FROM
chads2_prep s
INNER JOIN chads2_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.cohort_definition_id,
  s.score;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_9
 AS
SELECT
o.cohort_definition_id,
  CAST(1000 + 903 AS BIGINT) AS covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN score  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN score  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN score  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN score  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN score  END) 
    END AS p90_value    

FROM
chads2_prep2 p
INNER JOIN chads2_stats o
  ON p.cohort_definition_id = o.cohort_definition_id

GROUP BY o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.population_size,
  o.cohort_definition_id;
TRUNCATE TABLE chads2_data;
DROP TABLE chads2_data;
TRUNCATE TABLE chads2_stats;
DROP TABLE chads2_stats;
TRUNCATE TABLE chads2_prep;
DROP TABLE chads2_prep;
TRUNCATE TABLE chads2_prep2;
DROP TABLE chads2_prep2;
TRUNCATE TABLE chads2_concepts;
DROP TABLE chads2_concepts;
TRUNCATE TABLE chads2_scoring;
DROP TABLE chads2_scoring;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST('CHADS2' AS VARCHAR(512)) AS covariate_name,
  903 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_9
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 903 AS analysis_id,
  CAST('Chads2' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  0 AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS concept_count_data;
DROP TABLE IF EXISTS concept_count_stats;
DROP TABLE IF EXISTS concept_count_prep;
DROP TABLE IF EXISTS concept_count_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS concept_count_data

 AS
SELECT
cohort_definition_id,
  subject_id,
  cohort_start_date,
  

  concept_count

FROM
(
  SELECT 
  


    cohort_definition_id,
    subject_id,
    cohort_start_date,


    COUNT(DISTINCT condition_concept_id) AS concept_count

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND condition_era_end_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND condition_concept_id != 0



    AND cohort.cohort_definition_id IN (77049)
  GROUP BY 
  
 

    cohort_definition_id,
    subject_id,
    cohort_start_date
  
  ) raw_data;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77049)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
 
    MIN(concept_count) AS min_concept_count, 
    MAX(concept_count) AS max_concept_count, 
    SUM(CAST(concept_count AS BIGINT)) AS sum_concept_count,
    SUM(CAST(concept_count AS BIGINT) * CAST(concept_count AS BIGINT)) AS squared_concept_count
  FROM concept_count_data
  GROUP BY cohort_definition_id
 
  )
;
CREATE TEMPORARY TABLE concept_count_stats
 AS SELECT t1.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_concept_count ELSE 0 END AS min_value,
  t2.max_concept_count AS max_value,
 
  CAST(t2.sum_concept_count / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE
    WHEN t2.cnt = 1 THEN 0 
    ELSE SQRT((1.0 * t2.cnt*t2.squared_concept_count - 1.0 * t2.sum_concept_count*t2.sum_concept_count) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) 
  END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS concept_count_prep
 AS
SELECT
cohort_definition_id, 
  concept_count,
  COUNT(*) AS total,

  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY concept_count) AS rn


FROM
concept_count_data
GROUP BY cohort_definition_id,

  concept_count;
CREATE TEMPORARY TABLE IF NOT EXISTS concept_count_prep2  
 AS
SELECT
s.cohort_definition_id,
  s.concept_count,

  SUM(p.total) AS accumulated

FROM
concept_count_prep s
INNER JOIN concept_count_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id

GROUP BY s.cohort_definition_id,

  s.concept_count;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_10
 AS
SELECT
CAST(1000 + 907 AS BIGINT) AS covariate_id,


  o.cohort_definition_id,
  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN concept_count  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN concept_count  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN concept_count  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN concept_count  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN concept_count  END) 
    END AS p90_value    

FROM
concept_count_prep2 p

CROSS JOIN concept_count_stats o


GROUP BY o.cohort_definition_id,
  o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,

  o.population_size;
TRUNCATE TABLE concept_count_data;
DROP TABLE concept_count_data;
TRUNCATE TABLE concept_count_stats;
DROP TABLE concept_count_stats;
TRUNCATE TABLE concept_count_prep;
DROP TABLE concept_count_prep;
TRUNCATE TABLE concept_count_prep2;
DROP TABLE concept_count_prep2;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST('condition_era distinct concept count during day -30 through 0 concept_count relative to index' AS VARCHAR(512)) AS covariate_name,


  907 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_10
  ) t1

;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 907 AS analysis_id,
  CAST('DistinctConditionCountShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
insert into omop_orc_results_280.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id,
    count_value, min_value, max_value, avg_value, stdev_value, median_value,
    p10_value, p25_value, p75_value, p90_value, strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('DISTRIBUTION' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.count_value,
    f.min_value,
    f.max_value,
    f.average_value,
    f.standard_deviation,
    f.median_value,
    f.p10_value,
    f.p25_value,
    f.p75_value,
    f.p90_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    77049 as cohort_definition_id,
    1530 as cc_generation_id
  from (select 77049 as cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from (SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value
FROM (
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_2 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_3 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_6 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_9 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_10
) all_covariates) W) f
    join (select 77049 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 77049 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join omop_orc.concept c on c.concept_id = fr.concept_id;
insert into omop_orc_results_280.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id, count_value, avg_value,
                                                 strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('PREVALENCE' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.sum_value     as count_value,
    f.average_value as stat_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    77049 as cohort_definition_id,
    1530 as cc_generation_id
  from (select 77049 as cohort_definition_id, covariate_id, sum_value, average_value from (SELECT all_covariates.cohort_definition_id,
  all_covariates.covariate_id,
  all_covariates.sum_value,
  CAST(all_covariates.sum_value / (1.0 * total.total_count) AS FLOAT) AS average_value
FROM (SELECT cohort_definition_id, covariate_id, sum_value FROM cov_1 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_4 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_5 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_7 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_8
) all_covariates
INNER JOIN (
SELECT cohort_definition_id, COUNT(*) AS total_count
FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
WHERE cohort_definition_id IN (77049) GROUP BY cohort_definition_id
) total
  ON all_covariates.cohort_definition_id = total.cohort_definition_id) W) f
    join (select 77049 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 77049 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join omop_orc.concept c on c.concept_id = fr.concept_id;
TRUNCATE TABLE cov_1;
DROP TABLE cov_1;
TRUNCATE TABLE cov_2;
DROP TABLE cov_2;
TRUNCATE TABLE cov_3;
DROP TABLE cov_3;
TRUNCATE TABLE cov_4;
DROP TABLE cov_4;
TRUNCATE TABLE cov_5;
DROP TABLE cov_5;
TRUNCATE TABLE cov_6;
DROP TABLE cov_6;
TRUNCATE TABLE cov_7;
DROP TABLE cov_7;
TRUNCATE TABLE cov_8;
DROP TABLE cov_8;
TRUNCATE TABLE cov_9;
DROP TABLE cov_9;
TRUNCATE TABLE cov_10;
DROP TABLE cov_10;
TRUNCATE TABLE cov_ref;
DROP TABLE cov_ref;
TRUNCATE TABLE analysis_ref;
DROP TABLE analysis_ref;
DROP TABLE IF EXISTS cov_ref;
DROP TABLE IF EXISTS analysis_ref;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_ref  (covariate_id BIGINT,
  covariate_name VARCHAR(512),
  analysis_id INT,
  concept_id INT
  );
CREATE TEMPORARY TABLE IF NOT EXISTS analysis_ref  (analysis_id BIGINT,
  analysis_name VARCHAR(512),
  domain_id VARCHAR(20),
  
  start_day INT,
  end_day INT,

  is_binary VARCHAR(1),
  missing_means_zero VARCHAR(1)
  );
CREATE TEMPORARY TABLE IF NOT EXISTS cov_1
 AS
SELECT
(CAST(measurement_concept_id AS BIGINT) * 10000) + (range_group * 1000) + 712 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT measurement_concept_id,
    CASE 
      WHEN value_as_number < range_low THEN 1
      WHEN value_as_number > range_high THEN 3
      ELSE 2
    END AS range_group,   
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.measurement
    ON cohort.subject_id = measurement.person_id

  WHERE measurement_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
        AND measurement_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -365)
    AND measurement_concept_id != 0

    AND range_low IS NOT NULL
    AND range_high IS NOT NULL


    AND cohort.cohort_definition_id IN (77054)
) by_row_id

GROUP BY measurement_concept_id,
  range_group
    
  ,cohort_definition_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('measurement ', range_name, ' during day -365 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END) AS VARCHAR(512)) AS covariate_name,


  712 AS analysis_id,
  CAST(FLOOR(covariate_id / 10000.0) AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id,
     CASE 
      WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 1 THEN 'below normal range'
      WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 2 THEN 'within normal range'
      WHEN FLOOR(covariate_id / 1000.0) - (FLOOR(covariate_id / 10000.0) * 10) = 3 THEN 'above normal range'
    END AS range_name
  FROM cov_1
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = FLOOR(covariate_id / 10000.0);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 712 AS analysis_id,
  CAST('MeasurementRangeGroupShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('' AS VARCHAR(20)) AS domain_id,


  -365 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS meas_cov;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_cov
 AS
SELECT
DISTINCT measurement_concept_id,
  unit_concept_id,
  CAST((CAST(measurement_concept_id AS BIGINT) * 1000000) + ((unit_concept_id - (FLOOR(unit_concept_id / 1000) * 1000)) * 1000) + 708 AS BIGINT) AS covariate_id

FROM
omop_orc.measurement
WHERE value_as_number IS NOT NULL



;
DROP TABLE IF EXISTS meas_val_data;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_data
 AS
SELECT
cohort_definition_id,
    subject_id,
    cohort_start_date,

  
  covariate_id,
  value_as_number

FROM
(
  SELECT 

    cohort_definition_id,
    subject_id,
    cohort_start_date,

    ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, subject_id, cohort_start_date, measurement.measurement_concept_id ORDER BY measurement_date DESC, measurement.unit_concept_id, value_as_number) AS rn,


    covariate_id,
    value_as_number
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.measurement
    ON cohort.subject_id = measurement.person_id
  INNER JOIN meas_cov meas_cov
    ON meas_cov.measurement_concept_id = measurement.measurement_concept_id 
      AND meas_cov.unit_concept_id = measurement.unit_concept_id 

  WHERE measurement_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
        AND measurement_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND measurement.measurement_concept_id != 0
  
    AND value_as_number IS NOT NULL       
    AND cohort.cohort_definition_id IN (77054)
) temp
WHERE rn = 1;
DROP TABLE IF EXISTS meas_val_stats;
DROP TABLE IF EXISTS meas_val_prep;
DROP TABLE IF EXISTS meas_val_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_stats
 AS
SELECT
cohort_definition_id,
  covariate_id,

  MIN(value_as_number) AS min_value,
  MAX(value_as_number) AS max_value,
  CAST(AVG(value_as_number) AS FLOAT) AS average_value,
  CAST(STDDEV_POP(value_as_number) AS FLOAT) AS standard_deviation,
  COUNT(*) AS count_value

FROM
meas_val_data
GROUP BY cohort_definition_id,

  covariate_id;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_prep
 AS
SELECT
cohort_definition_id,
  covariate_id,
  
  value_as_number,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id, covariate_id ORDER BY value_as_number) AS rn

FROM
meas_val_data
GROUP BY cohort_definition_id,
  value_as_number,
  
  covariate_id;
CREATE TEMPORARY TABLE IF NOT EXISTS meas_val_prep2 
 AS
SELECT
s.cohort_definition_id,
  s.covariate_id,
  
  s.value_as_number,
  SUM(p.total) AS accumulated

FROM
meas_val_prep s
INNER JOIN meas_val_prep p
  ON p.rn <= s.rn
    AND p.covariate_id = s.covariate_id
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.cohort_definition_id,
  s.covariate_id,
      
  s.value_as_number;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_2
 AS
SELECT
o.cohort_definition_id,
  o.covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  MIN(CASE WHEN p.accumulated >= 0.50  *  o.count_value THEN value_as_number END) AS median_value,
  MIN(CASE WHEN p.accumulated >= 0.10  *  o.count_value THEN value_as_number END) AS p10_value,
  MIN(CASE WHEN p.accumulated >= 0.25  *  o.count_value THEN value_as_number END) AS p25_value,
  MIN(CASE WHEN p.accumulated >= 0.75  *  o.count_value THEN value_as_number END) AS p75_value,
  MIN(CASE WHEN p.accumulated >= 0.90  *  o.count_value THEN value_as_number END) AS p90_value  

FROM
meas_val_prep2 p
INNER JOIN meas_val_stats o
  ON o.covariate_id = p.covariate_id
    AND o.cohort_definition_id = p.cohort_definition_id
    
GROUP BY o.covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.cohort_definition_id;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT temp.covariate_id,


  CAST(CASE WHEN unit_concept.concept_id = 0 THEN
    CONCAT('measurement value during day -30 through 0 days relative to index: ', measurement_concept.concept_name, ' (Unknown unit)')
  ELSE  
    CONCAT('measurement value during day -30 through 0 days relative to index: ', measurement_concept.concept_name, ' (', unit_concept.concept_name, ')')
  END AS VARCHAR(512)) AS covariate_name,


  708 AS analysis_id,
  covariate_ids.measurement_concept_id AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_2
  ) temp
INNER JOIN meas_cov covariate_ids
  ON covariate_ids.covariate_id = temp.covariate_id
LEFT JOIN omop_orc.concept measurement_concept
  ON covariate_ids.measurement_concept_id = measurement_concept.concept_id
LEFT JOIN omop_orc.concept unit_concept
  ON covariate_ids.unit_concept_id = unit_concept.concept_id;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 708 AS analysis_id,
  CAST('MeasurementValueShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('N' AS VARCHAR(1)) AS missing_means_zero;
TRUNCATE TABLE meas_cov;
DROP TABLE meas_cov;
DROP TABLE IF EXISTS charlson_concepts;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_concepts  (diag_category_id INT,
  concept_id INT
  );
DROP TABLE IF EXISTS charlson_scoring;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_scoring  (diag_category_id INT,
  diag_category_name VARCHAR(255),
  weight INT
  );
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  1,
  'Myocardial infarction',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 1,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4329847);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  2,
  'Congestive heart failure',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 2,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (316139);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  3,
  'Peripheral vascular disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 3,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (321052);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  4,
  'Cerebrovascular disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 4,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  5,
  'Dementia',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 5,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4182210);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  6,
  'Chronic pulmonary disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 6,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4063381);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  7,
  'Rheumatologic disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 7,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (257628, 134442, 80800, 80809, 256197, 255348);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  8,
  'Peptic ulcer disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 8,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4247120);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  9,
  'Mild liver disease',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 9,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4064161, 4212540);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  10,
  'Diabetes (mild to moderate)',
  1
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 10,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (201820);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  11,
  'Diabetes with chronic complications',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 11,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4192279, 443767, 442793);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  12,
  'Hemoplegia or paralegia',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 12,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (192606, 374022);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  13,
  'Renal disease',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 13,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4030518);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  14,
  'Any malignancy',
  2
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 14,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (443392);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  15,
  'Moderate to severe liver disease',
  3
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 15,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (4245975, 4029488, 192680, 24966);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  16,
  'Metastatic solid tumor',
  6
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 16,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (432851);
INSERT INTO charlson_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  17,
  'AIDS',
  6
  );
INSERT INTO charlson_concepts (
  diag_category_id,
  concept_id
  )
SELECT 17,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (439727);
DROP TABLE IF EXISTS charlson_data;
DROP TABLE IF EXISTS charlson_stats;
DROP TABLE IF EXISTS charlson_prep;
DROP TABLE IF EXISTS charlson_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_data

 AS
SELECT
cohort_definition_id,
  subject_id,
  cohort_start_date,
  SUM(weight) AS score

FROM
(
  SELECT DISTINCT charlson_scoring.diag_category_id,
    charlson_scoring.weight,

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date
      
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era condition_era
    ON cohort.subject_id = condition_era.person_id
  INNER JOIN charlson_concepts charlson_concepts
    ON condition_era.condition_concept_id = charlson_concepts.concept_id
  INNER JOIN charlson_scoring charlson_scoring
    ON charlson_concepts.diag_category_id = charlson_scoring.diag_category_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)

    AND cohort.cohort_definition_id IN (77054)
  ) temp

GROUP BY cohort_definition_id,
  subject_id,
  cohort_start_date
  
;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77054)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
    MIN(score) AS min_score, 
    MAX(score) AS max_score, 
    SUM(score) AS sum_score,
    SUM(score * score) as squared_score
  FROM charlson_data
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE charlson_stats
 AS SELECT t1.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
  t2.max_score AS max_value,
  CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_prep
 AS
SELECT
cohort_definition_id,
  score,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY score) AS rn

FROM
charlson_data
GROUP BY cohort_definition_id,
  score;
CREATE TEMPORARY TABLE IF NOT EXISTS charlson_prep2 
 AS
SELECT
s.cohort_definition_id,
  s.score,
  SUM(p.total) AS accumulated

FROM
charlson_prep s
INNER JOIN charlson_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.cohort_definition_id,
  s.score;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_3
 AS
SELECT
o.cohort_definition_id,
  CAST(1000 + 901 AS BIGINT) AS covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN score  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN score  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN score  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN score  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN score  END) 
    END AS p90_value    

FROM
charlson_prep2 p
INNER JOIN charlson_stats o
  ON p.cohort_definition_id = o.cohort_definition_id

GROUP BY o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.population_size,
  o.cohort_definition_id;
TRUNCATE TABLE charlson_data;
DROP TABLE charlson_data;
TRUNCATE TABLE charlson_stats;
DROP TABLE charlson_stats;
TRUNCATE TABLE charlson_prep;
DROP TABLE charlson_prep;
TRUNCATE TABLE charlson_prep2;
DROP TABLE charlson_prep2;
TRUNCATE TABLE charlson_concepts;
DROP TABLE charlson_concepts;
TRUNCATE TABLE charlson_scoring;
DROP TABLE charlson_scoring;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST('Charlson index - Romano adaptation' AS VARCHAR(512)) AS covariate_name,
  901 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_3
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 901 AS analysis_id,
  CAST('CharlsonIndex' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  0 AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_4
 AS
SELECT
CAST(FLOOR((YEAR(cohort_start_date) - year_of_birth) / 5) * 1000 + 3 AS BIGINT) AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
INNER JOIN omop_orc.person
  ON cohort.subject_id = person.person_id


  WHERE cohort.cohort_definition_id IN (77054)

    
GROUP BY cohort_definition_id,
  FLOOR((YEAR(cohort_start_date) - year_of_birth) / 5)

;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST(CONCAT (
    'age group: ',
    SUBSTR(CONCAT('   ', CAST(5 * (covariate_id - 3) / 1000  AS VARCHAR(1000))),-3),
    ' - ',
    SUBSTR(CONCAT('   ', CAST((5 * (covariate_id - 3) / 1000) + 4  AS VARCHAR(1000))),-3)
    ) AS VARCHAR(512)) AS covariate_name,
  3 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_4
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 3 AS analysis_id,
  CAST('DemographicsAgeGroup' AS VARCHAR(512)) AS analysis_name,
  CAST('Demographics' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_5
 AS
SELECT
CAST(condition_concept_id AS BIGINT) * 1000 + 204 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT DISTINCT condition_concept_id,
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND condition_era_end_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND condition_concept_id != 0





    AND cohort.cohort_definition_id IN (77054)
) by_row_id
    
GROUP BY cohort_definition_id,
  condition_concept_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('condition_era during day -30 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END ) AS VARCHAR(512)) AS covariate_name,


  204 AS analysis_id,
  CAST((covariate_id - 204) / 1000 AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_5
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = CAST((covariate_id - 204) / 1000 AS INT);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 204 AS analysis_id,
  CAST('ConditionEraShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,


  -30 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS dem_age_data;
DROP TABLE IF EXISTS dem_age_stats;
DROP TABLE IF EXISTS dem_age_prep;
DROP TABLE IF EXISTS dem_age_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS dem_age_data

 AS
SELECT
subject_id,

  cohort_definition_id,

  cohort_start_date,
  age

FROM
(
  SELECT 

    subject_id,
    cohort_definition_id,
    cohort_start_date,  

    YEAR(cohort_start_date) - year_of_birth AS age
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.person
    ON cohort.subject_id = person.person_id
  WHERE cohort.cohort_definition_id IN (77054)
  ) raw_data;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77054)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
    MIN(age) AS min_age, 
    MAX(age) AS max_age, 
    SUM(CAST(age AS BIGINT)) AS sum_age, 
    SUM(CAST(age AS BIGINT) * CAST(age AS BIGINT)) AS squared_age 
  FROM dem_age_data
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE dem_age_stats
 AS SELECT t2.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_age ELSE 0 END AS min_value,
  t2.max_age AS max_value,
  CAST(t2.sum_age / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_age - 1.0 * t2.sum_age*t2.sum_age) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS dem_age_prep
 AS
SELECT
cohort_definition_id,
  age,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY age) AS rn

FROM
dem_age_data
GROUP BY cohort_definition_id,
  age;
CREATE TEMPORARY TABLE IF NOT EXISTS dem_age_prep2  
 AS
SELECT
s.cohort_definition_id,
  s.age,
  SUM(p.total) AS accumulated

FROM
dem_age_prep s
INNER JOIN dem_age_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.age,
  s.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_6
 AS
SELECT
o.cohort_definition_id,
  CAST(1000 + 2 AS BIGINT) AS covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN age  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN age  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN age  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN age  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN age  END) 
    END AS p90_value    

FROM
dem_age_prep2 p
INNER JOIN dem_age_stats o
  ON p.cohort_definition_id = o.cohort_definition_id

GROUP BY o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.population_size,
  o.cohort_definition_id;
TRUNCATE TABLE dem_age_data;
DROP TABLE dem_age_data;
TRUNCATE TABLE dem_age_stats;
DROP TABLE dem_age_stats;
TRUNCATE TABLE dem_age_prep;
DROP TABLE dem_age_prep;
TRUNCATE TABLE dem_age_prep2;
DROP TABLE dem_age_prep2;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST('age in years' AS VARCHAR(512)) AS covariate_name,
  2 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_6
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 2 AS analysis_id,
  CAST('DemographicsAge' AS VARCHAR(512)) AS analysis_name,
  CAST('Demographics' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_7
 AS
SELECT
CAST(measurement_concept_id AS BIGINT) * 1000 + 704 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT DISTINCT measurement_concept_id,
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.measurement
    ON cohort.subject_id = measurement.person_id

  WHERE measurement_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND measurement_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND measurement_concept_id != 0





    AND cohort.cohort_definition_id IN (77054)
) by_row_id
    
GROUP BY cohort_definition_id,
  measurement_concept_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('measurement during day -30 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END ) AS VARCHAR(512)) AS covariate_name,


  704 AS analysis_id,
  CAST((covariate_id - 704) / 1000 AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_7
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = CAST((covariate_id - 704) / 1000 AS INT);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 704 AS analysis_id,
  CAST('MeasurementShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Measurement' AS VARCHAR(20)) AS domain_id,


  -30 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_8
 AS
SELECT
CAST(condition_concept_id AS BIGINT) * 1000 + 202 AS covariate_id,
  

  cohort_definition_id,
  COUNT(*) AS sum_value


FROM
(
  SELECT DISTINCT condition_concept_id,
  

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND condition_era_end_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -365)
    AND condition_concept_id != 0





    AND cohort.cohort_definition_id IN (77054)
) by_row_id
    
GROUP BY cohort_definition_id,
  condition_concept_id
 
 
;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST(CONCAT('condition_era during day -365 through 0 days relative to index: ', CASE WHEN concept_name IS NULL THEN 'Unknown concept' ELSE concept_name END ) AS VARCHAR(512)) AS covariate_name,


  202 AS analysis_id,
  CAST((covariate_id - 202) / 1000 AS INT) AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_8
  ) t1
LEFT JOIN omop_orc.concept
  ON concept_id = CAST((covariate_id - 202) / 1000 AS INT);
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 202 AS analysis_id,
  CAST('ConditionEraLongTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,


  -365 AS start_day,

  0 AS end_day,

  CAST('Y' AS VARCHAR(1)) AS is_binary,
  CAST(NULL AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS chads2_concepts;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_concepts  (diag_category_id INT,
  concept_id INT
  );
DROP TABLE IF EXISTS chads2_scoring;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_scoring  (diag_category_id INT,
  diag_category_name VARCHAR(255),
  weight INT
  );
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  1,
  'Congestive heart failure',
  1
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 1,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (316139);
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  2,
  'Hypertension',
  1
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 2,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (316866);
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  4,
  'Diabetes',
  1
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 4,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (201820);
INSERT INTO chads2_scoring (
  diag_category_id,
  diag_category_name,
  weight
  )
VALUES (
  5,
  'Stroke',
  2
  );
INSERT INTO chads2_concepts (
  diag_category_id,
  concept_id
  )
SELECT 5,
  descendant_concept_id
FROM omop_orc.concept_ancestor
WHERE ancestor_concept_id IN (381591, 434056);
DROP TABLE IF EXISTS chads2_data;
DROP TABLE IF EXISTS chads2_stats;
DROP TABLE IF EXISTS chads2_prep;
DROP TABLE IF EXISTS chads2_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_data

 AS
SELECT
cohort_definition_id,
  subject_id,
  cohort_start_date,
  SUM(weight) AS score

FROM
(
  SELECT DISTINCT chads2_scoring.diag_category_id,
    chads2_scoring.weight,

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date
      
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id
  INNER JOIN chads2_concepts chads2_concepts
    ON condition_era.condition_concept_id = chads2_concepts.concept_id
  INNER JOIN chads2_scoring chads2_scoring
    ON chads2_concepts.diag_category_id = chads2_scoring.diag_category_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)

    AND cohort.cohort_definition_id IN (77054)

  UNION
  
  SELECT 3 AS diag_category_id,
    CASE WHEN (YEAR(cohort_start_date) - year_of_birth) >= 75 THEN 1 ELSE 0 END AS weight,

    cohort_definition_id,
    cohort.subject_id,
    cohort.cohort_start_date
    
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.person
    ON cohort.subject_id = person.person_id
  WHERE cohort.cohort_definition_id IN (77054)
  ) temp

GROUP BY cohort_definition_id,
  subject_id,
  cohort_start_date
  
;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77054)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
    MIN(score) AS min_score, 
    MAX(score) AS max_score, 
    SUM(score) AS sum_score, 
    SUM(score*score) AS squared_score 
  FROM chads2_data
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE chads2_stats
 AS SELECT t1.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_score ELSE 0 END AS min_value,
  t2.max_score AS max_value,
  CAST(t2.sum_score / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE WHEN t2.cnt = 1 THEN 0 ELSE SQRT((1.0 * t2.cnt*t2.squared_score - 1.0 * t2.sum_score*t2.sum_score) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_prep
 AS
SELECT
cohort_definition_id,
  score,
  COUNT(*) AS total,
  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY score) AS rn

FROM
chads2_data
GROUP BY cohort_definition_id,
  score;
CREATE TEMPORARY TABLE IF NOT EXISTS chads2_prep2 
 AS
SELECT
s.cohort_definition_id,
  s.score,
  SUM(p.total) AS accumulated

FROM
chads2_prep s
INNER JOIN chads2_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id
GROUP BY s.cohort_definition_id,
  s.score;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_9
 AS
SELECT
o.cohort_definition_id,
  CAST(1000 + 903 AS BIGINT) AS covariate_id,

  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN score  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN score  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN score  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN score  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN score  END) 
    END AS p90_value    

FROM
chads2_prep2 p
INNER JOIN chads2_stats o
  ON p.cohort_definition_id = o.cohort_definition_id

GROUP BY o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,
  o.population_size,
  o.cohort_definition_id;
TRUNCATE TABLE chads2_data;
DROP TABLE chads2_data;
TRUNCATE TABLE chads2_stats;
DROP TABLE chads2_stats;
TRUNCATE TABLE chads2_prep;
DROP TABLE chads2_prep;
TRUNCATE TABLE chads2_prep2;
DROP TABLE chads2_prep2;
TRUNCATE TABLE chads2_concepts;
DROP TABLE chads2_concepts;
TRUNCATE TABLE chads2_scoring;
DROP TABLE chads2_scoring;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,
  CAST('CHADS2' AS VARCHAR(512)) AS covariate_name,
  903 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_9
  ) t1;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 903 AS analysis_id,
  CAST('Chads2' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  0 AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
DROP TABLE IF EXISTS concept_count_data;
DROP TABLE IF EXISTS concept_count_stats;
DROP TABLE IF EXISTS concept_count_prep;
DROP TABLE IF EXISTS concept_count_prep2;
CREATE TEMPORARY TABLE IF NOT EXISTS concept_count_data

 AS
SELECT
cohort_definition_id,
  subject_id,
  cohort_start_date,
  

  concept_count

FROM
(
  SELECT 
  


    cohort_definition_id,
    subject_id,
    cohort_start_date,


    COUNT(DISTINCT condition_concept_id) AS concept_count

  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 cohort
  INNER JOIN omop_orc.condition_era
    ON cohort.subject_id = condition_era.person_id

  WHERE condition_era_start_date <= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), 0)
    AND condition_era_end_date >= DATE_ADD(CAST(cohort.cohort_start_date AS TIMESTAMP), -30)
    AND condition_concept_id != 0



    AND cohort.cohort_definition_id IN (77054)
  GROUP BY 
  
 

    cohort_definition_id,
    subject_id,
    cohort_start_date
  
  ) raw_data;
DROP TABLE IF EXISTS t1 ;

DROP TABLE IF EXISTS t1  ; DROP TABLE IF EXISTS t2 ; CREATE TEMPORARY TABLE t1   AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt 
  FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
  WHERE cohort_definition_id IN (77054)
  GROUP BY cohort_definition_id
  )
;
CREATE TEMPORARY TABLE t2  AS (SELECT cohort_definition_id,
    COUNT(*) AS cnt, 
 
    MIN(concept_count) AS min_concept_count, 
    MAX(concept_count) AS max_concept_count, 
    SUM(CAST(concept_count AS BIGINT)) AS sum_concept_count,
    SUM(CAST(concept_count AS BIGINT) * CAST(concept_count AS BIGINT)) AS squared_concept_count
  FROM concept_count_data
  GROUP BY cohort_definition_id
 
  )
;
CREATE TEMPORARY TABLE concept_count_stats
 AS SELECT t1.cohort_definition_id,
  CASE WHEN t2.cnt = t1.cnt THEN t2.min_concept_count ELSE 0 END AS min_value,
  t2.max_concept_count AS max_value,
 
  CAST(t2.sum_concept_count / (1.0 * t1.cnt) AS FLOAT) AS average_value,
  CAST(CASE
    WHEN t2.cnt = 1 THEN 0 
    ELSE SQRT((1.0 * t2.cnt*t2.squared_concept_count - 1.0 * t2.sum_concept_count*t2.sum_concept_count) / (1.0 * t2.cnt*(1.0 * t2.cnt - 1))) 
  END AS FLOAT) AS standard_deviation,
  t2.cnt AS count_value,
  t1.cnt - t2.cnt AS count_no_value,
  t1.cnt AS population_size
 FROM t1
INNER JOIN t2
  ON t1.cohort_definition_id = t2.cohort_definition_id;
CREATE TEMPORARY TABLE IF NOT EXISTS concept_count_prep
 AS
SELECT
cohort_definition_id, 
  concept_count,
  COUNT(*) AS total,

  ROW_NUMBER() OVER (PARTITION BY cohort_definition_id ORDER BY concept_count) AS rn


FROM
concept_count_data
GROUP BY cohort_definition_id,

  concept_count;
CREATE TEMPORARY TABLE IF NOT EXISTS concept_count_prep2  
 AS
SELECT
s.cohort_definition_id,
  s.concept_count,

  SUM(p.total) AS accumulated

FROM
concept_count_prep s
INNER JOIN concept_count_prep p
  ON p.rn <= s.rn
    AND p.cohort_definition_id = s.cohort_definition_id

GROUP BY s.cohort_definition_id,

  s.concept_count;
CREATE TEMPORARY TABLE IF NOT EXISTS cov_10
 AS
SELECT
CAST(1000 + 907 AS BIGINT) AS covariate_id,


  o.cohort_definition_id,
  o.count_value,
  o.min_value,
  o.max_value,
  CAST(o.average_value AS FLOAT) average_value,
  CAST(o.standard_deviation AS FLOAT) standard_deviation,
  CASE 
    WHEN 0.50  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.50  *  o.population_size THEN concept_count  END) 
    END AS median_value,
  CASE 
    WHEN 0.10  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.10  *  o.population_size THEN concept_count  END) 
    END AS p10_value,   
  CASE 
    WHEN 0.25  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.25  *  o.population_size THEN concept_count  END) 
    END AS p25_value, 
  CASE 
    WHEN 0.75  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.75  *  o.population_size THEN concept_count  END) 
    END AS p75_value, 
  CASE 
    WHEN 0.90  *  o.population_size < count_no_value THEN 0
    ELSE MIN(CASE WHEN p.accumulated + count_no_value >= 0.90  *  o.population_size THEN concept_count  END) 
    END AS p90_value    

FROM
concept_count_prep2 p

CROSS JOIN concept_count_stats o


GROUP BY o.cohort_definition_id,
  o.count_value,
  o.count_no_value,
  o.min_value,
  o.max_value,
  o.average_value,
  o.standard_deviation,

  o.population_size;
TRUNCATE TABLE concept_count_data;
DROP TABLE concept_count_data;
TRUNCATE TABLE concept_count_stats;
DROP TABLE concept_count_stats;
TRUNCATE TABLE concept_count_prep;
DROP TABLE concept_count_prep;
TRUNCATE TABLE concept_count_prep2;
DROP TABLE concept_count_prep2;
INSERT INTO cov_ref (
  covariate_id,
  covariate_name,
  analysis_id,
  concept_id
  )
SELECT covariate_id,


  CAST('condition_era distinct concept count during day -30 through 0 concept_count relative to index' AS VARCHAR(512)) AS covariate_name,


  907 AS analysis_id,
  0 AS concept_id
FROM (
  SELECT DISTINCT covariate_id
  FROM cov_10
  ) t1

;
INSERT INTO analysis_ref (
  analysis_id,
  analysis_name,
  domain_id,

  start_day,
  end_day,

  is_binary,
  missing_means_zero
  )
SELECT 907 AS analysis_id,
  CAST('DistinctConditionCountShortTerm' AS VARCHAR(512)) AS analysis_name,
  CAST('Condition' AS VARCHAR(20)) AS domain_id,

  CAST(NULL AS INT) AS start_day,
  CAST(NULL AS INT) AS end_day,

  CAST('N' AS VARCHAR(1)) AS is_binary,
  CAST('Y' AS VARCHAR(1)) AS missing_means_zero;
insert into omop_orc_results_280.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id,
    count_value, min_value, max_value, avg_value, stdev_value, median_value,
    p10_value, p25_value, p75_value, p90_value, strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('DISTRIBUTION' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.count_value,
    f.min_value,
    f.max_value,
    f.average_value,
    f.standard_deviation,
    f.median_value,
    f.p10_value,
    f.p25_value,
    f.p75_value,
    f.p90_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    77054 as cohort_definition_id,
    1530 as cc_generation_id
  from (select 77054 as cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value from (SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value
FROM (
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_2 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_3 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_6 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_9 UNION ALL
SELECT cohort_definition_id, covariate_id, count_value, min_value, max_value, average_value, standard_deviation, median_value, p10_value, p25_value, p75_value, p90_value FROM cov_10
) all_covariates) W) f
    join (select 77054 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 77054 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join omop_orc.concept c on c.concept_id = fr.concept_id;
insert into omop_orc_results_280.cc_results (type, fa_type, covariate_id, covariate_name, analysis_id, analysis_name, concept_id, count_value, avg_value,
                                                 strata_id, strata_name, cohort_definition_id, cc_generation_id)
  select CAST('PREVALENCE' AS VARCHAR(255)) as type,
    CAST('PRESET' AS VARCHAR(255)) as fa_type,
    f.covariate_id,
    fr.covariate_name,
    ar.analysis_id,
    ar.analysis_name,
    fr.concept_id,
    f.sum_value     as count_value,
    f.average_value as stat_value,
    0 as strata_id,
    CAST('' AS VARCHAR(1000)) as strata_name,
    77054 as cohort_definition_id,
    1530 as cc_generation_id
  from (select 77054 as cohort_definition_id, covariate_id, sum_value, average_value from (SELECT all_covariates.cohort_definition_id,
  all_covariates.covariate_id,
  all_covariates.sum_value,
  CAST(all_covariates.sum_value / (1.0 * total.total_count) AS FLOAT) AS average_value
FROM (SELECT cohort_definition_id, covariate_id, sum_value FROM cov_1 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_4 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_5 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_7 UNION ALL
SELECT cohort_definition_id, covariate_id, sum_value FROM cov_8
) all_covariates
INNER JOIN (
SELECT cohort_definition_id, COUNT(*) AS total_count
FROM omop_orc_results_280.temp_cohort_y2f4nuwx1 
WHERE cohort_definition_id IN (77054) GROUP BY cohort_definition_id
) total
  ON all_covariates.cohort_definition_id = total.cohort_definition_id) W) f
    join (select 77054 as cohort_definition_id, covariate_id, covariate_name, analysis_id, concept_id from (SELECT covariate_id, covariate_name, analysis_id, concept_id  FROM cov_ref) W) fr on fr.covariate_id = f.covariate_id and fr.cohort_definition_id = f.cohort_definition_id
    join (select 77054 as cohort_definition_id, CAST(analysis_id AS INT) analysis_id, analysis_name, domain_id, start_day, end_day, CAST(is_binary AS CHAR(1)) is_binary,CAST(missing_means_zero AS CHAR(1)) missing_means_zero from (SELECT analysis_id, analysis_name, domain_id, start_day, end_day, is_binary, missing_means_zero FROM analysis_ref) W) ar
      on ar.analysis_id = fr.analysis_id and ar.cohort_definition_id = fr.cohort_definition_id
    left join omop_orc.concept c on c.concept_id = fr.concept_id;
TRUNCATE TABLE cov_1;
DROP TABLE cov_1;
TRUNCATE TABLE cov_2;
DROP TABLE cov_2;
TRUNCATE TABLE cov_3;
DROP TABLE cov_3;
TRUNCATE TABLE cov_4;
DROP TABLE cov_4;
TRUNCATE TABLE cov_5;
DROP TABLE cov_5;
TRUNCATE TABLE cov_6;
DROP TABLE cov_6;
TRUNCATE TABLE cov_7;
DROP TABLE cov_7;
TRUNCATE TABLE cov_8;
DROP TABLE cov_8;
TRUNCATE TABLE cov_9;
DROP TABLE cov_9;
TRUNCATE TABLE cov_10;
DROP TABLE cov_10;
TRUNCATE TABLE cov_ref;
DROP TABLE cov_ref;
TRUNCATE TABLE analysis_ref;
DROP TABLE analysis_ref;