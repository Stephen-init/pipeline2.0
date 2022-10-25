with recursive

/* Variable */
entity as (
    select 'zoomdental' as et
),

fy_rules as (
SELECT '2015' as fy,
	  CAST('2014-07-01' as date) as start_of_fy,
	  CAST('2015-06-30' as date) as end_of_fy
UNION 
SELECT '2016' as fy,
	  CAST('2015-07-01' as date) as start_of_fy,
	  CAST('2016-06-30' as date) as end_of_fy   	  
UNION 
SELECT '2017' as fy,
	  CAST('2016-07-01' as date) as start_of_fy,
	  CAST('2017-06-30' as date) as end_of_fy
UNION 
SELECT '2018' as fy,
	  CAST('2017-07-01' as date) as start_of_fy,
	  CAST('2018-06-30' as date) as end_of_fy
UNION 
SELECT '2019' as fy,
	  CAST('2018-07-01' as date) as start_of_fy,
	  CAST('2019-06-30' as date) as end_of_fy 
UNION 
SELECT '2020' as fy,
	  CAST('2019-07-01' as date) as start_of_fy,
	  CAST('2020-06-30' as date) as end_of_fy
UNION
SELECT '2021' as fy,
	  CAST('2020-07-01' as date) as start_of_fy,
	  CAST('2021-06-30' as date) as end_of_fy       
),

bucket (bkt) as (   -- pay bucket
        VALUES ('AnnualLeave') ,
                ('BasePay') ,
                ('FirstMealAllowance') ,
                ('SubsequentMealAllowance'),
                ('HDA') ,
                ('P115') ,
                ('P150') ,
                ('P250') ,
                ('PaidLeave') ,
                ('PublicHoliday') ,
                ('PublicHolidayNotWorked') ,
                ('Saturday'),
                 ('Sunday') 

),

sm_date as ( -- settlement date
    select
    ('2021-12-31' - '2014-07-01'::date)::int+1 as "2015",
    ('2021-12-31' - '2015-07-01'::date)::int+1  as "2016",
    ('2021-12-31' - '2016-07-01'::date)::int+1  as "2017",
    ('2021-12-31' - '2017-07-01'::date)::int+1 as "2018",
    ('2021-12-31' - '2018-07-01'::date)::int+1 as "2019",
    ('2021-12-31' - '2019-07-01'::date)::int+1 as "2020",
    ('2021-12-31' - '2020-07-01'::date)::int+1 as "2021",
    ('2021-12-31' - '2021-07-01'::date)::int+1  as "2022"
),

rm_interests as ( -- interest rate on remediation payment, Assumption: 2022 has same rate to 2021
    select 
    0.408 as "2015",
    0.343 as "2016",
    0.283 as "2017",
    0.2255 as "2018",
    0.1705 as "2019",
    0.1155 as "2020",
    0.063 as "2021"
),

super_rate as ( -- Superannuation rate 
    select 0.1 as "sr"
),
scg_rate as ( -- Voluntary Interest rate
    select 0.1 as "scg"
),


tax_rate (region,"2015","2016","2017","2018","2019","2020","2021") -- payroll tax rate, Assumption: 2022 has the same rate as 2021
    AS (
        VALUES 
            ('VIC', 0, 0, 0, 0.0485, 0.0485, 0.0485, 0.0485), 
            ('NSW', 0.0545, 0.0545, 0.0545, 0.0545, 0.0545, 0.0545, 0.0485), 
            ('QLD', 0 ,0 ,0 ,0.0475, 0.0475, 0.0495, 0.0495), 
            ('SA', 0, 0, 0, 0.0495, 0.0495,0.0495,0.0495), 
            ('WA', 0, 0, 0, 0.055, 0.055, 0.06, 0.06), 
            ('ACT', 0 ,0 ,0 ,0.0685, 0.0685, 0.0685, 0.0685), 
            ('NT', 0, 0, 0, 0.055, 0.055, 0.055, 0.055),
            ('TAS',0 ,0 ,0 ,0.061, 0.061, 0.061, 0.061)
    ),
penalty_rate (region,"2015","2016","2017","2018","2019","2020","2021") -- payroll tax penalty rate, Assumption: 2022 has the same rate as 2021
    AS (
        VALUES 
            ('VIC',0,0,0,0.2,0.2,0.2,0.2), 
            ('NSW',0.2,0.2,0.2,0.2,0.2,0.2,0.2), 
            ('QLD',0,0,0,0.2,0.2,0.2,0.2), 
            ('SA',0,0,0,0.2,0.2,0.2,0.2), 
            ('WA',0,0,0.25,0.2,0.15,0.10,0.05), 
            ('ACT',0,0,0,0.2,0.2,0.2,0.2), 
            ('NT',0,0,0,0.05,0.05,0.05,0.05),
            ('TAS',0,0,0,0.2,0.2,0.2,0.2)
    ),
interest_rate (region,"2015","2016","2017","2018","2019","2020","2021") -- payroll tax interest rate, Assumption: 2022 has the same rate as 2021
    AS (
        VALUES 
            ('VIC',0,0,0.4138,0.3165,0.2169,0.1215,0.0405), 
            ('NSW',0.6171,0.5143,0.4142,0.3165,0.2169,0.1215,0.0405), 
            ('QLD',0,0,0.4138,0.3165,0.2169,0.1215,0.0405), 
            ('SA',0,0,0.4138,0.3165,0.2169,0.1215,0.0405), 
            ('WA',0,0,0,0,0,0,0), 
            ('ACT',0,0,0.4138,0.3165,0.2169,0.1215,0.0405), 
            ('NT',0,0,0.4138,0.3165,0.2169,0.1215,0.0405),
            ('TAS',0,0,0.3937,0.2964,0.1968,0.1014,0.0204)
    ),

/* pivot rates to use for calculate tax*/
pivot_tax_rate as (
    SELECT region,
       UNNEST(ARRAY [2015, 2016, 2017,2018,2019,2020,2021])::text  AS fy ,
       UNNEST(ARRAY ["2015", "2016", "2017","2018","2019","2020","2021"]) AS tax_rate
    FROM tax_rate
),
pivot_penalty_rate as (
    SELECT region,
       UNNEST(ARRAY [2015, 2016, 2017,2018,2019,2020,2021])::text AS fy,
       UNNEST(ARRAY ["2015", "2016", "2017","2018","2019","2020","2021"]) AS penalty_rate
    FROM penalty_rate
),
pivot_interest_rate as (
    SELECT region,
       UNNEST(ARRAY [2015, 2016, 2017,2018,2019,2020,2021])::text AS fy,
       UNNEST(ARRAY ["2015", "2016", "2017","2018","2019","2020","2021"]) AS interest_rate
    FROM interest_rate
),
pivot_rm_interests as (
    SELECT 
       UNNEST(ARRAY [2015, 2016, 2017,2018,2019,2020,2021])::text  AS fy,
       UNNEST(ARRAY ["2015", "2016", "2017","2018","2019","2020","2021"]) AS rm_interests
    FROM rm_interests
),
pivot_sm_date as (
    SELECT 
       UNNEST(ARRAY [2015, 2016, 2017,2018,2019,2020,2021])::text AS fy,
       UNNEST(ARRAY ["2015", "2016", "2017","2018","2019","2020","2021"]) AS sm_date
    FROM sm_date
),
/* Base model key by payslip_period */
base as ( -- original model from Marcus
    SELECT 
    employee_code,
    variance_id,
    variance.client_id,
    variance.employee_id,
    total_calculated_pay,
    total_actual_pay,
    variance.total_variance AS variance_total_variance,
    unders_only_variance,
    variance_period_id AS PeriodVarianceId,
    variance_id,
    variance_period."start",
    variance_period."end",
    (extract('year' from to_date(cast(case when extract('month' from (to_date(cast(variance_period."end" as TEXT),'YYYY-MM-DD'))) >= 7 
                        then (to_date(cast(variance_period."end" as TEXT),'YYYY')) + interval '1' year
                        else (to_date(cast(variance_period."end" as TEXT),'YYYY'))
            end as TEXT),'YYYY'))) as fyear, -- add financial year
    calculated_pay,
    actual_pay,
    variance_period.total_variance,
    variance_period.overs_variance,
    variance_period.unders_variance,
    variance_period.award,
    variance_period_payslip.payslip_id,
    variance_period_line_variance_id,
    pay_code,
    calculated,
    calculated_hours,
    actual,
    actual_hours,
    variance,
    classification,
    is_base_rate
FROM variance
INNER JOIN employee USING (employee_id)
INNER JOIN variance_period USING (variance_id)
LEFT JOIN variance_period_payslip USING (variance_period_id)
LEFT JOIN variance_period_line_variance USING (variance_period_id)
WHERE variance.client_id=(select * from entity)
),

pil as (
        select 
        employee_code, 
        fyear,
        sum(variance) as add_re_pil,
        sum(greatest(variance,0)) as add_re_pil_excess,
        sum(least(variance,0)) as add_re_pil_shortfall,
        COALESCE (sum(variance) FILTER (WHERE pay_code in ('TC','TLSC')),0) AS add_ote,
        COALESCE (sum(greatest(variance,0)) FILTER (WHERE pay_code in ('TC','TLSC')),0) AS add_ote_excess,
        COALESCE (sum(least(variance,0)) FILTER (WHERE pay_code in ('TC','TLSC')),0) AS add_ote_shatfall,
        sum(actual) as add_re_pil_actual,
        sum(calculated) as add_re_pil_calculated
        from
        (select *,(extract('year' from to_date(cast(case when extract('month' from (to_date(cast("date" as TEXT),'YYYY-MM-DD'))) >= 7 
                                then (to_date(cast("date" as TEXT),'YYYY')) + interval '1' year
                                else (to_date(cast("date" as TEXT),'YYYY'))
                    end as TEXT),'YYYY'))) as fyear
        from zoom_pil where client_id = (select * from entity)) as temp
        group by employee_code,fyear
),

km as (
        select 
        employee_code, 
        fyear,
        sum(variance) as add_re_km,
        sum(greatest(variance,0)) as add_re_km_excess,
        sum(least(variance,0)) as add_re_km_shortfall, 
        sum(actual) as add_re_km_actual,
        sum(calculated) as add_re_km_calculated
        from
        (select *,(extract('year' from to_date(cast(case when extract('month' from (to_date(cast("date" as TEXT),'YYYY-MM-DD'))) >= 7 
                                then (to_date(cast("date" as TEXT),'YYYY')) + interval '1' year
                                else (to_date(cast("date" as TEXT),'YYYY'))
                    end as TEXT),'YYYY'))) as fyear
        from zoom_km where client_id = (select * from entity)) as temp
        group by employee_code,fyear
),

base_group_fyear as ( ---- remediation , variance , ote , km, pil group by fyear
        select
        employee_code,
        fyear::text as fy,
        --summary
        coalesce(OTE_excess,0)+coalesce(add_ote_excess,0) as ote_excess,
        coalesce(OTE_shortfall,0)+coalesce(add_ote_shatfall,0) as ote_shortfall,
        coalesce(OTE_variance,0)+coalesce(add_ote,0) as gross_ote_variance,
        coalesce(non_OTE_excess,0)+coalesce(add_re_km_excess,0)+ coalesce(add_re_pil_excess,0) as non_ote_excess,
        coalesce(non_OTE_shortfall,0)+coalesce(add_re_km_shortfall,0)+ coalesce(add_re_pil_shortfall,0) as non_ote_shortfall,
        coalesce(non_OTE_variance,0)+coalesce(add_re_km,0)+coalesce(add_re_pil,0) as gross_non_ote_variance,
        least(coalesce(OTE_variance,0)+coalesce(add_ote,0),0) as ote_variance,
        least(coalesce(non_OTE_variance,0)+coalesce(add_re_km,0)+coalesce(add_re_pil,0),0) as non_ote_variance,
        coalesce(OTE_variance,0)+coalesce(add_ote,0)+coalesce(non_OTE_variance,0)+coalesce(add_re_km,0)+coalesce(add_re_pil,0) as gross_variance,
        least(coalesce(OTE_variance,0)+coalesce(add_ote,0)+coalesce(non_OTE_variance,0)+coalesce(add_re_km,0)+coalesce(add_re_pil,0),0) as variance,
        least(least(coalesce(OTE_variance,0)+coalesce(add_ote,0)+coalesce(non_OTE_variance,0)+coalesce(add_re_km,0)+coalesce(add_re_pil,0),0) , least(coalesce(OTE_variance,0)+coalesce(add_ote,0),0)) as remediation_payment, 
        case 
            when least(coalesce(OTE_variance,0)+coalesce(add_ote,0)+coalesce(non_OTE_variance,0)+coalesce(add_re_km,0)+coalesce(add_re_pil,0), coalesce(OTE_variance,0)+coalesce(add_ote,0))>0  then 'EXCESS' 
            when least(coalesce(OTE_variance,0)+coalesce(add_ote,0)+coalesce(non_OTE_variance,0)+coalesce(add_re_km,0)+coalesce(add_re_pil,0), coalesce(OTE_variance,0)+coalesce(add_ote,0))<0 then 'SHORTFALL' 
            else 'No Variance' 
            end as "status",
        --ote exceess
        coalesce(OTE_excess,0) as ote_wage_excess, 
        coalesce(add_ote_excess,0) as ote_pil_excess,
        --ote shortfall
        coalesce(OTE_shortfall,0) as ote_wage_shortfall,
        coalesce(add_ote_shatfall,0) as ote_pil_shortfall,
        --ote variance
        coalesce(OTE_variance,0) as ote_wage,
        coalesce(add_ote,0) as ote_pil,
        --non ote exceess
        coalesce(non_OTE_excess,0) as non_ote_wage_excess, 
        coalesce(add_re_km_excess,0) as non_ote_km_excess,
        coalesce(add_re_pil_excess,0) as non_ote_pil_excess, 
        --non ote shortfall
        coalesce(non_OTE_shortfall,0) as non_ote_wage_shortfall ,
        coalesce(add_re_km_shortfall,0) as non_ote_km_shortfall,
        coalesce(add_re_pil_shortfall,0) as non_ote_pil_shortfall,
        --non ote variance
        coalesce(non_OTE_variance,0) as non_ote_wage,
        coalesce(add_re_km,0) as non_ote_km,
        coalesce(add_re_pil,0) as non_ote_pil,
        -- actual vs cal
        coalesce(OTE_actual,0) as ote_actual,
        coalesce(OTE_calculated,0) as ote_calculated,
        coalesce(add_re_km_actual,0) as km_actual,
        coalesce(add_re_km_calculated,0) as km_calculated,
        coalesce(add_re_pil_actual,0) as pil_actual,
        coalesce(add_re_pil_calculated,0) as pil_calculated
        from
        (select 
        employee_code,
        fyear,
        COALESCE (sum(greatest(variance,0)) FILTER (WHERE pay_code in (select * from bucket)),0) as OTE_excess,
        COALESCE (sum(least(variance,0)) FILTER (WHERE pay_code in (select * from bucket)),0) as OTE_shortfall,
        COALESCE (sum(variance) FILTER (WHERE pay_code in (select * from bucket)),0) as OTE_variance,
        COALESCE (sum(greatest(variance,0)) FILTER (WHERE pay_code not in (select * from bucket)),0) as non_OTE_excess,
        COALESCE (sum(least(variance,0)) FILTER (WHERE pay_code not in (select * from bucket)),0) as non_OTE_shortfall,
        COALESCE (sum(variance) FILTER (WHERE pay_code not in (select * from bucket)),0) AS non_OTE_variance,
        COALESCE (sum(actual) FILTER (WHERE pay_code in (select * from bucket)),0) AS OTE_actual,
        COALESCE (sum(calculated) FILTER (WHERE pay_code in (select * from bucket)),0) AS OTE_calculated
        from base
        group by employee_code,fyear) as temp
        full outer JOIN pil using (employee_code,fyear) 
        full outer JOIN km using (employee_code,fyear)
        where fyear<>'2022' 
    ),
-- Below from Employee -> Attributes_Over_Time
 pay_region as ( --- state at each fyear
    SELECT  employee_code
        ,CAST(cast(replace(cast(arr.item_object->'StartDate' as TEXT),'"','') as Date)- INTERVAL '720 day' as date)  as effective_from
        ,CAST(cast(replace(cast(arr.item_object->'EndDate' as TEXT),'"','') as Date)+ INTERVAL '720 day' as date)  as effective_to
        ,replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') as ref
        ,case when replace(cast(arr.item_object->'Value'->'Value' as TEXT),'"','')='National' then 'ACT' else replace(cast(arr.item_object->'Value'->'Value' as TEXT),'"','') end as region
    FROM employee
    ,jsonb_array_elements(attributes_over_time) with ordinality arr(item_object, position)
    where replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') = 'state'
),

unionised_pay_region as (
SELECT employee_code,fy , region
from 
(SELECT a.*
	  ,ROW_NUMBER() OVER (PARTITION BY a.employee_code, a.fy ORDER BY a.rule_over_ride asc) rule_over_ride_keeper
FROM (
SELECT fy.fy
	  ,ch.*
	  ,1 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN pay_region ch on ch.effective_to between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,2 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN pay_region ch on ch.effective_from between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,3 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN pay_region ch on fy.end_of_fy between ch.effective_from and ch.effective_to
) a ) b 
WHERE rule_over_ride_keeper = 1
),

contract_hours as ( --- contracted hours at each fyear
    SELECT  employee_code
        ,cast(replace(cast(arr.item_object->'StartDate' as TEXT),'"','') as Date) as effective_from
        ,CAST(cast(replace(cast(arr.item_object->'EndDate' as TEXT),'"','') as Date)+ INTERVAL '720 day' as date)  as effective_to
        ,replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') as ref
        ,replace(cast(arr.item_object->'Value'->'Value' as TEXT),'"','') as WeeklyContractedhours
    FROM employee
    ,jsonb_array_elements(attributes_over_time) with ordinality arr(item_object, position)
    where replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') = 'WeeklyContractedHours'
),

unionised_contract_hours as (
SELECT employee_code,fy , weeklycontractedhours
from 
(SELECT a.*
	  ,ROW_NUMBER() OVER (PARTITION BY a.employee_code, a.fy ORDER BY a.rule_over_ride asc) rule_over_ride_keeper
FROM (
SELECT fy.fy
	  ,ch.*
	  ,1 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN contract_hours ch on ch.effective_to between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,2 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN contract_hours ch on ch.effective_from between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,3 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN contract_hours ch on fy.end_of_fy between ch.effective_from and ch.effective_to
) a ) b 
WHERE rule_over_ride_keeper = 1
),

positions as ( -- position title at each fyear
    SELECT  employee_code
        ,cast(replace(cast(arr.item_object->'StartDate' as TEXT),'"','') as Date) as effective_from
        ,CAST(cast(replace(cast(arr.item_object->'EndDate' as TEXT),'"','') as Date)+ INTERVAL '720 day' as date)  as effective_to
        ,replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') as ref
        ,replace(cast(arr.item_object->'Value'->'Value' as TEXT),'"','') as positions
    FROM employee
    ,jsonb_array_elements(attributes_over_time) with ordinality arr(item_object, position)
    where replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') = 'POSITION'
),

unionised_positions as (
SELECT employee_code,fy , positions
from 
(SELECT a.*
	  ,ROW_NUMBER() OVER (PARTITION BY a.employee_code, a.fy ORDER BY a.rule_over_ride asc) rule_over_ride_keeper
FROM (
SELECT fy.fy
	  ,ch.*
	  ,1 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN positions ch on ch.effective_to between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,2 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN positions ch on ch.effective_from between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,3 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN positions ch on fy.end_of_fy between ch.effective_from and ch.effective_to
) a ) b 
WHERE rule_over_ride_keeper = 1
),

company as ( -- Dental is pre-set but other clients are from Attribute_over_time at the final year
    SELECT  employee_code
        ,cast(replace(cast(arr.item_object->'StartDate' as TEXT),'"','') as Date) as effective_from
        ,CAST(cast(replace(cast(arr.item_object->'EndDate' as TEXT),'"','') as Date)+ INTERVAL '720 day' as date)  as effective_to
        ,replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') as ref
        ,replace(cast(arr.item_object->'Value'->'Value' as TEXT),'"','') as company
    FROM employee
    ,jsonb_array_elements(attributes_over_time) with ordinality arr(item_object, position)
    where replace(cast(arr.item_object->'Value'->'Key' as TEXT),'"','') = 'COMPANY'
),

unionised_company as (
select
employee_code,company
from
(SELECT employee_code,fy , company ,ROW_NUMBER() OVER (PARTITION BY employee_code ORDER BY fy desc) as kk
from 
(SELECT a.*
	  ,ROW_NUMBER() OVER (PARTITION BY a.employee_code, a.fy ORDER BY a.rule_over_ride asc) rule_over_ride_keeper
FROM (
SELECT fy.fy
	  ,ch.*
	  ,1 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN company ch on ch.effective_to between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,2 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN company ch on ch.effective_from between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,3 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN company ch on fy.end_of_fy between ch.effective_from and ch.effective_to
) a ) b 
WHERE rule_over_ride_keeper = 1) as c
where kk=1
),

--- Below are from employee -> Classifications
classification as ( -- classification/award in the award at each fyear
    SELECT  employee_code
        ,cast(replace(cast(arr.item_object->'StartDate' as TEXT),'"','') as Date) as effective_from
        ,CAST(cast(replace(cast(arr.item_object->'EndDate' as TEXT),'"','') as Date)+ INTERVAL '720 day' as date)  as effective_to
        ,replace(cast(arr.item_object->'Value'->'Classification' as TEXT),'"','') as classification
        ,replace(cast(arr.item_object->'Value'->'Award' as TEXT),'"','') as award
    FROM employee
    ,jsonb_array_elements(classifications) with ordinality arr(item_object, position)
),

unionised_classification as (
SELECT employee_code,fy , classification ,award
from 
(SELECT a.*
	  ,ROW_NUMBER() OVER (PARTITION BY a.employee_code, a.fy ORDER BY a.rule_over_ride asc) rule_over_ride_keeper
FROM (
SELECT fy.fy
	  ,ch.*
	  ,1 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN classification ch on ch.effective_to between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,2 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN classification ch on ch.effective_from between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,3 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN classification ch on fy.end_of_fy between ch.effective_from and ch.effective_to
) a ) b 
WHERE rule_over_ride_keeper = 1
),

--Below are from employee-> employee_type
employee_type as (
    SELECT  employee_code
        ,cast(replace(cast(arr.item_object->'StartDate' as TEXT),'"','') as Date) as effective_from
        ,CAST(cast(replace(cast(arr.item_object->'EndDate' as TEXT),'"','') as Date)+ INTERVAL '720 day' as date)  as effective_to
        ,case when replace(cast(arr.item_object->'Value' as TEXT),'"','') = '4' then 'Full Time'
        when replace(cast(arr.item_object->'Value' as TEXT),'"','') ='3' then 'Casual' 
        when replace(cast(arr.item_object->'Value' as TEXT),'"','') ='2' then 'Part Time' end as employee_type
    FROM employee
    ,jsonb_array_elements(employment_types) with ordinality arr(item_object, position)
),

unionised_employee_type as (
SELECT employee_code,fy , employee_type
from 
(SELECT a.*
	  ,ROW_NUMBER() OVER (PARTITION BY a.employee_code, a.fy ORDER BY a.rule_over_ride asc) rule_over_ride_keeper
FROM (
SELECT fy.fy
	  ,ch.*
	  ,1 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN employee_type ch on ch.effective_to between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,2 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN employee_type ch on ch.effective_from between fy.start_of_fy and fy.end_of_fy
union
SELECT fy.fy
	  ,ch.*
	  ,3 as rule_over_ride
FROM fy_rules fy	  	  
INNER JOIN employee_type ch on fy.end_of_fy between ch.effective_from and ch.effective_to
) a ) b 
WHERE rule_over_ride_keeper = 1
),

pay_code_detail as (
    SELECT 
    ref[1] as employee_code,
    ref[2]::text as fy,
    *
    FROM crosstab(
        $$
        with base as (
        SELECT 
        employee_code,
        variance_id,
        variance.client_id,
        variance.employee_id,
        total_calculated_pay,
        total_actual_pay,
        variance.total_variance AS variance_total_variance,
        unders_only_variance,
        variance_period_id AS PeriodVarianceId,
        variance_id,
        variance_period."start",
        variance_period."end",
        (extract('year' from to_date(cast(case when extract('month' from (to_date(cast(variance_period."end" as TEXT),'YYYY-MM-DD'))) >= 7 
                            then (to_date(cast(variance_period."end" as TEXT),'YYYY')) + interval '1' year
                            else (to_date(cast(variance_period."end" as TEXT),'YYYY'))
                end as TEXT),'YYYY'))) as fyear, -- add financial year
        calculated_pay,
        actual_pay,
        variance_period.total_variance,
        variance_period.overs_variance,
        variance_period.unders_variance,
        variance_period.award,
        variance_period_payslip.payslip_id,
        variance_period_line_variance_id,
        pay_code,
        calculated,
        calculated_hours,
        actual,
        actual_hours,
        variance,
        classification,
        is_base_rate
    FROM variance
    INNER JOIN employee USING (employee_id)
    INNER JOIN variance_period USING (variance_id)
    LEFT JOIN variance_period_payslip USING (variance_period_id)
    LEFT JOIN variance_period_line_variance USING (variance_period_id)
    WHERE variance.client_id='zoomdental'
    )
    select array[employee_code,fyear::text] as ref,
    pay_code,
    json_build_object('payslip', sum(actual),
                    'calculated', sum(calculated),
                    'gross_Variance',sum(variance),
                    'calculated_hours',sum(calculated_hours),
                    'actual_hours',sum(actual_hours)
                        )
    from base group by employee_code,fyear,pay_code
    $$
    ,$$
    VALUES 
    ('VehicleAllowance'),
    ('LaundryAllowancePartTimeCasual'),
    ('P150'),
    ('PaidLeave'),
    ('UniformAllowance'),
    ('P115'),
    ('AnnualLeave'),
    ('Ot200'),
    ('BasePay'),
    ('PublicHolidayNotWorked'),
    ('P250'),
    ('UniformAllowanceCasual'),
    ('Ot250'),
    ('LaundryAllowance'),
    ('Saturday'),
    ('Sunday'),
    ('Ot150'),
    ('FirstAidAllowance'),
    ('SubsequentMealAllowance'),
    ('FirstMealAllowance'),
    ('PublicHoliday')
    $$)
        AS variance (
                "ref" text [],  
                "VehicleAllowance" json,
                "LaundryAllowancePartTimeCasual" json,
                "P150" json,
                "PaidLeave" json,
                "UniformAllowance" json,
                "P115" json,
                "AnnualLeave" json,
                "Ot200" json,
                "BasePay" json,
                "PublicHolidayNotWorked" json,
                "P250" json,
                "UniformAllowanceCasual" json,
                "Ot250" json,
                "LaundryAllowance" json,
                "Saturday" json,
                "Sunday" json,
                "Ot150" json,
                "FirstAidAllowance" json,
                "SubsequentMealAllowance" json,
                "FirstMealAllowance" json,
                "PublicHoliday" json)
),

pay_code_detail_unset as (
select employee_code,
fy,
"AnnualLeave"->'actual_hours' as AnnualLeave_actual_hours,
"AnnualLeave"->'calculated_hours' as AnnualLeave_calculated_hours,
"AnnualLeave"->'calculated' as AnnualLeave_calculated,
"AnnualLeave"->'gross_Variance' as AnnualLeave_gross_Variance,
"AnnualLeave"->'payslip' as AnnualLeave_payslip,
"BasePay"->'actual_hours' as BasePay_actual_hours,
"BasePay"->'calculated_hours' as BasePay_calculated_hours,
"BasePay"->'calculated' as BasePay_calculated,
"BasePay"->'gross_Variance' as BasePay_gross_Variance,
"BasePay"->'payslip' as BasePay_payslip,
"FirstAidAllowance"->'actual_hours' as FirstAidAllowance_actual_hours,
"FirstAidAllowance"->'calculated_hours' as FirstAidAllowance_calculated_hours,
"FirstAidAllowance"->'calculated' as FirstAidAllowance_calculated,
"FirstAidAllowance"->'gross_Variance' as FirstAidAllowance_gross_Variance,
"FirstAidAllowance"->'payslip' as FirstAidAllowance_payslip,
"FirstMealAllowance"->'actual_hours' as FirstMealAllowance_actual_hours,
"FirstMealAllowance"->'calculated_hours' as FirstMealAllowance_calculated_hours,
"FirstMealAllowance"->'calculated' as FirstMealAllowance_calculated,
"FirstMealAllowance"->'gross_Variance' as FirstMealAllowance_gross_Variance,
"FirstMealAllowance"->'payslip' as FirstMealAllowance_payslip,
"LaundryAllowance"->'actual_hours' as LaundryAllowance_actual_hours,
"LaundryAllowance"->'calculated_hours' as LaundryAllowance_calculated_hours,
"LaundryAllowance"->'calculated' as LaundryAllowance_calculated,
"LaundryAllowance"->'gross_Variance' as LaundryAllowance_gross_Variance,
"LaundryAllowance"->'payslip' as LaundryAllowance_payslip,
"LaundryAllowancePartTimeCasual"->'actual_hours' as LaundryAllowancePartTimeCasual_actual_hours,
"LaundryAllowancePartTimeCasual"->'calculated_hours' as LaundryAllowancePartTimeCasual_calculated_hours,
"LaundryAllowancePartTimeCasual"->'calculated' as LaundryAllowancePartTimeCasual_calculated,
"LaundryAllowancePartTimeCasual"->'gross_Variance' as LaundryAllowancePartTimeCasual_gross_Variance,
"LaundryAllowancePartTimeCasual"->'payslip' as LaundryAllowancePartTimeCasual_payslip,
"Ot150"->'actual_hours' as Ot150_actual_hours,
"Ot150"->'calculated_hours' as Ot150_calculated_hours,
"Ot150"->'calculated' as Ot150_calculated,
"Ot150"->'gross_Variance' as Ot150_gross_Variance,
"Ot150"->'payslip' as Ot150_payslip,
"Ot200"->'actual_hours' as Ot200_actual_hours,
"Ot200"->'calculated_hours' as Ot200_calculated_hours,
"Ot200"->'calculated' as Ot200_calculated,
"Ot200"->'gross_Variance' as Ot200_gross_Variance,
"Ot200"->'payslip' as Ot200_payslip,
"Ot250"->'actual_hours' as Ot250_actual_hours,
"Ot250"->'calculated_hours' as Ot250_calculated_hours,
"Ot250"->'calculated' as Ot250_calculated,
"Ot250"->'gross_Variance' as Ot250_gross_Variance,
"Ot250"->'payslip' as Ot250_payslip,
"P115"->'actual_hours' as P115_actual_hours,
"P115"->'calculated_hours' as P115_calculated_hours,
"P115"->'calculated' as P115_calculated,
"P115"->'gross_Variance' as P115_gross_Variance,
"P115"->'payslip' as P115_payslip,
"P150"->'actual_hours' as P150_actual_hours,
"P150"->'calculated_hours' as P150_calculated_hours,
"P150"->'calculated' as P150_calculated,
"P150"->'gross_Variance' as P150_gross_Variance,
"P150"->'payslip' as P150_payslip,
"P250"->'actual_hours' as P250_actual_hours,
"P250"->'calculated_hours' as P250_calculated_hours,
"P250"->'calculated' as P250_calculated,
"P250"->'gross_Variance' as P250_gross_Variance,
"P250"->'payslip' as P250_payslip,
"PaidLeave"->'actual_hours' as PaidLeave_actual_hours,
"PaidLeave"->'calculated_hours' as PaidLeave_calculated_hours,
"PaidLeave"->'calculated' as PaidLeave_calculated,
"PaidLeave"->'gross_Variance' as PaidLeave_gross_Variance,
"PaidLeave"->'payslip' as PaidLeave_payslip,
"PublicHoliday"->'actual_hours' as PublicHoliday_actual_hours,
"PublicHoliday"->'calculated_hours' as PublicHoliday_calculated_hours,
"PublicHoliday"->'calculated' as PublicHoliday_calculated,
"PublicHoliday"->'gross_Variance' as PublicHoliday_gross_Variance,
"PublicHoliday"->'payslip' as PublicHoliday_payslip,
"PublicHolidayNotWorked"->'actual_hours' as PublicHolidayNotWorked_actual_hours,
"PublicHolidayNotWorked"->'calculated_hours' as PublicHolidayNotWorked_calculated_hours,
"PublicHolidayNotWorked"->'calculated' as PublicHolidayNotWorked_calculated,
"PublicHolidayNotWorked"->'gross_Variance' as PublicHolidayNotWorked_gross_Variance,
"PublicHolidayNotWorked"->'payslip' as PublicHolidayNotWorked_payslip,
"Saturday"->'actual_hours' as Saturday_actual_hours,
"Saturday"->'calculated_hours' as Saturday_calculated_hours,
"Saturday"->'calculated' as Saturday_calculated,
"Saturday"->'gross_Variance' as Saturday_gross_Variance,
"Saturday"->'payslip' as Saturday_payslip,
"SubsequentMealAllowance"->'actual_hours' as SubsequentMealAllowance_actual_hours,
"SubsequentMealAllowance"->'calculated_hours' as SubsequentMealAllowance_calculated_hours,
"SubsequentMealAllowance"->'calculated' as SubsequentMealAllowance_calculated,
"SubsequentMealAllowance"->'gross_Variance' as SubsequentMealAllowance_gross_Variance,
"SubsequentMealAllowance"->'payslip' as SubsequentMealAllowance_payslip,
"Sunday"->'actual_hours' as Sunday_actual_hours,
"Sunday"->'calculated_hours' as Sunday_calculated_hours,
"Sunday"->'calculated' as Sunday_calculated,
"Sunday"->'gross_Variance' as Sunday_gross_Variance,
"Sunday"->'payslip' as Sunday_payslip,
"UniformAllowance"->'actual_hours' as UniformAllowance_actual_hours,
"UniformAllowance"->'calculated_hours' as UniformAllowance_calculated_hours,
"UniformAllowance"->'calculated' as UniformAllowance_calculated,
"UniformAllowance"->'gross_Variance' as UniformAllowance_gross_Variance,
"UniformAllowance"->'payslip' as UniformAllowance_payslip,
"UniformAllowanceCasual"->'actual_hours' as UniformAllowanceCasual_actual_hours,
"UniformAllowanceCasual"->'calculated_hours' as UniformAllowanceCasual_calculated_hours,
"UniformAllowanceCasual"->'calculated' as UniformAllowanceCasual_calculated,
"UniformAllowanceCasual"->'gross_Variance' as UniformAllowanceCasual_gross_Variance,
"UniformAllowanceCasual"->'payslip' as UniformAllowanceCasual_payslip,
"VehicleAllowance"->'actual_hours' as VehicleAllowance_actual_hours,
"VehicleAllowance"->'calculated_hours' as VehicleAllowance_calculated_hours,
"VehicleAllowance"->'calculated' as VehicleAllowance_calculated,
"VehicleAllowance"->'gross_Variance' as VehicleAllowance_gross_Variance,
"VehicleAllowance"->'payslip' as VehicleAllowance_payslip
from pay_code_detail
),

base_info as (
    select 
    employee_code,
    'zoomdental' as client_id,
    fy,
    region,
    weeklycontractedhours,
    positions,
    classification,
    award,
    employee_type,
    'Dental Corporation Pty Ltd' as company
    FROM unionised_classification
    left join unionised_contract_hours using (employee_code,fy)
    left join unionised_positions using (employee_code,fy)
    left join unionised_pay_region using (employee_code,fy)
    left join unionised_employee_type using (employee_code,fy)
    left join unionised_company using (employee_code)
),

final_master_data as (
    select
    *
    from
    (select 
    *,
    (select * from super_rate) as sr,
    (select * from scg_rate) as scg
    from base_group_fyear
    left join base_info using (employee_code,fy)
    left join pivot_rm_interests using (fy)
    left join pivot_sm_date using (fy)) as temp
    left join pivot_tax_rate using (region,fy)
    left join pivot_penalty_rate using (region,fy)
    left join pivot_interest_rate using (region,fy)
    

),
report as (
    select 
    employee_code as "Employee Code",
    client_id as "Client ID",
    fy as "Financial Year",
    region as "State",
    company as "Entity",
    award as "Award",
    remediation_payment as "Remediation Payments",
    remediation_payment*rm_interests as "Interest on Remediation Payments", 
    ote_variance as "OTE Variance",
    ote_variance*sr as  "Superannuation", 
    ote_variance*sr*scg*sm_date/365 as "Voluntary Interest on Super",
    (remediation_payment+ote_variance*sr+ote_variance*sr*scg*sm_date/365)*tax_rate as "Payroll Tax",
    (remediation_payment+ote_variance*sr+ote_variance*sr*scg*sm_date/365)*tax_rate*penalty_rate as "Penalty on Payroll Tax",
    (remediation_payment+ote_variance*sr+ote_variance*sr*scg*sm_date/365)*tax_rate*penalty_rate*interest_rate  as "Interest on Payroll Tax",
    "status" as "Status",
    rm_interests as "Interest Rate on Remediation Payments",
    sm_date as "Settlement Date",
    sr as "Superannuation Rate",
    scg as "SCG Rate",
    tax_rate as "Payroll Tax Rate",
    penalty_rate as "Payroll Tax Penalty Rate",
    interest_rate as " Payroll Tax Interest Rate",
    positions as "Position Title",
    classification as "Classification in the Award",
    employee_type as "Employee Type",
    'Day Worker' as "Employee Status",
    weeklycontractedhours as "Contract Hours",
    ote_excess as "OTE Excess",
    ote_shortfall as "OTE Shortfall",
    gross_ote_variance as "Gross OTE Variance",
    ote_variance as "OTE Variance",
    non_ote_excess as "Non OTE Excess",
    non_ote_shortfall as "Non OTE Shortfall",
    gross_non_ote_variance as "Gross Non OTE Variance",
    non_ote_variance as "Non OTE Variance",
    gross_variance as "Gross Variance",
    variance as "Variance",
    ote_wage_shortfall,
    ote_pil_shortfall,
    ote_wage,
    ote_pil,
    non_ote_wage_excess,
    non_ote_km_excess,
    non_ote_pil_excess,
    non_ote_wage_shortfall,
    non_ote_km_shortfall,
    non_ote_pil_shortfall,
    non_ote_wage,
    non_ote_km,
    non_ote_pil,
    ote_actual,
    ote_calculated,
    km_actual,
    km_calculated,
    pil_actual,
    pil_calculated,
    pay_code_detail_unset.*
from final_master_data 
left join pay_code_detail_unset using (employee_code,fy)
where client_id is not null and region is not null
),
nagitive_variance as (
    select count(distinct "Employee Code") from report where "Financial Year"<>'2022' and "Status"='SHORTFALL'
)
select * from report