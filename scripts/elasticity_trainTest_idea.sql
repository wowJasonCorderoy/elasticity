declare trainToDate DATE;
declare testFromDate DATE;
DECLARE minUnders int64 DEFAULT 7;
DECLARE minOvers int64 DEFAULT 7;
declare gb_list array<STRING>;
DECLARE i INT64 DEFAULT 0;
declare sql_temp STRING;
declare tempString STRING;
declare xVar string default 'log_ASP_v_lag';
declare yVar string default 'log_Sales_Qty_SUoM_v_lag';
DECLARE dateFrom DATE;
DECLARE dateTo DATE;
DECLARE lagDays int64 DEFAULT 7;
DECLARE so ARRAY <STRING>;

DECLARE minElastSlope float64 DEFAULT -20;
DECLARE maxElastSlope float64 DEFAULT 0;
SET so = ["1005","1030"];

#################################################################################
######################################################### START set vars

set dateTo = DATE_ADD(CURRENT_DATE("Australia/Sydney"), INTERVAL -1 DAY);
set dateFrom = DATE_ADD(dateTo, INTERVAL -(364*3) DAY);

set trainToDate = DATE_ADD(dateTo, INTERVAL -(364) DAY);

set testFromDate = DATE_ADD(trainToDate, INTERVAL 1 DAY);

set gb_list = [
'Site, Article, Sales_Unit',
'Site, Price_Family_Description, Sales_Unit',
'Site, Category_Description, SubCategory_Description, Segment_Description, Sales_Unit',
'Site, Category_Description, SubCategory_Description, Sales_Unit',
'Price_Family_Description',
'Price_Family_Description, Sales_Unit',
'Category_Description, SubCategory_Description, Segment_Description',
'Category_Description, SubCategory_Description',
'SalesOrg'
];

######################################################### END set vars
#################################################################################


######################################################### START create main data
#################################################################################

create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData` as (
    with asp_dat as
    (
    SELECT
    ifnull(SalesOrg,'') as SalesOrg,
    ifnull(Site,'') as Site,
    ifnull(Article,'') as Article,
    ifnull((case when Sales_Unit in ('CA1','CA2','CA3') then 'CAR' else Sales_Unit end),'') as Sales_Unit, 
    
    ifnull(Article_Description, '') as Article_Description,
    ifnull(Category_Description, '') as Category_Description, 
    ifnull(SubCategory_Description, '') as SubCategory_Description, 
    ifnull(Segment_Description, '') as Segment_Description, 
    ifnull(Department_Description, '') AS DEPARTMENT_DESCRIPTION,
    ifnull(GeneralManager_Name, '') as GM_DESCRIPTION,
    ifnull(MerchandiseManager_Name, '') AS MM_DESCRIPTION,
    ifnull(MerchandiseManager_Department, '') as MM_Department,
    ifnull((case when Price_Family_Description is null or Price_Family_Description = '' then Article_Description else Price_Family_Description end), '') as Price_Family_Description,
    
    Calendar_Day
    , DATE_ADD(Calendar_Day, INTERVAL -lagDays DAY) as lag_Calendar_Day
    , sum(Sales_ExclTax) as Sales_ExclTax
    , sum(Sales_Qty_SUoM) as Sales_Qty_SUoM
    , (case when sum(Sales_Qty_SUoM) = 0 then NULL else sum(Sales_ExclTax)/sum(Sales_Qty_SUoM) end) as ASP
    , sum(Promo_Sales) as Promo_Sales
    , sum(Promo_Sales_Qty_SUoM) as Promo_Sales_Qty_SUoM
    FROM  `gcp-wow-ent-im-tbl-prod.gs_allgrp_fin_data.fin_group_profit_v`
    WHERE
    Sales_Channel NOT IN ('HDY','HDZ','CCY','CCZ','HD1','CC1') and #instore only. This will help (later) to control for SOH issues.
    SalesOrg in unnest(so) AND
    Calendar_Day >= dateFrom AND 
    Calendar_Day < dateTo
    group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
    )
    select FARM_FINGERPRINT(concat(cast(a.SalesOrg as string), cast(a.Site as string), cast(a.Article as string), cast(a.Sales_Unit as string), cast(a.Article_Description as string), cast(a.Category_Description as string), cast(a.SubCategory_Description as string), cast(a.Segment_Description as string), cast(a.DEPARTMENT_DESCRIPTION as string), cast(a.GM_DESCRIPTION as string), cast(a.MM_DESCRIPTION as string), cast(a.MM_Department as string), cast(a.Price_Family_Description as string), cast(a.Calendar_Day as string)))
    AS row_fingerprint, 
    a.*,
    a.Sales_ExclTax-b.Sales_ExclTax as Sales_ExclTax_diff,
    a.Sales_Qty_SUoM-b.Sales_Qty_SUoM as Sales_Qty_SUoM_diff,
    b.Sales_ExclTax as Sales_ExclTax_lag,
    b.Sales_Qty_SUoM as Sales_Qty_SUoM_lag,
    b.ASP as ASP_lag,
    a.ASP-b.ASP as ASP_diff,
    (case when b.ASP = 0 then null else a.ASP/b.ASP end) as ASP_v_lag,
    (case when b.Sales_Qty_SUoM = 0 then null else a.Sales_Qty_SUoM/b.Sales_Qty_SUoM end) as Sales_Qty_SUoM_v_lag,
    log(case when b.ASP = 0 then null
    when a.ASP/b.ASP < 0 then null
    else a.ASP/b.ASP end) as log_ASP_v_lag,
    log(case when b.Sales_Qty_SUoM = 0 then null
    when a.Sales_Qty_SUoM/b.Sales_Qty_SUoM < 0 then null
    else a.Sales_Qty_SUoM/b.Sales_Qty_SUoM end) as log_Sales_Qty_SUoM_v_lag,
    
    (case when a.Sales_ExclTax = 0 then 0
    when a.Promo_Sales/a.Sales_ExclTax > 1 then 1
    else a.Promo_Sales/a.Sales_ExclTax
    end) as promo_perc,
    
    (case when b.Sales_ExclTax = 0 then 0
    when b.Promo_Sales/b.Sales_ExclTax > 1 then 1
    else b.Promo_Sales/b.Sales_ExclTax
    end) as promo_perc_lag
    
    from asp_dat a
    left join (
    SELECT *
    from asp_dat
    ) b
    on (a.lag_Calendar_Day=b.Calendar_Day) AND
    (a.SalesOrg = b.SalesOrg) and
    (a.Site = b.Site) and
    (a.Article=b.Article) AND
    (a.Sales_Unit=b.Sales_Unit) AND
    (a.Article_Description=b.Article_Description) AND
    (a.Category_Description=b.Category_Description) AND
    (a.SubCategory_Description=b.SubCategory_Description) AND
    (a.Segment_Description=b.Segment_Description) AND
    (a.Department_Description=b.Department_Description) AND
    (a.GM_DESCRIPTION=b.GM_DESCRIPTION) AND
    (a.MM_DESCRIPTION=b.MM_DESCRIPTION) AND
    (a.MM_Department=b.MM_Department) AND
    (a.Price_Family_Description=b.Price_Family_Description)  
        
    where
    b.Sales_Qty_SUoM > 0 AND
    b.Sales_Qty_SUoM is not null AND
    (case when b.ASP = 0 then null else a.ASP/b.ASP end) between 0.25 and 4 AND
    (case when b.ASP = 0 then null else a.ASP/b.ASP end) is not null AND
    log(case when b.ASP = 0 then null when a.ASP/b.ASP < 0 then null else a.ASP/b.ASP end) is not null AND
    log(case when b.Sales_Qty_SUoM = 0 then null when a.Sales_Qty_SUoM/b.Sales_Qty_SUoM < 0 then null else a.Sales_Qty_SUoM/b.Sales_Qty_SUoM end) is not null AND
    (case when b.Sales_Qty_SUoM = 0 then null else a.Sales_Qty_SUoM/b.Sales_Qty_SUoM end) < 100 # remove non-sensicle perc growth
    );

######################################################### END create main data
#################################################################################


#################################################################################
######################################################### START Define functions
CREATE TEMP FUNCTION pasteStrings(a STRING, b STRING, c STRING)
RETURNS STRING
LANGUAGE js AS r"""
return a+b+c;
""";

### Initial ingredients (for calc) sql generator
CREATE TEMP FUNCTION slr(gb STRING, x STRING, y STRING, tbl STRING, minUnders int64, minOvers int64, minElastSlope float64, maxElastSlope float64)
RETURNS STRING
LANGUAGE js AS r"""
  String.prototype.format = function() {
  var args = arguments;
  this.unkeyed_index = 0;
  return this.replace(/\{(\w*)\}/g, function(match, key) { 
    if (key === '') {
      key = this.unkeyed_index;
      this.unkeyed_index++
    }
    if (key == +key) {
      return args[key] !== 'undefined'
      ? args[key]
      : match;
    } else {
      for (var i = 0; i < args.length; i++) {
        if (typeof args[i] === 'object' && typeof args[i][key] !== 'undefined') {
          return args[i][key];
        }
      }
      return match;
    }
  }.bind(this));
};

 gb_string = gb;
 
 gb_list = gb.split(',');
 gb_concat = gb_list.join(", ',', ");
  
  return `SELECT '{gb}' as gb_var,
  concat({gb_concat}) as gb_value,
       (CASE 
       WHEN SLOPE < {minElastSlope} then {minElastSlope} 
       when SLOPE > {maxElastSlope} then {maxElastSlope}
       else SLOPE
       end) as SLOPE,
       (SUM_OF_Y - SLOPE * SUM_OF_X) / N AS INTERCEPT,
       CORRELATION
FROM (
    SELECT {gb},
           N,
           SUM_OF_X,
           SUM_OF_Y,
           (case 
           when STDDEV_OF_X = 0 then null 
           else CORRELATION * STDDEV_OF_Y / STDDEV_OF_X
           end) AS SLOPE,
           CORRELATION
    FROM (
        SELECT {gb},
               COUNT(*) AS N,
               SUM({x}) AS SUM_OF_X,
               SUM({y}) AS SUM_OF_Y,
               STDDEV_POP({x}) AS STDDEV_OF_X,
               STDDEV_POP({y}) AS STDDEV_OF_Y,
               CORR({x},{y}) AS CORRELATION,
               sum(case when {x} > 0.1 then 1 else 0 end) as cnt_overs,
                sum(case when {x} < -0.1 then 1 else 0 end) as cnt_unders
        FROM {tbl}
        GROUP BY {gb})
        where cnt_overs >= {minOvers} and cnt_unders >= {minUnders})`.format({gb:gb, gb_concat:gb_concat, x:x, y:y, tbl: tbl, minOvers:minOvers, minUnders:minUnders, minElastSlope:minElastSlope, maxElastSlope:maxElastSlope});
  """;
  
######################################################### END Define functions
#################################################################################

#################################################################################
######################################################### START train test data generation

create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_train` as (
select *
from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData`
where Calendar_Day <= trainToDate and
(promo_perc > 0.5 or promo_perc_lag > 0.5 and # train only of big asp movements if on promo
ASP_v_lag < 0.9 and Sales_Qty_SUoM_v_lag > 1)
or 
(promo_perc > 0.5 or promo_perc_lag > 0.5 and # train only of big asp movements if on promo
ASP_v_lag > 1.1 and Sales_Qty_SUoM_v_lag < 1)
or
(ASP_v_lag between 0.95 and 1.05) # to control the intercept we train model on near 0% price change week on week

);

create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test` as (
select *
from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData`
where Calendar_Day >= testFromDate and
(promo_perc > 0.5 or promo_perc_lag > 0.5) and
( # test is different to training in that we want to evaluate on only promo days
ASP_v_lag < 0.9 and Sales_Qty_SUoM_v_lag > 1 or # for price drops <p% filter on data with sales increases period on period
ASP_v_lag > 1.1 and Sales_Qty_SUoM_v_lag < 1 or # for price increases >p% filter on data with sales decreases period on period
ASP_v_lag between 0.9 and 1.1 # else if consecutive week promo's just keep all this data.
)

);

# this is all test period data. Whether or not promo sale. Rationale being to store this to fit model on promo days else assume 0 movements.
# will come in handy for cross product elasticity modelling.
create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_allDat` as (
select *
from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData`
where Calendar_Day >= testFromDate and
(promo_perc > 0.5 or promo_perc_lag > 0.5)

);

######################################################### END train test data generation
#################################################################################

#################################################################################
######################################################### START fit models to training data

set sql_temp = "create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_fit` as (";
SET i = 0; # ensure i begins at zero.
LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(gb_list) THEN 
    LEAVE;
  END IF;
  
  # will need to pre-pend with union all all except 1st instance
  set tempString = '';
  
  IF i > 1 THEN 
    set tempString = ' union all ';
  END IF;
  #
   
  set tempString = concat(tempString, '(',
                  slr(gb_list[ORDINAL(i)], xVar, yVar, "`gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_train`", minUnders, minOvers, minElastSlope, maxElastSlope),
                  ')');
    
  set sql_temp = concat(sql_temp, tempString);

END LOOP;

set sql_temp = concat(sql_temp, ');');
execute immediate sql_temp;


######################################################### END fit models to training data
#################################################################################

#################################################################################
######################################################### START predict fit model on test data

-- create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction` as (
-- select a.*, 
-- b.gb_var, b.gb_value,
-- b.INTERCEPT, b.SLOPE,

-- b.INTERCEPT+b.SLOPE*a.log_ASP_v_lag as prediction,
-- a.log_Sales_Qty_SUoM_v_lag-(b.INTERCEPT+b.SLOPE*a.log_ASP_v_lag) as residual

-- from  `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test` a
-- left join
-- (select *
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_fit`
-- --where gb_var = 'Site, Article, Sales_Unit'
-- ) b
-- on  ( concat(a.Site, ',',a.Article, ',',a.Sales_Unit) =b.gb_value)

-- );

### Predict from fitted model to test data
CREATE TEMP FUNCTION mod_pred(gb STRING, x STRING, y STRING, test_tbl STRING, fit_tbl STRING)
RETURNS STRING
LANGUAGE js AS r"""
  String.prototype.format = function() {
  var args = arguments;
  this.unkeyed_index = 0;
  return this.replace(/\{(\w*)\}/g, function(match, key) { 
    if (key === '') {
      key = this.unkeyed_index;
      this.unkeyed_index++
    }
    if (key == +key) {
      return args[key] !== 'undefined'
      ? args[key]
      : match;
    } else {
      for (var i = 0; i < args.length; i++) {
        if (typeof args[i] === 'object' && typeof args[i][key] !== 'undefined') {
          return args[i][key];
        }
      }
      return match;
    }
  }.bind(this));
};

 gb_string = gb;
 
 gb_list = gb.split(',');
 gb_concat = gb_list.join(", ',', ");
  
  return `select a.*,
                  b.INTERCEPT, b.SLOPE,

                  b.INTERCEPT+b.SLOPE*a.{x} as prediction,
                  a.{y}-(b.INTERCEPT+b.SLOPE*a.{x}) as residual

                  from  (
                  select *,
                  '{gb}' as gb_var, concat({gb_concat}) as gb_value
                  from {test_tbl}
                  ) a
                  left join
                  (select *
                  from {fit_tbl}
                  ) b
                  on  ( a.gb_value = b.gb_value) and (a.gb_var = b.gb_var)`.format({gb:gb, gb_concat:gb_concat, x:x, y:y, test_tbl: test_tbl, fit_tbl:fit_tbl});
  """;


set sql_temp = "create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction` as (";
SET i = 0; # ensure i begins at zero.
LOOP
  SET i = i + 1;
  IF i > ARRAY_LENGTH(gb_list) THEN 
    LEAVE;
  END IF;
  
  # will need to pre-pend with union all all except 1st instance
  set tempString = '';
  
  IF i > 1 THEN 
    set tempString = ' union all ';
  END IF;
  #
   
  set tempString = concat(tempString, '(',
                  mod_pred(gb_list[ORDINAL(i)], xVar, yVar, "`gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test`", "`gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_fit`"),
                  ')');
    
  set sql_temp = concat(sql_temp, tempString);

END LOOP;

set sql_temp = concat(sql_temp, ');');
execute immediate sql_temp;


######################################################### END predict fit model on test data
#################################################################################

# calc r-squared
select
a.gb_var,
1- ( sum( pow(residual,2) ) / sum( pow(log_Sales_Qty_SUoM_v_lag-b.mean_log_Sales_Qty_SUoM_v_lag,2) ) ) as r2,
count(*) as n
from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction` a,
(select AVG(log_Sales_Qty_SUoM_v_lag) as mean_log_Sales_Qty_SUoM_v_lag
from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction`
) b
where abs(residual) > 0 and
abs(log_Sales_Qty_SUoM_v_lag) > 0
group by a.gb_var
order by r2 desc;



-- # calc r-squared
-- select
-- a.gb_var,
-- a.Site, a.Article, a.Sales_Unit,
-- 1- ( sum( pow(residual,2) ) / sum( pow(log_Sales_Qty_SUoM_v_lag-b.mean_log_Sales_Qty_SUoM_v_lag,2) ) ) as r2,
-- count(*) as n
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction` a,
-- (select AVG(log_Sales_Qty_SUoM_v_lag) as mean_log_Sales_Qty_SUoM_v_lag
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction`
-- ) b
-- where abs(residual) > 0 and
-- abs(log_Sales_Qty_SUoM_v_lag) > 0 and
-- gb_var = 'Site, Article, Sales_Unit'
-- group by 1,2,3,4
-- --order by r2 desc
-- order by rand()
-- limit 2000;

-- # calc r-squared
-- select
-- a.gb_var,
-- a.Site, a.Article, a.Sales_Unit,
-- c.Sales_ExclTax, c.Promo_Sales,
-- 1- ( sum( pow(residual,2) ) / sum( pow(log_Sales_Qty_SUoM_v_lag-b.mean_log_Sales_Qty_SUoM_v_lag,2) ) ) as r2,
-- count(*) as n
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction` a,
-- (select AVG(log_Sales_Qty_SUoM_v_lag) as mean_log_Sales_Qty_SUoM_v_lag
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData_test_prediction`
-- ) b
-- left join 
-- (
-- select ifnull(Site,'') as Site,
--     ifnull(Article,'') as Article,
--     ifnull((case when Sales_Unit in ('CA1','CA2','CA3') then 'CAR' else Sales_Unit end),'') as Sales_Unit,
--     sum(Sales_ExclTax) as Sales_ExclTax,
--     sum(Promo_Sales) as Promo_Sales
--     FROM  `gcp-wow-ent-im-tbl-prod.gs_allgrp_fin_data.fin_group_profit_v`
--     where Calendar_Day between DATE_ADD(CURRENT_DATE("Australia/Sydney"), INTERVAL -365 DAY) and DATE_ADD(CURRENT_DATE("Australia/Sydney"), INTERVAL -1 DAY)
-- group by 1,2,3
-- ) c on (a.Site=c.Site) and (a.Article=c.Article) and (a.Sales_Unit=c.Sales_Unit)
-- where abs(residual) > 0 and
-- abs(log_Sales_Qty_SUoM_v_lag) > 0 and
-- gb_var = 'Site, Article, Sales_Unit'
-- group by 1,2,3,4,5,6
-- --order by r2 desc
-- --order by rand()
-- order by c.Promo_Sales desc
-- limit 2000
-- ;
