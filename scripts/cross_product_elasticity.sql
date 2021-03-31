
DECLARE l_articles ARRAY <STRING>;
DECLARE alreadyRun_articles ARRAY <STRING>;
DECLARE n FLOAT64;
DECLARE nstring STRING;
DECLARE minSales INT64;
DECLARE tempString STRING;

############# declare functions

CREATE TEMP FUNCTION convertNumberToString(a FLOAT64)
RETURNS STRING
LANGUAGE js AS r"""
return a.toString();
""";

### Create function that generates sql string for create table with different indexes
CREATE TEMP FUNCTION genSQL(i STRING)
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
  
  return `create or replace table {backTick}gcp-wow-finance-de-lab-dev.price_elasticity.toDelete_crossProductElasticity_summary_{i}{backTick} as (
  select *
  from {backTick}gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity_int_slope{backTick}
  );`.format({i:i, backTick:"`"});
  """;
  
#############

SET minSales = 10000000;
SET n = 0;
SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales limit 5);
SET alreadyRun_articles = [""];

select ARRAY_LENGTH(l_articles);

create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.temp_dat` as (
  #create temp table dat as (
  SELECT a.*, b.y_intercept, b.slope,
  b.y_intercept+b.slope*log_ASP_v_lag as pred_log_Sales_Qty_SUoM_v_lag
  from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElastData` a
  left join `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_site_article` b
  on (a.SalesOrg=b.SalesOrg) and (a.Site=b.Site) and (a.Article=b.Article) and (a.Sales_Unit=b.Sales_Unit)
  where --a.Site = '1004' and
  b.slope is not null 
  --and a.Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` limit 10
  --where past12m_Sales_ExclTax > 4e7 or
  --past13w_Sales_ExclTax > (4e7/4) or
  --past4w_Sales_ExclTax > (4e7/12)
  --)
  );

while ARRAY_LENGTH(l_articles) > 0 DO

  set nstring = convertNumberToString(n);
  
  create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.temp_all_comb` as (
  #create temp table join_article_Dates as (
  select a.SalesOrg,	a.Site, a.Calendar_Day,
  a.Article as Article_a, b.Article as Article_b,
  a.Sales_Unit as Sales_Unit_a, b.Sales_Unit as Sales_Unit_b,

  a.log_ASP_v_lag as log_ASP_v_lag_a, b.log_ASP_v_lag as log_ASP_v_lag_b,
  --a.log_Sales_Qty_SUoM_v_lag as log_Sales_Qty_SUoM_v_lag_a, b.log_Sales_Qty_SUoM_v_lag as log_Sales_Qty_SUoM_v_lag_b,

  a.log_Sales_Qty_SUoM_v_lag-a.pred_log_Sales_Qty_SUoM_v_lag as pred_residual_a,
  b.log_Sales_Qty_SUoM_v_lag-b.pred_log_Sales_Qty_SUoM_v_lag as pred_residual_b

  from (
  select * 
  from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_dat`
  #from dat 
  where --abs(log_ASP_v_lag) > 0.1 and
  Article in unnest(l_articles)
  #and Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` limit 10)

  ) a

  inner join (select * from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_dat` where Article not in unnest(alreadyRun_articles) and
  Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > 1e6)
  ) b on (a.SalesOrg=b.SalesOrg) and (a.Site=b.Site) and (a.Calendar_Day=b.Calendar_Day)
  --where a.Article != b.Article and a.Sales_Unit != b.Sales_Unit

  --where a.Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` limit 10)
  --and b.Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > (1e6))

  order by a.SalesOrg,	a.Site, a.Article, b.Article, a.Calendar_Day
  );
  
  # we want to get both directions so do the switcheroo!
  create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.temp_join_article_Dates` as (
  (
  select SalesOrg,	Site,	Calendar_Day,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b,	log_ASP_v_lag_a,	pred_residual_b
  from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_all_comb`
  )
  union all
  (
  select SalesOrg,	Site,	Calendar_Day,	Article_b as Article_a,	Article_a as Article_b,	Sales_Unit_b as Sales_Unit_a,	Sales_Unit_a as Sales_Unit_b,	log_ASP_v_lag_b as log_ASP_v_lag_a,	pred_residual_a as pred_residual_b
  from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_all_comb`
  where Article_a != Article_b # don't need 2 records of same article
  )
  );
  

  create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity` as (
  #create temp table crossProductElasticity as (
      SELECT SalesOrg,	Site,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b,
      avg(log_ASP_v_lag_a) as mean_x,
      avg(pred_residual_b) as mean_y,
      corr(log_ASP_v_lag_a, pred_residual_b) as corr_xy,
      stddev(log_ASP_v_lag_a) as sd_x,
      stddev(pred_residual_b) as sd_y,
      count(*) as n
      from  `gcp-wow-finance-de-lab-dev.price_elasticity.temp_join_article_Dates`
      #FROM join_article_Dates
      group by SalesOrg,	Site,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b
      );

  create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity_int_slope` as (
  #create temp table crossProductElasticity_int_slope as (
      select a.*,
      if( a.sd_x=0,Null,a.corr_xy*a.sd_y/a.sd_x ) as slope,
      if( a.sd_x=0,Null,a.mean_y-a.mean_x*(a.corr_xy*a.sd_y/a.sd_x) ) as y_intercept
      from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity` a
      #from crossProductElasticity a
      where ifnull(a.n,0)>=14
      order by a.SalesOrg,	a.Article_a,	a.Article_b,	a.Sales_Unit_a,	a.Sales_Unit_b
      );
 
--   IF n = 0 then
--       create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary` as (
--       select *
--       from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity_int_slope`
--       #from crossProductElasticity_int_slope
--       );
--   ELSE
--       create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary` as (
--       select * from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary`
--       union all
--       #(select * from crossProductElasticity_int_slope)
--       (select * from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity_int_slope`)
--       );
--   END IF;

    set tempString = genSQL(nstring);
    EXECUTE IMMEDIATE tempString;

    SET n = n+1;
    SET alreadyRun_articles = ARRAY_CONCAT(alreadyRun_articles, l_articles);
    #SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > 10e6 and Article not in unnest(alreadyRun_articles) limit 10);
    #SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales and Article not in (select distinct Article_a from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary`) limit 5);
    SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales and Article not in unnest(alreadyRun_articles) limit 5);
  
  END WHILE;
    



select count(*)
from `gcp-wow-finance-de-lab-dev.price_elasticity.toDelete_crossProductElasticity_summary_*`
group by Article_a
having n > 2e6



-- #######
-- with abc as (
-- select a.SalesOrg,	a.Article_a,	b.Article_Description as Article_a_Description, a.Article_b,	c.Article_Description as Article_b_Description, a.Sales_Unit_a,	a.Sales_Unit_b,
-- avg((case when a.slope<0 then 1 else 0 end)) as perc_slope_lt_0,
-- avg((case when a.slope>0 then 1 else 0 end)) as perc_slope_gt_0,
-- e.median_slope,
-- e.median_intercept,
-- d.lift,
-- count(*) as n,
-- b.past12m_Sales_ExclTax as Article_a_past12m_Sales_ExclTax,
-- c.past12m_Sales_ExclTax as Article_b_past12m_Sales_ExclTax

-- from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary_*` a
-- left join `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` b on (a.SalesOrg=b.SalesOrg) and (a.Article_a=b.Article) and (a.Sales_Unit_a=b.Sales_Unit)
-- left join `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` c on (a.SalesOrg=c.SalesOrg) and (a.Article_b=c.Article) and (a.Sales_Unit_b=c.Sales_Unit)
-- left join `gcp-wow-finance-de-lab-dev.price_elasticity.itemAssociationPairs`  d on (a.Article_a=d.article_a) and (a.Article_b=d.article_b)

-- left join (select distinct SalesOrg,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b,
-- percentile_cont(slope,0.5) over(partition by SalesOrg,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b) as median_slope,
-- percentile_cont(y_intercept,0.5) over(partition by SalesOrg,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b) as median_intercept
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary_*`) e on (a.SalesOrg=e.SalesOrg) and (a.Article_a=e.Article_a) and (a.Article_b=e.Article_b) and (a.Sales_Unit_a=e.Sales_Unit_a) and (a.Sales_Unit_b=e.Sales_Unit_b)
-- where c.past12m_Sales_ExclTax > 1e6 and
-- b.past12m_Sales_ExclTax > 1e6 and
-- a.Article_a != a.Article_b and
-- d.lift is not null

-- group by a.SalesOrg,	a.Article_a,	b.Article_Description, a.Article_b,	c.Article_Description, a.Sales_Unit_a,	a.Sales_Unit_b, e.median_slope, e.median_intercept, d.lift, b.past12m_Sales_ExclTax, c.past12m_Sales_ExclTax
-- having count(*) > 100
-- ) select *
-- from abc
-- --where Article_a in ('133211','143737') and
-- --Article_b in ('133211','143737')
-- --where perc_slope_gt_0 > 0.7 or  perc_slope_lt_0 > 0.7
-- order by (case when perc_slope_lt_0>perc_slope_gt_0 then perc_slope_lt_0 else perc_slope_gt_0 end)*log(n)*( log(Article_a_past12m_Sales_ExclTax)+log(Article_b_past12m_Sales_ExclTax) ) * abs(median_slope) desc



