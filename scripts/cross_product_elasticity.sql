
DECLARE l_articles ARRAY <STRING>;
DECLARE alreadyRun_articles ARRAY <STRING>;
DECLARE n INT64;
DECLARE minSales INT64;

SET minSales = 10000000;
SET n = 0;
SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales limit 10);
SET alreadyRun_articles = [];

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
  
  create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.temp_join_article_Dates` as (
  #create temp table join_article_Dates as (
  select a.SalesOrg,	a.Site, a.Calendar_Day,
  a.Article as Article_a, b.Article as Article_b,
  a.Sales_Unit as Sales_Unit_a, b.Sales_Unit as Sales_Unit_b,

  a.log_ASP_v_lag as log_ASP_v_lag_a, --b.log_ASP_v_lag as log_ASP_v_lag_b,
  --a.log_Sales_Qty_SUoM_v_lag as log_Sales_Qty_SUoM_v_lag_a, b.log_Sales_Qty_SUoM_v_lag as log_Sales_Qty_SUoM_v_lag_b,

  --a.pred_log_Sales_Qty_SUoM_v_lag-a.log_Sales_Qty_SUoM_v_lag as pred_residual_a,
  b.log_Sales_Qty_SUoM_v_lag-b.pred_log_Sales_Qty_SUoM_v_lag as pred_residual_b

  from (
  select * 
  from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_dat`
  #from dat 
  where abs(log_ASP_v_lag) > 0.1
  and Article in unnest(l_articles)
  #and Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` limit 10)

  ) a

  inner join `gcp-wow-finance-de-lab-dev.price_elasticity.temp_dat` b on (a.SalesOrg=b.SalesOrg) and (a.Site=b.Site) and (a.Calendar_Day=b.Calendar_Day)
  --where a.Article != b.Article and a.Sales_Unit != b.Sales_Unit

  --where a.Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` limit 10)
  --and b.Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > (1e6))

  order by a.SalesOrg,	a.Site, a.Article, b.Article, a.Calendar_Day
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
      a.corr_xy*a.sd_y/a.sd_x as slope,
      a.mean_y-a.mean_x*(a.corr_xy*a.sd_y/a.sd_x) as y_intercept
      from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity` a
      #from crossProductElasticity a
      where ifnull(a.n,0)>=14
      order by a.SalesOrg,	a.Article_a,	a.Article_b,	a.Sales_Unit_a,	a.Sales_Unit_b
      );
 
  IF n = 0 then
      create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary` as (
      select *
      from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity_int_slope`
      #from crossProductElasticity_int_slope
      );
  ELSE
      create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary` as (
      select * from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary`
      union all
      #(select * from crossProductElasticity_int_slope)
      (select * from `gcp-wow-finance-de-lab-dev.price_elasticity.temp_crossProductElasticity_int_slope`)
      );
  END IF;
    
    SET n = n+1;
    SET alreadyRun_articles = ARRAY_CONCAT(alreadyRun_articles, l_articles);
    #SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > 10e6 and Article not in unnest(alreadyRun_articles) limit 10);
    SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales and Article not in (select distinct Article_a from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary`) limit 10);
  
  END WHILE;
    