
DECLARE l_articles ARRAY <STRING>;
DECLARE alreadyRun_articles ARRAY <STRING>;
DECLARE n INT64;
DECLARE minSales INT64;

SET minSales = 1000000;
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
  Article in (select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales)
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
    #SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales and Article not in (select distinct Article_a from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary`) limit 5);
    SET l_articles = ARRAY(select distinct Article from `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` where past12m_Sales_ExclTax > minSales and Article not in unnest(alreadyRun_articles) limit 5);
  
  END WHILE;
    
    



-- with abc as (
-- select a.SalesOrg,	a.Article_a,	b.Article_Description as Article_a_Description, a.Article_b,	c.Article_Description as Article_b_Description, a.Sales_Unit_a,	a.Sales_Unit_b,
-- avg((case when a.slope<0 then 1 else 0 end)) as perc_slope_lt_0,
-- avg((case when a.slope>0 then 1 else 0 end)) as perc_slope_gt_0,
-- e.median_slope,
-- e.median_intercept,
-- count(*) as n,
-- b.past12m_Sales_ExclTax as Article_a_past12m_Sales_ExclTax,
-- c.past12m_Sales_ExclTax as Article_b_past12m_Sales_ExclTax
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary` a
-- left join `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` b on (a.SalesOrg=b.SalesOrg) and (a.Article_a=b.Article) and (a.Sales_Unit_a=b.Sales_Unit)
-- left join `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details` c on (a.SalesOrg=c.SalesOrg) and (a.Article_b=c.Article) and (a.Sales_Unit_b=c.Sales_Unit)

-- left join (select distinct SalesOrg,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b,
-- percentile_cont(slope,0.5) over(partition by SalesOrg,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b) as median_slope,
-- percentile_cont(y_intercept,0.5) over(partition by SalesOrg,	Article_a,	Article_b,	Sales_Unit_a,	Sales_Unit_b) as median_intercept
-- from `gcp-wow-finance-de-lab-dev.price_elasticity.crossProductElasticity_summary`) e on (a.SalesOrg=e.SalesOrg) and (a.Article_a=e.Article_a) and (a.Article_b=e.Article_b) and (a.Sales_Unit_a=e.Sales_Unit_a) and (a.Sales_Unit_b=e.Sales_Unit_b)
-- where c.past12m_Sales_ExclTax > 1e6 and
-- b.past12m_Sales_ExclTax > 1e6 and
-- a.Article_a != a.Article_b

-- group by a.SalesOrg,	a.Article_a,	b.Article_Description, a.Article_b,	c.Article_Description, a.Sales_Unit_a,	a.Sales_Unit_b, e.median_slope, e.median_intercept, b.past12m_Sales_ExclTax, c.past12m_Sales_ExclTax
-- having count(*) > 100
-- ) select *
-- from abc
-- --where Article_a in ('133211','143737') and
-- --Article_b in ('133211','143737')
-- order by (case when perc_slope_lt_0>perc_slope_gt_0 then perc_slope_lt_0 else perc_slope_gt_0 end)*n*( log(Article_a_past12m_Sales_ExclTax)+log(Article_b_past12m_Sales_ExclTax) ) desc

