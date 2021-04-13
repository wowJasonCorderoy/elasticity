DECLARE so ARRAY <STRING>;
DECLARE dateFrom DATE;
DECLARE dateTo DATE;

SET so = ["1005","1030"];

set dateTo = CURRENT_DATE("Australia/Sydney");
set dateFrom = DATE_ADD(dateTo, INTERVAL -(364*2) DAY);

create temp table dat as (
SELECT LAG(TXNStartTimestamp)
    OVER (PARTITION BY SiteNumber, businessdate, Article, RetailUOM ORDER BY TXNStartTimestamp ASC) AS preceding_TXNStartTimestamp,
    DATETIME_DIFF(TXNStartTimestamp,
    LAG(TXNStartTimestamp)
    OVER (PARTITION BY SiteNumber, businessdate, Article, RetailUOM ORDER BY TXNStartTimestamp ASC),
    SECOND) as diff_seconds,
TXNStartTimestamp, salesorg, SiteNumber, businessdate, Article, RetailUOM
FROM (select
distinct TXNStartTimestamp, salesorg, SiteNumber, businessdate, Article, RetailUOM from `gcp-wow-ent-im-tbl-prod.adp_dm_basket_sales_view.pos_item_line_detail`  
where salesorg in unnest(so)
and businessdate between dateFrom and dateTo
--and Article in ('117822')
--and SiteNumber in ('1086','1242')
--and SalesChannelCode NOT IN ('HDY','HDZ','CCY','CCZ','HD1','CC1') #instore only.  Else you get wierd midnight postings for online.
--and SiteNumber = '2523'
and EXTRACT(HOUR FROM TXNStartTimestamp) between 9 and 17
and ItemTXNType in  ('S201') #,'S202') --S201 sales, S202 returns.
)
);

-- select *
-- from dat
-- where preceding_TXNStartTimestamp is not null
-- order by TXNStartTimestamp, SiteNumber, businessdate, Article, RetailUOM;

create temp table summaryStatsDaily as (

with gbData as (
select salesorg, SiteNumber, businessdate, Article, RetailUOM,
count(*) as n,
avg(diff_seconds) as mean_diff_seconds,
max(diff_seconds) as max_diff_seconds,
min(diff_seconds) as min_diff_seconds,
max(EXTRACT(HOUR FROM TXNStartTimestamp)) as max_TXNStartTimestamp,
min(EXTRACT(HOUR FROM TXNStartTimestamp)) as min_TXNStartTimestamp

from dat
group by salesorg, SiteNumber, businessdate, Article, RetailUOM
),
windowedData as (
select salesorg, SiteNumber, businessdate, Article, RetailUOM,
PERCENTILE_CONT(diff_seconds, 0.5) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile50_diff_seconds,
--PERCENTILE_CONT(diff_seconds, 0.25) over(partition by SiteNumber, businessdate, Article, RetailUOM) as percentile25_diff_seconds,
--PERCENTILE_CONT(diff_seconds, 0.75) over(partition by SiteNumber, businessdate, Article, RetailUOM) as percentile75_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.10) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile10_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.90) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile90_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.05) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile05_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.95) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile95_diff_seconds
from dat
)
select distinct a.*,
b.percentile50_diff_seconds, b.percentile10_diff_seconds, b.percentile90_diff_seconds, b.percentile05_diff_seconds, b.percentile95_diff_seconds
from gbData a
left join
windowedData b on (a.salesorg=b.salesorg) and (a.SiteNumber=b.SiteNumber) and (a.businessdate=b.businessdate) and (a.Article=b.Article) and (a.RetailUOM=b.RetailUOM)

);

create temp table summaryStatsALL as (

with gbData as (
select salesorg, SiteNumber, Article, RetailUOM,
count(*) as n,
avg(diff_seconds) as mean_diff_seconds,
max(diff_seconds) as max_diff_seconds,
min(diff_seconds) as min_diff_seconds,
max(EXTRACT(HOUR FROM TXNStartTimestamp)) as max_TXNStartTimestamp,
min(EXTRACT(HOUR FROM TXNStartTimestamp)) as min_TXNStartTimestamp

from dat
group by salesorg, SiteNumber, Article, RetailUOM
),
windowedData as (
select salesorg, SiteNumber, Article, RetailUOM,
PERCENTILE_CONT(diff_seconds, 0.5) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile50_diff_seconds,
--PERCENTILE_CONT(diff_seconds, 0.25) over(partition by SiteNumber, businessdate, Article, RetailUOM) as percentile25_diff_seconds,
--PERCENTILE_CONT(diff_seconds, 0.75) over(partition by SiteNumber, businessdate, Article, RetailUOM) as percentile75_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.10) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile10_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.90) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile90_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.05) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile05_diff_seconds,
PERCENTILE_CONT(diff_seconds, 0.95) over(partition by salesorg, SiteNumber, businessdate, Article, RetailUOM) as percentile95_diff_seconds
from dat
)
select distinct a.*,
b.percentile50_diff_seconds, b.percentile10_diff_seconds, b.percentile90_diff_seconds, b.percentile05_diff_seconds, b.percentile95_diff_seconds
from gbData a
left join
windowedData b on (a.salesorg=b.salesorg) and (a.SiteNumber=b.SiteNumber) and (a.Article=b.Article) and (a.RetailUOM=b.RetailUOM)

);


create temp table summaryStatsOfSummaryStatsDaily as (
select salesorg, SiteNumber, Article, RetailUOM,
avg(n) as mean_n,
stddev(n) as sd_n,

avg(mean_diff_seconds) as mean_mean_diff_seconds,
stddev(mean_diff_seconds) as sd_mean_diff_seconds,

avg(max_diff_seconds) as mean_max_diff_seconds,
stddev(max_diff_seconds) as sd_max_diff_seconds,

avg(min_diff_seconds) as mean_min_diff_seconds,
stddev(min_diff_seconds) as sd_min_diff_seconds,

avg(max_TXNStartTimestamp) as mean_max_TXNStartTimestamp,
stddev(max_TXNStartTimestamp) as sd_max_TXNStartTimestamp,

avg(min_TXNStartTimestamp) as mean_min_TXNStartTimestamp,
stddev(min_TXNStartTimestamp) as sd_min_TXNStartTimestamp

from summaryStatsDaily
group by salesorg, SiteNumber, Article, RetailUOM
order by salesorg, SiteNumber, Article, RetailUOM
);



-- select *
-- from summaryStatsOfSummaryStatsDaily
-- --where mean_n > 1e6
-- order by mean_n desc
-- limit 100000

create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.timeBasedSiteArticleStats` as (
select distinct *,
max_diff_seconds-percentile50_diff_seconds as max_less_median_diff_seconds,
percentile50_diff_seconds-min_diff_seconds as median_less_min_diff_seconds,

max_diff_seconds-mean_diff_seconds as max_less_mean_diff_seconds,
mean_diff_seconds-min_diff_seconds as mean_less_min_diff_seconds,

mean_diff_seconds-percentile50_diff_seconds as mean_less_median_diff_seconds

from summaryStatsDaily
);

### Now replace with a bunch more columns!
create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.timeBasedSiteArticleStats` as (
with dat as (
select distinct DATE_ADD(businessdate, INTERVAL -7 DAY) as businessdate_lag7d,
*
from `gcp-wow-finance-de-lab-dev.price_elasticity.timeBasedSiteArticleStats`
)
select a.salesorg, a.SiteNumber, a.businessdate, a.businessdate_lag7d, a.Article, a.RetailUOM, 

a.max_diff_seconds, 
a.min_diff_seconds, 
a.max_TXNStartTimestamp,
a.min_TXNStartTimestamp, 
a.percentile50_diff_seconds,
a.percentile10_diff_seconds, 
a.percentile90_diff_seconds, 
a.percentile05_diff_seconds, 
a.percentile95_diff_seconds, 
a.max_less_median_diff_seconds, 
a.median_less_min_diff_seconds, 
a.max_less_mean_diff_seconds, 
a.mean_less_min_diff_seconds, 
a.mean_less_median_diff_seconds,

b.max_diff_seconds as max_diff_seconds_lag7d, 
b.min_diff_seconds as min_diff_seconds_lag7d, 
b.max_TXNStartTimestamp as max_TXNStartTimestamp_lag7d,
b.min_TXNStartTimestamp as min_TXNStartTimestamp_lag7d, 
b.percentile50_diff_seconds as percentile50_diff_seconds_lag7d,
b.percentile10_diff_seconds as percentile10_diff_seconds_lag7d, 
b.percentile90_diff_seconds as percentile90_diff_seconds_lag7d, 
b.percentile05_diff_seconds as percentile05_diff_seconds_lag7d, 
b.percentile95_diff_seconds as percentile95_diff_seconds_lag7d, 
b.max_less_median_diff_seconds as max_less_median_diff_seconds_lag7d, 
b.median_less_min_diff_seconds as median_less_min_diff_seconds_lag7d, 
b.max_less_mean_diff_seconds as max_less_mean_diff_seconds_lag7d, 
b.mean_less_min_diff_seconds as mean_less_min_diff_seconds_lag7d, 
b.mean_less_median_diff_seconds as mean_less_median_diff_seconds_lag7d,

a.max_diff_seconds - b.max_diff_seconds as max_diff_seconds_diff_v_lag7d, 
a.min_diff_seconds - b.min_diff_seconds as min_diff_seconds_diff_v_lag7d, 
a.max_TXNStartTimestamp - b.max_TXNStartTimestamp as max_TXNStartTimestamp_diff_v_lag7d, 
a.min_TXNStartTimestamp - b.min_TXNStartTimestamp as min_TXNStartTimestamp_diff_v_lag7d, 
a.percentile50_diff_seconds - b.percentile50_diff_seconds as percentile50_diff_seconds_diff_v_lag7d, 
a.percentile10_diff_seconds - b.percentile10_diff_seconds as percentile10_diff_seconds_diff_v_lag7d, 
a.percentile90_diff_seconds - b.percentile90_diff_seconds as percentile90_diff_seconds_diff_v_lag7d, 
a.percentile05_diff_seconds - b.percentile05_diff_seconds as percentile05_diff_seconds_diff_v_lag7d, 
a.percentile95_diff_seconds - b.percentile95_diff_seconds as percentile95_diff_seconds_diff_v_lag7d, 
a.max_less_median_diff_seconds - b.max_less_median_diff_seconds as max_less_median_diff_seconds_diff_v_lag7d, 
a.median_less_min_diff_seconds - b.median_less_min_diff_seconds as median_less_min_diff_seconds_diff_v_lag7d, 
a.max_less_mean_diff_seconds - b.max_less_mean_diff_seconds as max_less_mean_diff_seconds_diff_v_lag7d, 
a.mean_less_min_diff_seconds - b.mean_less_min_diff_seconds as mean_less_min_diff_seconds_diff_v_lag7d, 
a.mean_less_median_diff_seconds - b.mean_less_median_diff_seconds as mean_less_median_diff_seconds_diff_v_lag7d

from dat a
left join
dat b
on (a.salesorg=b.salesorg) and (a.SiteNumber=b.SiteNumber) and (a.Article=b.Article) and (a.RetailUOM=b.RetailUOM) and (a.businessdate_lag7d=b.businessdate)
where b.max_diff_seconds is not null
order by a.salesorg, a.SiteNumber, a.businessdate, a.Article, a.RetailUOM, a.businessdate
);

