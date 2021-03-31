declare from_date DATE;
declare to_date DATE;
DECLARE so STRING;

set to_date = CURRENT_DATE("Australia/Sydney");
#set from_date = DATE_ADD(to_date, INTERVAL -364 DAY);
set from_date = DATE_ADD(to_date, INTERVAL -364 DAY);
set so = '1005';

-- create temp function fitSlope(x float64,y float64) as 
-- ( corr(x, y)*stddev(y)/stddev(x) );

create temp function fitSlope(corrxy float64,sdx float64, sdy float64) as 
( (case when sdx = 0 or sdx is null then null else corrxy*sdy/sdx end) );

create temp function fityIntercept(corrxy float64,sdx float64, sdy float64, mean_x float64, mean_y float64) as 
( (case when sdx = 0 or sdx is null then null else mean_y-mean_x*(corrxy*sdy/sdx) end) );

###
create temp table distincts_00 as (
SELECT a.article, a.RetailUOM,
a.businessdate,
FORMAT_DATE('%A', a.businessdate) AS dow,
BasketKey,
sum(a.RetailQuantity) as RetailQuantity

FROM

`gcp-wow-ent-im-tbl-prod.adp_dm_basket_sales_view.pos_item_line_detail` a,
`gcp-wow-ent-im-tbl-prod.adp_dm_masterdata_view.dim_article_hierarchy_v` b

WHERE
a.BasketKey in (
select distinct BasketKey
from `gcp-wow-ent-im-tbl-prod.adp_dm_basket_sales_view.pos_item_line_detail`
where --rand() < 0.000000001 and
salesorg = so
and businessdate >= from_date
and businessdate < to_date
and itemvoidflag is null
and ItemTXNType in  ('S201') #,'S202') --S201 sales, S202 returns.
order by rand()
--limit 10000000
) and
a.salesorg = so  
--and a.article='136341'
and ltrim(a.article,'0') = ltrim(b.article,'0')
and b.salesorg = so
and b.Department not in ('W100','W120')
and businessdate >= from_date
and businessdate < to_date  
and itemvoidflag is null
and ItemTXNType in  ('S201') #,'S202') --S201 sales, S202 returns.
--and (a.SalesChannelCode is null or a.SalesChannelCode = 'BR1') # Bricks and mortar where salesChannel is null or BR1

-- and a.article in (select distinct Article FROM `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details`  where Sales_Unit = 'EA' and SalesOrg = '1005')
-- and
-- a.article in (select distinct Article FROM `gcp-wow-finance-de-lab-dev.price_elasticity.PriceElast_dist_details`  where Sales_Unit = 'CAR' and SalesOrg = '1005')

group by 1,2,3,4,5
#ORDER BY a.article, a.RetailUOM, a.businessdate
);
###

create temp table addBasketSize as (
SELECT BasketKey, businessdate, dow,
count(*) as basketSize

FROM distincts_00
group by 1,2,3
);

create temp table articleDat as (
SELECT a.article, a.RetailUOM, a.businessdate, a.dow, b.BasketKey,
--count(distinct article)/count(distinct BasketKey) as basketSize,
b.basketSize,
a.RetailQuantity

FROM distincts_00 a
left join addBasketSize b on (a.BasketKey=b.BasketKey)
);

create temp table lmIngredients as (
select article, 
ifnull( (case when RetailUOM like '%CA%' then 'CAR' else RetailUOM end), '') as RetailUOM,
dow,
avg(RetailQuantity) as mean_x,
avg(basketSize) as mean_y,
corr(RetailQuantity, basketSize) as corr_xy,
stddev(RetailQuantity) as sd_x,
stddev(basketSize) as sd_y,
count(distinct BasketKey) as n
from articleDat
group by 1,2,3
);

create temp table finalDat as (
select from_date as from_date, to_date as to_date,
'RetailQuantity' as x, 'basketSize' as y,
article,RetailUOM,dow,
mean_x, mean_y, sd_x, sd_y,
n,
fityIntercept(corr_xy,sd_x, sd_y, mean_x, mean_y) as y_intercept,
fitSlope(corr_xy, sd_x, sd_y) as slope
from lmIngredients
);

CREATE OR REPLACE TABLE  `gcp-wow-finance-de-lab-dev.price_elasticity.regress_article_qty_basketSize` as (
select *,
concat("between ",from_date," and ",to_date," for ",article,"(",RetailUOM,"), ",dow,"s, given ", n, " baskets containing this article, the number of distinct articles in baskets that contain this article move by ", round(slope,3)," for each increase in RetailQuantity.") as description
from finalDat
);


