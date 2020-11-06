def bq_sql_mainData(so=['1005', '1030'], lagDays=7):
    query = """
    #standardSQL
    create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.elastData` as (
    with asp_dat as 
    (
    SELECT 
    ifnull(SalesOrg,'') as SalesOrg, 
    ifnull(Site,'') as Site, 
    ifnull(Article,'') as Article,
    ifnull((case when Sales_Unit in ('CA1','CA2','CA3') then 'CAR' else Sales_Unit end),'') as Sales_Unit, Calendar_Day
    , DATE_ADD(Calendar_Day, INTERVAL -{ld} DAY) as lag_Calendar_Day
    , sum(Sales_ExclTax) as Sales_ExclTax
    , sum(Sales_Qty_SUoM) as Sales_Qty_SUoM
    , (case when sum(Sales_Qty_SUoM) = 0 then NULL else sum(Sales_ExclTax)/sum(Sales_Qty_SUoM) end) as ASP
    , sum(Promo_Sales) as Promo_Sales
    , sum(Promo_Sales_Qty_SUoM) as Promo_Sales_Qty_SUoM
    FROM  `gcp-wow-ent-im-tbl-prod.gs_allgrp_fin_data.fin_group_profit_v` 
    WHERE 
    SalesOrg in ('{so}') AND
    Department_Description not in ('', 'FRONT OF STORE', 'NON TRADE', 'NON TRADING') AND
    Department_Description is not null AND
    Calendar_Day between '2018-01-01' AND '2020-01-01'
    group by ifnull(SalesOrg,''),
    ifnull(Site,''),
    ifnull(Article,''),
    ifnull((case when Sales_Unit in ('CA1','CA2','CA3') then 'CAR' else Sales_Unit end),''), Calendar_Day
    )
    select a.*,
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
    end) as promo_perc
    from asp_dat a
    left join (
    SELECT *
    from asp_dat
    ) b
    on (a.lag_Calendar_Day=b.Calendar_Day) AND
    (a.SalesOrg = b.SalesOrg) and
    (a.Site = b.Site) and
    (a.Article=b.Article) AND
    (a.Sales_Unit=b.Sales_Unit)
    where
    b.Sales_Qty_SUoM > 0 AND 
    b.Sales_Qty_SUoM is not null AND 
    (case when b.ASP = 0 then null else a.ASP/b.ASP end) between 0.1 and 10 AND
    (case when b.ASP = 0 then null else a.ASP/b.ASP end) is not null AND
    log(case when b.ASP = 0 then null when a.ASP/b.ASP < 0 then null else a.ASP/b.ASP end) is not null AND
    log(case when b.Sales_Qty_SUoM = 0 then null when a.Sales_Qty_SUoM/b.Sales_Qty_SUoM < 0 then null else a.Sales_Qty_SUoM/b.Sales_Qty_SUoM end) is not null AND
    (case when b.Sales_Qty_SUoM = 0 then null else a.Sales_Qty_SUoM/b.Sales_Qty_SUoM end) < 100 # remove non-sensicle perc growth
    )
    """.format(so="', '".join(so), ld=lagDays)
    return query


def run_bq(sql, client, location="US"):
    query_job = client.query(sql, location=location)
    # wait for query to run_bq
    query_job.result()


def get_bq_data(sql, project_id):
    import pandas_gbq
    return pandas_gbq.read_gbq(sql,
                               project_id=project_id,
                               use_bqstorage_api=True,
                               progress_bar_type="tqdm_notebook")


def bq_sql_simple_site_article_elasticity():
    query = """
    #standardSQL
    create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.site_article_elasticity` as (
    with dat as (
    SELECT SalesOrg, Site, Article, Sales_Unit,
    avg(log_ASP_v_lag) as mean_x,
    avg(log_Sales_Qty_SUoM_v_lag) as mean_y,
    corr(log_ASP_v_lag, log_Sales_Qty_SUoM_v_lag) as corr_xy,
    stddev(log_ASP_v_lag) as sd_x,
    stddev(log_Sales_Qty_SUoM_v_lag) as sd_y,
    sum(case when log_ASP_v_lag > 0.1 then 1 else 0 end) as cnt_overs,
    sum(case when log_ASP_v_lag < -0.1 then 1 else 0 end) as cnt_unders
    FROM `gcp-wow-finance-de-lab-dev.price_elasticity.elastData`
    where abs(log_ASP_v_lag) > 0.1
    group by SalesOrg, Site, Article, Sales_Unit
    )
    select b.Department, b.DepartmentDescription, b.Category, b.CategoryDescription, b.Sub_Category, b.Sub_CategoryDescription, b.Segment, b.SegmentDescription, b.GeneralManagerName, b.MerchandiseManagerName, b.CategoryManagerName, b.NationalBuyDepartmentCode,
    a.*,
    a.corr_xy*a.sd_y/a.sd_x as slope,
    a.mean_y-a.mean_x*(a.corr_xy*a.sd_y/a.sd_x) as y_intercept,
    from dat a
    left join `gcp-wow-ent-im-tbl-prod.adp_masterdata_view.dim_article_hierarchy_curr_v` b on (a.Article = b.Article) and (a.SalesOrg=b.SalesOrg)
    where a.cnt_overs >=10 and a.cnt_unders >= 10
    order by a.Article
    )
    """
    return query


def bq_get_article_coef(
        sql="SELECT SalesOrg, Site,Article, Sales_Unit, y_intercept, slope FROM `gcp-wow-finance-de-lab-dev.price_elasticity.site_article_elasticity`",
        project_id=None):
    return get_bq_data(sql, project_id=project_id)
