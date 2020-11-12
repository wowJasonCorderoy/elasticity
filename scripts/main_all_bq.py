from google.cloud import bigquery
from sklearn.cluster import KMeans
import statsmodels.api as sm
import umap
import pandas_gbq
import pandas as pd
import pickle
import numpy as np
import sys
import os
sys.path.append("scripts")
from elasticipy import *
import termplotlib as tpl

location = "US"
project = "gcp-wow-finance-de-lab-dev"
client = bigquery.Client(location=location, project=project)
print("Client creating using default project: {}".format(client.project))

run4salesOrgs = ['1005', '1030']

# Create the main elasticity data table in gcp?
# no need to recreate the elasticity data every time.
if (1):
    run_bq(sql=bq_sql_mainData(so=run4salesOrgs, lagDays=7),
           client=client,
           location=location)

# Create bq table with site article elasticities
if (1):
    run_bq(sql=bq_sql_simple_site_article_elasticity(),
           client=client,
           location=location)

df = bq_get_article_coef(project_id=project)
df_wide = df.pivot(index="Site",
                   columns=["Article", "Sales_Unit"],
                   values="slope")

del df

# drop column if all na's
df_wide = df_wide.dropna(axis=1, how='all')
# fill na with column medians
df_wide = df_wide.replace([np.inf, -np.inf], np.nan)
df_wide = df_wide.apply(lambda x: x.fillna(x.median()))
df_wide = df_wide.apply(lambda x: np.where(x < -10, -10, x))
df_wide = df_wide.apply(lambda x: np.where(x > 0, 0, x))

# find best number of clusters using elbow method:
embedding = umap.UMAP(n_neighbors=10, n_components=20).fit_transform(df_wide)

if (1):
    l_clusters = [
        x for x in np.array(range(1, min(101, df_wide.shape[0])))
        if x == 1 or x % 5 == 0
    ]
    kmeans_var_explained = kmeans_diff_n(embedding, l_clusters)
    print(kmeans_var_explained)

plot_termPlot(x=l_clusters, y=kmeans_var_explained)

my_kmeans = KMeans(n_clusters=20, random_state=0).fit(df_wide)

df_site_cluster_pair = pd.DataFrame({
    'site': list(df_wide.index),
    'cluster': list(my_kmeans.labels_)
})

write_df_to_bq(df_site_cluster_pair, "price_elasticity.site_cluster",
               "gcp-wow-finance-de-lab-dev")

# now create the cluster_elasticity_table
run_bq("""
#standardSQL
create or replace table `gcp-wow-finance-de-lab-dev.price_elasticity.test_cluster_elasticity` as (
with dat as (
SELECT b.cluster, a.Article, a.Sales_Unit,
avg(a.log_ASP_v_lag) as mean_x,
avg(a.log_Sales_Qty_SUoM_v_lag) as mean_y,
corr(a.log_ASP_v_lag, log_Sales_Qty_SUoM_v_lag) as corr_xy,
stddev(a.log_ASP_v_lag) as sd_x,
stddev(a.log_Sales_Qty_SUoM_v_lag) as sd_y,
sum(case when a.log_ASP_v_lag > 0.1 then 1 else 0 end) as cnt_overs,
sum(case when a.log_ASP_v_lag < -0.1 then 1 else 0 end) as cnt_unders
FROM `gcp-wow-finance-de-lab-dev.price_elasticity.elastData` a inner join
`gcp-wow-finance-de-lab-dev.price_elasticity.site_cluster` b on (a.site=b.Site)
group by b.cluster, a.Article, a.Sales_Unit
)
select a.*, 
a.corr_xy*a.sd_y/a.sd_x as slope,
a.mean_y-a.mean_x*(a.corr_xy*a.sd_y/a.sd_x) as y_intercept
from dat a
where (ifnull(a.cnt_overs,0)+ifnull(a.cnt_unders,0))>=10
order by a.cluster, a.Article, a.Sales_Unit
)
""",
       client,
       location="US")
