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

run4salesOrgs = ['1005']

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

bucket = getBucket(project="gcp-wow-finance-de-lab-dev",
                   bucket_name="article_elasticity")

## Run 1 model per cluster, article, sales_unit
dict_cluster_models = {}
list_bucket_files = []
for i in np.unique(df_site_cluster_pair.cluster):
    dict_all_models = {}
    print(i)
    i_sites = list(
        df_site_cluster_pair[df_site_cluster_pair.cluster == i]['site'])
    dat = get_bq_data("""
    SELECT a.Article, a.Sales_Unit, a.log_ASP_v_lag, a.log_Sales_Qty_SUoM_v_lag
    FROM `gcp-wow-finance-de-lab-dev.price_elasticity.elastData` a inner join
    (
    select Site, Article, Sales_Unit, count(*) as n
    from`gcp-wow-finance-de-lab-dev.price_elasticity.elastData`
    where abs(log_ASP_v_lag) > 0.1 and  Site in ('{i_s}')
    group by Site, Article, Sales_Unit
    ) b
    on (a.Article=b.Article) and (a.Sales_Unit=b.Sales_Unit) and (a.Site=b.Site)
    where b.n > 20
    """.format(i_s="', '".join(i_sites)),
                      project_id=project)
    models_gb = ['Article', 'Sales_Unit']
    dict_modelData = dict(iter(dat.groupby(models_gb)))
    for key, value in dict_modelData.items():
        dict_all_models[key] = pd.read_html(
            run_lm(
                dict_modelData[key]['log_ASP_v_lag'],
                dict_modelData[key]['log_Sales_Qty_SUoM_v_lag']
            ).summary().tables[1].as_html(), header=0, index_col=0)[0]

    fname = "_".join(run4salesOrgs) + "_cluster_" + str(
        i) + "_dict_cluster_models.pickle"
    save_pickle_to_gStorage(obj=dict_all_models,
                            pickle_fname="_".join(run4salesOrgs) +
                            "_cluster_" + str(i) +
                            "_dict_cluster_models.pickle",
                            bucket=bucket)
    list_bucket_files.append(fname)
    # dict_cluster_models[i] = dict_all_models

