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

run4salesOrgs = ['1030']

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
embedding = umap.UMAP(n_neighbors=10, n_components=5).fit_transform(df_wide)

if (1):
    l_clusters = [
        x for x in np.array(range(1, min(101, df_wide.shape[0])))
        if x == 1 or x % 5 == 0
    ]
    kmeans_var_explained = kmeans_diff_n(embedding, l_clusters)
    print(kmeans_var_explained)

plot_termPlot(x=l_clusters, y=kmeans_var_explained)

my_kmeans = KMeans(n_clusters=5, random_state=0).fit(df_wide)

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
    select
    Article, Sales_Unit,
    log_ASP_v_lag,
    log_Sales_Qty_SUoM_v_lag 
    from `gcp-wow-finance-de-lab-dev.price_elasticity.elastData`
    where abs(log_ASP_v_lag) > 0.1 and  Site in ('{i_s}')
    """.format(i_s="', '".join(i_sites)),
                      project_id=project)
    models_gb = ['Article', 'Sales_Unit']
    dict_modelData = dict(iter(dat.groupby(models_gb)))
    for key, value in dict_modelData.items():
        dict_all_models[key] = run_lm(
            dict_modelData[key]['log_ASP_v_lag'],
            dict_modelData[key]['log_Sales_Qty_SUoM_v_lag'])
    fname = "_".join(run4salesOrgs) + "_cluster_" + str(
        i) + "_dict_cluster_models.pickle"
    save_pickle_to_gStorage(obj=dict_all_models,
                            pickle_fname="_".join(run4salesOrgs) +
                            "_cluster_" + str(i) +
                            "_dict_cluster_models.pickle",
                            bucket=bucket)
    list_bucket_files.append(fname)
    # dict_cluster_models[i] = dict_all_models

for i in range(len(list_bucket_files)):
    fname = list_bucket_files[i]
    print(fname)
    bb = bucket.blob(fname)
    bb.download_to_filename(fname)
    file_to_read = open(fname, "rb")
    tempDict = pickle.load(file_to_read)
    df_models = pd.DataFrame.from_dict(tempDict.keys()).reset_index()
    df_models.columns = ['index_x'] + models_gb
    df_models = df_models.drop(columns='index_x')
    df_models['cluster'] = re.search(".*cluster_(.*)_dict_.*", fname,
                                     re.IGNORECASE).group(1)
    df_models['intercept'] = [tempDict[x].params[0] for x in tempDict.keys()]
    df_models['coef_log_ASP_v_lag7'] = [
        tempDict[x].params[1] for x in tempDict.keys()
    ]
    df_models['conf_int_025_intercept'] = [
        tempDict[x].conf_int(alpha=0.05).iloc[0, 0] for x in tempDict.keys()
    ]
    df_models['conf_int_975_intercept'] = [
        tempDict[x].conf_int(alpha=0.05).iloc[0, 1] for x in tempDict.keys()
    ]
    df_models['conf_int_025_coef_log_ASP_v_lag7'] = [
        tempDict[x].conf_int(alpha=0.05).iloc[1, 0] for x in tempDict.keys()
    ]
    df_models['conf_int_975_coef_log_ASP_v_lag7'] = [
        tempDict[x].conf_int(alpha=0.05).iloc[1, 1] for x in tempDict.keys()
    ]
    if i == 0:
        df_all_models = df_models
    else:
        df_all_models = pd.concat([df_all_models, df_models])
    os.system("rm {}".format(fname))

save_pickle_to_gStorage(obj=df_all_models,
                        pickle_fname="_".join(run4salesOrgs) +
                        "_df_all_models.pickle",
                        bucket=bucket)

write_df_to_bq(df_all_models, "price_elasticity.cluster_elasticity",
               "gcp-wow-finance-de-lab-dev")
