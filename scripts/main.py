from google.cloud import bigquery
from sklearn.cluster import KMeans
import statsmodels.api as sm
import umap
import pandas_gbq
import pandas as pd
import numpy as np
import sys
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

# fill na with column medians
df_wide = df_wide.apply(lambda x: x.fillna(x.median()), axis=0)
df_wide = df_wide.apply(lambda x: np.where(x.isna(), 0, x))
df_wide = df_wide.apply(lambda x: np.where(x < -10, -10, x))
df_wide = df_wide.apply(lambda x: np.where(x > 0, 0, x))


# find best number of clusters using elbow method:
embedding = umap.UMAP(n_neighbors=10, n_components=5).fit_transform(df_wide)

if (1):
    l_clusters = [x for x in np.array(range(1, min(101,df_wide.shape[0]))) if x == 1 or x % 5 == 0]
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

## Run 1 model per cluster, article, sales_unit
dict_cluster_models = {}
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
    """.format(i_s="', '".join(i_sites)), project_id=project)
    models_gb = ['Article', 'Sales_Unit']
    dict_modelData = dict(iter(dat.groupby(models_gb)))
    for key, value in dict_modelData.items():
        dict_all_models[key] = run_lm(
            dict_modelData[key]['log_ASP_v_lag'],
            dict_modelData[key]['log_Sales_Qty_SUoM_v_lag'])
    dict_cluster_models[i] = dict_all_models
    


#### Now store in bucket
from google.cloud import storage
project="gcp-wow-finance-de-lab-dev"
bucket_name="article_elasticity"

client = storage.Client(project=project)
print("Client created using default project: {}".format(client.project))
try:
    bucket = client.create_bucket(bucket_name)
    print("Bucket {} created.".format(bucket.name))
except:
    print("Bucket already exists. No bucket created.")

bucket = client.get_bucket(bucket_name)

# save models to pickle
import pickle
pickle_fname = "dict_cluster_models.pickle"
with open(pickle_fname, 'wb') as handle:
    pickle.dump(dict_cluster_models, handle, protocol=pickle.HIGHEST_PROTOCOL)
blob_name = pickle_fname
blob = bucket.blob(pickle_fname)
blob.upload_from_filename(pickle_fname)
os.system("rm {}".format(pickle_fname))

