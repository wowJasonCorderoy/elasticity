from google.cloud import bigquery
from sklearn.cluster import KMeans
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

# Create the main elasticity data table in gcp?
# no need to recreate the elasticity data every time.
if (0):
    run_bq(sql=bq_sql_mainData(so=['1005'], lagDays=7),
           client=client,
           location=location)

# Create bq table with site article elasticities
if (0):
    run_bq(sql=bq_sql_simple_site_article_elasticity(),
           client=client,
           location=location)

df = bq_get_article_coef(project_id=project)
df_wide = df.pivot(index="Site",
                   columns=["Article", "Sales_Unit"],
                   values="slope")

# fill na with column medians
df_wide = df_wide.apply(lambda x: x.fillna(x.median()), axis=0)
df_wide = df_wide.apply(lambda x: np.where(x < -10, -10, x))
df_wide = df_wide.apply(lambda x: np.where(x > 0, 0, x))

# find best number of clusters using elbow method:
embedding = umap.UMAP(n_neighbors=20, n_components=20).fit_transform(df_wide)

if (1):
    l_clusters = [x for x in np.array(range(1, 101)) if x == 1 or x % 5 == 0]
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
