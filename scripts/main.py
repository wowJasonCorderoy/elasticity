from google.cloud import bigquery
from sklearn.cluster import KMeans
import pandas_gbq
import pandas as pd
import numpy as np
import sys
sys.path.append("scripts")
from elasticipy import *

location = "US"
project = "gcp-wow-finance-de-lab-dev"
client = bigquery.Client(location=location, project=project)
print("Client creating using default project: {}".format(client.project))

# Create the main elasticity data table in gcp?
# no need to recreate the elasticity data every time.
if (0):
    run_bq(sql=bq_sql_mainData(so='1005', lagDays=7),
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

l_clusters = [1, 20, 40, 60, 80, 100]
kmeans_var_explained = kmeans_diff_n(df_wide, l_clusters)
print(kmeans_var_explained)

import termplotlib as tpl
fig = tpl.figure()
fig.plot(x=l_clusters, y=kmeans_var_explained, width=50, height=15)
fig.show()

my_kmeans = KMeans(n_clusters=20, random_state=0).fit(df_wide)

df_site_cluster_pair = pd.DataFrame({
    'site': list(df_wide.index),
    'cluster': list(my_kmeans.labels_)
})

pd.io.gbq.to_gbq(df_site_cluster_pair,"price_elasticity.site_cluster","gcp-wow-finance-de-lab-dev", chunksize=100000, verbose=True, reauth=False, if_exists='replace', private_key=None) 
