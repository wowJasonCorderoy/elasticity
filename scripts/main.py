from google.cloud import bigquery
import pandas_gbq
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
    run_bq(sql=bq_sql_mainData(), client=client, location=location)

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
