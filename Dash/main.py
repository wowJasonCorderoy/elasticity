import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.express as px
import dash_table
import pandas as pd
import numpy as np
import plotly.graph_objects as go

app = dash.Dash()

#df = pd.read_csv("./Data/elast_Dist.csv", low_memory=False, dtype={
    #'SalesOrg':str,
    #'Article':str,
    #'Sales_Unit':str,
    #'mean_y_intercept':np.float,
    #'mean_slope':np.float,
    #'stddev_slope':np.float,
    #'n':np.int
#}).query("(SalesOrg == '1005')")

df = pd.DataFrame(
    {'SalesOrg':['1005','1030']*10,
    'Article':[str(x) for x in range(20)],
    'Sales_Unit':['EA']*20,
    'mean_y_intercept':list( np.random.uniform(-0.1,0.1,20) ),
    'mean_slope':list( np.random.uniform(-3,0,20) ),
    'stddev_slope':list( np.random.uniform(0,2,20) ),
    'n':list( np.random.uniform(20,1000,20) )}
)

nSamplesPerDist = 10_000

artA = df.iloc[0]
artB = df.iloc[1]

distA = np.random.normal(artA.mean_slope,artA.stddev_slope,nSamplesPerDist )
distB = np.random.normal(artB.mean_slope,artB.stddev_slope,nSamplesPerDist)
distDiff = distB-distA

probBmoreElastThanA = np.mean(distDiff<0)

artA_title = "Article A"
artB_title = "Article B"
distDiff_title = f"B less A. p(B<A) = {round(probBmoreElastThanA*100,0)}%"

figDistDiff = px.histogram(x=distDiff, nbins=100, title=distDiff_title)
#figA = px.histogram(x=distA, nbins=100, title=artA_title)
#figB = px.histogram(x=distB, nbins=100, title=artB_title)

fig = go.Figure()
fig.add_trace(go.Histogram(x=distA, name="A"))
fig.add_trace(go.Histogram(x=distB, name="B"))
# Overlay both histograms
fig.update_layout(barmode='overlay')
# Reduce opacity to see both histograms
fig.update_traces(opacity=0.75)
#fig.show()

app.layout = html.Div(
    [
        html.Div(
            [
                html.Div(
                    [
                        dcc.Input(
                            id="nSamples",
                            type="number",
                            value=nSamplesPerDist,
                        ),
                        html.H1("A"),
                        dcc.Dropdown(
                            id="Article_A",
                            options=[
                                {"label": i, "value": i}
                                for i in sorted(df.Article.unique())
                            ],
                            multi=False,
                            value=artA.Article,
                            style={"width": "100%"},
                        ),
                    ],
                    style={"width": "40%", "display": "inline-block"},
                ),
                html.Div([], style={"width": "10%", "display": "inline-block"}),
                html.Div(
                    [
                        html.H1("B"),
                        dcc.Dropdown(
                            id="Article_B",
                            options=[
                                {"label": i, "value": i}
                                for i in sorted(df.Article.unique())
                            ],
                            multi=False,
                            value=artB.Article,
                            style={"width": "100%"},
                        ),
                    ],
                    style={"width": "40%", "display": "inline-block"},
                ),
            ]
        ),
        html.Br(),
        html.Button("Generate samples", id="btn-1"),
        html.Br(),
        html.Br(),
        #html.Div([dcc.Graph(id="histA", figure=figA)]),
        #html.Div([dcc.Graph(id="histB", figure=figB)]),
        #html.Div([dcc.Graph(id="diffAB", figure=figDistDiff)]),
        html.Div([
            #dcc.Graph(id="histA", figure=figA),
            #dcc.Graph(id="histB", figure=figB),
            dcc.Graph(id="histAnB", figure=fig),
            dcc.Graph(id="diffAB", figure=figDistDiff),
        ]),
    ]
)


@app.callback(
    [Output("histAnB", "figure"), Output("diffAB", "figure"), Output("nSamples", "value")],
    [Input("btn-1", "n_clicks")],
    state=[
        State("Article_A", "value"),
        State("Article_B", "value"),
        State("nSamples", "value"),
    ],
)
def update_graph(
    n_clicks,
    Article_A,
    Article_B,
    nSamples,
):
    print("Refreshing data ...")

    if nSamples>10_000_000:
        nSamples = 1_000_000

    artA = df.query(f"(Article == '{Article_A}')")
    artB = df.query(f"(Article == '{Article_B}')")


    distA = np.random.normal(artA.mean_slope,artA.stddev_slope,nSamples)
    distB = np.random.normal(artB.mean_slope,artB.stddev_slope,nSamples)
    distDiff = distB-distA

    probBmoreElastThanA = np.mean(distDiff<0)
    distDiff_title = f"B less A. p(B<A) = {round(probBmoreElastThanA*100,0)}%"

    figDistDiff = px.histogram(x=distDiff, nbins=100, title=distDiff_title)
    #figA = px.histogram(x=distA, nbins=100, title=artA_title)
    #figB = px.histogram(x=distB, nbins=100, title=artB_title)

    fig = go.Figure()
    fig.add_trace(go.Histogram(x=distA, name="A"))
    fig.add_trace(go.Histogram(x=distB, name="B"))
    # Overlay both histograms
    fig.update_layout(barmode='overlay')
    # Reduce opacity to see both histograms
    fig.update_traces(opacity=0.75)
    #fig.show()

    return (
       fig, figDistDiff, nSamples
    )


if __name__ == "__main__":
    app.run_server(debug=True)
