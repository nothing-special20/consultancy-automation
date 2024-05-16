import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import numpy as np
import pandas as pd

import dash_ag_grid as dag

np.random.seed(42)

def plotly_card_plot():
    # Generate dummy data for the metrics
    metrics = {
        "Calls Scheduled": 1572,
        "No-Shows": 264,
        "Disqualified": 107,
        "Closed Won": 619,
        "Cash Collected": 2101923,
        "Revenue": 3375823,
        "Cash Collected - Fees": 2046941,
    }

    # Metric titles and values
    titles = list(metrics.keys())
    values = list(metrics.values())

    # Create a figure for the indicators
    fig = go.Figure()

    for i, (title, value) in enumerate(zip(titles, values)):
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=value,
                title={"text": title, "font": {"size": 16, "color": "black"}},
                number={"font": {"size": 36, "color": "black"}},
                domain={"row": 0, "column": i},
            )
        )
        # Add a white border around each card
        fig.add_shape(
            type="rect",
            x0=i / 7,
            x1=(i + 1) / 7,
            y0=0,
            y1=1,
            line=dict(color="white", width=2),
            xref="paper",
            yref="paper",
        )

    # Update the layout
    fig.update_layout(
        grid={"rows": 1, "columns": 7, "pattern": "independent"},
        template="plotly_dark",
        height=150,
        margin=dict(l=20, r=20, t=20, b=20),
        paper_bgcolor="#F1F1F1",
        showlegend=False,
    )

    return fig


def plotly_bar_plot():
    # Generate dummy data for the bar chart and line plot
    months = ["Nov 2023", "Dec 2023", "Jan 2024", "Feb 2024", "Mar 2024"]
    fees = [294184, 272536, 669330, 757695, 119750]
    line_values = [300000, 400000, 700000, 800000, 600000]

    # Create the bar chart
    bar_trace = go.Bar(
        x=months,
        y=fees,
        text=[f"${v:,}" for v in fees],
        textposition="auto",
        marker=dict(
            color="#2c7be5", line=dict(color="#1b4d7e", width=1.5)  # Blue color
        ),
        name="Sales",
    )

    # Create the line plot
    line_trace = go.Scatter(
        x=months,
        y=line_values,
        mode="lines+markers+text",
        text=[f"${v:,}" for v in line_values],
        textposition="top center",
        line=dict(color="#e06666", width=3),
        marker=dict(color="#e06666", size=8),
        name="Target",
    )

    # Create the figure and add both traces
    fig = go.Figure(data=[bar_trace, line_trace])

    # Update the layout
    fig.update_layout(
        title="Sales vs Target",
        title_font=dict(size=24, color="#333333"),
        title_font_family="Arial, sans-serif",
        title_x=0.5,
        xaxis=dict(
            title="",
            color="#333333",
            showgrid=False,
            zeroline=False,
            tickangle=45,
            tickfont=dict(size=12),
        ),
        yaxis=dict(
            title="",
            color="#333333",
            showgrid=True,
            zeroline=True,
            gridcolor="lightgrey",
        ),
        plot_bgcolor="#ffffff",
        paper_bgcolor="#ffffff",
        height=450,
        margin=dict(l=40, r=40, t=80, b=40),
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    return fig


#
def single_metric_card(header_text, metric_value, line_percent):
    # card_description = 'You made an extra 35,000 this daily'
    card_description = "test"
    percent_change = "36%"
    return html.Div(
        style={
            "font-family": "Arial, sans-serif",
            "display": "flex",
            "justify-content": "center",
            "align-items": "center",
            "height": "200px",
            "margin": "0",
        },
        children=[
            html.Div(
                style={
                    "background-color": "#ffffff",
                    "border-radius": "10px",
                    "box-shadow": "0 4px 8px rgba(0, 0, 0, 0.1)",
                    "padding": "15px",
                    "width": "200px",
                    "text-align": "center",
                    "position": "relative",
                },
                children=[
                    html.H2(
                        header_text,
                        style={"margin": "0", "font-size": "1em", "color": "#333333"},
                    ),
                    html.Div(
                        style={
                            "font-size": "1.5em",
                            "color": "#333333",
                            "margin": "10px 0",
                        },
                        children=[
                            metric_value,
                            # html.Span(percent_change, style={'background-color': '#e6f4ea', 'color': '#27ae60', 'border-radius': '5px', 'padding': '2px 5px', 'font-size': '0.8em', 'display': 'inline-block', 'vertical-align': 'middle'})
                        ],
                    ),
                    # html.Div(card_description, style={'color': '#666666', 'margin': '10px 0'}),
                    html.Div(
                        style={
                            "height": "5px",
                            "width": "100%",
                            "background-color": "#e0e0e0",
                            "border-radius": "5px",
                            "margin-top": "20px",
                            "position": "relative",
                        },
                        children=[
                            html.Div(
                                style={
                                    "height": "100%",
                                    "width": line_percent,
                                    "background-color": "#3498db",
                                    "border-radius": "5px",
                                }
                            )
                        ],
                    ),
                ],
            )
        ],
    )


def dollar_format(value):
    return f"${value:,.0f}"


def closer_data_ele():
    data = {
        "Closer": ["James", "Vanessa", "Steve", "Allison", "Roger"],
        "Calls Taken": [671, 315, 296, 134, 56],
        "Cash Collected": [
            dollar_format(x) for x in [878175, 496871, 276288, 194555, 134463]
        ],
        "Fees": [dollar_format(x) for x in [18741, 16992, 8066, 6755, 4400]],
        "Revenue": [
            dollar_format(x) for x in [1471876, 824777, 419247, 348705, 158569]
        ],
        "Commission": [dollar_format(x) for x in [60160, 33592, 18776, 13146, 9104]],
    }

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Define column definitions
    column_defs = [
        {
            "headerName": "Closer",
            "field": "Closer",
            "cellStyle": {"textAlign": "center"},
            "width": "100",
        },
        {
            "headerName": "Calls Taken",
            "field": "Calls Taken",
            "cellStyle": {"textAlign": "center"},
            "width": "150",
        },
        {
            "headerName": "Cash Collected",
            "field": "Cash Collected",
            "cellStyle": {"textAlign": "center"},
            "width": "200",
        },
        {"headerName": "Fees", "field": "Fees", "cellStyle": {"textAlign": "center"}, "width": "100",},
        {
            "headerName": "Revenue",
            "field": "Revenue",
            "cellStyle": {"textAlign": "center"},
            "width": "150",
        },
        {
            "headerName": "Commission",
            "field": "Commission",
            "cellStyle": {"textAlign": "center"},
            "width": "150",
        },
    ]

    return dag.AgGrid(
        id="table",
        columnDefs=column_defs,
        rowData=df.to_dict("records"),
        defaultColDef={
            "sortable": True,
            "filter": True,
            "resizable": True,
        },
        style={
            # "height": "300px",
            "width": "100%",
            "textAlign": "center",
            # "backgroundColor": "#F1F1F1",
            "color": "white",
            "fontSize": "16px",
            "padding": "5px",
            "height": "200px",
        },
        className="ag-theme-alpine-light",
    )


# Generate the plots
plotly_card = plotly_card_plot()
bar_plot = plotly_bar_plot()


# Initialize the Dash app
app = dash.Dash(__name__)

app.layout = html.Div(
    children=[
        html.H1(
            children="Sales Command Center",
            style={
                "textAlign": "center",
                "color": "black",
                "font-family": "Arial, sans-serif",
            },
        ),
        html.Div(
            children="Your overview of your sales team's performance",
            style={
                "textAlign": "center",
                "color": "black",
                "font-family": "Arial, sans-serif",
            },
        ),
        html.Div(
            children=[
                single_metric_card("Calls Scheduled", "1,572", "20%"),
                single_metric_card("No-Shows", "264", "50%"),
                single_metric_card("Disqualified", "107", "30%"),
                single_metric_card("Closed Won", "619", "80%"),
                single_metric_card("Cash Collected", "$2,101,923", "100%"),
                single_metric_card("Revenue", "$3,375,823", "25%"),
                single_metric_card("Cash Collected Less Fees", "$2,046,941", "90%"),
            ],
            style={
                "display": "flex",
                "flexDirection": "row",
                "justifyContent": "space-around",
                "alignItems": "center",
            },
        ),
        html.Div(
            children=[
                html.Div(
                    style={
                        "background-color": "#ffffff",
                        "border-radius": "10px",
                        "box-shadow": "0 4px 8px rgba(0, 0, 0, 0.1)",
                        "padding": "15px",
                        "text-align": "center",
                        "position": "relative",
                    },
                    children=[
                        dcc.Graph(id="bar-plot", figure=bar_plot, style={"flex": "1"})
                    ],
                ),
                html.Div(closer_data_ele(), style={"flex": "1", "vertical-align": "top"}),
            ],
            style={
                "display": "flex",
                "flexDirection": "row",
                "justifyContent": "space-between",
                # "alignItems": "center",
                "alignItems": "flex-start",
            },
        ),
    ],
)


# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
    #
