import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from datetime import timedelta
from time import time

import mongots


MONGOTS_DB = 'WeatherData'
MONGOTS_COLLECTION = 'cityAndStationTemperatures'


mongots_collection = mongots.MongoTSClient() \
    .get_database(MONGOTS_DB) \
    .get_collection(MONGOTS_COLLECTION)

min_date, max_date = mongots_collection.get_timerange()
days_in_range = (max_date - min_date).days

tags = mongots_collection.get_tags()

start_end_datepicker_range = dcc.RangeSlider(
    id='date-picker-range',
    value=[0, days_in_range],
    min=0,
    max=days_in_range,
)

aggregateby_interval_dropdown = dcc.Dropdown(
    id='aggregateby_interval',
    options=[{
        'label': 'hours',
        'value': 'h',
    }, {
        'label': 'days',
        'value': 'd',
    }, {
        'label': 'months',
        'value': 'm',
    }, {
        'label': 'years',
        'value': 'y',
    }],
    value='y',
)

aggregateby_number_slider = dcc.Slider(
    id='aggregateby_numberx',
    min=1,
    max=30,
    value=1,
)

aggregateby_number_input = dcc.Input(
    id='aggregateby_number',
    type='number',
    value=1,
)


def make_filter(tag):
    return dcc.Dropdown(
        id='tag-filter-{tag}'.format(tag=tag),
        options=[{
            'label': value,
            'value': value,
        } for value in tags[tag]
        ],
        multi=True,
        value=None,
        placeholder='Select a {tag}'.format(tag=tag),
    )


groupbys = dcc.Dropdown(
    id='groupby',
    options=[{
        'label': tag,
        'value': tag,
    } for tag in list(tags)],
    multi=True,
    value=None,
    placeholder='Select a groupby key',
)


graph_type = dcc.Tabs(
    tabs=[{
        'label': 'Mean',
        'value': 'mean',
    }, {
        'label': 'Count',
        'value': 'count',
    }, {
        'label': 'Std',
        'value': 'std',
    }, {
        'label': 'Min',
        'value': 'min',
    }, {
        'label': 'Max',
        'value': 'max',
    }],
    value='mean',
    id='graph_type'
)


graph = dcc.Graph(
    id='graph'
)

app = dash.Dash()

data_selection = html.Div(
    children=[
        html.Div(
            children=[
                html.B(children='Select start/end date'),
                start_end_datepicker_range,
            ],
            style={'marginBottom': '20px'},
        ),
        html.Div(
            children=[
                html.B(children='Select aggregation'),
                aggregateby_number_input,
                aggregateby_interval_dropdown,
            ],
            style={'marginBottom': '20px'},
        ),
        html.Div(
            children=[
                html.B(children='Select filters'),
                make_filter('city'),
            ],
            style={'marginBottom': '20px'},
        ),
        html.Div(
            children=[
                html.B(children='Select groupby keys'),
                groupbys,
            ],
            style={'marginBottom': '20px'},
        ),
    ],
)

app.layout = html.Div(
    children=[
        html.H1(children='MongoTs demo dashboard', style={
            'textAlign': 'center',
        }),
        data_selection,
        graph_type,
        graph,
    ],
)


def get_series(df, tab, groupbys):
    if groupbys is None or len(groupbys) == 0:
        return [get_series0(df[tab], tab, '')]
    else:
        return [
            get_series0(
                df[df.index.get_level_values(groupbys[0]) == key][tab],
                tab,
                key,
            )
            for key in df.index.levels[1]
        ]


def get_series0(serie, tab, name):
    if 'count' == tab:
        return go.Bar(
            x=serie.index.get_level_values('datetime'),
            y=serie,
            name=name,
        )
    else:
        return go.Scatter(
            x=serie.index.get_level_values('datetime'),
            y=serie,
            mode='line',
            name=name,
        )


def get_start_end_date(value):
    start_days, end_days = value

    return min_date + timedelta(days=start_days), \
        min_date + timedelta(days=end_days)


@app.callback(
    dash.dependencies.Output('graph', 'figure'),
    [
        dash.dependencies.Input('date-picker-range', 'value'),
        dash.dependencies.Input('aggregateby_number', 'value'),
        dash.dependencies.Input('aggregateby_interval', 'value'),
        dash.dependencies.Input('tag-filter-city', 'value'),
        dash.dependencies.Input('groupby', 'value'),
        dash.dependencies.Input('graph_type', 'value'),
    ])
def update_graph(
    date_range_slider_value,
    aggregateby_number,
    aggregateby_interval,
    tag_filter_cities,
    groupby,
    graph_type,
):
    """Update the graph."""
    start, end = get_start_end_date(date_range_slider_value)
    if tag_filter_cities and len(tag_filter_cities) > 0:
        tags = {
            'city': {'$in': tag_filter_cities},
        }
    else:
        tags = None

    t0 = time()
    df = mongots_collection.query(
        start=start,
        end=end,
        aggregateby='{}{}'.format(aggregateby_number, aggregateby_interval),
        tags=tags,
        groupby=groupby,
    )
    t1 = time()
    print('Query to Db ! {len} lines fetched, it took {t:.3f} s'.format(
        len=len(df),
        t=t1 - t0,
    ))

    return {
        'data': get_series(df, graph_type, groupby),
        'layout': go.Layout(
            xaxis={'title': 'Date'},
            yaxis={'title': 'Temperature'},
        )
    }


if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8080)
