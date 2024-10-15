from dash import Dash, dcc, html
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import plotly.express as px
import plotly.graph_objects as go
import pymongo
import pandas as pd

#Establishing connection to MongoDB
client = pymongo.MongoClient("mongodb://192.168.64.5:27017")
db = client["yelp"]
app = Dash(__name__)
# making lookup table for business names
def get_business_names():
    businesses = db["businesses"]
    business_data = list(businesses.find({}, {"business_id": 1, "name": 1, "_id": 0}))
    business_lookup = {item['business_id']: item['name'] for item in business_data}
    return business_lookup
def add_business_names(df, lookup_dict):
    df['name'] = df['business_id'].map(lookup_dict)
    return df

def get_mongo_data(collection_name):
    collection = db[collection_name]
    data = list(collection.find())
    return pd.DataFrame(data)

# Dashboard layout
app.layout = html.Div([
    html.H1(
        "Advanced Business Analytics Dashboard", 
        style={'textAlign': 'center'}
    ),    
    dcc.Interval(
        id='interval-component',
        interval=10*1000, 
        n_intervals=0
    ),
    dcc.Graph(id='sentiment-bar'),
    dcc.Graph(id='popularity-bar'),
    dcc.Graph(id='geospatial-map'),
    dcc.Graph(id='user-engagement-bar'),
    dcc.Graph(id='category-pie'),
    dcc.Graph(id='top-rated-categories-bar'),
    dcc.Graph(id='city-business-count-bar'),
    dcc.Graph(id='tip-sentiment-bar'),
    dcc.Graph(id='review-trend-graph'),
    dcc.Graph(id='business-predictions-bar')
])
# Different types of charts and bars for the dashboard
def generate_sentiment_bar(sentiment_df, business_lookup):
    if isinstance(business_lookup, dict):
        business_lookup = pd.DataFrame(business_lookup.items(), columns=['business_id', 'name'])
    sentiment_df = pd.merge(sentiment_df, business_lookup, on='business_id', how='inner')
    sentiment_df = sentiment_df.drop_duplicates(subset='business_id')
    top_positive_df = sentiment_df.sort_values(by=['positive_reviews', 'average_rating'], ascending=[False, False]).head(10)
    top_negative_df = sentiment_df.sort_values(by=['negative_reviews', 'average_rating'], ascending=[False, True]).head(10)    
    fig = make_subplots(rows=1, cols=2, shared_yaxes=False, subplot_titles=("Top 10 Businesses by Positive Reviews", "Worst 10 Businesses by Negative Reviews"))
    fig.add_trace(
        px.bar(top_positive_df, x='name', y='positive_reviews', hover_name='name', color='average_rating').data[0],
        row=1, col=1
    )
    fig.add_trace(
        px.bar(top_negative_df, x='name', y='negative_reviews', hover_name='name', color='average_rating').data[0],
        row=1, col=2
    )
    fig.update_layout(
        title={
            'text': "<b>Business Sentiment Analysis</b>",
            'y': 0.9,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(
                size=24
            )
        },
        coloraxis=dict(colorscale='Viridis', cmin=0, cmax=5), 
        showlegend=False, 
        coloraxis_colorbar=dict(title="Average Rating")
    )
    fig.update_traces(marker=dict(coloraxis="coloraxis"))
    return fig

def generate_popularity_bar(popularity_df, business_lookup):
    if isinstance(business_lookup, dict):
        business_lookup = pd.DataFrame(business_lookup.items(), columns=['business_id', 'name'])

    popularity_df = pd.merge(popularity_df, business_lookup, on='business_id', how='inner')
    popularity_df = popularity_df.drop_duplicates(subset='business_id')
    top_popular_df = popularity_df.sort_values(by='count', ascending=False).head(10)
    top_unpopular_df = popularity_df.sort_values(by='count', ascending=True).head(10)

    fig = make_subplots(
        rows=1, cols=2,
        shared_yaxes=True,
        subplot_titles=("Top 10 Popular Businesses", "Top 10 Unpopular Businesses")
    )
    fig.add_trace(
        go.Bar(
            x=top_popular_df['name'], y=top_popular_df['count'],
            name='Top Popular',
            marker=dict(color=top_popular_df['count'], colorscale='Viridis', colorbar=dict(title="Check-in Count"))
        ),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(
            x=top_unpopular_df['name'], y=top_unpopular_df['count'],
            name='Top Unpopular',
            marker=dict(color=top_unpopular_df['count'], colorscale='Viridis')
        ),
        row=1, col=2
    )
    fig.update_layout(
        title={
            'text': "<b>Business Check-In Popularity</b>",
            'y': .98,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': dict(size=24)
        },
        showlegend=False,
        height=500,
        coloraxis=dict(colorscale='Spectral'),
        margin=dict(t=50, b=50)
    )    
    fig.update_traces(marker=dict(coloraxis="coloraxis"))
    return fig

def generate_geospatial_map(geospatial_df):
    geospatial_fig = px.scatter_mapbox(
        geospatial_df,
        lat='latitude',
        lon='longitude',
        hover_name='name',
        hover_data={
            'categories': True,
            'stars': True,
            'review_count': True,
            'latitude': False,
            'longitude': False
        },
        color='stars', 
        size='review_count',
        size_max=15,
        color_continuous_scale=px.colors.sequential.Viridis,
        zoom=10,
    )
    geospatial_fig.update_layout(
        mapbox_style="open-street-map", 
        mapbox_zoom=10,
        mapbox=dict(
            center=dict(lat=39.7684, lon=-86.1581),  
            pitch=0  
        ),
        margin={"r": 0, "t": 50, "l": 0, "b": 0},
        title={
            'text': "<b>Geospatial Distribution of Businesses</b>",
            'y': 0.95,  
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 24}
        },
        hovermode='closest' 
    )
    geospatial_fig.update_traces(
        marker=dict(
            symbol='circle', 
            size=12, 
            opacity=0.9, 
            sizemin=8  
        )
    )
    geospatial_fig.update_coloraxes(
        cmin=1,
        cmax=5,
        colorbar=dict(
            title="Star Rating",
            ticks="outside",
            tickvals=[1, 2, 3, 4, 5],
            ticktext=["1 Star", "2 Stars", "3 Stars", "4 Stars", "5 Stars"]
        )
    )

    return geospatial_fig

def generate_user_engagement_bar(user_engagement_df):
    unique_users_df = user_engagement_df.drop_duplicates(subset=['user_id'])
    top_users_df = unique_users_df.sort_values(by='review_count', ascending=False).head(20)
    top_users_bar = px.bar(
        top_users_df, x='name', y='review_count', hover_name='name',
        hover_data={
            'review_count': True, 
            'useful': True, 
            'funny': True, 
            'cool': True
        },
        title="Top 20 Users by Review Count", 
        color='review_count'
    )
    bottom_users_df = unique_users_df.sort_values(by='review_count', ascending=True).head(20)
    bottom_users_bar = px.bar(
        bottom_users_df, x='name', y='review_count', hover_name='name',
        hover_data={
            'review_count': True, 
            'useful': True, 
            'funny': True, 
            'cool': True
        },
        title="Bottom 20 Users by Review Count", 
        color='review_count'
    )
    fig = make_subplots(
        rows=1, cols=2, subplot_titles=("Top 20 Users by Review Count", "Bottom 20 Users by Review Count"),
        horizontal_spacing=0.05 
    )
    fig.add_trace(top_users_bar['data'][0], row=1, col=1)
    fig.add_trace(bottom_users_bar['data'][0], row=1, col=2)
    fig.update_layout(
        height=500, 
        title={
            'text': "<b>User Engagement</b>",
            'y':0.95,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 24, 'color': 'black'} 
        },
        showlegend=False
    )
    return fig

def generate_category_pie(category_distribution_df):
    sorted_df = category_distribution_df.sort_values(by='count', ascending=False)
    top_categories = sorted_df.head(10)
    other_count = sorted_df['count'][10:].sum()
    other_categories = pd.DataFrame({
        'category': ['Other'],
        'count': [other_count]
    })
    final_df = pd.concat([top_categories, other_categories])
    pie_chart = px.pie(final_df, names='category', values='count',
                       title="Category Distribution",
                       color_discrete_sequence=px.colors.qualitative.Vivid,
                       hole=0.3)
    pie_chart.update_traces(
        hoverinfo='label+percent', textinfo='percent',
        marker=dict(line=dict(color='#000000', width=1)))
    pie_chart.update_layout(
        title=dict(text="<b>Category Distribution</b>", x=0.5, y=0.95, font=dict(size=24)),
        legend_title_text='Category',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.2,
            xanchor="center",
            x=0.5
        )
    )
    return pie_chart

def generate_top_rated_categories_bar(top_rated_categories_df):
    top_rated_categories_df = top_rated_categories_df.drop_duplicates(subset=['category'])
    top_20_categories = top_rated_categories_df.sort_values(by='average_rating', ascending=False).head(20)
    worst_20_categories = top_rated_categories_df.sort_values(by='average_rating', ascending=True).head(20)
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Top 20 Rated Categories", "Worst 20 Rated Categories"))
    fig.add_trace(
        go.Bar(x=top_20_categories['category'], y=top_20_categories['average_rating'], 
               hovertext=top_20_categories['category'], name='Top 20'),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(x=worst_20_categories['category'], y=worst_20_categories['average_rating'], 
               hovertext=worst_20_categories['category'], name='Worst 20'),
        row=1, col=2
    )
    fig.update_layout(
        title={
            'text': "<b>Business Category Rating</b>",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 24}
        },
        showlegend=False,
        height=600
    )
    fig.update_xaxes(title_text="Category", tickangle=-45)
    fig.update_yaxes(title_text="Average Rating")
    return fig

def generate_city_business_count_bar(city_business_count_df):
    city_business_count_df = city_business_count_df.drop_duplicates(subset=['city'])
    top_20_cities = city_business_count_df.sort_values(by='count', ascending=False).head(20)
    bottom_20_cities = city_business_count_df.sort_values(by='count', ascending=True).head(20)
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Top 20 Cities by Business Count", "Worst 20 Cities by Business Count"))
    fig.add_trace(
        go.Bar(x=top_20_cities['city'], y=top_20_cities['count'], 
               hovertext=top_20_cities['city'], name='Top 20 Cities'),
        row=1, col=1
    )
    fig.add_trace(
        go.Bar(x=bottom_20_cities['city'], y=bottom_20_cities['count'], 
               hovertext=bottom_20_cities['city'], name='Bottom 20 Cities'),
        row=1, col=2
    )
    fig.update_layout(
        title={
            'text': "<b>City-wise Business Distribution</b>",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 24}
        },
        showlegend=False,
        height=500
    )
    fig.update_xaxes(title_text="City", tickangle=-45)
    fig.update_yaxes(title_text="Business Count")
    return fig

def generate_tip_sentiment_bar(tip_sentiment_df, business_lookup):
    tip_sentiment_df = add_business_names(tip_sentiment_df, business_lookup)
    tip_sentiment_df = tip_sentiment_df.drop_duplicates(subset=['name'])
    top_positive_df = tip_sentiment_df.sort_values(by=['positive_tips'], ascending=False).head(10)
    top_negative_df = tip_sentiment_df.sort_values(by=['negative_tips'], ascending=False).head(10)
    fig = make_subplots(rows=1, cols=2, shared_yaxes=False, subplot_titles=("Top 10 Businesses by Positive Tips", "Top 10 Businesses by Negative Tips"))
    fig.add_trace(
        px.bar(top_positive_df, x='name', y='positive_tips', hover_name='name',
               color='positive_tips').data[0],
        row=1, col=1
    )
    fig.add_trace(
        px.bar(top_negative_df, x='name', y='negative_tips', hover_name='name',
               color='negative_tips').data[0],
        row=1, col=2
    )
    fig.update_layout(
        title={
            'text': "<b>Tip Sentiment Analysis</b>",
            'y': 0.95,
            'x': 0.5,
            'xanchor': 'center',
            'yanchor': 'top',
            'font': {'size': 24}
        },
        coloraxis=dict(colorscale='Viridis', cmin=0, cmax=max(top_positive_df['positive_tips'].max(), top_negative_df['negative_tips'].max())),
        showlegend=False,
        coloraxis_colorbar=dict(title="Tip Count"),
        height=500
    )
    fig.update_traces(marker=dict(coloraxis="coloraxis"))
    return fig

def generate_review_trend_chart(trend_df, business_lookup):
    trend_df['name'] = trend_df['business_id'].map(business_lookup)
    trend_df['date'] = pd.to_datetime(trend_df['review_year'].astype(str) + '-' + trend_df['review_month'].astype(str))
    total_reviews = trend_df.groupby(['business_id', 'name', 'date']).agg({
        'positive_reviews': 'sum',
        'negative_reviews': 'sum'
    }).reset_index()
    summary_reviews = total_reviews.groupby(['business_id', 'name']).agg({
        'positive_reviews': 'sum',
        'negative_reviews': 'sum'
    }).reset_index()
    significant_reviews = summary_reviews[(summary_reviews['positive_reviews'] > 10) | (summary_reviews['negative_reviews'] > 10)]
    significant_reviews['total_reviews'] = significant_reviews['positive_reviews'] + significant_reviews['negative_reviews']
    top_businesses = significant_reviews.nlargest(10, 'total_reviews')['business_id']
    filtered_df = total_reviews[total_reviews['business_id'].isin(top_businesses)]
    fig = make_subplots(rows=1, cols=2, subplot_titles=("Positive Review Trends", "Negative Review Trends"))
    for business_id in top_businesses:
        business_data = filtered_df[filtered_df['business_id'] == business_id]
        business_name = business_lookup.get(business_id, "Unknown Business")
        fig.add_trace(
            go.Scatter(x=business_data['date'], y=business_data['positive_reviews'], 
                       name=business_name + " (Positive)", mode='lines'),
            row=1, col=1
        )
        fig.add_trace(
            go.Scatter(x=business_data['date'], y=business_data['negative_reviews'], 
                       name=business_name + " (Negative)", mode='lines'),
            row=1, col=2
        )
    fig.update_layout(
        title="<b>Review Trends for Selected Businesses</b>",
        title_x=0.5,
        title_font=dict(size=24),
        yaxis_title="Review Count",
        height=600,
        showlegend=True
    )
    fig.update_xaxes(tickangle=-45)
    fig.update_yaxes(type='linear')
    return fig

def get_top_business_predictions():
    collection = db["predictions"]
    data = list(collection.find().sort("prediction", -1))
    return pd.DataFrame(data)

def generate_business_predictions_bar(predictions_df, business_lookup):
    predictions_df = add_business_names(predictions_df, business_lookup)
    predictions_df = predictions_df.drop_duplicates(subset='business_id')
    predictions_df = predictions_df.sort_values(by='prediction', ascending=False)
    top_predictions = predictions_df.head(50)
    fig = px.bar(
        top_predictions,
        x='name', 
        y='prediction', 
        title="Top 50 Businesses based on Rating Prediction",
        labels={"name": "Business Name", "prediction": "Predicted Rating"}
    )
    fig.update_layout(
        title=dict(
            text="<b>Top 50 Businesses based on Rating Prediction</b>",
            x=0.5,
            y=0.95,
            xanchor='center',
            yanchor='top',
            font=dict(size=24)
        )
    )
    return fig

@app.callback(
    [
        Output('sentiment-bar', 'figure'),
        Output('popularity-bar', 'figure'),
        Output('geospatial-map', 'figure'),
        Output('user-engagement-bar', 'figure'),
        Output('category-pie', 'figure'),
        Output('top-rated-categories-bar', 'figure'),
        Output('city-business-count-bar', 'figure'),
        Output('review-trend-graph', 'figure'),
        Output('tip-sentiment-bar', 'figure'),
        Output('business-predictions-bar', 'figure')
    ],
    [Input('interval-component', 'n_intervals')]
)
def update_dashboard(n, selected_business_id=None):
    business_lookup = get_business_names()
    sentiment_df = get_mongo_data("sentiment_aggregation")
    popularity_df = get_mongo_data("popularity_aggregation")
    geospatial_df = get_mongo_data("geospatial_aggregation")
    user_engagement_df = get_mongo_data("user_engagement")
    category_distribution_df = get_mongo_data("category_distribution")
    top_rated_categories_df = get_mongo_data("top_rated_categories")
    city_business_count_df = get_mongo_data("city_wise_business_count")
    review_trend_df=get_mongo_data("review_trend_aggregation")
    tip_sentiment_df = get_mongo_data("tip_sentiment_aggregation")
    sentiment_bar = generate_sentiment_bar(sentiment_df, business_lookup)
    popularity_bar = generate_popularity_bar(popularity_df, business_lookup)
    geospatial_map = generate_geospatial_map(geospatial_df)
    user_engagement_bar = generate_user_engagement_bar(user_engagement_df)
    category_pie = generate_category_pie(category_distribution_df)
    top_rated_categories_bar = generate_top_rated_categories_bar(top_rated_categories_df)
    city_business_count_bar = generate_city_business_count_bar(city_business_count_df)
    review_trend=generate_review_trend_chart(review_trend_df, business_lookup)
    tip_sentiment_fig = generate_tip_sentiment_bar(tip_sentiment_df, business_lookup)
    predictions_df = get_top_business_predictions()
    business_predictions_bar = generate_business_predictions_bar(predictions_df, business_lookup)
    return (
        sentiment_bar,
        popularity_bar,
        geospatial_map,
        user_engagement_bar,
        category_pie,
        top_rated_categories_bar,
        city_business_count_bar,
        review_trend,
        tip_sentiment_fig,
        business_predictions_bar
    )

if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
