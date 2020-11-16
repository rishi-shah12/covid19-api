import flask
from flask import request, jsonify
import requests
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
import simplejson
import numpy as np

app = flask.Flask(__name__)
app.config["DEBUG"] = True

# Create some test data for our catalog in the form of a list of dictionaries.

url = 'https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/ed270bb8-340b-41f9-a7c6-e8ef587e6d11/download/covidtesting.csv'
r = requests.get(url, allow_redirects=True)
open('covid_data.csv', 'wb').write(r.content)

df = pd.read_csv('covid_data.csv')
disk_engine = create_engine('sqlite:///covid19_data_ontario.db')
df.to_sql('covid19_data_ontario', disk_engine, if_exists='replace')

covid19_ontario = pd.read_sql_query('SELECT * FROM covid19_data_ontario', disk_engine)
covid19_ontario = covid19_ontario.where(covid19_ontario.notnull(), [None])
print(covid19_ontario)



data = [ {'id': 0,
     'title': 'A Fire Upon the Deep',
     'author': 'Vernor Vinge',
     'first_sentence': 'The coldsleep itself was dreamless.',
     'year_published': '1992'}]


@app.route('/', methods=['GET'])
def home():
    return '''<h1>Covid 19 Data Ontario</h1>
<p>A prototype API for providing the statistics of Covid-19 in Ontario </p>'''


@app.route('/v1', methods=['GET'])
def api_all():
    data = []
    for x in range(0, len(covid19_ontario)):
        if (covid19_ontario.iloc[x][2] == None):
            confirmed_negative = covid19_ontario.iloc[x][2]
        else:
            confirmed_negative = float(covid19_ontario.iloc[x][2])

        if (covid19_ontario.iloc[x][3] == None):
            presumptive_negative = covid19_ontario.iloc[x][3]
        else:
            presumptive_negative = float(covid19_ontario.iloc[x][3])

        if (covid19_ontario.iloc[x][4] == None):
            presumptive_positive = covid19_ontario.iloc[x][4]
        else:
            presumptive_positive = float(covid19_ontario.iloc[x][4])

        if (covid19_ontario.iloc[x][5] == None):
            confirmed_positive = covid19_ontario.iloc[x][5]
        else:
            confirmed_positive = float(covid19_ontario.iloc[x][5])

        if (covid19_ontario.iloc[x][6] == None):
            resolved = covid19_ontario.iloc[x][6]
        else:
            resolved = float(covid19_ontario.iloc[x][6])
        current = [
            {'Reported_Date': str(covid19_ontario.iloc[x][1]),
             'Confirmed_Negative': confirmed_negative,
             'Presumptive_Negative': presumptive_negative,
             'Presumptive_Positive': presumptive_positive,
             'Confirmed_Positive': confirmed_positive,
             'Resolved': resolved
            }
        ]
        data = data + current
    return jsonify(data)


@app.route('/api/v1/resources/books', methods=['GET'])
def api_id():
    # Check if an ID was provided as part of the URL.
    # If ID is provided, assign it to a variable.
    # If no ID is provided, display an error in the browser.
    if 'id' in request.args:
        id = int(request.args['id'])
    else:
        return "Error: No id field provided. Please specify an id."

    # Create an empty list for our results
    results = []

    # Loop through the data and match results that fit the requested ID.
    # IDs are unique, but other fields might return many results
    for book in books:
        if book['id'] == id:
            results.append(book)

    # Use the jsonify function from Flask to convert our list of
    # Python dictionaries to the JSON format.
    return jsonify(results)

app.run()