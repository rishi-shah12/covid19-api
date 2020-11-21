import time
import flask
from flask import request, jsonify
import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import datetime
import gunicorn

app = flask.Flask(__name__)
app.config["DEBUG"] = True

df_ontario = pd.read_csv('covid19_data_ontario.csv')
disk_engine = create_engine('sqlite:///covid19_data_ontario.db')
df_ontario.to_sql('covid19_data_ontario', disk_engine, if_exists='replace')

covid19_ontario = pd.read_sql_query('SELECT * FROM covid19_data_ontario', disk_engine)
covid19_ontario = covid19_ontario.where(covid19_ontario.notnull(), [None])

df_canada = pd.read_csv('covid19_data_canada.csv')
disk_engine = create_engine('sqlite:///covid19_data_canada.db')
df_canada.to_sql('covid19_data_canada', disk_engine, if_exists='replace')

covid19_all_canada = pd.read_sql_query('SELECT * FROM covid19_data_canada', disk_engine)
covid19_all_canada = covid19_all_canada.where(covid19_all_canada.notnull(), [None])
covid19_canada = (covid19_all_canada[covid19_all_canada['prname'].str.contains("Canada")])


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found. Please Try Again</p>", 404

@app.route('/', methods=['GET'])
def home():
    return '''<h1>Covid 19 Data Ontario</h1>
<p>A prototype API for providing the statistics of Covid-19 in Ontario. Enter ./canada to get all the available data for canada
and /ontario to get all available data for ontario</p>'''


@app.route('/ontario', methods=['GET'])
def api_ontario():
    data_ontario = []

    query_parameters = request.args
    begin = query_parameters.get('begin')
    end = query_parameters.get('end')

    if begin:
        begin_date = begin
    else:
        begin_date = covid19_ontario.head(1).iloc[0]["Reported Date"]

    begin_year = int(begin_date.split('-')[0])
    begin_month = int(begin_date.split('-')[1])
    begin_day = int(begin_date.split('-')[2])
    begin_datetime = datetime.datetime(begin_year,begin_month,begin_day)

    if end:
        end_date = end
    else:
        end_date = covid19_ontario.tail(1).iloc[0]["Reported Date"]

    end_year = int(end_date.split('-')[0])
    end_month = int(end_date.split('-')[1])
    end_day = int(end_date.split('-')[2])
    end_datetime = datetime.datetime(end_year,end_month,end_day)

    if begin_datetime < datetime.datetime(int(covid19_ontario.head(1).iloc[0]["Reported Date"].split('-')[0]),
                                        int(covid19_ontario.head(1).iloc[0]["Reported Date"].split('-')[1]),
                                        int(covid19_ontario.head(1).iloc[0]["Reported Date"].split('-')[2])):
        return "Error: The begin date is before the first provincial data"

    if end_datetime < datetime.datetime(int(covid19_ontario.head(1).iloc[0]["Reported Date"].split('-')[0]),
                                        int(covid19_ontario.head(1).iloc[0]["Reported Date"].split('-')[1]),
                                        int(covid19_ontario.head(1).iloc[0]["Reported Date"].split('-')[2])):
        return "Error: The end date is before the first provincial data"

    if end_datetime > datetime.datetime(int(covid19_ontario.tail(1).iloc[0]["Reported Date"].split('-')[0]),
                                        int(covid19_ontario.tail(1).iloc[0]["Reported Date"].split('-')[1]),
                                        int(covid19_ontario.tail(1).iloc[0]["Reported Date"].split('-')[2])):
        return "Error: The end date is past the current provincial data"

    if begin_datetime > datetime.datetime(int(covid19_ontario.tail(1).iloc[0]["Reported Date"].split('-')[0]),
                                        int(covid19_ontario.tail(1).iloc[0]["Reported Date"].split('-')[1]),
                                        int(covid19_ontario.tail(1).iloc[0]["Reported Date"].split('-')[2])):
        return "Error: The begin date is past the current provincial data"

    if end_datetime < begin_datetime:
        return "Error: The end date is before the begin date"

    for x in range(0, len(covid19_ontario)):
        current_date = str(covid19_ontario.iloc[x][1])
        current_year = int(current_date.split('-')[0])
        current_month = int(current_date.split('-')[1])
        current_day = int(current_date.split('-')[2])
        current_datetime = datetime.datetime(current_year,current_month,current_day)

        if (current_datetime >= begin_datetime) and (current_datetime <= end_datetime):

            confirmed_negative = covid19_ontario.iloc[x][2] if covid19_ontario.iloc[x][2] == None else float(covid19_ontario.iloc[x][2])
            presumptive_negative = covid19_ontario.iloc[x][3] if covid19_ontario.iloc[x][3] == None else float(covid19_ontario.iloc[x][3])
            presumptive_positive = covid19_ontario.iloc[x][4] if covid19_ontario.iloc[x][4] == None else float(covid19_ontario.iloc[x][4])
            confirmed_positive = covid19_ontario.iloc[x][5] if covid19_ontario.iloc[x][5] == None else float(covid19_ontario.iloc[x][5])
            recovered = covid19_ontario.iloc[x][6] if covid19_ontario.iloc[x][6] == None else float(covid19_ontario.iloc[x][6])
            deaths = covid19_ontario.iloc[x][7] if covid19_ontario.iloc[x][7] == None else float(covid19_ontario.iloc[x][7])
            total_cases = covid19_ontario.iloc[x][8] if covid19_ontario.iloc[x][8] == None else float(covid19_ontario.iloc[x][8])
            previous_day_tests = covid19_ontario.iloc[x][10] if covid19_ontario.iloc[x][10] == None else float(covid19_ontario.iloc[x][10])
            previous_day_pct_positive = covid19_ontario.iloc[x][11] if covid19_ontario.iloc[x][11] == None else float(covid19_ontario.iloc[x][11])
            under_investigation = covid19_ontario.iloc[x][12] if covid19_ontario.iloc[x][12] == None else float(covid19_ontario.iloc[x][12])
            hospitalized_patients = covid19_ontario.iloc[x][13] if covid19_ontario.iloc[x][13] == None else float(covid19_ontario.iloc[x][13])
            icu_patients = covid19_ontario.iloc[x][14] if covid19_ontario.iloc[x][14] == None else float(covid19_ontario.iloc[x][14])
            ventilator_patients = covid19_ontario.iloc[x][15] if covid19_ontario.iloc[x][15] == None else float(covid19_ontario.iloc[x][15])
            ltc_cases = covid19_ontario.iloc[x][16] if covid19_ontario.iloc[x][16] == None else float(covid19_ontario.iloc[x][16])
            ltc_hcw_cases = covid19_ontario.iloc[x][17] if covid19_ontario.iloc[x][17] == None else float(covid19_ontario.iloc[x][17])
            ltc_deaths = covid19_ontario.iloc[x][18] if covid19_ontario.iloc[x][18] == None else float(covid19_ontario.iloc[x][18])
            ltc_hcw_deaths = covid19_ontario.iloc[x][19] if covid19_ontario.iloc[x][19] == None else float(covid19_ontario.iloc[x][19])

            if (x == 0 or x == 1 ):
                new_cases = None
            elif (x == 2):
                new_cases = float(covid19_ontario.iloc[x][8])
            else:
                new_cases = float(covid19_ontario.iloc[x][8]) - float(covid19_ontario.iloc[x-1][8])

            if (x < 40):
                active_cases = None
            else:
                active_cases = total_cases - recovered - deaths

            current = [
                {
                 'Reported_Date': str(covid19_ontario.iloc[x][1]),
                 'Confirmed_Negative': confirmed_negative,
                 'Presumptive_Negative': presumptive_negative,
                 'Presumptive_Positive': presumptive_positive,
                 'Confirmed_Positive': confirmed_positive,
                 'Recovered': recovered,
                 'Deaths': deaths,
                 'Total_Cases': total_cases,
                 'Previous_Day_Tests': previous_day_tests,
                 'Previous_Day_Pct_Positive': previous_day_pct_positive,
                 'Under_Investigation': under_investigation,
                 'Hospitalized_Patients': hospitalized_patients,
                 'ICU_Patients': icu_patients,
                 'Ventilator_Patients': ventilator_patients,
                 'LTC_Resident_Positive_Cases': ltc_cases,
                 'LTC_Healthcare_Workers_Positive_Cases': ltc_hcw_cases,
                 'LTC_Resident_Deaths': ltc_deaths,
                 'LTC_Healthcare_Workers_Deaths': ltc_hcw_deaths,
                 'New_Cases': new_cases,
                 'Active_Cases': active_cases
                 }
            ]
            data_ontario = data_ontario + current
    return jsonify(data_ontario)

@app.route('/canada', methods=['GET'])
def api_canada():
    data_canada = []

    query_parameters = request.args
    begin = query_parameters.get('begin')
    end = query_parameters.get('end')

    if begin:
        begin_date = begin
    else:
        begin_date = covid19_canada.head(1).iloc[0]["date"]

    begin_year = int(begin_date.split('-')[0])
    begin_month = int(begin_date.split('-')[1])
    begin_day = int(begin_date.split('-')[2])
    begin_datetime = datetime.datetime(begin_year,begin_month,begin_day)

    if end:
        end_date = end
    else:
        end_date = covid19_canada.tail(1).iloc[0]["date"]

    end_year = int(end_date.split('-')[0])
    end_month = int(end_date.split('-')[1])
    end_day = int(end_date.split('-')[2])
    end_datetime = datetime.datetime(end_year,end_month,end_day)

    if begin_datetime < datetime.datetime(int(covid19_canada.head(1).iloc[0]["date"].split('-')[0]),
                                        int(covid19_canada.head(1).iloc[0]["date"].split('-')[1]),
                                        int(covid19_canada.head(1).iloc[0]["date"].split('-')[2])):
        return "Error: The begin date is before the first federal data"

    if end_datetime < datetime.datetime(int(covid19_canada.head(1).iloc[0]["date"].split('-')[0]),
                                        int(covid19_canada.head(1).iloc[0]["date"].split('-')[1]),
                                        int(covid19_canada.head(1).iloc[0]["date"].split('-')[2])):
        return "Error: The end date is before the first federal data"

    if end_datetime > datetime.datetime(int(covid19_canada.tail(1).iloc[0]["date"].split('-')[0]),
                                        int(covid19_canada.tail(1).iloc[0]["date"].split('-')[1]),
                                        int(covid19_canada.tail(1).iloc[0]["date"].split('-')[2])):
        return "Error: The end date is past the current federal data"

    if begin_datetime > datetime.datetime(int(covid19_canada.tail(1).iloc[0]["date"].split('-')[0]),
                                        int(covid19_canada.tail(1).iloc[0]["date"].split('-')[1]),
                                        int(covid19_canada.tail(1).iloc[0]["date"].split('-')[2])):
        return "Error: The begin date is past the current federal data"

    if end_datetime < begin_datetime:
        return "Error: The end date is before the begin date"

    for x in range(0, len(covid19_canada)):
        current_date = str(covid19_canada.iloc[x]["date"])
        current_year = int(current_date.split('-')[0])
        current_month = int(current_date.split('-')[1])
        current_day = int(current_date.split('-')[2])
        current_datetime = datetime.datetime(current_year,current_month,current_day)

        if (current_datetime >= begin_datetime) and (current_datetime <= end_datetime):
            confirmed_positive = covid19_canada.iloc[x]["numtoday"] if covid19_canada.iloc[x]["numtoday"] == None else float(covid19_canada.iloc[x]["numtoday"])
            presumptive_positive = covid19_canada.iloc[x]["numprob"] if covid19_canada.iloc[x]["numprob"] == None else float(covid19_canada.iloc[x]["numprob"])
            recovered = covid19_canada.iloc[x]["numrecover"] if covid19_canada.iloc[x]["numrecover"] == None else float(covid19_canada.iloc[x]["numrecover"])
            deaths = covid19_canada.iloc[x]["numdeaths"] if covid19_canada.iloc[x]["numdeaths"] == None else float(covid19_canada.iloc[x]["numdeaths"])
            total_cases = covid19_canada.iloc[x]["numtotal"] if covid19_canada.iloc[x]["numtotal"] == None else float(covid19_canada.iloc[x]["numtotal"])
            previous_day_tests = covid19_canada.iloc[x]["numtestedtoday"] if covid19_canada.iloc[x]["numtestedtoday"] == None else float(covid19_canada.iloc[x]["numtestedtoday"])
            new_cases = covid19_canada.iloc[x]["numtoday"] if covid19_canada.iloc[x]["numtoday"] == None else float(covid19_canada.iloc[x]["numtoday"])
            previous_day_pct_positive = covid19_canada.iloc[x]["percentoday"] if covid19_canada.iloc[x]["percentoday"] == None else float(covid19_canada.iloc[x]["percentoday"])
            active_cases = covid19_canada.iloc[x]["numactive"] if covid19_canada.iloc[x]["numactive"] == None else float(covid19_canada.iloc[x]["numactive"])

            current_canada = [
                {
                    'Reported_Date': str(covid19_canada.iloc[x]["date"]),
                    'Confirmed_Positive': confirmed_positive,
                    'Total_Cases': total_cases,
                    'Presumptive_Positive': presumptive_positive,
                    'Deaths': deaths,
                    'Previous_Day_Tests': previous_day_tests,
                    'Recovered': recovered,
                    'New_Cases': new_cases,
                    'Previous_Day_Pct_Positive': previous_day_pct_positive,
                    'Active_Cases': active_cases
                }
            ]
            data_canada = data_canada + current_canada
    return jsonify(data_canada)

app.run()