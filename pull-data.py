import time
import flask
from flask import request, jsonify
import requests
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import datetime

while True:
    url = 'https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/ed270bb8-340b-41f9-a7c6-e8ef587e6d11/download/covidtesting.csv'
    r = requests.get(url, allow_redirects=True)
    open('covid_data.csv', 'wb').write(r.content)
    time.sleep(10000)