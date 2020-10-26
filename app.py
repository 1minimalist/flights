from flask import Flask
from redis import Redis
import requests
import json
import time
from datetime import date
from dateutil.relativedelta import relativedelta
from flask_crontab import Crontab
import logging

app = Flask(__name__)
crontab = Crontab(app)
redis = Redis(host='redis', port=6379)

def parse_flights_data(data):
    prices = []
    logging.info('Fetching response')
    for j in data:
        btoken = j['booking_token']
        price = j['price']
        if check_flights_data(btoken):
            prices.append(price)
    return str(min(prices))

def get_url_data(url):
    r = requests.get(url)
    return r

def check_flights_data(token):
    check_url = "https://booking-api.skypicker.com/api/v0.1/check_flights?v=2&booking_token={}&bnum=3&pnum=2&affily=picky_us&currency=EUR&adults=2&children=0&infants=0".format(token)
    res = json.loads(get_url_data(check_url).text)
    logging.info('Checking flights status')
    fl_check = res['flights_checked']
    logging.info('Fetching response', fl_check)
    while fl_check==False:
        time.sleep(5)
        res = json.loads(get_url_data(check_url).text)
        fl_check = res['flights_checked']
    invalid_check = res['flights_invalid']
    logging.info('Fetching response', invalid_check)
    if invalid_check==False:
        return True
    return False

# @app.route("/flights")
def fetch_flights():
    dirs = ('ALA - MOW','MOW - ALA','ALA - CIT','CIT - ALA','TSE - MOW','MOW - TSE','TSE - LED','LED - TSE', 'ALA - TSE', 'TSE - ALA')
    for i in dirs:
        dfrom = date.today().strftime("%d/%m/%Y")
        dto = (date.today() + relativedelta(months=+1)).strftime("%d/%m/%Y")
        print(dfrom, dto)
        ffrom = i[:i.find('-')-1]
        fto = i[i.find('-')+2:]
        url = "https://api.skypicker.com/flights?flyFrom={}&to={}&dateFrom={}&dateTo={}&partner=picky".format(ffrom, fto, dfrom, dto)
        # print(url)
        r = requests.get(url)
        logging.info('Requested '+url)
        res = parse_flights_data(json.loads(r.text)['data'])
        redis.set(i, res)
    return 'Done'

@crontab.job(minute="05", hour="0")
def schedule_job():
    return fetch_flights()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)