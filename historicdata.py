#!/home/me/miniconda3/envs/binance/bin/python3.8

import modin.pandas as pd
from binance.client import Client
import datetime

# YOUR API KEYS HERE
api_key = "wtqoMQp21dTGKL4qrZeHaxv0UZETf55sHOqaWLgu6wAKfmB3N7hTBPjGoMFpHAv3"    #Enter your own API-key here
api_secret = "v1YKCDgI11Tl7czwUUgAfBvDWaZjg12hiN3HTMVen8EqDdZuVJxPiPZ3scKW1pBa" #Enter your own API-secret here

bclient = Client(api_key=api_key, api_secret=api_secret)

start_date = datetime.datetime.strptime('1 Jan 2016', '%d %b %Y')
today = datetime.datetime.today()

def binanceBarExtractor(symbol):
    print('working...')
    filename = '{}_MinuteBars.csv'.format(symbol)

    klines = bclient.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, start_date.strftime("%d %b %Y %H:%M:%S"), today.strftime("%d %b %Y %H:%M:%S"), 1000)
    data = pd.DataFrame(klines, columns = ['open_timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_timestamp', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore' ])
    # data['open_time'] = pd.to_datetime(data['open_time'], unit='ms')
    # data['close_time'] = pd.to_datetime(data['close_time'], unit='ms')
    # data = data.reset_index()

    # data.set_index('open_time', inplace=True)
    data.to_csv(filename)
    print('finished!')


if __name__ == '__main__':
    # Obviously replace BTCUSDT with whichever symbol you want from binance
    # Wherever you've saved this code is the same directory you will find the resulting CSV file
    binanceBarExtractor('BTCUSDT')