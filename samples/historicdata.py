import modin.pandas as pd
from binance.client import Client
import datetime
from s3fs.core import S3FileSystem


# s3fs client
s3 = S3FileSystem(
    anon=False,
    key='access-key',
    secret='secret-key',
    use_ssl=False,
    client_kwargs={'endpoint_url': 'http://10.152.183.240:9000'})


# Binance client
with s3.open('repo/binance/binance_cred.txt', 'r') as f:
    cred = json.loads(f.read())
api_key = cred["API_KEY"]
api_secret = cred["SECRET_KEY"]
client = Client(api_key=api_key, api_secret=api_secret)


start_date = datetime.datetime.strptime('1 Jan 2016', '%d %b %Y')
today = datetime.datetime.today()

def binanceBarExtractor(symbol):
    print('working...')
    filename = '{}_MinuteBars.csv'.format(symbol)

    klines = client.get_historical_klines(symbol, Client.KLINE_INTERVAL_1MINUTE, start_date.strftime("%d %b %Y %H:%M:%S"), today.strftime("%d %b %Y %H:%M:%S"), 1000)
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