import json
from datetime import datetime
from alpaca.data import StockHistoricalDataClient, StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from kafka import KafkaProducer
import boto3

from alpaca_config.keys import config

def get_producer(brokers):
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def produce_historical_price(kinesis_client, start_date, end_date, symbol, stream_name):
    api = StockHistoricalDataClient(api_key=config['key_id'], secret_key=config['secret_key'])

    start_date = datetime.strptime(start_date, '%Y-%m-%d')
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    granularity = TimeFrame.Minute

    request_params = StockBarsRequest(
        symbol_or_symbols=symbol,
        timeframe=granularity,
        start=start_date,
        end=end_date
    )

    prices_df = api.get_stock_bars(request_params).df
    prices_df.reset_index(inplace=True)

    records = json.loads(prices_df.to_json(orient='records'))
    for idx, record in enumerate(records):
        record['provider'] = 'alpaca'

        try:
            response = kinesis_client.put_record(
                StreamName=stream_name,
                Data=json.dumps(record),
                PartitionKey=symbol
            )
            print(f'Record sent successfully to {stream_name}: {response}')
        except Exception as e:
            print(f'Error sending message for symbol {symbol}: {e.__class__.__name__} - {e}')

if __name__ == '__main__':
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')  # Replace with your AWS region
    produce_historical_price(
        kinesis_client,
        start_date='2024-01-01',
        end_date='2024-06-08',
        symbol='AAPL',
        stream_name='stock-prices-stream'
    )
