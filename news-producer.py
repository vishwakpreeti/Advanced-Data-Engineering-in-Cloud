import json
from datetime import datetime
from alpaca_trade_api import REST
from alpaca_trade_api.common import URL
from kafka import KafkaProducer
import boto3

from utils import get_sentiment
from alpaca_config.keys import config

def get_producer(brokers):
    producer = KafkaProducer(
        bootstrap_servers=brokers,
        key_serializer=str.encode,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer

def produce_historical_news(kinesis_client, start_date, end_date, symbols, stream_name):
    key_id = config['key_id']
    secret_key = config['secret_key']
    base_url = config['base_url']

    api = REST(key_id=key_id, secret_key=secret_key, base_url=URL(base_url))

    for symbol in symbols:
        news = api.get_news(
            symbol=symbol,
            start=start_date,
            end=end_date,
            limit=5000,
            sort='asc',
            include_content=False,
        )

        for i, row in enumerate(news):
            article = row._raw
            should_proceed = any(term in article['headline'] for term in symbols)
            if not should_proceed:
                continue

            timestamp_ms = int(row.created_at.timestamp() * 1000)
            timestamp = datetime.fromtimestamp(row.created_at.timestamp())

            article['timestamp'] = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            article['timestamp_ms'] = timestamp_ms
            article['data_provider'] = 'alpaca'
            article['sentiment'] = get_sentiment(article['headline'])
            article.pop('symbols')
            article['symbol'] = symbol

            try:
                response = kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(article),
                    PartitionKey=symbol
                )
                print(f'Sent {i+1} articles to {stream_name}: {response}')
            except Exception as e:
                print(f'Failed to send article: {article}')
                print(e)

if __name__ == '__main__':
    kinesis_client = boto3.client('kinesis', region_name='us-east-1')  # Replace with your AWS region
    produce_historical_news(
        kinesis_client,
        start_date='2024-01-01',
        end_date='2024-06-08',
        symbols=['AAPL', 'Apple'],
        stream_name='market-news-stream'
    )
