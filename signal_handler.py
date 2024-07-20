import json
import boto3
import requests
from alpaca_trade_api import REST
from alpaca_trade_api.common import URL
from alpaca.common import Sort
from alpaca_config.keys import config

api = REST(key_id=config['key_id'], secret_key=config['secret_key'], base_url=URL(config['base_url']))


def send_to_slack(message, token, channel_id):
    url = 'https://slack.com/api/chat.postMessage'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    data = {
        'channel': channel_id,
        'text': message
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code != 200:
        raise ValueError(f'Failed to send message to Slack, {response.status_code}, response: {response.text}')


def place_order(symbol, qty, side, order_type, time_in_force):
    try:
        order = api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type=order_type,
            time_in_force=time_in_force
        )
        print(f'Order submitted: {order}')
        return order
    except Exception as e:
        print(f'An error occurred while submitting order {e}')
        return None


def lambda_handler(event, context):
    for record in event['Records']:
        payload = json.loads(record['kinesis']['data'])

        symbol = payload['symbol']
        sentiment = payload['sentiment']
        event_time = payload['event_time']

        if sentiment > 0:
            signal = 'BUY'
        elif sentiment < 0:
            signal = 'SELL'
        else:
            signal = 'HOLD'

        formatted_message = f"""
        =============================
        ALERT ⚠️ New Trading Signal!\n
        Symbol: {symbol} \n
        Signal: {signal} \n
        Time: {event_time}
        =============================
        """

        send_to_slack(formatted_message, config['slack_token'], config['slack_channel_id'])

        # Place order based on signal
        if signal in ['BUY', 'SELL']:
            place_order(symbol=symbol, qty=5, side=signal.lower(), order_type='market', time_in_force='gtc')

    return {
        'statusCode': 200,
        'body': json.dumps('Signals processed successfully')
    }