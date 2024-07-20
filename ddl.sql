-- Define source table for market news from AWS Kinesis
CREATE TABLE market_news (
    id BIGINT,
    author VARCHAR,
    headline VARCHAR,
    source VARCHAR,
    summary VARCHAR,
    data_provider VARCHAR,
    url VARCHAR,
    symbol VARCHAR,
    sentiment DECIMAL,
    timestamp_ms BIGINT,
    event_time TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kinesis',
    'stream' = 'your-kinesis-stream-name',
    'aws.region' = 'your-aws-region',
    'scan.stream.initpos' = 'LATEST',
    'format' = 'json'
);

-- Define sink table for trading signals to AWS Kinesis
CREATE TABLE trading_signals_sink (
    symbol STRING,
    signal_time TIMESTAMP(3),
    signal STRING
) WITH (
    'connector' = 'kinesis',
    'stream' = 'your-kinesis-sink-stream-name',
    'aws.region' = 'your-aws-region',
    'format' = 'json'
);

-- Define query to process market news and generate trading signals
INSERT INTO trading_signals_sink
SELECT
    symbol,
    event_time AS signal_time,
    CASE
        WHEN sentiment > 0 AND lag(sentiment, 1) OVER (PARTITION BY symbol ORDER BY event_time) < 0 THEN 'BUY'
        WHEN sentiment < 0 AND lag(sentiment, 1) OVER (PARTITION BY symbol ORDER BY event_time) > 0 THEN 'SELL'
        ELSE 'HOLD'
    END AS signal
FROM market_news;
