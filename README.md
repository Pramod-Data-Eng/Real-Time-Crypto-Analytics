# Real-Time-Crypto-Analytics
AWS End to End Data Pipeline to analyze Global Crypto Market Pulse. 

The data flow is as follows:

Source Data (Public API) --> AWS Lambda --> Kinesis Firehose --> S3 --> AWS Glue --> S3 --> Athena --> S3

AWS Step Function is used for Orchestration 
