
import json
import boto3
import urllib.request
from datetime import datetime
import json
import time
def api_to_kinesis(event, context):
    with open("./env.json", 'r') as f:
        all_configs = json.load(f)
        config = all_configs['CallApiFunction']
    api_key = config.get("OPENWEATHER_API_KEY")
    base_url = config.get("OPENWEATHER_BASE_URL", "https://api.openweathermap.org/data/2.5/weather")
    lat = config.get("LAT")
    lon = config.get("LON")
    units = config.get("UNITS", "metric")
    region = config.get("AWS_REGION")
    stream_name = config.get("KINESIS_STREAM_NAME")
    kinesis = boto3.client('kinesis', region_name=region)
    
    url = f"{base_url}?lat={lat}&lon={lon}&units={units}&appid={api_key}"
    
    try:
        with urllib.request.urlopen(url) as  response:
            api_data = json.loads(response.read().decode())
            api_data['timestamp'] = int(time.time() * 1000)
            print(f"API Data Pulled Successfully: {api_data}")
    except Exception as e:
        print(f"❌ Lỗi khi gọi API: {e}")
        raise e
    try:
        
        payload = json.dumps(api_data)
        partition_key = str(api_data['timestamp'])
        response = kinesis.put_record(
            StreamName=stream_name,
            Data=payload,
            PartitionKey=partition_key
        )
        print("✅ Đẩy vào Kinesis thành công.", api_data)
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data pushed to Kinesis", "data": payload})
        }
    except Exception as e:
        print(f"❌ Lỗi khi đẩy vào Kinesis: {e}")
        raise e
def main():
    api_to_kinesis({}, {})
    

if __name__ == "__main__":
    main()