import requests

url = "http://localhost:8080/api/run-etl/1"

try:
    response = requests.post(url)
    print("Status Code:", response.status_code)
    print("Response:", response.json())
except Exception as e:
    print("Lỗi:", e)