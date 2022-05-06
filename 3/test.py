import requests
data = {
    "table": "SITES"
}

r = requests.post("http://10.3.5.211:5000/get-data", data=data)
print(r.text)