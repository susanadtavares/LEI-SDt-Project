import requests

url = "http://localhost:5000/search"
body = {
    "prompt": "receita panqueca",
    "top_k": 3
}

r = requests.post(url, json=body)
print(r.status_code, r.json())
