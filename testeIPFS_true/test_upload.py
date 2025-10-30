import requests

# Upload de ficheiro
with open('teste.txt', 'rb') as f:
    files = {'file': f}
    response = requests.post('http://localhost:5000/upload', files=files)
    print(response.json())
