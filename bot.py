import requests

ED_API = "https://us.edstem.org/api"
TOKEN = "3_hHeV.wyFNkToVijUizA28jgxKDS3UlQPN7sqNExiTKcvH"

headers = {
    "Authorization": f"Bearer {TOKEN}"
}

r = requests.get(f"{ED_API}/user", headers=headers)
data = r.json()

for c in data["courses"]:
    print(c["course"]["id"], c["course"]["name"], c["role"])