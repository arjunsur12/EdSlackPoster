import os, requests

ED_API = "https://us.edstem.org/api"
TOKEN = os.environ["ED_API_TOKEN"]
COURSE_ID = 81634

headers = {"Authorization": f"Bearer {TOKEN}"}

candidates = [
    f"{ED_API}/courses/{COURSE_ID}/discussions",
    f"{ED_API}/courses/{COURSE_ID}/discussion",
    f"{ED_API}/courses/{COURSE_ID}/threads",
    f"{ED_API}/courses/{COURSE_ID}/posts",
    f"{ED_API}/courses/{COURSE_ID}/feed",
    f"{ED_API}/courses/{COURSE_ID}/activity",
]

for url in candidates:
    r = requests.get(url, headers=headers, timeout=20)
    print(url, "->", r.status_code)
    if r.status_code != 404:
        print((r.text or "")[:300])
        print("-----")