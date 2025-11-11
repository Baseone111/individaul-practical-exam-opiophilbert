import os
import requests
import asyncio
import aiohttp
import aiofiles
import random
import time
from typing import List, Dict, Any, Callable

# === Part A: Basic API Interaction (sync) ===
GITHUB_API = "https://api.github.com/users/octocat"
token = os.getenv("GITHUB_TOKEN")
headers = {"Accept": "application/vnd.github.v3+json"}
if token:
    headers["Authorization"] = f"token {token}"

resp = requests.get(GITHUB_API, headers=headers, timeout=10)
resp.raise_for_status()
data = resp.json()

print("=== Part A: Basic API Interaction (sync) ===")
print(f"Login / username: {data.get('login')}")
print(f"Name: {data.get('name')}")
print(f"Public repositories: {data.get('public_repos')}")
print(f"Profile URL: {data.get('html_url')}")
print()


# === Async sections ===
GITHUB_USER_URL = "https://api.github.com/users/{}"
usernames = ["octocat", "torvalds", "mojombo", "defunkt", "pjhyett"]

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
auth_headers = {"Accept": "application/vnd.github.v3+json"}
if GITHUB_TOKEN:
    auth_headers["Authorization"] = f"token {GITHUB_TOKEN}"

async def fetch_user(session: aiohttp.ClientSession, username: str) -> Dict[str, Any]:
    url = GITHUB_USER_URL.format(username)
    async with session.get(url, headers=auth_headers, timeout=10) as r:
        r.raise_for_status()
        return await r.json()

async def fetch_many_users(usernames: List[str]) -> List[Dict[str, Any]]:
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch_user(session, u)) for u in usernames]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        processed = []
        for username, res in zip(usernames, results):
            if isinstance(res, Exception):
                processed.append({"login": username, "error": str(res), "public_repos": -1})
            else:
                processed.append(res)
        return processed


# === Weather + Users Concurrent Fetch ===
WEATHER_API = "https://api.open-meteo.com/v1/forecast?latitude=0.3&longitude=32.6&current_weather=true"

async def fetch_weather(session: aiohttp.ClientSession) -> Dict[str, Any]:
    async with session.get(WEATHER_API, timeout=10) as r:
        r.raise_for_status()
        return await r.json()

async def fetch_users_and_weather(usernames):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch_user(session, u)) for u in usernames]
        weather_task = asyncio.create_task(fetch_weather(session))
        results = await asyncio.gather(*tasks, weather_task, return_exceptions=True)
        weather_res = results[-1]
        user_results = results[:-1]
        processed_users = []
        for username, res in zip(usernames, user_results):
            if isinstance(res, Exception):
                processed_users.append({"login": username, "error": str(res), "public_repos": -1})
            else:
                processed_users.append(res)
        processed_weather = weather_res if not isinstance(weather_res, Exception) else {"error": str(weather_res)}
        return processed_users, processed_weather


# === Async Retry + Logging ===
MAX_RETRIES = 4
BASE_BACKOFF = 0.5

async def async_request_with_retries(session: aiohttp.ClientSession, method: str, url: str, **kwargs):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with session.request(method, url, **kwargs) as resp:
                if 500 <= resp.status < 600:
                    text = await resp.text()
                    raise aiohttp.ClientResponseError(
                        resp.request_info, resp.history,
                        status=resp.status, message=f"Server error: {text}", headers=resp.headers
                    )
                resp.raise_for_status()
                return await resp.json(), None
        except Exception as e:
            if attempt == MAX_RETRIES:
                return None, e
            backoff = BASE_BACKOFF * (2 ** (attempt - 1))
            jitter = random.uniform(0, backoff * 0.3)
            await asyncio.sleep(backoff + jitter)
    return None, Exception("Unknown retry failure")

async def fetch_many_with_retry(usernames, logfile="async_results.log"):
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(async_request_with_retries(session, "GET", GITHUB_USER_URL.format(u),
                                                                headers=auth_headers, timeout=10)) for u in usernames]
        results = await asyncio.gather(*tasks)
        async with aiofiles.open(logfile, mode="a") as f:
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
            await f.write(f"--- Fetch batch at {ts} ---\n")
            for username, (res_json, err) in zip(usernames, results):
                if err:
                    await f.write(f"{username} -> ERROR: {err}\n")
                else:
                    await f.write(f"{username} -> OK: public_repos={res_json.get('public_repos')}, name={res_json.get('name')}\n")
        return results


# === Run everything inside async main ===
async def main():
    print("=== Part B: Async concurrent GitHub fetch ===")
    users = await fetch_many_users(usernames)
    users_sorted = sorted(users, key=lambda u: u.get("public_repos", -1), reverse=True)
    for u in users_sorted:
        if u.get("error"):
            print(f"{u.get('login')}: ERROR -> {u.get('error')}")
        else:
            print(f"{u.get('login')} | name: {u.get('name')} | public_repos: {u.get('public_repos')} | url: {u.get('html_url')}")
    print()

    print("=== Part C: Concurrent GitHub + Weather ===")
    users_c, weather = await fetch_users_and_weather(usernames)
    valid_users = [u for u in users_c if not u.get("error")]
    top_user = max(valid_users, key=lambda u: u.get("public_repos", -1)) if valid_users else None
    print("Top GitHub user (by public_repos):")
    if top_user:
        print(f"  {top_user.get('login')} ({top_user.get('name')}) - public_repos: {top_user.get('public_repos')}")
        print(f"  Profile: {top_user.get('html_url')}")
    else:
        print("  No valid GitHub user data.")

    print("\nWeather (current):")
    if weather.get("error"):
        print("  Weather fetch error:", weather.get("error"))
    else:
        cw = weather.get("current_weather")
        if cw:
            print(f"  Temperature: {cw.get('temperature')} °C")
            print(f"  Wind speed: {cw.get('windspeed')} m/s")
            print(f"  Wind direction: {cw.get('winddirection')}°")
            print(f"  Time: {cw.get('time')}")
        else:
            print("  No current_weather in response.")
    print()

    print("=== Part D: Async retry + async logging ===")
    await fetch_many_with_retry(usernames, logfile="async_results.log")
    print("Completed. Logs appended to 'async_results.log'.")


# 🚀 Run the main async function
if __name__ == "__main__":
    asyncio.run(main())
