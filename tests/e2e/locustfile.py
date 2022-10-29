import json
import random

import requests

from locust import HttpUser, task

word_site = "https://www.mit.edu/~ecprice/wordlist.10000"

response = requests.get(word_site)
WORDS = response.content.splitlines()


class User(HttpUser):
    def on_start(self):
        """on_start is called when a Locust start before any task is scheduled"""
        self.client.verify = False


class Get(User):
    @task
    def get(self):
        key = random.choice(WORDS)
        self.client.get(f"/get?table=default&key={str(key)}")


class Set(User):
    @task
    def set(self):
        key = random.choice(WORDS)
        value = random.choice(WORDS)
        self.client.post(
            "/set",
            json=dict(table="default", key=str(key), value=str(value)),
            headers={"Content-Type": "application/json"},
        )
