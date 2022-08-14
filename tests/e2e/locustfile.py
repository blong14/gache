from locust import HttpUser, task


class User(HttpUser):
    def on_start(self):
        """on_start is called when a Locust start before any task is scheduled"""
        self.client.verify = False


class Get(User):
    @task
    def get(self):
        self.client.get("/get?table=default&key=foo")


class Set(User):
    @task
    def set(self):
        self.client.post("/set", data=dict(table="default", key="foo", value="bar"))

