import datetime as dt
from unittest.mock import patch

from django.test import Client, TestCase
import requests

from .fetch import load_planet
from .models import Dataset


NOW = dt.datetime.now(dt.timezone.utc)


c = Client()


class FetchApiViewTestCase(TestCase):
    def test_get_runs_celery_task(self):
        with patch("swapi.api.views.fetch_dataset") as task:
            resp = c.get("/api/fetch/") 
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json()["message"], "Fetching dataset")
            task.delay.assert_called_once()
            


class DatasetViewTestCase(TestCase):
    def test_retrieve_not_found(self):
        with patch(
                 "swapi.api.views.Dataset.objects.get",
                 side_effect=Dataset.DoesNotExist
            ):
            resp = c.get("/api/datasets/1")
            data = resp.json()
            self.assertEquals(resp.status_code, 404)
            self.assertEquals(data["error"], "Dataset not found")

    def test_retrieve(self):
        dataset = Dataset(filename="foo", timestamp=NOW - dt.timedelta(days=1))
        dataset.save()
        data = {
            "id": dataset.pk,
            "filename": dataset.filename,
            "timestamp": dataset.timestamp.isoformat()
        }


        with patch("swapi.api.views.read_dataset", return_value=data):
            resp = c.get("/api/datasets/1")
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json(), data)


class ColumnsViewTestCase(TestCase):
    def setUp(self):
        Dataset.objects.create(filename="foo", timestamp=NOW - dt.timedelta(days=1))



    def test_get_not_found(self):
        with patch(
                 "swapi.api.views.Dataset.objects.get",
                 side_effect=Dataset.DoesNotExist
            ):
            resp = c.get("/api/datasets/1/columns")
            data = resp.json()
            self.assertEquals(resp.status_code, 404)
            self.assertEquals(data["error"], "Dataset not found")

    def test_get_no_columns(self):
        resp = c.get("/api/datasets/1/columns")
        self.assertEquals(resp.status_code, 400)
        self.assertEquals(resp.json()["error"], "No columns specified")

    def test_get_columns(self):
        data = {
            "foo": "bar",
            "baz": "quux"
        }

        with patch("swapi.api.views.read_and_aggregate_columns", return_value=data):
            resp = c.get("/api/datasets/1/columns", {"columns": "foo,baz"})
            self.assertEquals(resp.status_code, 200)
            self.assertEquals(resp.json(), data)


class APIFetchTestCase(TestCase):
    def test_load_planet_cached(self):
        url = "planets/1"
        cache = {url: "foo"}

        # Assert that request has not been made.
        with patch("swapi.api.fetch.s.get") as s:
            load_planet(url, cache)
            s.assert_not_called()

    def test_load_planet_caches_new_one(self):
        url = "planets/1"
        cache = {}

        resp = requests.Response()
        resp._content = b'{"name": "foo"}'

        with patch("swapi.api.fetch.s.get", return_value=resp):
            load_planet(url, cache)

        self.assertIn(url, cache)
        self.assertEquals(cache[url], "foo")
