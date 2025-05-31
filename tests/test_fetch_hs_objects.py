# tests/test_fetch_hs_objects.py

import os
import importlib
import pytest
import logging

# We'll import the module as `fh` so we can reload it after setting env
import hubspot_pipeline.fetch_hubspot_data as fh


class DummyResponse:
    def __init__(self, json_data, status_code):
        self._json = json_data
        self.status_code = status_code
        # .text used in logging.error
        self.text = json_data.get("message", "")

    def json(self):
        return self._json


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    # Ensure the module picks up our test key
    monkeypatch.setenv("HUBSPOT_API_KEY", "test-key")
    importlib.reload(fh)
    return fh


def test_invalid_object_type(set_api_key):
    with pytest.raises(ValueError):
        fh.fetch_hs_objects("not_a_valid_type")


def test_list_single_page(set_api_key, monkeypatch):
    # Simulate one page of 3 results
    dummy = {"results": [{"id": "A"}, {"id": "B"}, {"id": "C"}]}
    def fake_get(url, headers, params):
        # URL should match the 'list' endpoint for companies
        assert url == fh.ENDPOINTS["companies"]["list"]
        # Default limit capped to <=100
        assert params["limit"] <= 100
        # Default properties come from FIELD_MAPS
        expected_props = ",".join(fh.FIELD_MAPS["companies"].values())
        assert params["properties"] == expected_props
        return DummyResponse(dummy, 200)

    monkeypatch.setattr(fh.requests, "get", fake_get)

    results, calls = fh.fetch_hs_objects("companies", limit=3)
    assert calls == 1
    assert isinstance(results, list)
    assert [r["id"] for r in results] == ["A", "B", "C"]


def test_list_multiple_pages(set_api_key, monkeypatch):
    # Two pages: first has next token, second ends
    pages = [
        {"results": [{"id": "X"}], "paging": {"next": {"after": "tok1"}}},
        {"results": [{"id": "Y"}], "paging": {}},
    ]
    def fake_get(url, headers, params):
        return DummyResponse(pages.pop(0), 200)

    monkeypatch.setattr(fh.requests, "get", fake_get)

    results, calls = fh.fetch_hs_objects("deals")
    assert calls == 2
    assert [r["id"] for r in results] == ["X", "Y"]

def test_search_endpoint(set_api_key, monkeypatch):
    # Simulate search with a filter
    filters = [{"propertyName": "name", "operator": "EQ", "value": "TestCo"}]
    dummy = {"results": [{"id": "Z"}]}

    def fake_post(url, headers, json):
        assert url == fh.ENDPOINTS["companies"]["search"]
        # filterGroups should wrap our filters
        assert json["filterGroups"][0]["filters"] == filters
        # properties as list
        assert isinstance(json["properties"], list)
        return DummyResponse(dummy, 200)

    monkeypatch.setattr(fh.requests, "post", fake_post)

    results, calls = fh.fetch_hs_objects("companies", filters=filters, limit=1)
    assert calls == 1
    assert results == dummy["results"]


def test_custom_properties_and_limit(set_api_key, monkeypatch):
    # If properties is provided, it should use them instead of defaults
    custom_props = ["foo", "bar"]
    dummy = {"results": [{"id": "1"}, {"id": "2"}, {"id": "3"}]}

    def fake_get(url, headers, params):
        # Ensure our custom props are joined
        assert params["properties"] == "foo,bar"
        return DummyResponse(dummy, 200)

    monkeypatch.setattr(fh.requests, "get", fake_get)

    results, calls = fh.fetch_hs_objects(
        "companies",
        limit=2,
        properties=custom_props
    )
    # Should enforce limit of 2
    assert calls == 1
    assert len(results) == 2


def test_list_error_handling(set_api_key, monkeypatch, caplog):
    # Simulate an HTTP 500 on list
    def fake_get(url, headers, params):
        return DummyResponse({"message": "Server Error"}, 500)

    monkeypatch.setattr(fh.requests, "get", fake_get)
    caplog.set_level(logging.ERROR)

    results, calls = fh.fetch_hs_objects("contacts")
    assert calls == 1
    # on error, results is empty
    assert results == []
    assert "HubSpot contacts list error" in caplog.text


def test_search_error_handling(set_api_key, monkeypatch, caplog):
    # Simulate an HTTP 404 on search
    def fake_post(url, headers, json):
        return DummyResponse({"message": "Not Found"}, 404)

    monkeypatch.setattr(fh.requests, "post", fake_post)
    caplog.set_level(logging.ERROR)

    results, calls = fh.fetch_hs_objects("deals", filters=[{}])
    assert calls == 1
    assert results == []
    assert "HubSpot deals search error" in caplog.text
