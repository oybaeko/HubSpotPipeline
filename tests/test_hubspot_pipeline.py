def test_fetch_companies_is_callable():
    import hubspot_pipeline
    # Basic smoke-test: the top-level fetch_companies function must exist and be callable
    assert hasattr(hubspot_pipeline, "fetch_companies")
    assert callable(hubspot_pipeline.fetch_companies)

