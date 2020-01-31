import pytest

def pytest_configure(config):
    config.addinivalue_line("markers", "api_job: this one is for tests with jobs.")
    config.addinivalue_line("markers", "case_a: this is case_a.")
    config.addinivalue_line("markers", "case_b: this is case_b")
    config.addinivalue_line("markers", "case_c: this is case_cs")
