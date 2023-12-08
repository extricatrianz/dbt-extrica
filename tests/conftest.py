import os

import pytest
import trino
from trino.auth import JWTAuthentication

# Import the functional fixtures as a plugin
# Note: fixtures with session scope need to be local

pytest_plugins = ["dbt.tests.fixtures.project"]


def pytest_addoption(parser):
    parser.addoption("--profile", action="store", default="dbt_extrica", type=str)


# The profile dictionary, used to write out profiles.yml
@pytest.fixture(scope="class")
def dbt_profile_target(request):
    profile_type = request.config.getoption("--profile")
    if profile_type == "dbt_extrica":
        target = get_extrica_profile()
    else:
        raise ValueError(f"Invalid profile type '{profile_type}'")

    prepared_statements_disabled = request.node.get_closest_marker("prepared_statements_disabled")
    if prepared_statements_disabled:
        target.update({"prepared_statements_enabled": False})

    return target


def get_extrica_profile():
    return {
        "type": "extrica",
        "method": "jwt",
        "threads": 0,
        "host": "",
        "port": 0,
        "catalog": "",
        "schema": "",
        "username": "",
        "password": ""
    }

@pytest.fixture(autouse=True)
def skip_by_profile_type(request):
    profile_type = request.config.getoption("--profile")
    if request.node.get_closest_marker("skip_profile"):
        for skip_profile_type in request.node.get_closest_marker("skip_profile").args:
            if skip_profile_type == profile_type:
                pytest.skip(f"skipped on {profile_type} profile")


@pytest.fixture(scope="class")
def trino_connection(dbt_profile_target):    
    return trino.dbapi.connect(
        host=dbt_profile_target["host"],
        port=dbt_profile_target["port"],
        auth=JWTAuthentication("<jwt_token>"),
        catalog=dbt_profile_target["catalog"],
        schema=dbt_profile_target["schema"],
    )


def get_engine_type():
    conn = trino.dbapi.connect(host="localhost", port=8080, user="dbt-extrica")
    cur = conn.cursor()
    cur.execute("SELECT version()")
    version = cur.fetchone()
    if "-e" in version[0]:
        return "starburst"
    else:
        return "trino"
