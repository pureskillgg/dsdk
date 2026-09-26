"""Keep every test offline: no test builds a real AWS client."""

import boto3
import pytest


class UnstubbedAwsClient:
    """Returned by boto3.client in tests; a test swaps in its own stub before use."""

    def __init__(self, service_name):
        self._service_name = service_name

    def __getattr__(self, name):
        raise AssertionError(
            f"Test called {self._service_name}.{name} on an unstubbed AWS client"
        )


@pytest.fixture(autouse=True)
def offline_aws(monkeypatch):
    # boto3.client runs the credential chain on construction, which can reach
    # EC2 instance metadata when no credentials are configured.
    monkeypatch.setattr(
        boto3,
        "client",
        lambda service_name, *_args, **_kwargs: UnstubbedAwsClient(service_name),
    )
    # Anything that reaches botocore another way (aiobotocore, s3fs) gets
    # dummy credentials and no instance metadata lookup.
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.delenv("AWS_SESSION_TOKEN", raising=False)
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
