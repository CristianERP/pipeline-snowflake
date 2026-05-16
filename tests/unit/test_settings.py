import importlib

from meetup_pipeline.config import settings as settings_module


def test_default_settings_are_available():
    assert settings_module.settings.s3.bucket
    assert settings_module.settings.s3.source_prefix == "landing/"
    assert settings_module.settings.s3.normalized_prefix == "landing_normalized/"
    assert settings_module.settings.s3.incremental_prefix == "incremental/events/"


def test_settings_can_be_overridden_with_environment_variables(monkeypatch):
    monkeypatch.setenv("MEETUP_S3_BUCKET", "test-bucket")

    reloaded_settings_module = importlib.reload(settings_module)

    assert reloaded_settings_module.settings.s3.bucket == "test-bucket"