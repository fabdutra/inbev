from bees_breweries.config.settings import get_settings


def test_default_settings_have_expected_paths() -> None:
    settings = get_settings()

    assert settings.api_per_page == 200
    assert settings.api_base_url.endswith("/v1")
    assert settings.bronze_root.name == "bronze"
