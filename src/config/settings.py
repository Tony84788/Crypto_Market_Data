from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    coingecko_base_url: str
    coingecko_api_key: str = ""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
    )


settings = Settings()