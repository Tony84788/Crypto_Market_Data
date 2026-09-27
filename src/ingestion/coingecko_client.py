import requests

from src.config.settings import settings


class CoinGeckoClient:
    """Client for communicating with the CoinGecko API."""

    def __init__(self):
        self.base_url = settings.coingecko_base_url
        self.api_key = settings.coingecko_api_key

    def get_market_data(
        self,
        vs_currency: str = "usd",
        coin_ids: list[str] | None = None,
    ) -> list[dict]:

        endpoint = f"{self.base_url}/coins/markets"

        params = {
            "vs_currency": vs_currency,
        }

        if coin_ids:
            params["ids"] = ",".join(coin_ids)

        headers = {}

        if self.api_key:
            headers["x-cg-demo-api-key"] = self.api_key

        response = requests.get(
            endpoint,
            params=params,
            headers=headers,
            timeout=30,
        )

        response.raise_for_status()

        return response.json()