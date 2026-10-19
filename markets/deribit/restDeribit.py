from core.restClient import RestClient

class RestDeribit(RestClient):
    BASE_URL = "https://www.deribit.com/api/v2/public"

    @classmethod
    async def fetch_contracts(cls, currency: str = "BTC") -> list[dict] | None:
        url = f"{cls.BASE_URL}/get_instruments?currency={currency}&kind=option&expired=false"
        data = await cls.get(url)
        return data.get("result") if data else None

    @classmethod
    async def fetch_book_summary(cls, currency: str = "BTC") -> list[dict] | None:
        url = f"{cls.BASE_URL}/get_book_summary_by_currency?currency={currency}&kind=option"
        data = await cls.get(url)
        return data.get("result") if data else None