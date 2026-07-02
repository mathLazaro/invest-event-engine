from decimal import ROUND_HALF_UP, Decimal
import os
import asyncio
import json
import nats
import yfinance as yf

from model import TICKERS, TICKER_SECTOR_MAP
from nats.js.client import JetStreamContext


def map_to_event(invest: dict) -> dict:
    """Faz o mapeamento das informações de investimento para um dto padronizado

    Args:
        invest (dict): Dicionário com os dados do recurso de investimento

    Returns:
        dict: { ticker: str, price: float, sector: str, event_type: 'INVEST' | 'CONFIG', timestamp: int}
    """

    if invest.get("price") is None or invest.get("id") is None:
        raise RuntimeError("Invalid arguments")

    ticker = invest.get("id").replace("-", "_").replace(".", "_")
    price = str(
        Decimal(invest.get("price")).quantize(
            Decimal("0.00000001"), rounding=ROUND_HALF_UP
        )
    )
    sector = TICKER_SECTOR_MAP.get(ticker, "UNKNOWN")
    timestamp = invest.get("time")

    return {
        "ticker": ticker,
        "price": price,
        "sector": sector,
        "timestamp": timestamp,
    }


async def handle_message(invest_info: dict, js: JetStreamContext):
    """(Async) Lida com o recebimento das mensagens da API: mapea para o padrão de evento esperado e publica no tópico 'market.events'

    Args:
        invest_info (dict): Dicionário com as informações do recurso de investimento
        js (JetStreamContext): Referência da conexão com o JetStream

    """
    try:
        payload = json.dumps(map_to_event(invest_info))
        print(f"market.events: {payload}")
        await js.publish("market.events", payload.encode())
    except RuntimeError:
        print("Ticker com valores inválidos")


async def create_stream(js: JetStreamContext):
    try:
        await js.find_stream_name_by_subject("market.events")
    except nats.js.errors.NotFoundError:
        await js.add_stream(name="market-events", subjects=["market.events"])
        
    try:
        await js.find_stream_name_by_subject("subscription")
    except nats.js.errors.NotFoundError:
        await js.add_stream(name="subscription", subjects=["subscription"])


async def main():
    NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()

    await create_stream(js)

    ws = yf.AsyncWebSocket()

    await ws.subscribe(list(TICKERS))
    await ws.listen(lambda info: asyncio.ensure_future(handle_message(info, js)))


asyncio.run(main())
