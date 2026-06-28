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
        dict: { ticker: str, price: float, sector: str, event: 'INVEST' | 'CONFIG'}
    """

    ticker = invest.get("id")
    price = invest.get("price")
    sector = TICKER_SECTOR_MAP.get(ticker, "DESCONHECIDO")

    if price is None or ticker is None:
        raise RuntimeError("Invalid arguments")

    return {"ticker": ticker, "price": price, "sector": sector, "event": "INVEST"}


async def handle_message(invest_info: dict, js: JetStreamContext):
    """(Async) Lida com o recebimento das mensagens da API: mapea para o padrão de evento esperado e publica no tópico 'market.events'

    Args:
        invest_info (dict): Dicionário com as informações do recurso de investimento
        js (JetStreamContext): Referência da conexão com o JetStream

    """
    try:
        payload = json.dumps(map_to_event(invest_info))
        print(f"Envio: {payload}")
        await js.publish("market.events", payload.encode())
    except RuntimeError:
        print("Ticker com valores inválidos")


async def create_stream(js: JetStreamContext):
    try:
        await js.find_stream_name_by_subject("market.events")
    except nats.js.errors.NotFoundError:
        await js.add_stream(name="market-events", subjects=["market.events"])


async def main():
    NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()

    await create_stream(js)

    ws = yf.AsyncWebSocket()

    await ws.subscribe(list(TICKERS))
    await ws.listen(lambda info: asyncio.ensure_future(handle_message(info, js)))


asyncio.run(main())
