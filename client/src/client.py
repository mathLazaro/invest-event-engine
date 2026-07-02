import uuid
import asyncio
import json
import nats
import os

from nats.js.client import JetStreamContext

from model import SECTORS, TICKERS


def choose_from(label: str, options: list[str]) -> str:
    print(f"\n{label}:")
    for i, opt in enumerate(options):
        print(f"  [{i}] {opt}")
    while True:
        try:
            idx = int(input("Escolha: "))
            if 0 <= idx < len(options):
                return options[idx]
        except ValueError:
            pass
        print("Opção inválida, tente novamente.")


def ask_price(label: str) -> float | None:
    val = input(f"{label} (deixe em branco para ignorar): ").strip()
    if not val:
        return None
    try:
        return float(val)
    except ValueError:
        print("Valor inválido, ignorando.")
        return None


def build_subscription(user_id: str) -> dict:
    print("\n=== Configurar Subscrição ===")

    sector = choose_from("Setor", SECTORS)
    ticker = choose_from("Ticker", TICKERS)
    higher_than = ask_price("Preço mínimo (higherThan)")
    smaller_than = ask_price("Preço máximo (smallerThan)")

    return {
        "user_id": user_id,
        "sector": sector,
        "ticker": ticker,
        "higher_than": higher_than,
        "smaller_than": smaller_than,
    }


async def listen_notifications(nc, user_id: str):
    subject = f"user.{user_id}.notifications"
    print(f"\nEscutando notificações em: {subject}\n")

    async def handler(msg):
        data = json.loads(msg.data.decode())
        print(f"\nNotificação recebida:")
        print(json.dumps(data, indent=2))

    await nc.subscribe(subject, cb=handler)


async def main():
    NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
    user_id = str(uuid.uuid4())
    print(f"Seu userId: {user_id}")

    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()

    await listen_notifications(nc, user_id)

    while True:
        print("\n=== Menu ===")
        print("  [1] Criar/atualizar subscrição")
        print("  [2] Sair")
        choice = input("Opção: ").strip()

        if choice == "1":
            subscription = build_subscription(user_id)
            await js.publish("subscription", json.dumps(subscription).encode())
            print("\Subscrição publicada:")
            print(json.dumps(subscription, indent=2))

        elif choice == "2":
            print("Encerrando...")
            await nc.close()
            break

        else:
            print("Opção inválida.")


asyncio.run(main())
