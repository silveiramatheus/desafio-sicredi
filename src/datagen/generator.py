import csv
import itertools
import json
from collections import defaultdict
from datetime import date, datetime, time, timedelta
from pathlib import Path

from faker import Faker

from src.config import landing_file_name

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

COLUMNS = {
    "clientes": ["id_cliente", "nome", "cpf", "data_nascimento", "cidade", "estado", "data_atualizacao"],
    "contas": ["id_conta", "id_cliente", "tipo_conta", "status_conta", "data_abertura"],
    "cartoes": ["id_cartao", "id_conta", "bandeira", "tipo_cartao", "status_cartao", "data_emissao", "data_atualizacao"],
    "transacoes_cartao": ["id_transacao", "id_cartao", "data_transacao", "valor", "nome_estabelecimento", "mcc", "cidade_estabelecimento"],
    "estornos": ["id_estorno", "id_transacao", "data_estorno", "motivo_estorno"],
}

DEFAULT_NOISE_RATES = {
    "clientes": 0.10,
    "contas": 0.10,
    "cartoes": 0.05,
    "transacoes_cartao": 0.05,
    "estornos": 0.20,
}

ACCOUNT_TYPES = ("CORRENTE", "POUPANCA", "SALARIO")
ACCOUNT_STATUSES = ("ATIVA", "BLOQUEADA", "ENCERRADA")
INVALID_ACCOUNT_STATUSES = ("SUSPENSA", "PENDENTE", "")
CARD_BRANDS = ("VISA", "MASTERCARD", "ELO")
CARD_TYPES = ("CREDITO", "DEBITO", "MULTIPLO")
CARD_STATUSES = ("ATIVO", "BLOQUEADO", "CANCELADO")
CHARGEBACK_REASONS = ("FRAUDE", "DESACORDO_COMERCIAL", "DUPLICIDADE", "PRODUTO_NAO_RECEBIDO")
INVALID_DATES = ("2026-02-30", "31/13/2026", "data_invalida", "")
MCC_CATEGORIES = {
    "5411": "Supermercado",
    "5812": "Restaurante",
    "5541": "Posto",
    "5912": "Farmacia",
    "5311": "Magazine",
    "4121": "Transporte",
    "5732": "Eletronicos",
    "7832": "Cinema",
}

ACCOUNT_STATUS_WEIGHTS = (90, 6, 4)
CARD_STATUS_WEIGHTS = (90, 5, 5)
ONLY_ACTIVE = (1, 0, 0)
CARD_STATUS_BY_ACCOUNT_STATUS = {"BLOQUEADA": "BLOQUEADO", "ENCERRADA": "CANCELADO"}
LATEST_BIRTH_DATE = date(2004, 1, 1)

ORPHAN_ID_BASE = 900_000
NEW_CUSTOMERS_PER_DAY = 15
CUSTOMER_UPDATES_PER_DAY = 15
ACCOUNT_STATUS_CHANGES_PER_DAY = 10
CARD_CANCELLATIONS_PER_DAY = 10
LATE_TRANSACTIONS_PER_DAY = 10
CHARGEBACKS_PER_DAY = 8


def generate_landing(
    output_path: Path | str,
    start_date: date = date(2026, 10, 1),
    days: int = 3,
    history_days: int = 61,
    num_customers: int = 150,
    transactions_per_day: int = 40,
    noise_rates: dict[str, float] | None = None,
    seed: int = 42,
) -> list[Path]:
    fake = Faker("pt_BR")
    fake.seed_instance(seed)
    ids = defaultdict(lambda: itertools.count(1))
    rates = {**DEFAULT_NOISE_RATES, **(noise_rates or {})}

    merchants = build_merchants(fake, 30)
    day_before_start = start_date - timedelta(days=1)
    customers = build_customers(fake, ids, num_customers, start_date - timedelta(days=365), day_before_start)
    accounts = build_accounts(fake, ids, customers, start_date - timedelta(days=1500), day_before_start)
    cards = build_cards(fake, ids, accounts, day_before_start)
    all_transactions = []
    folders = []

    for offset in range(days):
        reference_date = start_date + timedelta(days=offset)
        if offset == 0:
            history = date_range(reference_date - timedelta(days=history_days), reference_date)
            clean = {
                "clientes": list(customers),
                "contas": list(accounts),
                "cartoes": list(cards),
                "transacoes_cartao": build_transactions(fake, ids, cards, merchants, history, transactions_per_day),
            }
        else:
            updated_customers = update_customers(fake, customers, CUSTOMER_UPDATES_PER_DAY, reference_date)
            updated_accounts, cards_of_updated_accounts = update_accounts(
                fake, accounts, cards, ACCOUNT_STATUS_CHANGES_PER_DAY, reference_date
            )
            cancelled_cards = cancel_cards(fake, cards, CARD_CANCELLATIONS_PER_DAY, reference_date)
            new_customers, new_accounts, new_cards = onboard_customers(
                fake, ids, NEW_CUSTOMERS_PER_DAY, reference_date, customers, accounts, cards
            )
            late_days = [reference_date - timedelta(days=fake.random.randint(1, 3)) for _ in range(LATE_TRANSACTIONS_PER_DAY)]
            clean = {
                "clientes": updated_customers + new_customers,
                "contas": updated_accounts + new_accounts,
                "cartoes": cards_of_updated_accounts + cancelled_cards + new_cards,
                "transacoes_cartao": (
                    build_transactions(fake, ids, cards, merchants, [reference_date], transactions_per_day)
                    + build_transactions(fake, ids, cards, merchants, late_days, 1)
                ),
            }
        all_transactions += clean["transacoes_cartao"]
        clean["estornos"] = build_chargebacks(fake, ids, all_transactions, CHARGEBACKS_PER_DAY, reference_date)

        noise = {entity: add_noise(fake, ids, entity, rows, rates[entity]) for entity, rows in clean.items()}
        folders.append(write_day(fake, output_path, reference_date, clean, noise))

    return folders


def build_merchants(fake: Faker, count: int) -> list[dict]:
    merchants = {}
    while len(merchants) < count:
        mcc, category = fake.random.choice(list(MCC_CATEGORIES.items()))
        name = f"{category} {fake.last_name()}"
        if name not in merchants:
            merchants[name] = {"nome_estabelecimento": name, "mcc": mcc, "cidade_estabelecimento": fake.city()}
    return list(merchants.values())


def build_customers(fake: Faker, ids: dict, count: int, first_day: date, last_day: date) -> list[dict]:
    customers = []
    for _ in range(count):
        updated_on = fake.date_between_dates(first_day, last_day)
        customers.append({
            "id_cliente": next(ids["clientes"]),
            "nome": fake.name(),
            "cpf": fake.cpf(),
            "data_nascimento": fake.date_between_dates(date(1950, 1, 1), LATEST_BIRTH_DATE).isoformat(),
            "cidade": fake.city(),
            "estado": fake.estado_sigla(),
            "data_atualizacao": random_timestamp(fake, updated_on),
        })
    return customers


def build_accounts(
    fake: Faker,
    ids: dict,
    customers: list[dict],
    first_day: date,
    last_day: date,
    status_weights: tuple[int, ...] = ACCOUNT_STATUS_WEIGHTS,
) -> list[dict]:
    accounts = []
    for customer in customers:
        for _ in range(fake.random.choice((1, 1, 2))):
            opened_on = fake.date_between_dates(first_day, last_day)
            accounts.append({
                "id_conta": next(ids["contas"]),
                "id_cliente": customer["id_cliente"],
                "tipo_conta": fake.random.choice(ACCOUNT_TYPES),
                "status_conta": fake.random.choices(ACCOUNT_STATUSES, weights=status_weights)[0],
                "data_abertura": opened_on.isoformat(),
            })
    return accounts


def card_status(fake: Faker, account_status: str, status_weights: tuple[int, ...]) -> str:
    if account_status in CARD_STATUS_BY_ACCOUNT_STATUS:
        return CARD_STATUS_BY_ACCOUNT_STATUS[account_status]
    return fake.random.choices(CARD_STATUSES, weights=status_weights)[0]


def build_cards(
    fake: Faker,
    ids: dict,
    accounts: list[dict],
    last_day: date,
    status_weights: tuple[int, ...] = CARD_STATUS_WEIGHTS,
) -> list[dict]:
    cards = []
    for account in accounts:
        for _ in range(fake.random.choice((1, 1, 2))):
            issued_on = fake.date_between_dates(date.fromisoformat(account["data_abertura"]), last_day)
            updated_on = fake.date_between_dates(issued_on, last_day)
            cards.append({
                "id_cartao": next(ids["cartoes"]),
                "id_conta": account["id_conta"],
                "bandeira": fake.random.choice(CARD_BRANDS),
                "tipo_cartao": fake.random.choice(CARD_TYPES),
                "status_cartao": card_status(fake, account["status_conta"], status_weights),
                "data_emissao": issued_on.isoformat(),
                "data_atualizacao": random_timestamp(fake, updated_on),
            })
    return cards


def build_transactions(fake: Faker, ids: dict, cards: list[dict], merchants: list[dict], days: list[date], per_day: int) -> list[dict]:
    active_cards = [card for card in cards if card["status_cartao"] == "ATIVO"]
    transactions = []
    for day in days:
        issued_cards = [card for card in active_cards if card["data_emissao"] <= day.isoformat()]
        for _ in range(per_day):
            transactions.append({
                "id_transacao": next(ids["transacoes_cartao"]),
                "id_cartao": fake.random.choice(issued_cards)["id_cartao"],
                "data_transacao": random_timestamp(fake, day),
                "valor": f"{fake.random.uniform(5, 800):.2f}",
                **fake.random.choice(merchants),
            })
    return transactions


def build_chargebacks(fake: Faker, ids: dict, transactions: list[dict], count: int, reference_date: date) -> list[dict]:
    return [
        {
            "id_estorno": next(ids["estornos"]),
            "id_transacao": transaction["id_transacao"],
            "data_estorno": reference_date.isoformat(),
            "motivo_estorno": fake.random.choice(CHARGEBACK_REASONS),
        }
        for transaction in fake.random.sample(transactions, count)
    ]


def onboard_customers(
    fake: Faker,
    ids: dict,
    count: int,
    reference_date: date,
    customers: list[dict],
    accounts: list[dict],
    cards: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    new_customers = build_customers(fake, ids, count, reference_date, reference_date)
    new_accounts = build_accounts(fake, ids, new_customers, reference_date, reference_date, ONLY_ACTIVE)
    new_cards = build_cards(fake, ids, new_accounts, reference_date, ONLY_ACTIVE)
    customers += new_customers
    accounts += new_accounts
    cards += new_cards
    return new_customers, new_accounts, new_cards


def update_customers(fake: Faker, customers: list[dict], count: int, reference_date: date) -> list[dict]:
    updated = []
    for index in fake.random.sample(range(len(customers)), count):
        customers[index] = {
            **customers[index],
            "cidade": fake.city(),
            "estado": fake.estado_sigla(),
            "data_atualizacao": random_timestamp(fake, reference_date),
        }
        updated.append(customers[index])
    return updated


def update_accounts(
    fake: Faker,
    accounts: list[dict],
    cards: list[dict],
    count: int,
    reference_date: date,
) -> tuple[list[dict], list[dict]]:
    active = [index for index, account in enumerate(accounts) if account["status_conta"] == "ATIVA"]
    updated_accounts, updated_cards = [], []
    for index in fake.random.sample(active, count):
        status = fake.random.choice(("BLOQUEADA", "ENCERRADA"))
        accounts[index] = {**accounts[index], "status_conta": status}
        updated_accounts.append(accounts[index])
        for card_index, card in enumerate(cards):
            if card["id_conta"] == accounts[index]["id_conta"] and card["status_cartao"] == "ATIVO":
                cards[card_index] = {
                    **card,
                    "status_cartao": CARD_STATUS_BY_ACCOUNT_STATUS[status],
                    "data_atualizacao": random_timestamp(fake, reference_date),
                }
                updated_cards.append(cards[card_index])
    return updated_accounts, updated_cards


def cancel_cards(fake: Faker, cards: list[dict], count: int, reference_date: date) -> list[dict]:
    active = [index for index, card in enumerate(cards) if card["status_cartao"] == "ATIVO"]
    cancelled = []
    for index in fake.random.sample(active, count):
        cards[index] = {**cards[index], "status_cartao": "CANCELADO", "data_atualizacao": random_timestamp(fake, reference_date)}
        cancelled.append(cards[index])
    return cancelled


def add_noise(fake: Faker, ids: dict, entity: str, rows: list[dict], rate: float) -> dict[str, list[dict]]:
    new_id = lambda: next(ids[entity])
    orphan_id = lambda: ORPHAN_ID_BASE + next(ids["orphan"])
    noise_rules = {
        "clientes": {
            "invalid_cpf": lambda row: {"id_cliente": new_id(), "cpf": break_cpf(fake, row["cpf"])},
            "older_version": lambda row: {"cidade": fake.city(), "data_atualizacao": shift_days(row["data_atualizacao"], -30)},
            "exact_duplicate": lambda row: {},
        },
        "contas": {
            "orphan_customer": lambda row: {"id_conta": new_id(), "id_cliente": orphan_id()},
            "invalid_status": lambda row: {"id_conta": new_id(), "status_conta": fake.random.choice(INVALID_ACCOUNT_STATUSES)},
            "exact_duplicate": lambda row: {},
        },
        "cartoes": {
            "orphan_account": lambda row: {"id_cartao": new_id(), "id_conta": orphan_id()},
            "exact_duplicate": lambda row: {},
        },
        "transacoes_cartao": {
            "negative_amount": lambda row: {"id_transacao": new_id(), "valor": f"-{row['valor']}"},
            "invalid_date": lambda row: {"id_transacao": new_id(), "data_transacao": fake.random.choice(INVALID_DATES)},
            "orphan_card": lambda row: {"id_transacao": new_id(), "id_cartao": orphan_id()},
            "exact_duplicate": lambda row: {},
        },
        "estornos": {
            "orphan_transaction": lambda row: {"id_estorno": new_id(), "id_transacao": orphan_id()},
            "exact_duplicate": lambda row: {},
        },
    }
    rules = noise_rules[entity]
    per_type = round(len(rows) * rate / len(rules))
    return {
        noise_type: [{**row, **change(row)} for row in fake.random.sample(rows, min(per_type, len(rows)))]
        for noise_type, change in rules.items()
    }


def write_day(fake: Faker, output_path: Path | str, reference_date: date, clean: dict, noise: dict) -> Path:
    folder = Path(output_path) / reference_date.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    manifest = {"data_referencia": reference_date.isoformat(), "files": {}}

    for entity, rows in clean.items():
        noisy_rows = [row for rows_by_type in noise[entity].values() for row in rows_by_type]
        all_rows = rows + noisy_rows
        fake.random.shuffle(all_rows)

        name = landing_file_name(entity, reference_date)
        with open(folder / name, "w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS[entity])
            writer.writeheader()
            writer.writerows(all_rows)

        manifest["files"][name] = {
            "clean_rows": len(rows),
            "noise_rows": {noise_type: len(noise_rows) for noise_type, noise_rows in noise[entity].items()},
        }

    (folder / "_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return folder


def break_cpf(fake: Faker, cpf: str) -> str:
    digits = "".join(char for char in cpf if char.isdigit())
    wrong_check_digit = digits[:-1] + str((int(digits[-1]) + 1) % 10)
    return fake.random.choice([wrong_check_digit, digits[0] * 11, digits[:9]])


def random_timestamp(fake: Faker, day: date) -> str:
    moment = datetime.combine(day, time.min) + timedelta(seconds=fake.random.randrange(86_400))
    return moment.strftime(TIMESTAMP_FORMAT)


def shift_days(timestamp: str, days: int) -> str:
    return (datetime.strptime(timestamp, TIMESTAMP_FORMAT) + timedelta(days=days)).strftime(TIMESTAMP_FORMAT)


def date_range(start: date, end: date) -> list[date]:
    return [start + timedelta(days=offset) for offset in range((end - start).days + 1)]
