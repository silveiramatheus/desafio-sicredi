import csv
import json
from datetime import date

import pytest

from src.datagen.generator import generate_landing
from src.quality.cpf import is_valid_cpf

START = date(2026, 10, 1)


def read_csv(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_manifest(folder):
    return json.loads((folder / "_manifest.json").read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def landing(tmp_path_factory):
    output = tmp_path_factory.mktemp("landing")
    generate_landing(output, start_date=START, days=3, history_days=40, num_customers=60)
    return output


def test_creates_one_folder_per_day_with_all_files(landing):
    folders = sorted(path.name for path in landing.iterdir())
    assert folders == ["2026-10-01", "2026-10-02", "2026-10-03"]
    files = sorted(path.name for path in (landing / "2026-10-02").glob("*.csv"))
    assert files == ["cartoes.csv", "clientes.csv", "contas.csv", "estornos.csv", "transacoes_cartao_20261002.csv"]


def test_same_seed_generates_same_files(tmp_path):
    generate_landing(tmp_path / "a", start_date=START, days=2, history_days=10, num_customers=20)
    generate_landing(tmp_path / "b", start_date=START, days=2, history_days=10, num_customers=20)
    for file_a in sorted((tmp_path / "a").rglob("*.csv")):
        file_b = tmp_path / "b" / file_a.relative_to(tmp_path / "a")
        assert file_a.read_text() == file_b.read_text()


def test_file_rows_are_clean_plus_noise(landing):
    for folder in landing.iterdir():
        for name, counts in read_manifest(folder)["files"].items():
            assert len(read_csv(folder / name)) == counts["clean_rows"] + sum(counts["noise_rows"].values())


def test_invalid_cpf_count_matches_manifest(landing):
    folder = landing / "2026-10-01"
    customers = read_csv(folder / "clientes.csv")
    expected = read_manifest(folder)["files"]["clientes.csv"]["noise_rows"]["invalid_cpf"]
    assert expected > 0
    assert sum(not is_valid_cpf(row["cpf"]) for row in customers) == expected


def test_negative_amount_count_matches_manifest(landing):
    for folder in landing.iterdir():
        name = f"transacoes_cartao_{folder.name.replace('-', '')}.csv"
        transactions = read_csv(folder / name)
        expected = read_manifest(folder)["files"][name]["noise_rows"]["negative_amount"]
        assert sum(float(row["valor"]) < 0 for row in transactions) == expected


def test_first_load_has_history_and_later_loads_have_late_transactions(landing):
    first = read_csv(landing / "2026-10-01" / "transacoes_cartao_20261001.csv")
    months = {row["data_transacao"][:7] for row in first if row["data_transacao"][:2] == "20"}
    assert {"2026-08", "2026-09", "2026-10"} <= months

    second = read_csv(landing / "2026-10-02" / "transacoes_cartao_20261002.csv")
    late = [row for row in second if row["data_transacao"][:2] == "20" and row["data_transacao"][:10] < "2026-10-02"]
    assert len(late) >= 10


def test_later_loads_bring_new_and_updated_customers(landing):
    first_ids = {row["id_cliente"] for row in read_csv(landing / "2026-10-01" / "clientes.csv")}
    second_ids = {row["id_cliente"] for row in read_csv(landing / "2026-10-02" / "clientes.csv")}
    assert second_ids - first_ids
    assert second_ids & first_ids


def test_new_cards_reference_accounts_from_the_same_load(landing):
    folder = landing / "2026-10-02"
    account_ids = {row["id_conta"] for row in read_csv(folder / "contas.csv")}
    first_cards = {row["id_cartao"] for row in read_csv(landing / "2026-10-01" / "cartoes.csv")}
    new_cards = [row for row in read_csv(folder / "cartoes.csv") if row["id_cartao"] not in first_cards]
    valid_new_cards = [row for row in new_cards if int(row["id_conta"]) < 900_000]
    assert valid_new_cards
    assert all(row["id_conta"] in account_ids for row in valid_new_cards)


def test_transactions_happen_after_card_issue_date(landing):
    issue_dates = {}
    for folder in sorted(landing.iterdir()):
        for card in read_csv(folder / "cartoes.csv"):
            issue_dates.setdefault(card["id_cartao"], card["data_emissao"])

    for folder in landing.iterdir():
        name = f"transacoes_cartao_{folder.name.replace('-', '')}.csv"
        for row in read_csv(folder / name):
            if row["id_cartao"] in issue_dates and len(row["data_transacao"]) == len("2026-10-01 00:00:00"):
                assert row["data_transacao"][:10] >= issue_dates[row["id_cartao"]], row


def latest_state(landing, file_name, key, order_by):
    state = {}
    for folder in sorted(landing.iterdir()):
        for row in read_csv(folder / file_name):
            row = {**row, "_folder": folder.name}
            current = state.get(row[key])
            if current is None or (row[order_by], row["_folder"]) >= (current[order_by], current["_folder"]):
                state[row[key]] = row
    return state


def test_active_cards_belong_to_active_accounts(landing):
    accounts = latest_state(landing, "contas.csv", "id_conta", "_folder")
    cards = latest_state(landing, "cartoes.csv", "id_cartao", "data_atualizacao")

    for card in cards.values():
        account = accounts.get(card["id_conta"])
        if card["status_cartao"] == "ATIVO" and account is not None:
            assert account["status_conta"] == "ATIVA", card


def test_customers_are_adults_when_accounts_are_opened(landing):
    birth_dates = {}
    for folder in sorted(landing.iterdir()):
        for row in read_csv(folder / "clientes.csv"):
            birth_dates[row["id_cliente"]] = date.fromisoformat(row["data_nascimento"])

    for folder in landing.iterdir():
        for account in read_csv(folder / "contas.csv"):
            if account["id_cliente"] in birth_dates:
                opened_on = date.fromisoformat(account["data_abertura"])
                assert (opened_on - birth_dates[account["id_cliente"]]).days >= 18 * 365, account


def test_merchant_names_identify_a_single_merchant(landing):
    merchants = {}
    for folder in landing.iterdir():
        name = f"transacoes_cartao_{folder.name.replace('-', '')}.csv"
        for row in read_csv(folder / name):
            merchants.setdefault(row["nome_estabelecimento"], set()).add((row["cidade_estabelecimento"], row["mcc"]))

    assert all(len(locations) == 1 for locations in merchants.values())


def test_new_accounts_and_cards_start_active(landing):
    first_accounts = {row["id_conta"] for row in read_csv(landing / "2026-10-01" / "contas.csv")}
    first_cards = {row["id_cartao"] for row in read_csv(landing / "2026-10-01" / "cartoes.csv")}
    folder = landing / "2026-10-02"

    new_accounts = [row for row in read_csv(folder / "contas.csv") if row["id_conta"] not in first_accounts and int(row["id_cliente"]) < 900_000]
    new_cards = [row for row in read_csv(folder / "cartoes.csv") if row["id_cartao"] not in first_cards and int(row["id_conta"]) < 900_000]

    assert new_accounts and new_cards
    valid_statuses = {"ATIVA", "BLOQUEADA", "ENCERRADA"}
    assert {row["status_conta"] for row in new_accounts if row["status_conta"] in valid_statuses} == {"ATIVA"}
    assert {row["status_cartao"] for row in new_cards} == {"ATIVO"}


def test_chargebacks_happen_after_transaction_date(landing):
    transaction_dates = {}
    for folder in landing.iterdir():
        name = f"transacoes_cartao_{folder.name.replace('-', '')}.csv"
        for row in read_csv(folder / name):
            transaction_dates[row["id_transacao"]] = row["data_transacao"][:10]

    for folder in landing.iterdir():
        for row in read_csv(folder / "estornos.csv"):
            if row["id_transacao"] in transaction_dates:
                assert row["data_estorno"] >= transaction_dates[row["id_transacao"]], row


@pytest.mark.parametrize("rate", [0.0, 0.3])
def test_noise_rate_is_configurable_per_entity(tmp_path, rate):
    generate_landing(tmp_path, start_date=START, days=1, history_days=5, num_customers=100, noise_rates={"contas": rate})
    counts = read_manifest(tmp_path / "2026-10-01")["files"]["contas.csv"]
    noise_rows = sum(counts["noise_rows"].values())
    assert abs(noise_rows - counts["clean_rows"] * rate) <= len(counts["noise_rows"])
