import pytest

from src.quality.cpf import is_valid_cpf


@pytest.mark.parametrize("cpf", ["529.982.247-25", "52998224725", "111.444.777-35"])
def test_valid_cpf(cpf):
    assert is_valid_cpf(cpf)


@pytest.mark.parametrize(
    "cpf",
    [
        "529.982.247-26",
        "11111111111",
        "529982247",
        "",
        None,
        "abc.def.ghi-jk",
    ],
)
def test_invalid_cpf(cpf):
    assert not is_valid_cpf(cpf)
