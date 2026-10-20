import re


def is_valid_cpf(value: str | None) -> bool:
    if value is None:
        return False
    digits = re.sub(r"\D", "", value)
    if len(digits) != 11 or len(set(digits)) == 1:
        return False
    for position in (9, 10):
        total = sum(int(digit) * weight for digit, weight in zip(digits[:position], range(position + 1, 1, -1)))
        if (total * 10) % 11 % 10 != int(digits[position]):
            return False
    return True
