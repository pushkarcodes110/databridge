import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from main import correct_domain_typos


def test_common_domain_misspellings_are_fixed():
    cases = {
        "gmal.com": "gmail.com",
        "gmial.com": "gmail.com",
        "gmaill.com": "gmail.com",
        "gmai.com": "gmail.com",
        "gmail.co": "gmail.com",
        "gmail.con": "gmail.com",
        "gmail.cm": "gmail.com",
        "gmail.om": "gmail.com",
        "gmail.cmo": "gmail.com",
        "gmail.comm": "gmail.com",
        "gnail.com": "gmail.com",
        "gamil.com": "gmail.com",
        "yaho.com": "yahoo.com",
        "yahooo.com": "yahoo.com",
        "yhoo.com": "yahoo.com",
        "yahoo.co": "yahoo.com",
        "yhaoo.com": "yahoo.com",
        "yaoo.com": "yahoo.com",
        "hotnail.com": "hotmail.com",
        "hotmal.com": "hotmail.com",
        "outloo.com": "outlook.com",
        "outlok.com": "outlook.com",
    }

    for typo, expected_domain in cases.items():
        corrected, typo_fixed, reason = correct_domain_typos(f"user@{typo}")

        assert corrected == f"user@{expected_domain}"
        assert typo_fixed is True
        assert reason == f"{typo} -> {expected_domain}"


def test_valid_emails_are_not_modified():
    emails = [
        "user@gmail.com",
        "person@yahoo.com",
        "name@hotmail.com",
        "hello@outlook.com",
        "test@icloud.com",
    ]

    for email in emails:
        corrected, typo_fixed, reason = correct_domain_typos(email)

        assert corrected == email
        assert typo_fixed is False
        assert reason is None
