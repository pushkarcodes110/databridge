import asyncio
import os
import random
import string
from enum import Enum
from typing import Literal

import dns.asyncresolver
import gender_guesser.detector as gender_detector
import httpx
from email_validator import EmailNotValidError, validate_email
from fastapi import FastAPI
from fuzzywuzzy import process
from pydantic import BaseModel, Field


app = FastAPI(title="DataBridge Email Validator")

KNOWN_DOMAINS = [
    "gmail.com",
    "yahoo.com",
    "hotmail.com",
    "outlook.com",
    "icloud.com",
    "aol.com",
    "proton.me",
    "protonmail.com",
    "live.com",
    "msn.com",
    "comcast.net",
    "me.com",
    "mac.com",
    "googlemail.com",
    "ymail.com",
    "rocketmail.com",
    "mail.com",
    "zoho.com",
    "gmx.com",
    "gmx.net",
    "fastmail.com",
    "yandex.com",
    "hey.com",
]

TYPO_DOMAINS = {
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
    "gmaul.com": "gmail.com",
    "gmaik.com": "gmail.com",
    "googlemail.con": "googlemail.com",
    "yaho.com": "yahoo.com",
    "yahooo.com": "yahoo.com",
    "yhoo.com": "yahoo.com",
    "yahoo.co": "yahoo.com",
    "yahoo.con": "yahoo.com",
    "yhaoo.com": "yahoo.com",
    "yaoo.com": "yahoo.com",
    "yaho.co": "yahoo.com",
    "ymial.com": "ymail.com",
    "hotnail.com": "hotmail.com",
    "hotmal.com": "hotmail.com",
    "hotmai.com": "hotmail.com",
    "hotmail.co": "hotmail.com",
    "hotmail.con": "hotmail.com",
    "hotmil.com": "hotmail.com",
    "hotmaill.com": "hotmail.com",
    "outloo.com": "outlook.com",
    "outlok.com": "outlook.com",
    "outlook.co": "outlook.com",
    "outlook.con": "outlook.com",
    "outllok.com": "outlook.com",
    "icloud.co": "icloud.com",
    "icloud.con": "icloud.com",
    "iclod.com": "icloud.com",
    "aol.co": "aol.com",
    "protonmal.com": "protonmail.com",
}

resolver = dns.asyncresolver.Resolver()
resolver.lifetime = 5.0
resolver.timeout = 5.0
gender_detector_instance = gender_detector.Detector(case_sensitive=False)
genderize_api_key = os.getenv("GENDERIZE_API_KEY", "")


class ValidationStatus(str, Enum):
    valid = "valid"
    invalid = "invalid"
    typo_fixed = "typo_fixed"
    undeliverable = "undeliverable"
    unknown = "unknown"


class ValidationOptions(BaseModel):
    fix_typos: bool = Field(default=True, alias="fixTypos")
    remove_invalid: bool = Field(default=True, alias="removeInvalid")
    verify_mailbox: bool = Field(default=False, alias="verifyMailbox")
    normalize: bool = True


class ValidateEmailsRequest(BaseModel):
    emails: list[str]
    options: ValidationOptions = Field(default_factory=ValidationOptions)


class EmailValidationResult(BaseModel):
    original: str
    cleaned: str | None
    status: Literal["valid", "invalid", "typo_fixed", "undeliverable", "unknown"]
    reason: str


class ValidateEmailsResponse(BaseModel):
    results: list[EmailValidationResult]


class ClassifyGenderRequest(BaseModel):
    names: list[str]


class GenderClassificationResult(BaseModel):
    name: str
    firstName: str
    gender: Literal["male", "female", "unknown"]
    confidence: float


class ClassifyGenderResponse(BaseModel):
    results: list[GenderClassificationResult]


def normalize_email(email: str) -> str:
    return email.strip().lower()


def extract_first_name(name: str) -> str:
    return name.strip().split()[0].split("-")[0].strip()


def split_email(email: str) -> tuple[str, str]:
    local_part, domain = email.rsplit("@", 1)
    return local_part, domain.lower()


def edit_distance(left: str, right: str) -> int:
    rows = range(len(right) + 1)
    previous = list(rows)

    for i, left_char in enumerate(left, start=1):
        current = [i]
        for j, right_char in enumerate(right, start=1):
            insert = current[j - 1] + 1
            delete = previous[j] + 1
            replace = previous[j - 1] + (left_char != right_char)
            current.append(min(insert, delete, replace))
        previous = current

    return previous[-1]


def correct_domain_typos(email: str) -> tuple[str, bool, str | None]:
    if "@" not in email:
        return email, False, None

    local_part, domain = split_email(email)
    corrected_domain = TYPO_DOMAINS.get(domain)

    if not corrected_domain:
        fuzzy_match = process.extractOne(domain, KNOWN_DOMAINS)
        if fuzzy_match:
            candidate, score = fuzzy_match
            if score >= 85 and edit_distance(domain, candidate) <= 2:
                corrected_domain = candidate

    if corrected_domain and corrected_domain != domain:
        return f"{local_part}@{corrected_domain}", True, f"{domain} -> {corrected_domain}"

    return email, False, None


def validate_format(email: str) -> tuple[str | None, str | None]:
    try:
        validation = validate_email(email, check_deliverability=False)
        return validation.normalized, None
    except EmailNotValidError as error:
        return None, str(error)


async def mx_hosts(domain: str) -> list[str]:
    try:
        answers = await resolver.resolve(domain, "MX")
    except Exception:
        return []

    records = sorted(answers, key=lambda item: int(item.preference))
    return [str(record.exchange).rstrip(".") for record in records]


async def read_smtp_response(reader: asyncio.StreamReader) -> tuple[int | None, str]:
    lines: list[str] = []

    while True:
        line = await asyncio.wait_for(reader.readline(), timeout=5)
        if not line:
            break

        decoded = line.decode("utf-8", errors="replace").strip()
        lines.append(decoded)
        if len(decoded) < 4 or decoded[3] != "-":
            break

    if not lines:
        return None, "No SMTP response"

    code_text = lines[-1][:3]
    return int(code_text) if code_text.isdigit() else None, " ".join(lines)


async def smtp_command(writer: asyncio.StreamWriter, reader: asyncio.StreamReader, command: str) -> tuple[int | None, str]:
    writer.write(command.encode("utf-8"))
    await asyncio.wait_for(writer.drain(), timeout=5)
    return await read_smtp_response(reader)


def random_probe_email(domain: str) -> str:
    token = "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(24))
    return f"databridge-probe-{token}@{domain}"


def classify_with_gender_guesser(first_name: str) -> tuple[str, float]:
    if not first_name:
        return "unknown", 0.0

    result = gender_detector_instance.get_gender(first_name)
    if result == "male":
        return "male", 0.95
    if result == "mostly_male":
        return "male", 0.75
    if result == "female":
        return "female", 0.95
    if result == "mostly_female":
        return "female", 0.75
    return "unknown", 0.0


async def classify_with_genderize(first_name: str) -> tuple[str, float]:
    if not first_name:
        return "unknown", 0.0

    params = {"name": first_name}
    if genderize_api_key:
        params["apikey"] = genderize_api_key

    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get("https://api.genderize.io", params=params)
            if response.status_code != 200:
                return "unknown", 0.0
            data = response.json()
    except Exception:
        return "unknown", 0.0

    gender = data.get("gender")
    probability = data.get("probability") or 0.0
    if gender in {"male", "female"} and probability >= 0.6:
        return gender, float(probability)
    return "unknown", float(probability)


async def classify_one_name(name: str) -> GenderClassificationResult:
    first_name = extract_first_name(name)
    gender, confidence = classify_with_gender_guesser(first_name)

    if gender == "unknown":
        gender, confidence = await classify_with_genderize(first_name)

    return GenderClassificationResult(
        name=name,
        firstName=first_name,
        gender=gender,
        confidence=confidence,
    )


async def smtp_mailbox_check(email: str, hosts: list[str]) -> tuple[ValidationStatus, str]:
    if not hosts:
        return ValidationStatus.undeliverable, "Domain has no MX records."

    domain = email.rsplit("@", 1)[1]

    for host in hosts[:3]:
        writer: asyncio.StreamWriter | None = None
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, 25), timeout=5)
            code, _message = await read_smtp_response(reader)
            if code is None or code >= 500:
                continue

            code, _ = await smtp_command(writer, reader, "EHLO databridge.local\r\n")
            if code is None or code >= 500:
                await smtp_command(writer, reader, "HELO databridge.local\r\n")

            await smtp_command(writer, reader, "MAIL FROM:<validator@databridge.local>\r\n")
            rcpt_code, rcpt_message = await smtp_command(writer, reader, f"RCPT TO:<{email}>\r\n")
            probe_code, _ = await smtp_command(writer, reader, f"RCPT TO:<{random_probe_email(domain)}>\r\n")
            await smtp_command(writer, reader, "QUIT\r\n")

            if rcpt_code in {250, 251, 252}:
                if probe_code in {250, 251, 252}:
                    return ValidationStatus.unknown, "Mailbox accepted, but domain appears to be catch-all."
                return ValidationStatus.valid, "Mailbox accepted by MX server."

            if rcpt_code and rcpt_code >= 500:
                return ValidationStatus.undeliverable, rcpt_message or "Mailbox rejected by MX server."

            return ValidationStatus.unknown, rcpt_message or "SMTP server did not provide a definitive mailbox result."
        except Exception:
            continue
        finally:
            if writer:
                writer.close()
                await writer.wait_closed()

    return ValidationStatus.unknown, "SMTP mailbox verification timed out or was blocked by the MX server."


async def validate_one_email(email: str, options: ValidationOptions) -> EmailValidationResult:
    original = email
    cleaned = normalize_email(email) if options.normalize else email.strip()
    typo_fixed = False
    typo_reason = None

    if options.fix_typos:
        cleaned, typo_fixed, typo_reason = correct_domain_typos(cleaned)

    normalized, format_error = validate_format(cleaned)
    if format_error:
        status = ValidationStatus.invalid if options.remove_invalid else ValidationStatus.unknown
        return EmailValidationResult(
            original=original,
            cleaned=None if options.remove_invalid else cleaned,
            status=status.value,
            reason=format_error,
        )

    cleaned = normalized or cleaned
    _, domain = split_email(cleaned)
    hosts = await mx_hosts(domain)
    if not hosts:
        return EmailValidationResult(
            original=original,
            cleaned=None,
            status=ValidationStatus.undeliverable.value,
            reason="Domain has no MX records.",
        )

    if options.verify_mailbox:
        smtp_status, smtp_reason = await smtp_mailbox_check(cleaned, hosts)
        if smtp_status == ValidationStatus.valid and typo_fixed:
            return EmailValidationResult(
                original=original,
                cleaned=cleaned,
                status=ValidationStatus.typo_fixed.value,
                reason=f"{typo_reason}; {smtp_reason}",
            )

        return EmailValidationResult(
            original=original,
            cleaned=cleaned if smtp_status != ValidationStatus.undeliverable else None,
            status=smtp_status.value,
            reason=smtp_reason,
        )

    return EmailValidationResult(
        original=original,
        cleaned=cleaned,
        status=ValidationStatus.typo_fixed.value if typo_fixed else ValidationStatus.valid.value,
        reason=typo_reason or "Valid format and domain has MX records.",
    )


@app.post("/validate-emails", response_model=ValidateEmailsResponse)
async def validate_emails(payload: ValidateEmailsRequest) -> ValidateEmailsResponse:
    semaphore = asyncio.Semaphore(20)

    async def bounded_validate(email: str) -> EmailValidationResult:
        async with semaphore:
            return await validate_one_email(email, payload.options)

    results = await asyncio.gather(*(bounded_validate(email) for email in payload.emails))
    return ValidateEmailsResponse(results=list(results))


@app.post("/classify-gender", response_model=ClassifyGenderResponse)
async def classify_gender(payload: ClassifyGenderRequest) -> ClassifyGenderResponse:
    semaphore = asyncio.Semaphore(20)

    async def bounded_classify(name: str) -> GenderClassificationResult:
        async with semaphore:
            return await classify_one_name(name)

    results = await asyncio.gather(*(bounded_classify(name) for name in payload.names))
    return ClassifyGenderResponse(results=list(results))


@app.post("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}
