# DataBridge Email Validation API Deployment

This service is the only production email validation backend. Reacher, RabbitMQ, and Reacher workers are not required.

## Docker Compose

Use `services/email-validator/docker-compose.yml` in Coolify or on a VPS:

```bash
cd services/email-validator
docker compose up -d --build
```

The API listens on port `8001` and exposes:

- `GET /health`
- `POST /validate-emails`
- `POST /classify-gender`

## Environment Variables

Required:

- `SMTP_FROM_EMAIL`: sender used in SMTP `MAIL FROM`, for example `validator@yourdomain.com`.
- `SMTP_HELO_NAME`: hostname used in SMTP `EHLO`/`HELO`, preferably the VPS hostname or your mail domain.

Recommended:

- `EMAIL_VALIDATOR_PORT`: host port, default `8001`.
- `EMAIL_VALIDATION_CONCURRENCY`: concurrent email validations, default `20`.
- `SMTP_CONNECT_TIMEOUT_SECONDS`: SMTP connection timeout, default `8`.
- `SMTP_READ_TIMEOUT_SECONDS`: SMTP read timeout, default `8`.
- `SMTP_MAX_MX_HOSTS`: MX hosts attempted per domain, default `3`.
- `GENDERIZE_API_KEY`: optional fallback for gender classification.

## Coolify Deployment

1. Create a new Coolify resource from the Git repository.
2. Select Docker Compose as the build pack.
3. Set compose file path to `services/email-validator/docker-compose.yml`.
4. Set build context to `services/email-validator` if Coolify asks for it.
5. Add the required environment variables.
6. Deploy.
7. Expose port `8001` internally or attach a Coolify domain to the service.
8. Confirm health: `https://<api-domain>/health`.

## SMTP Verification On VPS

SMTP mailbox verification opens outbound TCP connections to recipient MX servers on port `25`.

Before relying on mailbox checks, verify the VPS provider allows outbound port `25`:

```bash
docker compose exec email-validator python - <<'PY'
import socket
socket.create_connection(("gmail-smtp-in.l.google.com", 25), timeout=8).close()
print("outbound port 25 works")
PY
```

If this fails with timeout/refused/network unreachable, ask the VPS provider to unblock outbound SMTP or use a provider that permits port `25`. Local macOS networks commonly block this, so local SMTP failures are expected.

## API Test Steps

Health:

```bash
curl -fsS https://<api-domain>/health
```

Format and MX validation only:

```bash
curl -fsS https://<api-domain>/validate-emails \
  -H "Content-Type: application/json" \
  -d '{"emails":["User@gmal.com","bad-email","test@example.invalid"],"options":{"fixTypos":true,"removeInvalid":true,"verifyMailbox":false,"normalize":true}}'
```

SMTP mailbox validation:

```bash
curl -fsS https://<api-domain>/validate-emails \
  -H "Content-Type: application/json" \
  -d '{"emails":["valid-address@yourdomain.com"],"options":{"fixTypos":true,"removeInvalid":true,"verifyMailbox":true,"normalize":true}}'
```

Expected statuses are `valid`, `typo_fixed`, `invalid`, `undeliverable`, or `unknown`. `unknown` is normal for catch-all domains, greylisting, anti-enumeration, or blocked port `25`.

## Transform Pipeline Verification

1. Set the frontend/server env var `EMAIL_VALIDATOR_URL=https://<api-domain>`.
2. Run a transform with email filtering enabled and `Verify mailbox exists via API` turned on.
3. Confirm the transform does not emit `Email validation service unreachable`.
4. Confirm output rows remove `invalid` and `undeliverable` addresses; `unknown` is retained because SMTP can be inconclusive.
5. Confirm typo fixes still apply, for example `user@gmal.com` becomes `user@gmail.com`.

## Vercel Frontend Deployment

1. Import `databridge-frontend` into Vercel.
2. Set root directory to `databridge-frontend`.
3. Use the default Next.js build settings.
4. Add environment variables:
   - `EMAIL_VALIDATOR_URL=https://<api-domain>`
   - `GENDER_SERVICE_URL=https://<api-domain>` only if you want it separate; otherwise omit it.
5. Deploy.
6. Run a production transform from the Vercel URL and confirm email validation calls the Coolify API.
