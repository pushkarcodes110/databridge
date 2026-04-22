This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## Getting Started

First, run the development server:

```bash
npm run dev
# or
yarn dev
# or
pnpm dev
# or
bun dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

## Environment variables

The frontend uses server-side `process.env` values for API/service URLs and feature flags.

- Local dev env file: `../.env.dev` (repo root; gitignored).
- Quick way to load it for your shell session:

```bash
set -a; source ../.env.dev; set +a
npm run dev
```

Reacher mailbox validation is feature-flagged:

- `REACHER_ENABLED=true|false` (server-side transform runner + health check)
- `NEXT_PUBLIC_REACHER_ENABLED=true|false` (controls whether the UI shows the Reacher option)
- `REACHER_URL=http://localhost:8088` when the Next.js server runs on the same host as the Coolify Reacher port mapping.
- `REACHER_URL=http://reacher:8080` when the Next.js server runs inside the same Docker network as the `reacher` service.
- `REACHER_URL=https://<coolify-app>.<server-ip>.sslip.io` is supported, but internal URLs above are preferred because Node-side DNS for `sslip.io` can be intermittent. When an `sslip.io` URL contains an IPv4 address, the runner also tries `http://<server-ip>:${REACHER_PORT:-8088}`.
- `REACHER_FALLBACK_URLS=http://localhost:8088,http://host.docker.internal:8088` optional comma-separated fallbacks. The runner tries these after `REACHER_URL`.
- `REACHER_CHECK_PATH=/v1/check_email` defaults to Reacher's email check endpoint.

You can start editing the page by modifying `app/page.tsx`. The page auto-updates as you edit the file.

This project uses [`next/font`](https://nextjs.org/docs/app/building-your-application/optimizing/fonts) to automatically optimize and load [Geist](https://vercel.com/font), a new font family for Vercel.

## Learn More

To learn more about Next.js, take a look at the following resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.

You can check out [the Next.js GitHub repository](https://github.com/vercel/next.js) - your feedback and contributions are welcome!

## Deploy on Vercel

The easiest way to deploy your Next.js app is to use the [Vercel Platform](https://vercel.com/new?utm_medium=default-template&filter=next.js&utm_source=create-next-app&utm_campaign=create-next-app-readme) from the creators of Next.js.

Check out our [Next.js deployment documentation](https://nextjs.org/docs/app/building-your-application/deploying) for more details.
