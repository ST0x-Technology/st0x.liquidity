# Dashboard

Real-time monitoring dashboard for the st0x liquidity system.

## Panels

- Dashboard: inventory, recent trades, and transfers.
- Orders: active Raindex order view.
- PnL: backend-computed persisted fill replay for counter-trade PnL, directional
  exposure PnL, total PnL, current replay exposure, and tracked costs/revenues.
- Logs: structured production log browser.

## Development

Install dependencies:

```sh
bun install
```

Start a development server:

```sh
bun run dev

# or start the server and open the app in a new browser tab
bun run dev --open
```

Run the dashboard with mock API responses for non-PnL panels:

```sh
PUBLIC_DASHBOARD_MOCK_MODE=1 bun run dev
```

The PnL panel does not provide generated fallback data and does not query the
SQLite/Datasette endpoint directly. The backend `/pnl` endpoint must respond;
otherwise the panel shows the live load/configuration failure.

Run the full dashboard locally against a backend origin:

```sh
PUBLIC_BACKEND_API_URL=https://st0x-liquidity-nixos.taile5cf8a.ts.net \
bun run dev
```

In local dev, `PUBLIC_BACKEND_API_URL` points the Vite proxy at a backend
origin. The browser calls same-origin dashboard paths such as `/pnl`, `/health`,
`/trades`, and `/transfers`; Vite forwards those requests to the configured
backend. Production uses the same browser contract through the Nginx dashboard
proxy.

## Building

To create a production version:

```sh
bun run build
```

Preview the production build:

```sh
bun run preview
```
