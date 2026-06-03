# Dashboard

Real-time monitoring dashboard for the st0x liquidity system.

## Panels

- Dashboard: inventory, recent trades, and transfers.
- Orders: active Raindex order view.
- PnL: persisted fill replay for counter-trade PnL, directional exposure PnL,
  total PnL, and current replay exposure.
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

Run the dashboard with mock API responses and synthetic PnL data:

```sh
PUBLIC_DASHBOARD_MOCK_MODE=1 bun run dev
```

Run the PnL tab against a Datasette-style SQL JSON endpoint:

```sh
PUBLIC_PNL_SQL_API_URL=http://st0x-liquidity-nixos.taile5cf8a.ts.net:8081/st0x-hedge.json \
bun run dev
```

Run the full dashboard locally against production backend data and production
PnL SQL data:

```sh
PUBLIC_BACKEND_API_URL=https://st0x-liquidity-nixos.taile5cf8a.ts.net \
PUBLIC_PNL_SQL_API_URL=http://st0x-liquidity-nixos.taile5cf8a.ts.net:8081/st0x-hedge.json \
bun run dev
```

In local dev, absolute SQL URLs are proxied through `/__pnl_sql` so the browser
does not depend on CORS headers from the SQL endpoint. `PUBLIC_BACKEND_API_URL`
points the existing dashboard API proxy at a backend origin. In production, set
`PUBLIC_PNL_SQL_API_URL` to a same-origin path or to an endpoint that allows the
dashboard origin.

The adapter expects URLs with this shape:

```text
http://<host>:8081/st0x-hedge.json?sql=SELECT+symbol,net_position+FROM+position_view&_shape=array
```

## Building

To create a production version:

```sh
bun run build
```

Preview the production build:

```sh
bun run preview
```
