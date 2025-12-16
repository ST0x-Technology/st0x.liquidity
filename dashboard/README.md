# Dashboard

Real-time monitoring dashboard for the st0x liquidity system.

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

## Building

To create a production version:

```sh
bun run build
```

Preview the production build:

```sh
bun run preview
```

## Docker

Build the dashboard Docker image:

```sh
docker build -t st0x-dashboard .
```

Run the container (configure `BACKEND_HOST` to point to the backend server):

```sh
docker run -p 8080:80 -e BACKEND_HOST=localhost st0x-dashboard
```

> Note: This project uses `adapter-static` for SPA deployment. The Docker image
> uses nginx to serve static files and proxy WebSocket connections to the
> backend.
