# Dashboard AGENTS.md

This file provides guidance to AI agents working with the dashboard.

**IMPORTANT**: All agents MUST obey the root @AGENTS.md first.

## Dashboard-Specific Guidelines

### Package Manager

Use `bun` for all operations. Run from repo root with `--cwd dashboard`.

### Component Library

Uses shadcn-svelte. Add components via:
`(cd dashboard && bunx shadcn-svelte@latest add <component>)`

### State Management

- **Runed**: Local reactive state, persisted state, FSM
- **TanStack Query**: Server state cache (populated via WebSocket)
