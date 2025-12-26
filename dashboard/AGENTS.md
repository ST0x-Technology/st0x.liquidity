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

### Testing

**Avoid mutable module-level state in tests.** Mutable variables shared across
tests create hidden coupling - understanding what a test does requires tracking
mutations from other tests or beforeEach hooks. This makes tests harder to read
in isolation and can cause flaky failures when test order changes.

```typescript
// BAD - mutable module-level state makes tests hard to reason about
let mockValue = 'default'
vi.mock('some-module', () => ({ ... uses mockValue ... }))
```

Instead, create mock configuration per-test via a setup function. Each test
explicitly declares its configuration, making it self-contained and readable:

```typescript
// GOOD - each test creates its own isolated mock
const setupTest = async (config: MockConfig) => {
  const Mock = createMock(config);
  vi.doMock("some-module", () => ({ SomeClass: Mock }));
  return import("./module-under-test");
};
```
