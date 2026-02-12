# Dashboard AGENTS.md

This file provides guidance to AI agents working with the dashboard.

**IMPORTANT**: All agents MUST obey the root @AGENTS.md first.

## Dashboard-Specific Guidelines

### Package Manager

Use `bun` for all operations. Run from repo root with `--cwd dashboard`.

**CRITICAL: After any change to `bun.lock`**, regenerate `bun.nix` and format
it:

```bash
nix run .#genBunNix
nix fmt -- dashboard/bun.nix
```

CI will fail if `bun.nix` is out of sync with `bun.lock`.

### Component Library

Uses shadcn-svelte. Add components via:
`(cd dashboard && bunx shadcn-svelte@latest add <component>)`

### State Management

- **Runed**: Local reactive state, persisted state, FSM
- **TanStack Query**: Server state cache (populated via WebSocket)

### FP and FRP Patterns

This codebase uses functional programming utilities from two modules:

**`$lib/fp` - Pure functional utilities** (non-reactive):

- `Result<T, E>` type with `ok`, `err`, `map`, `flatMap`, `unwrapOr`
- `matcher` for exhaustive pattern matching on discriminated unions
- `tryCatch` / `tryCatchAsync` for error handling
- `pipe` for left-to-right composition

**`$lib/frp.svelte` - Functional Reactive Programming** (Svelte 5 runes):

- `reactive<T>(initial)` - creates reactive state with explicit mutations

```typescript
// FRP: Explicit mutations via update()
const count = reactive(0);
count.update((n) => n + 1); // transform
count.update(() => 0); // reset
console.log(count.current); // read

// FP: Pattern matching on discriminated unions
type State = { type: "loading" } | { type: "ready"; data: string };
const matchState = matcher<State>()("type");
matchState(state, {
  loading: () => "Loading...",
  ready: (s) => s.data,
});
```

**Prefer these patterns over**:

- Direct `$state` mutations → use `reactive()` for explicit updates
- Nested if/else on union types → use `matcher()` for exhaustive matching
- Try/catch blocks → use `tryCatch()` / `Result` type

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
