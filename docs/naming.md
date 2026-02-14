# Naming Conventions

Source of truth for type naming conventions in the st0x liquidity codebase.

## Config / Secrets / Ctx / Env / Job

Types follow a strict naming convention based on their role:

| Suffix     | Meaning                  | Source                                       | Example                                 |
| ---------- | ------------------------ | -------------------------------------------- | --------------------------------------- |
| `*Env`     | CLI args / env vars      | clap-powered struct                          | `ServerEnv`, `ReporterEnv`              |
| `*Config`  | Non-secret settings      | Plaintext TOML (`config/*.toml`)             | `SchwabConfig`, `EvmConfig`             |
| `*Secrets` | Secret credentials       | Encrypted TOML (`secret/*.toml.age`)         | `SchwabSecrets`, `EvmSecrets`           |
| `*Ctx`     | Combined runtime context | Assembled from runtime state                 | `SchwabCtx`, `EvmCtx`                   |
| `*Job`     | Apalis job unit          | Serializable struct implementing `Job` trait | `PollOrderStatusJob`, `MintRecoveryJob` |

## Rules

- A `*Env` type is a clap-powered struct that captures CLI flags and environment
  variables. Its only purpose is to specify paths to config and secrets files.
  It should not contain application logic or configuration values itself.
- A type named `*Config` must never contain secret data (API keys, private keys,
  RPC URLs with credentials).
- A type named `*Secrets` must never contain non-secret data.
- A `*Ctx` type represents everything a subsystem needs to operate. It combines
  config and secrets, and may include additional runtime state (e.g., a database
  pool). It is the type passed to constructors and initialization functions.
- The Config/Secrets distinction is a concern of the crate that loads TOML
  files. Library crates (e.g., `st0x-execution`) may use `*Ctx` as their public
  construction interface without splitting Config from Secrets - that split
  belongs in the caller.
- A `*Job` type is a serializable struct representing a unit of work for the job
  queue. It implements the `Job` trait with associated `Ctx` and `Error` types.
  Each job bundles its dependencies into a single `*Ctx` type rather than using
  multiple extractors. Job names describe what they do in imperative form (e.g.,
  `PollOrderStatus`, not `OrderStatusPoll`).
