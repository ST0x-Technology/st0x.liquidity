#!/usr/bin/env nu

# Edit an age-encrypted secret with ragenix, rekeying every recipient if the
# file actually changed. Rust/structured-arg rewrite of the old bash wrapper:
# `def main` gives self-documenting `--help`, typed flags, and a clear
# "missing file" error instead of the old positional `$1`/`$2` fall-through
# that crashed on an unbound `$HOME`.

# Resolves the age/SSH identity used to decrypt, mirroring the deploy tooling's
# precedence: --op (1Password) > --identity/-i > $SSH_IDENTITY >
# ~/.ssh/id_ed25519. Returns the key path plus an optional temp file the caller
# must delete once both ragenix calls are done.
# Returns a `{ path, tmpfile }` record; `tmpfile` is "" unless the key came from
# 1Password, in which case it is the temp file the caller must delete.
def resolve-identity [
  --identity: string
  --op: string
  --op-account: string
] {
  if ($op | is-not-empty) {
    let found = (which op)
    let op_bin = if ($found | is-not-empty) { $found.0.path } else { "/opt/homebrew/bin/op" }
    if not ($op_bin | path exists) {
      error make --unspanned {
        msg: "1Password CLI (op) not found -- install it or pass -i <key> instead"
      }
    }

    # mktemp creates the file 0600; chmod again so a key never lands group/other
    # readable even if save were to recreate it.
    let tmp = (mktemp --tmpdir "secret-identity-XXXXXX")
    ^chmod 600 $tmp

    let op_args = if ($op_account | is-not-empty) { ["--account" $op_account] } else { [] }
    let read = (do { ^$op_bin read $op ...$op_args } | complete)
    if $read.exit_code != 0 {
      rm --force $tmp
      error make --unspanned {
        msg: $"reading identity from 1Password failed: ($read.stderr | str trim)"
      }
    }

    $read.stdout | save --raw --force $tmp
    return { path: $tmp, tmpfile: $tmp }
  }

  if ($identity | is-not-empty) {
    if not ($identity | path exists) {
      error make --unspanned { msg: $"identity key not found: ($identity)" }
    }
    return { path: $identity, tmpfile: "" }
  }

  let ssh_identity = ($env.SSH_IDENTITY? | default "")
  if ($ssh_identity | is-not-empty) {
    if not ($ssh_identity | path exists) {
      error make --unspanned { msg: $"SSH_IDENTITY key not found: ($ssh_identity)" }
    }
    return { path: $ssh_identity, tmpfile: "" }
  }

  # Guard $HOME: under nushell it is simply absent rather than an "unbound
  # variable" crash, but a missing HOME must still fall through to a clear error
  # rather than probing a bogus "/.ssh/id_ed25519".
  let home = ($env.HOME? | default "")
  if ($home | is-not-empty) {
    let default_key = ([$home ".ssh" "id_ed25519"] | path join)
    if ($default_key | path exists) {
      return { path: $default_key, tmpfile: "" }
    }
  }

  error make --unspanned {
    msg: ("no decryption identity available -- pass one of:\n"
      + "  -i <key>              explicit age/SSH private key\n"
      + "  --op op://vault/item  read the key from 1Password\n"
      + "  SSH_IDENTITY=<key>    environment variable\n"
      + "  ~/.ssh/id_ed25519     default key")
  }
}

def file-hash [file: string]: nothing -> any {
  if ($file | path exists) { open --raw $file | hash sha256 } else { null }
}

def cleanup-identity [identity: record<path: string, tmpfile: string>] {
  if ($identity.tmpfile | is-not-empty) { rm --force $identity.tmpfile }
}

# Edit an age secret and rekey all recipients if it changed.
def main [
  file: string             # secret file to edit, e.g. secret/terraform.tfvars.age
  --identity (-i): string  # path to an age/SSH private key used to decrypt
  --op: string             # 1Password op:// URI to read the identity from
  --op-account: string     # 1Password account to use with --op
] {
  let identity = (resolve-identity --identity $identity --op $op --op-account $op_account)

  let before = (file-hash $file)

  # ragenix -e launches $EDITOR, so run it directly to inherit the terminal --
  # capturing it via `complete` would break the interactive editor. try/catch
  # guarantees the 1Password temp key is removed even when the edit fails.
  try {
    ^ragenix --rules ./secret/secrets.nix -i $identity.path -e $file
  } catch {|err|
    cleanup-identity $identity
    error make --unspanned { msg: $"failed to edit ($file): ($err.msg)" }
  }

  let after = (file-hash $file)

  if $before != $after {
    print $"($file) changed -- rekeying all recipients"
    try {
      ^ragenix --rules ./secret/secrets.nix -i $identity.path -r
    } catch {|err|
      cleanup-identity $identity
      error make --unspanned {
        msg: ($"rekey failed: ($err.msg)\n"
          + $"WARNING: ($file) may not be encrypted for every recipient yet -- "
          + "re-run this command to finish rekeying before deploying.")
      }
    }
  } else {
    print $"($file) unchanged -- skipping rekey"
  }

  cleanup-identity $identity
}
