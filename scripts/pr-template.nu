#!/usr/bin/env nu

# Audits and maintains GitHub PR bodies against the repository PR template.
# The script is idempotent: rerunning --apply updates only missing sections it
# can fill mechanically and skips PRs that already match the template.

const REQUIRED_SECTIONS = [
  "## What"
  "## Why"
  "## How"
  "## Testing"
  "## Screenshots"
  "## Anything else"
]

def section-block [section: string]: nothing -> string {
  match $section {
    "## Screenshots" => "## Screenshots\n\nN/A"
    _ => $"($section)\n\nTODO: fill this section before requesting review."
  }
}

def insert-section [body: string, section: string]: nothing -> string {
  let block = section-block $section

  if ($body | str contains $section) {
    $body
  } else if $section == "## Screenshots" and ($body | str contains "\n## Anything else") {
    $body | str replace "\n## Anything else" $"\n($block)\n\n## Anything else"
  } else {
    $"($body | str trim)\n\n($block)\n"
  }
}

def normalize-body [body: string]: nothing -> string {
  mut updated = $body

  for section in $REQUIRED_SECTIONS {
    $updated = (insert-section $updated $section)
  }

  $updated
}

def missing-sections [body: string]: nothing -> list<string> {
  $REQUIRED_SECTIONS | where {|section| not ($body | str contains $section) }
}

def unresolved-sections [body: string]: nothing -> list<string> {
  $REQUIRED_SECTIONS
  | where {|section|
    let marker = $"($section)\n\nTODO: fill this section before requesting review."
    $body | str contains $marker
  }
}

def audit-body [body: string]: nothing -> record {
  {
    missing: (missing-sections $body)
    unresolved: (unresolved-sections $body)
  }
}

def resolve-pr [identifier: string]: nothing -> record {
  if ($identifier | str starts-with "#") {
    let number = ($identifier | str substring 1..)
    return (view-pr $number)
  }

  if ($identifier | str contains "/") {
    let prs = (^gh pr list --state all --head $identifier --json number --limit 1 | from json)
    if ($prs | is-empty) {
      error make { msg: $"no PR found for branch ($identifier)" }
    }

    return (view-pr ($prs.0.number | into string))
  }

  view-pr $identifier
}

def view-pr [number: string]: nothing -> record {
  ^gh pr view $number --json number,title,headRefName,url,body | from json
}

def check-pr [pr: record]: nothing -> record {
  let audit = audit-body ($pr.body? | default "")

  {
    number: $pr.number
    branch: $pr.headRefName
    title: $pr.title
    url: $pr.url
    changed: false
    missing: $audit.missing
    unresolved: $audit.unresolved
  }
}

def apply-pr [pr: record]: nothing -> record {
  let body = ($pr.body? | default "")
  let updated = normalize-body $body
  let changed = $updated != $body

  if $changed {
    $updated | ^gh pr edit $pr.number --body-file -
  }

  let audit = audit-body $updated

  {
    number: $pr.number
    branch: $pr.headRefName
    title: $pr.title
    url: $pr.url
    changed: $changed
    missing: $audit.missing
    unresolved: $audit.unresolved
  }
}

def assert [condition: bool, message: string] {
  if not $condition {
    error make { msg: $message }
  }
}

def run-self-test [] {
  let body = "## What\n\nThing\n\n## Why\n\nReason\n\n## How\n\nMechanism\n\n## Testing\n\nTests\n\n## Anything else\n\nNone"
  let updated = normalize-body $body

  assert ($updated | str contains "## Screenshots\n\nN/A\n\n## Anything else") "screenshots section inserted before anything-else"
  assert ((normalize-body $updated) == $updated) "normalization is idempotent"

  let audit = audit-body $updated
  assert (($audit.missing | length) == 0) "self-test body has no missing sections"
  assert (($audit.unresolved | length) == 0) "self-test body has no unresolved sections"

  print "pr-template self-test passed"
}

def main [
  ...identifiers: string
  --apply
  --check
  --self-test
] {
  if $self_test {
    run-self-test
    return
  }

  if ($identifiers | is-empty) {
    error make { msg: "usage: pr-template [--check|--apply] <pr-number|branch>..." }
  }

  if $apply and $check {
    error make { msg: "choose only one of --check or --apply" }
  }

  let mode = if $apply { "apply" } else { "check" }
  let results = (
    $identifiers
    | each {|identifier|
      let pr = resolve-pr $identifier
      if $mode == "apply" { apply-pr $pr } else { check-pr $pr }
    }
  )

  $results | table --expand

  let failures = (
    $results
    | where {|result| ($result.missing | length) > 0 or ($result.unresolved | length) > 0 }
  )

  if ($failures | is-not-empty) {
    error make { msg: "one or more PR bodies are missing required template content" }
  }
}
