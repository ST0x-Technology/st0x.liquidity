#!/usr/bin/env nu

# Tests for scripts/patch-rain-math-float.nu. Run with `nu
# scripts/patch-rain-math-float.test.nu`. Each case sets up a fixture tree
# under a tempdir, invokes the patch script, and asserts on the result.

const PATCH = path self ../scripts/patch-rain-math-float.nu

def make-fixture [body: string]: nothing -> path {
  let root = (mktemp -d)
  let target_dir = ([$root crates float src] | path join)
  mkdir $target_dir
  $body | save --raw ([$target_dir fuzz_ops.rs] | path join)
  $root
}

# Helper: run the patch script against a fixture and capture exit code +
# stderr without aborting this test runner.
def run-patch [root: path]: nothing -> record {
  let result = (do { nu $PATCH $root } | complete)
  {
    exit_code: $result.exit_code,
    stderr: $result.stderr,
    contents: (open --raw ([$root crates float src fuzz_ops.rs] | path join)),
  }
}

def assert [cond: bool, msg: string] {
  if not $cond {
    print -e $"FAIL: ($msg)"
    exit 1
  }
}

def test-rewrites-all-anchors [] {
  let fixture = (make-fixture "prop_compose! {
    /// Generate floats in a range where f64 can represent them
    /// overflow/underflow. Coefficients up to ~1e15 keep things sane,
    /// keep values in f64's comfortable range.
    fn fuzz_floats()(...) -> Float { ... }
}
")
  let result = (run-patch $fixture)
  assert ($result.exit_code == 0) $"expected exit 0, got ($result.exit_code); stderr: ($result.stderr)"
  assert (not ($result.contents | str contains "    /// Generate floats")) "anchor 1 not rewritten"
  assert (not ($result.contents | str contains "    /// overflow/underflow")) "anchor 2 not rewritten"
  assert (not ($result.contents | str contains "    /// keep values in f64")) "anchor 3 not rewritten"
  assert ($result.contents | str contains "    // Generate floats") "anchor 1 missing line-comment form"
  assert ($result.contents | str contains "    // overflow/underflow") "anchor 2 missing line-comment form"
  assert ($result.contents | str contains "    // keep values in f64") "anchor 3 missing line-comment form"
  print "ok: rewrites all anchors"
}

def test-fails-loudly-on-drift [] {
  let fixture = (make-fixture "prop_compose! {
    /// Generate floats in a range where f64 can represent them
    /// keep values in f64's comfortable range.
    fn fuzz_floats()(...) -> Float { ... }
}
")
  let result = (run-patch $fixture)
  assert ($result.exit_code != 0) "expected non-zero exit on missing anchor"
  assert ($result.stderr | str contains "anchor not found") $"expected drift message, got: ($result.stderr)"
  print "ok: fails loudly on upstream drift"
}

def test-leaves-unrelated-content-untouched [] {
  let original = "// pre-existing line comment
fn unrelated() {
    /// genuine doc comment that should remain
    let x = 1;
}

prop_compose! {
    /// Generate floats in a range where f64 can represent them
    /// overflow/underflow. Coefficients up to ~1e15
    /// keep values in f64's range.
    fn fuzz_floats()(...) -> Float { ... }
}
"
  let fixture = (make-fixture $original)
  let result = (run-patch $fixture)
  assert ($result.exit_code == 0) $"unexpected failure: ($result.stderr)"
  assert ($result.contents | str contains "/// genuine doc comment that should remain") "unrelated doc comment was rewritten"
  assert ($result.contents | str contains "// pre-existing line comment") "pre-existing comment was disturbed"
  print "ok: leaves unrelated content untouched"
}

def main [] {
  test-rewrites-all-anchors
  test-fails-loudly-on-drift
  test-leaves-unrelated-content-untouched
  print "all tests passed"
}
