#!/usr/bin/env nu

# Patches upstream rain.math.float to silence `unused_doc_comments` on the
# `///` block above a `prop_compose!` macro invocation. The workspace builds
# with `-D warnings`, which rejects the upstream as-is. Invoked from the
# `rainMathFloatSrc` derivation in flake.nix. Remove once RAI-418 is fixed
# upstream.
#
# Anchors on content rather than line numbers so the build fails loudly on
# upstream drift instead of silently producing an unpatched source.

def main [src_dir: path] {
  let target = ([$src_dir crates float src fuzz_ops.rs] | path join)

  let anchors = [
    "Generate floats in a range where f64"
    "overflow/underflow. Coefficients up to"
    "keep values in f64"
  ]

  $anchors
    | reduce --fold (open --raw $target) {|anchor, contents|
        let needle = $"    /// ($anchor)"
        if not ($contents | str contains $needle) {
          print -e $"patch-rain-math-float: anchor not found in ($target): ($needle)"
          print -e "                        upstream drift -- update anchors or remove the patch."
          exit 1
        }
        $contents | str replace --all $needle $"    // ($anchor)"
      }
    | save -f --raw $target
}
