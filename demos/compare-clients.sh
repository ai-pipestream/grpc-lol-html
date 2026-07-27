#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
#
# Run all three demo clients over every fixture and diff their output.
#
# The three clients exist to be compared. Each one reads the same contract in
# a different language with a different generated-code toolchain, so agreement
# across all three is evidence the contract says what it means, and a
# disagreement is evidence that one of them is guessing.
#
# Needs a running server:  cargo run --release
#
#   ./compare-clients.sh                 # every fixture
#   ./compare-clients.sh cdata_svg.html  # one fixture

set -uo pipefail
cd "$(dirname "$0")"

# Globbing off for the whole script: several fixtures use `*` as a selector,
# and the shell will happily turn that into a directory listing.
set -f

# Arguments a fixture wants beyond the `*` default: its own encoding where that
# is not UTF-8, and the flags that make its interesting events show up.
declare -A ARGS=(
  [cdata_svg.html]="* --spans --all-text"
  [charset_meta_windows1251.html]="* --encoding=windows-1251"
  [duplicate_and_bare_attrs.html]="* --spans"
  # `script[type='application/ld+json']` is the selector you would actually
  # write, and it cannot be compared: Maven flattens `-Dexec.args` into one
  # string and strips the quotes, so Java alone receives
  # `script[type=application/ld+json]`, which is not valid CSS. Any selector
  # with a quoted attribute value has the same problem. `$=` says the same
  # thing here without them.
  [json_ld_product.html]="script[type\$=json] --script-text"
  [plaintext_tail.html]="* --all-text"
  [script_and_style_text.html]="* --script-text"
  [spa_shell.html]="* --script-text"
  [text_split_boundary.html]="p"
)

# Fixtures the comparison cannot say anything about, with the reason. Kept as
# an explicit list rather than by omission, so that "not compared" is a
# decision somebody made rather than one that happened.
declare -A SKIP=(
  [utf16.html]="rejected before any events, so all three print nothing and agree vacuously"
  [deep_nesting.html]="2000 nested divs, minutes of Maven startup for no additional coverage"
)

# The sweep is the directory, not a hand-kept list. A fixture added for a Rust
# test used to have to be remembered here as well, and twice it was not:
# json_ld_product.html and spa_shell.html sat outside the comparison for as
# long as they existed.
if [ "$#" -gt 0 ]; then
  fixtures=("$@")
else
  set +f
  mapfile -t all < <(cd sample-data && printf '%s\n' *.html | sort)
  set -f
  fixtures=()
  for fixture in "${all[@]}"; do
    if [ -n "${SKIP[$fixture]:-}" ]; then
      printf '  skipped   %-40s %s\n' "$fixture" "${SKIP[$fixture]}"
    else
      fixtures+=("$fixture")
    fi
  done
fi

TMP="${TMPDIR:-/tmp}"
failures=0

# Build the Java client once, up front. `exec:java` runs whatever is in
# target/classes and does not compile first, so without this the comparison
# will happily run a stale client and report agreement about code that is no
# longer there.
( cd java-client && mvn -q compile ) || { echo "the Java client did not build"; exit 1; }

for fixture in "${fixtures[@]}"; do
  read -r -a args <<< "${ARGS[$fixture]:-*}"

  node node-client/cli.js "sample-data/$fixture" "${args[@]}" \
      > "$TMP/lolhtml-node.txt" 2>/dev/null

  ( cd python-client && ./run.sh "../sample-data/$fixture" "${args[@]}" ) \
      > "$TMP/lolhtml-python.txt" 2>/dev/null

  # exec.args is one string, so the argument list has to be flattened. Paths
  # are resolved from java-client/, hence the extra `../`.
  ( cd java-client && mvn -q exec:java \
      -Dexec.args="../sample-data/$fixture ${args[*]}" 2>/dev/null ) \
      | grep -v '^WARNING' > "$TMP/lolhtml-java.txt"

  if diff -q "$TMP/lolhtml-node.txt" "$TMP/lolhtml-python.txt" >/dev/null \
     && diff -q "$TMP/lolhtml-node.txt" "$TMP/lolhtml-java.txt" >/dev/null; then
    printf '  ok        %-40s %4s lines\n' "$fixture" "$(wc -l < "$TMP/lolhtml-node.txt")"
  else
    printf '  DIVERGED  %s\n' "$fixture"
    diff -u "$TMP/lolhtml-node.txt" "$TMP/lolhtml-python.txt" | head -20
    diff -u "$TMP/lolhtml-node.txt" "$TMP/lolhtml-java.txt" | head -20
    failures=$((failures + 1))
  fi
done

if [ "$failures" -gt 0 ]; then
  echo "$failures fixture(s) diverged"
  exit 1
fi
echo "all three clients agree on every fixture"
