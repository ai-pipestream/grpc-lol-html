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

# Each fixture with the arguments it wants: its own encoding where that is not
# UTF-8, and the flags that make the interesting events show up.
declare -A ARGS=(
  [ambiguity_select_xmp_script.html]="*"
  [cdata_svg.html]="* --spans"
  [charset_meta_windows1251.html]="* --encoding=windows-1251"
  [doctype_legacy.html]="*"
  [duplicate_and_bare_attrs.html]="* --spans"
  [script_and_style_text.html]="* --script-text"
  [text_split_boundary.html]="p"
  [unclosed_tags.html]="*"
)

if [ "$#" -gt 0 ]; then
  fixtures=("$@")
else
  mapfile -t fixtures < <(printf '%s\n' "${!ARGS[@]}" | sort)
fi

TMP="${TMPDIR:-/tmp}"
failures=0

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
