#!/usr/bin/env bash
# Copyright The Linux Foundation and each contributor to CDP.
# SPDX-License-Identifier: MIT
#
# Guard hook: keeps `gh api` read-only.
# Used by Claude Code PreToolUse hook on Bash operations.
#
# The permissions allow-list auto-approves `gh api repos*` so the review-pr
# skill can read PR comments, reviews, and commits. A prefix rule cannot see
# the HTTP method, and `gh api` switches from GET to POST the moment any
# parameter is added (`-f`/`-F`), or to anything at all via `-X/--method`.
# This hook closes that gap: reads pass through, writes are blocked.
#
# Exit code 0 = allow. Exit code 2 = block (stderr is shown to Claude).

set -euo pipefail

# Read tool input from stdin (JSON with a command field). The flags are
# matched on the raw JSON rather than an extracted command string, so a
# quote inside the command cannot hide a flag from the guard. That can
# over-match a flag mentioned inside e.g. a --jq expression; the failure
# mode is a block message asking to rephrase, never a silent write.
INPUT=$(cat)

# Only guard gh api invocations.
if ! echo "$INPUT" | grep -q 'gh api'; then
  exit 0
fi

block() {
  echo "" >&2
  echo "✗ BLOCKED: gh api is allowed for reads only." >&2
  echo "Reason: $1" >&2
  echo "The allow-list approves gh api so /review-pr can fetch PR comments, reviews, and commits." >&2
  echo "A request that changes state needs to be run by a human, outside this rule." >&2
  echo "" >&2
  exit 2
}

# Long flags are matched as substrings so that quoting cannot hide them:
# the shell strips quotes before gh sees the flag, but the raw hook input
# still contains them, and "--method" is "--method" inside quotes or out.
if echo "$INPUT" | grep -qE -- '--method'; then
  block "--method overrides the GET default."
fi
if echo "$INPUT" | grep -qE -- '--(field|raw-field|input)'; then
  block "request parameters switch gh api from GET to POST."
fi

# Short flags cluster: gh accepts -iX DELETE and -XDELETE as readily as
# -X DELETE (measured; --verbose shows the DELETE request line for each).
# So the guard blocks any short-flag cluster containing X, f, or F. The
# leading character class keeps a path segment like r-Xtra innocent while
# still catching a quoted "-X", whose preceding character is a quote.
if echo "$INPUT" | grep -qE -- '(^|[^[:alnum:]_-])-[[:alpha:]]*X'; then
  block "-X (alone or in a flag cluster) overrides the GET default."
fi
if echo "$INPUT" | grep -qE -- '(^|[^[:alnum:]_-])-[[:alpha:]]*[fF]([^[:alpha:]]|$)'; then
  block "request parameters (-f/-F) switch gh api from GET to POST."
fi

exit 0
