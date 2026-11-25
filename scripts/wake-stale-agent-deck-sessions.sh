#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  wake-stale-agent-deck-sessions.sh [options]
  wake-stale-agent-deck-sessions.sh install [options]

Purpose:
  Find agent-deck sessions that are idle or waiting and have stale unread
  waypost items, then send a wakeup message only after a short confirm-delay
  recheck.

Options:
  --older-than DURATION       Required stale threshold for waypost stale
  --confirm-delay SECONDS     Delay before rechecking a candidate session (default: 2)
  --profile NAME             Agent-deck profile to query
  --state-dir PATH           Waypost state directory to query
  --wake-message TEXT        Ignored; agent-deck wake instruction is fixed
  --all-delivery-states      Pass --all to agent-deck list (include sessions hidden by default)
  -h, --help                 Show help

Install options:
  --prefix PATH              Install under PATH/bin (default: $HOME/.local)
  --bin-dir PATH             Install directly into this directory
  --name NAME                Installed filename (default: wake-stale-agent-deck-sessions.sh)

Exit codes:
  0  Success, or no sessions needed waking
  2  Usage or dependency error
  1  Operational failure while querying or waking one or more sessions
EOF
}

die() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 2
}

fail_op() {
  printf 'ERROR: %s\n' "$*" >&2
  exit 1
}

warn() {
  printf 'WARN: %s\n' "$*" >&2
}

info() {
  printf 'INFO: %s\n' "$*" >&2
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "$1 not found in PATH"
}

script_source_path() {
  local source_dir
  source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
  printf '%s/%s\n' "$source_dir" "$(basename "${BASH_SOURCE[0]}")"
}

install_self() {
  local prefix_dir="$1"
  local bin_dir="$2"
  local install_name="$3"
  local source_path target_path

  source_path="$(script_source_path)"
  target_path="${bin_dir%/}/${install_name}"

  if [[ -e "$source_path" && -e "$target_path" && "$source_path" == "$target_path" ]]; then
    info "already installed at $target_path"
    return 0
  fi

  if command -v install >/dev/null 2>&1; then
    mkdir -p "$bin_dir"
    install -m 0755 "$source_path" "$target_path"
  else
    mkdir -p "$bin_dir"
    cp "$source_path" "$target_path"
    chmod 0755 "$target_path"
  fi

  info "installed $install_name to $target_path"
}

ad() {
  if [[ -n "$profile" ]]; then
    agent-deck -p "$profile" "$@"
  else
    agent-deck "$@"
  fi
}

if [[ "${1:-}" == "install" ]]; then
  shift

  prefix_dir="${HOME}/.local"
  bin_dir=""
  install_name="wake-stale-agent-deck-sessions.sh"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      --prefix)
        (( $# >= 2 )) || die "$1 requires a value"
        prefix_dir="${2:-}"
        shift 2
        ;;
      --bin-dir)
        (( $# >= 2 )) || die "$1 requires a value"
        bin_dir="${2:-}"
        shift 2
        ;;
      --name)
        (( $# >= 2 )) || die "$1 requires a value"
        install_name="${2:-}"
        shift 2
        ;;
      -h|--help) usage; exit 0 ;;
      *) die "unknown install option: $1" ;;
    esac
  done

  if [[ -z "$bin_dir" ]]; then
    [[ -n "$prefix_dir" ]] || die "--prefix is required when --bin-dir is not provided"
    bin_dir="${prefix_dir%/}/bin"
  fi
  [[ -n "$bin_dir" ]] || die "--bin-dir is required"
  [[ -n "$install_name" ]] || die "--name is required"

  install_self "$prefix_dir" "$bin_dir" "$install_name"
  exit 0
fi

run_waypost() {
  if [[ -n "$state_dir" ]]; then
    waypost --state-dir "$state_dir" "$@"
  else
    waypost "$@"
  fi
}

session_status_is_wakeable() {
  local status="${1:-}"
  case "${status,,}" in
    waiting|idle) return 0 ;;
    *) return 1 ;;
  esac
}

list_sessions_json() {
  if (( list_all )); then
    ad list --all --json
  else
    ad list --json
  fi
}

readonly jq_common_defs='
  def single_doc: if length != 1 then error("expected single JSON document") else .[0] end;
  def clean: tostring | gsub("[\u001f\r\n]+"; " ");
  def valid_token: (type == "string") and test("\\A[^[:space:][:cntrl:]]+\\z");
  def non_neg_int: (type == "number") and (floor == .) and (. >= 0) and (tostring | test("\\A[0-9]+\\z"));
'

extract_candidate_sessions() {
  jq -s -r "${jq_common_defs}"'
    single_doc
    | def sessions:
      if type == "array" then
        .
      elif type == "object" and has("sessions") and (.sessions == null or (.sessions | type) == "array") then
        (.sessions // [])
      elif type == "object" and has("items") and (.items == null or (.items | type) == "array") then
        (.items // [])
      else
        error("expected session list array or object with array sessions/items")
      end;

    sessions
    | .[]
    | if type != "object" then
        error("expected session object")
      elif .status != null and (.status | type) != "string" then
        error("expected string session status")
      elif .id != null and (.id | type) != "string" then
        error("expected string session id")
      elif .title != null and (.title | type) != "string" then
        error("expected string session title")
      else
        .
      end
    | ((.status // "") | ascii_downcase) as $status
    | select($status == "waiting" or $status == "idle")
    | select((.id // "") != "")
    | if (.id | valid_token | not) then
        error("expected valid session id")
      else
        .
      end
    | [.id, $status, ((.title // "") | clean)] | join("\u001f")
  '
}

older_than=""
confirm_delay_seconds=2
profile=""
state_dir=""
readonly fixed_wake_message="NOTICE: There might be new delivery in waypost."
list_all=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --older-than)
      (( $# >= 2 )) || die "$1 requires a value"
      older_than="${2:-}"
      shift 2
      ;;
    --confirm-delay)
      (( $# >= 2 )) || die "$1 requires a value"
      confirm_delay_seconds="${2:-}"
      shift 2
      ;;
    --profile)
      (( $# >= 2 )) || die "$1 requires a value"
      profile="${2:-}"
      shift 2
      ;;
    --state-dir)
      (( $# >= 2 )) || die "$1 requires a value"
      state_dir="${2:-}"
      shift 2
      ;;
    --wake-message)
      (( $# >= 2 )) || die "$1 requires a value"
      shift 2
      ;;
    --all-delivery-states) list_all=1; shift 1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[[ -n "$older_than" ]] || die "--older-than is required"
[[ "$confirm_delay_seconds" =~ ^[0-9]+$ ]] || die "--confirm-delay must be a non-negative integer"

(( ${BASH_VERSINFO[0]:-0} >= 4 )) || die "bash 4+ is required (found ${BASH_VERSION:-unknown})"

require_cmd agent-deck
require_cmd waypost
require_cmd jq
require_cmd sleep
require_cmd mktemp

cmd_stderr_file="$(mktemp)"
cleanup() {
  rm -f "$cmd_stderr_file"
}
trap cleanup EXIT
trap 'cleanup; trap - INT; kill -INT $$' INT
trap 'cleanup; trap - TERM; kill -TERM $$' TERM

flatten_text() {
  local text="${1:-}"
  text="${text//$'\r'/ }"
  text="${text//$'\n'/ }"
  printf '%s' "$text"
}

read_cmd_stderr() {
  local text=""
  if [[ -s "$cmd_stderr_file" ]]; then
    text="$(<"$cmd_stderr_file")"
  fi
  flatten_text "$text"
}

active_ids=()
declare -A active_status_by_id=()
declare -A active_title_by_id=()

set +e
sessions_json="$(list_sessions_json 2>"$cmd_stderr_file")"
sessions_status=$?
set -e
if (( sessions_status != 0 )); then
  detail="$(read_cmd_stderr)"
  [[ -n "$detail" ]] || detail="$(flatten_text "$sessions_json")"
  fail_op "agent-deck list failed: $detail"
fi

set +e
active_records="$(extract_candidate_sessions <<<"$sessions_json" 2>"$cmd_stderr_file")"
active_parse_status=$?
set -e
if (( active_parse_status != 0 )); then
  detail="$(read_cmd_stderr)"
  [[ -n "$detail" ]] || detail="invalid JSON output"
  fail_op "failed to parse agent-deck list output: $detail"
fi

while IFS=$'\x1f' read -r session_id session_status session_title; do
  [[ -n "$session_id" ]] || continue
  active_ids+=("$session_id")
  active_status_by_id["$session_id"]="$session_status"
  active_title_by_id["$session_id"]="$session_title"
done <<<"$active_records"

if (( ${#active_ids[@]} == 0 )); then
  info "no idle/waiting sessions found"
  exit 0
fi

stale_addresses=()
declare -A stale_count_by_address=()

stale_args=(stale --older-than "$older_than" --json)
for session_id in "${active_ids[@]}"; do
  stale_args+=(--for "agent-deck/${session_id}")
done

set +e
stale_json="$(run_waypost "${stale_args[@]}" 2>"$cmd_stderr_file")"
stale_status=$?
set -e
if (( stale_status != 0 )); then
  detail="$(read_cmd_stderr)"
  [[ -n "$detail" ]] || detail="$(flatten_text "$stale_json")"
  fail_op "waypost stale failed: $detail"
fi

set +e
stale_records="$(jq -s -r "${jq_common_defs}"'
  single_doc
  | if type != "array" then error("expected stale array") else . end
  | .[]
  | if type != "object" then
      error("expected stale object")
    elif (.address | valid_token | not) or (.address | startswith("agent-deck/") | not) or (.address | length <= 11) then
      error("expected non-empty agent-deck/ address")
    elif (.claimable_count | non_neg_int | not) then
      error("expected non-negative integer claimable_count")
    elif .oldest_eligible_at != null and (.oldest_eligible_at | type) != "string" then
      error("expected string oldest_eligible_at")
    else
      .
    end
  | select(.claimable_count > 0)
  | [.address, ((.oldest_eligible_at // "") | clean), (.claimable_count | tostring)] | join("\u001f")
' <<<"$stale_json" 2>"$cmd_stderr_file")"
stale_parse_status=$?
set -e
if (( stale_parse_status != 0 )); then
  detail="$(read_cmd_stderr)"
  [[ -n "$detail" ]] || detail="invalid JSON output"
  fail_op "failed to parse waypost stale output: $detail"
fi

while IFS=$'\x1f' read -r address oldest_eligible_at claimable_count; do
  [[ -n "$address" ]] || continue
  stale_addresses+=("$address")
  stale_count_by_address["$address"]="$claimable_count"
done <<<"$stale_records"

if (( ${#stale_addresses[@]} == 0 )); then
  info "no stale unread delivery found for active sessions"
  exit 0
fi

failures=0
woken=0
skipped_status=0
skipped_fresh=0

for address in "${stale_addresses[@]}"; do
  session_id="${address#agent-deck/}"
  original_status="${active_status_by_id["$session_id"]:-unknown}"
  title="${active_title_by_id["$session_id"]:-}"
  stale_count="${stale_count_by_address["$address"]}"

  info "candidate session_id=${session_id} status=${original_status} title=${title:-none} stale_count=${stale_count} confirm_delay=${confirm_delay_seconds}s"
  sleep "$confirm_delay_seconds"

  set +e
  session_json="$(ad session show "$session_id" --json 2>"$cmd_stderr_file")"
  session_status=$?
  set -e
  if (( session_status != 0 )); then
    detail="$(read_cmd_stderr)"
    [[ -n "$detail" ]] || detail="$(flatten_text "$session_json")"
    warn "skip session_id=${session_id} reason=session_show_failed details=${detail}"
    failures=$((failures + 1))
    continue
  fi

  set +e
  current_status="$(jq -s -r "${jq_common_defs}"'
    single_doc
    | if type != "object" then
      error("expected session object")
    elif has("success") and .success == false then
      error("session show reported success=false")
    elif (.status | valid_token | not) then
      error("expected non-empty token session status")
    else
      .status
    end
  ' <<<"$session_json" 2>"$cmd_stderr_file")"
  session_parse_status=$?
  set -e
  if (( session_parse_status != 0 )); then
    detail="$(read_cmd_stderr)"
    [[ -n "$detail" ]] || detail="$(flatten_text "$current_status")"
    [[ -n "$detail" ]] || detail="invalid JSON output"
    warn "skip session_id=${session_id} reason=session_show_parse_failed details=${detail}"
    failures=$((failures + 1))
    continue
  fi
  if ! session_status_is_wakeable "$current_status"; then
    printf 'skip session_id=%s reason=status_changed current_status=%s stale_count=%s\n' \
      "$session_id" "${current_status:-unknown}" "$stale_count"
    skipped_status=$((skipped_status + 1))
    continue
  fi

  set +e
  current_stale_json="$(run_waypost stale --for "$address" --older-than "$older_than" --json 2>"$cmd_stderr_file")"
  current_stale_status=$?
  set -e
  if (( current_stale_status != 0 )); then
    detail="$(read_cmd_stderr)"
    [[ -n "$detail" ]] || detail="$(flatten_text "$current_stale_json")"
    warn "skip session_id=${session_id} reason=stale_recheck_failed details=${detail}"
    failures=$((failures + 1))
    continue
  fi
  set +e
  current_stale_count="$(jq -s -r --arg address "$address" "${jq_common_defs}"'
    single_doc
    | if type != "array" then
      error("expected stale array")
    elif length == 0 then
      0
    elif length != 1 then
      error("expected at most one stale entry")
    elif (.[0] | type) != "object" then
      error("expected stale object")
    elif .[0].address != $address then
      error("stale recheck address mismatch")
    elif (.[0].claimable_count | non_neg_int | not) then
      error("expected non-negative integer claimable_count")
    else
      .[0].claimable_count
    end
  ' <<<"$current_stale_json" 2>"$cmd_stderr_file")"
  current_stale_parse_status=$?
  set -e
  if (( current_stale_parse_status != 0 )) || ! [[ "$current_stale_count" =~ ^[0-9]+$ ]]; then
    detail="$(read_cmd_stderr)"
    [[ -n "$detail" ]] || detail="$(flatten_text "$current_stale_count")"
    [[ -n "$detail" ]] || detail="invalid JSON output"
    warn "skip session_id=${session_id} reason=stale_recheck_parse_failed details=${detail}"
    failures=$((failures + 1))
    continue
  fi
  if (( current_stale_count == 0 )); then
    printf 'skip session_id=%s reason=mail_no_longer_stale current_status=%s\n' \
      "$session_id" "${current_status:-unknown}"
    skipped_fresh=$((skipped_fresh + 1))
    continue
  fi

  set +e
  wake_output="$(ad session send --no-wait "$session_id" "$fixed_wake_message" 2>&1)"
  wake_status=$?
  set -e
  if (( wake_status != 0 )); then
    warn "wake failed for session_id=${session_id}: $(flatten_text "$wake_output")"
    failures=$((failures + 1))
    continue
  fi

  printf 'woke session_id=%s status=%s stale_count=%s\n' \
    "$session_id" "$current_status" "$current_stale_count"
  woken=$((woken + 1))
done

printf 'summary active=%d stale=%d woken=%d skipped_status=%d skipped_fresh=%d failures=%d\n' \
  "${#active_ids[@]}" \
  "${#stale_addresses[@]}" \
  "$woken" \
  "$skipped_status" \
  "$skipped_fresh" \
  "$failures"

if (( failures != 0 )); then
  exit 1
fi
