#!/usr/bin/env bash
set -euo pipefail

# Daily storage report for operational capacity planning.
# Writes markdown report + appends machine-readable history CSV.

REPORT_PATH="${1:-/opt/icarus/audit-results/storage_weekly_capacity.md}"
HISTORY_CSV="${2:-/opt/icarus/audit-results/storage-capacity/history.csv}"
NOW_UTC="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
NOW_DATE="$(date -u +'%Y-%m-%d')"

mkdir -p "$(dirname "$REPORT_PATH")" "$(dirname "$HISTORY_CSV")"

root_total_gb=$(df -BG --output=size / | tail -n 1 | tr -dc '0-9')
root_used_gb=$(df -BG --output=used / | tail -n 1 | tr -dc '0-9')
root_avail_gb=$(df -BG --output=avail / | tail -n 1 | tr -dc '0-9')
root_used_pct=$(df -P / | awk 'NR==2{print $5}')

data_total_gb=$(df -BG --output=size /data | tail -n 1 | tr -dc '0-9')
data_used_gb=$(df -BG --output=used /data | tail -n 1 | tr -dc '0-9')
data_avail_gb=$(df -BG --output=avail /data | tail -n 1 | tr -dc '0-9')
data_used_pct=$(df -P /data | awk 'NR==2{print $5}')

if [[ ! -f "$HISTORY_CSV" ]]; then
  echo "date,root_used_gb,data_used_gb" > "$HISTORY_CSV"
fi
echo "${NOW_DATE},${root_used_gb},${data_used_gb}" >> "$HISTORY_CSV"

prev_line="$(tail -n 2 "$HISTORY_CSV" | head -n 1)"
growth_data_per_day="n/a"
days_to_90_data="n/a"
if [[ -n "$prev_line" && "$prev_line" != date,* ]]; then
  prev_data_used="$(echo "$prev_line" | awk -F',' '{print $3}')"
  if [[ "$prev_data_used" =~ ^[0-9]+$ ]]; then
    delta=$((data_used_gb - prev_data_used))
    growth_data_per_day="${delta}G/day"
    target_90=$(( (data_total_gb * 90) / 100 ))
    if (( delta > 0 )); then
      remaining=$((target_90 - data_used_gb))
      if (( remaining > 0 )); then
        days_to_90_data="$((remaining / delta)) days"
      else
        days_to_90_data="at_or_above_90%"
      fi
    fi
  fi
fi

{
  echo "# Storage Weekly Capacity"
  echo
  echo "- generated_at_utc: ${NOW_UTC}"
  echo "- policy_thresholds: /data free >= 80G, / free >= 15G"
  echo
  echo "## Filesystem Summary"
  echo
  echo "| mount | total_gb | used_gb | avail_gb | used_pct |"
  echo "|---|---:|---:|---:|---:|"
  echo "| / | ${root_total_gb} | ${root_used_gb} | ${root_avail_gb} | ${root_used_pct} |"
  echo "| /data | ${data_total_gb} | ${data_used_gb} | ${data_avail_gb} | ${data_used_pct} |"
  echo
  echo "## Trend"
  echo
  echo "- /data growth: ${growth_data_per_day}"
  echo "- estimate to 90% /data: ${days_to_90_data}"
  echo
  echo "## Top Directories (/data)"
  echo
  (sudo du -x -h --max-depth=1 /data 2>/dev/null | sort -h | tail -n 15) || true
  echo
  echo "## Top Directories (/opt/icarus)"
  echo
  (du -x -h --max-depth=1 /opt/icarus 2>/dev/null | sort -h | tail -n 20) || true
} > "$REPORT_PATH"

echo "report_path=$REPORT_PATH"
echo "history_path=$HISTORY_CSV"
