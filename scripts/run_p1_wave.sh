#!/usr/bin/env bash
# Run P1 pipelines serially against Neo4j local (fiscal-neo4j docker).
# Usage: bash scripts/run_p1_wave.sh <src1> <src2> ...
set -u
cd "$(dirname "$0")/../etl"

TARGET="--neo4j-uri bolt://localhost:7687 --neo4j-user neo4j --neo4j-database neo4j --neo4j-password changeme"
LOGDIR="../logs/p1_runs"
mkdir -p "$LOGDIR"

SUMMARY="$LOGDIR/_summary.log"
: > "$SUMMARY"

for src in "$@"; do
    echo "=== $(date -Is) $src: download ===" | tee -a "$SUMMARY"
    DL="../scripts/download_${src}.py"
    if [ -f "$DL" ]; then
        if uv run python "$DL" --output-dir "../data/${src}" > "$LOGDIR/${src}.download.log" 2>&1; then
            echo "  download OK" | tee -a "$SUMMARY"
        else
            echo "  download FAIL ($?)  -> see ${src}.download.log" | tee -a "$SUMMARY"
            continue
        fi
    else
        echo "  no download script ($DL) — skipping download, assuming data present" | tee -a "$SUMMARY"
    fi

    echo "=== $(date -Is) $src: etl ===" | tee -a "$SUMMARY"
    if uv run bracc-etl run --source "$src" --data-dir ../data $TARGET > "$LOGDIR/${src}.etl.log" 2>&1; then
        TAIL=$(grep -E "Pipeline complete|Batch written" "$LOGDIR/${src}.etl.log" | tail -3 | tr '\n' '|')
        echo "  etl OK | $TAIL" | tee -a "$SUMMARY"
    else
        EC=$?
        TAIL=$(tail -2 "$LOGDIR/${src}.etl.log" | tr '\n' '|')
        echo "  etl FAIL ($EC) | $TAIL" | tee -a "$SUMMARY"
    fi
done

echo "=== $(date -Is) done ===" | tee -a "$SUMMARY"
