#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$(dirname "$SCRIPT_DIR")")"
DB_DIR="$PROJECT_DIR/db_files"

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <SYMBOL> [--all]"
    echo ""
    echo "Examples:"
    echo "  $0 VVVUSDT        # check single symbol"
    echo "  $0 --all          # check all symbols"
    echo ""
    echo "Available symbols:"
    ls -1 "$DB_DIR" 2>/dev/null | sed 's/^/  /'
    exit 1
fi

check_symbol() {
    local symbol="$1"
    local db="$DB_DIR/$symbol/trades.db"

    if [[ ! -f "$db" ]]; then
        echo "[$symbol] trades.db not found"
        return
    fi

    echo "=== $symbol ==="

    # Basic stats
    sqlite3 "$db" "
    SELECT
      '  Range:    ' || MIN(datetime(trade_ts_ms/1000, 'unixepoch')) || ' → ' || MAX(datetime(trade_ts_ms/1000, 'unixepoch')),
      '  Trades:   ' || printf('%,d', COUNT(*)),
      '  Span:     ' || ROUND((MAX(trade_ts_ms) - MIN(trade_ts_ms)) / 3600000.0, 1) || 'h'
    FROM agg_trade;
    " | tr '|' '\n'

    # Missing hours
    local missing
    missing=$(sqlite3 "$db" "
    WITH hours AS (
      SELECT DISTINCT trade_ts_ms / 3600000 AS hour_bucket
      FROM agg_trade
    ),
    hour_range AS (
      SELECT MIN(hour_bucket) AS min_h, MAX(hour_bucket) AS max_h FROM hours
    ),
    expected AS (
      SELECT min_h + value AS h
      FROM hour_range, generate_series(0, (SELECT max_h - min_h FROM hour_range))
    )
    SELECT datetime(e.h * 3600, 'unixepoch')
    FROM expected e
    LEFT JOIN hours h ON h.hour_bucket = e.h
    WHERE h.hour_bucket IS NULL
    ORDER BY e.h;
    ")

    if [[ -z "$missing" ]]; then
        echo "  Gaps:     NONE ✓"
    else
        local count
        count=$(echo "$missing" | wc -l)
        echo "  Gaps:     $count missing hour(s) ✗"
        echo "$missing" | head -20 | sed 's/^/    /'
        if [[ $count -gt 20 ]]; then
            echo "    ... and $((count - 20)) more"
        fi
    fi

    # Duplicate check
    local dupes
    dupes=$(sqlite3 "$db" "
    SELECT COUNT(*) FROM (
      SELECT agg_trade_id, COUNT(*) as c
      FROM agg_trade
      GROUP BY agg_trade_id
      HAVING c > 1
      LIMIT 1
    );
    ")
    if [[ "$dupes" == "0" ]]; then
        echo "  Dupes:    NONE ✓"
    else
        local dupe_count
        dupe_count=$(sqlite3 "$db" "
        SELECT COUNT(*) FROM (
          SELECT agg_trade_id FROM agg_trade GROUP BY agg_trade_id HAVING COUNT(*) > 1
        );
        ")
        echo "  Dupes:    $dupe_count duplicate agg_trade_id(s) ✗"
    fi

    echo ""
}

if [[ "$1" == "--all" ]]; then
    for dir in "$DB_DIR"/*/; do
        symbol=$(basename "$dir")
        check_symbol "$symbol"
    done
else
    check_symbol "$1"
fi
