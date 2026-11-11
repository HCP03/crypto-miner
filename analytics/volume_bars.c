#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <duckdb.h>

typedef struct {
    double price; double qty; double quoteQty; long long timestamp; int taker;
} Trade;

typedef struct {
    long long start_timestamp; long long end_timestamp;
    double open, high, low, close; double volume, contracts;
    double taker_buy_vol, taker_sell_vol, taker_delta;
    double taker_buy_contracts, taker_sell_contracts, taker_delta_contracts;
    double active_threshold; // Saved to DB so we know what threshold built this bar
    int is_new_bar;
} BarState;

typedef struct {
    long long day_start_ms;
    double threshold;
} DailyThreshold;

void append_bar_to_db(duckdb_appender *appender, BarState *state) {
    duckdb_append_int64(*appender, state->start_timestamp);
    duckdb_append_int64(*appender, state->end_timestamp);
    duckdb_append_double(*appender, state->open);
    duckdb_append_double(*appender, state->high);
    duckdb_append_double(*appender, state->low);
    duckdb_append_double(*appender, state->close);
    duckdb_append_double(*appender, state->volume);
    duckdb_append_double(*appender, state->contracts);
    duckdb_append_double(*appender, state->taker_buy_vol);
    duckdb_append_double(*appender, state->taker_sell_vol);
    duckdb_append_double(*appender, state->taker_delta);
    duckdb_append_double(*appender, state->taker_buy_contracts);
    duckdb_append_double(*appender, state->taker_sell_contracts);
    duckdb_append_double(*appender, state->taker_delta_contracts);
    duckdb_append_double(*appender, state->active_threshold);
    duckdb_appender_end_row(*appender);
}

void compute_volume_bars(const char *db_path, int threshold_dollars, long long req_start_ms, long long req_end_ms) {
    duckdb_database db; 
    duckdb_connection conn;
    
    if (duckdb_open(db_path, &db) == DuckDBError) {
        fprintf(stderr, "Error: Could not open DuckDB. Close the Python connection first!\n");
        return;
    }
    
    if (duckdb_connect(db, &conn) == DuckDBError) {
        fprintf(stderr, "Error: Could not connect to DuckDB.\n");
        duckdb_close(&db);
        return;
    }

    duckdb_query(conn, "PRAGMA memory_limit='2GB'", NULL);

    char table_name[64];
    if (threshold_dollars == 0) {
        snprintf(table_name, sizeof(table_name), "candles_volume_dinamic");
    } else {
        snprintf(table_name, sizeof(table_name), "bars_volume_%d", threshold_dollars);
    }

    // 1. Ensure Table Exists
    char create_sql[1024];
    snprintf(create_sql, sizeof(create_sql), 
        "CREATE TABLE IF NOT EXISTS %s ("
        "start_timestamp BIGINT PRIMARY KEY, end_timestamp BIGINT, "
        "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
        "volume DOUBLE, contracts DOUBLE, "
        "taker_buy_vol DOUBLE, taker_sell_vol DOUBLE, taker_delta DOUBLE, "
        "taker_buy_contracts DOUBLE, taker_sell_contracts DOUBLE, taker_delta_contracts DOUBLE, "
        "active_threshold DOUBLE)", table_name);
    duckdb_query(conn, create_sql, NULL);

    // 2. Load Dynamic Thresholds into memory (if dynamic mode)
    DailyThreshold *thresholds = NULL;
    int num_days = 0;
    int current_day_idx = 0;
    double target_volume = (double)threshold_dollars; // Defaults to static if > 0

    if (threshold_dollars == 0) {
        duckdb_result thresh_res;
        if (duckdb_query(conn, "SELECT day_start_ms, active_threshold FROM daily_thresholds WHERE active_threshold IS NOT NULL ORDER BY day_start_ms ASC", &thresh_res) != DuckDBError) {
            num_days = (int)duckdb_row_count(&thresh_res);
            thresholds = malloc(num_days * sizeof(DailyThreshold));
            for (int i = 0; i < num_days; i++) {
                thresholds[i].day_start_ms = duckdb_value_int64(&thresh_res, 0, i);
                thresholds[i].threshold = duckdb_value_double(&thresh_res, 1, i);
            }
            duckdb_destroy_result(&thresh_res);
            if (num_days > 0) target_volume = thresholds[0].threshold; // Init to first day
        }
    }

    // 3. Smart Check: Resume logic (Delete incomplete bar)
    long long pagination_timestamp = req_start_ms - 1; 
    char check_sql[256];
    snprintf(check_sql, sizeof(check_sql), "SELECT start_timestamp, end_timestamp FROM %s ORDER BY start_timestamp DESC LIMIT 1", table_name);
    
    duckdb_result res;
    if (duckdb_query(conn, check_sql, &res) != DuckDBError && duckdb_row_count(&res) > 0) {
        long long last_start = duckdb_value_int64(&res, 0, 0);
        long long last_end = duckdb_value_int64(&res, 1, 0);
        duckdb_destroy_result(&res);
        
        char new_trades_sql[256];
        snprintf(new_trades_sql, sizeof(new_trades_sql), "SELECT 1 FROM trades WHERE timestamp > %lld LIMIT 1", last_end);
        duckdb_result new_trades_res;
        
        if (duckdb_query(conn, new_trades_sql, &new_trades_res) != DuckDBError) {
            if ((int)duckdb_row_count(&new_trades_res) > 0) {
                char delete_sql[256];
                snprintf(delete_sql, sizeof(delete_sql), "DELETE FROM %s WHERE start_timestamp >= %lld", table_name, last_start);
                duckdb_query(conn, delete_sql, NULL);
                pagination_timestamp = ((req_start_ms > last_start) ? req_start_ms : last_start) - 1;
            } else {
                pagination_timestamp = last_end; // Skip loop entirely, no new data
            }
            duckdb_destroy_result(&new_trades_res);
        }
    } else {
        duckdb_destroy_result(&res);
    }

    // 4. Chunk iteration
    duckdb_appender appender;
    duckdb_appender_create(conn, NULL, table_name, &appender);
    
    BarState state = {0};
    state.is_new_bar = 1;
    int chunk_size = 100000;
    int has_more_data = 1;

    while (has_more_data) {
        char fetch_sql[512];
        char end_cond[64] = "";
        if (req_end_ms > 0) snprintf(end_cond, sizeof(end_cond), "AND timestamp < %lld", req_end_ms);
        
        snprintf(fetch_sql, sizeof(fetch_sql), 
            "SELECT price, quoteQty, qty, timestamp, taker FROM trades "
            "WHERE timestamp > %lld %s ORDER BY timestamp ASC LIMIT %d", 
            pagination_timestamp, end_cond, chunk_size);

        if (duckdb_query(conn, fetch_sql, &res) == DuckDBError) break;
        int num_trades = (int)duckdb_row_count(&res);
        if (num_trades == 0) {
            duckdb_destroy_result(&res);
            break;
        }

        for (int r = 0; r < num_trades; r++) {
            double price = duckdb_value_double(&res, 0, r);
            double rem_qty = duckdb_value_double(&res, 1, r);
            double rem_contracts = duckdb_value_double(&res, 2, r);
            long long ts = duckdb_value_int64(&res, 3, r);
            int taker = duckdb_value_int32(&res, 4, r);
            pagination_timestamp = ts;

            // DYNAMIC UPDATE: Fast array lookup to shift the threshold when day changes
            if (threshold_dollars == 0 && num_days > 0) {
                while (current_day_idx < num_days - 1 && ts >= thresholds[current_day_idx + 1].day_start_ms) {
                    current_day_idx++;
                    target_volume = thresholds[current_day_idx].threshold;
                }
            }

            while (rem_qty > 0) {
                if (state.is_new_bar) {
                    memset(&state, 0, sizeof(BarState));
                    state.open = state.high = state.low = price;
                    state.start_timestamp = ts;
                    state.active_threshold = target_volume; // Save active threshold
                    state.is_new_bar = 0;
                }

                if (price > state.high) state.high = price;
                if (price < state.low) state.low = price;
                state.close = price;
                state.end_timestamp = ts;

                double qty_needed = target_volume - state.volume;
                double fraction = (rem_qty >= qty_needed) ? (qty_needed / rem_qty) : 1.0;
                
                double used_qty = rem_qty * fraction;
                double used_contracts = rem_contracts * fraction;

                state.volume += used_qty;
                state.contracts += used_contracts;

                if (taker == 1) { 
                    state.taker_buy_vol += used_qty; state.taker_delta += used_qty;
                    state.taker_buy_contracts += used_contracts; state.taker_delta_contracts += used_contracts;
                } else if (taker == -1) { 
                    state.taker_sell_vol += used_qty; state.taker_delta -= used_qty;
                    state.taker_sell_contracts += used_contracts; state.taker_delta_contracts -= used_contracts;
                }

                rem_qty -= used_qty;
                rem_contracts -= used_contracts;

                if (state.volume >= target_volume - 0.00001) { 
                    append_bar_to_db(&appender, &state);
                    state.is_new_bar = 1;
                }
            }
        }
        duckdb_destroy_result(&res);
    }

    if (!state.is_new_bar && state.volume > 0) {
        append_bar_to_db(&appender, &state);
    }

    if (thresholds != NULL) free(thresholds);
    duckdb_appender_flush(appender);
    duckdb_appender_destroy(&appender);
    duckdb_disconnect(&conn);
    duckdb_close(&db);
}