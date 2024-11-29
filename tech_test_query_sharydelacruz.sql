WITH AllDates AS (
    SELECT generate_series(DATE '2020-06-01', DATE '2020-09-30', INTERVAL '1 day')::date AS dt_report
),
ExistingUsers AS (
    SELECT
          login_hash
        , currency
    FROM users
    WHERE enable = 1
),
TradeData AS (
    SELECT
          t.close_time::date AS dt_report
        , t.login_hash
        , t.server_hash
        , t.symbol
        , eu.currency
        , t.volume
        , t.close_time
        , COUNT(*) OVER (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.close_time) AS trade_count
        , SUM(t.volume) OVER (PARTITION BY t.login_hash, t.server_hash, t.symbol ORDER BY t.close_time) AS cumulative_volume
    FROM trades t
    INNER JOIN ExistingUsers eu ON t.login_hash = eu.login_hash
    WHERE (t.symbol ~ '[a-zA-Z0-9]'
        AND t.volume > 0)
),
AllTradeData AS (
    SELECT
          ad.dt_report
        , td.login_hash
        , td.server_hash
        , td.symbol
        , td.currency
        , COALESCE(td.volume, 0) AS volume
        , td.close_time
    FROM AllDates ad
    LEFT JOIN TradeData td ON ad.dt_report = td.dt_report
    ORDER BY ad.dt_report
),
TradeCalculations AS (
    SELECT
          dt_report
        , login_hash
        , server_hash
        , symbol
        , currency
        , SUM(volume) OVER (PARTITION BY login_hash, server_hash, symbol
            ORDER BY dt_report ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS sum_volume_prev_7d
        , SUM(volume) OVER (PARTITION BY login_hash, server_hash, symbol
            ORDER BY dt_report ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS sum_volume_prev_all
        , COUNT(volume) OVER (PARTITION BY login_hash, server_hash, symbol
            ORDER BY dt_report ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS trade_count
        , SUM(CASE WHEN EXTRACT(MONTH FROM dt_report) = 8 THEN volume ELSE 0 END)
            OVER (PARTITION BY login_hash, server_hash, symbol ORDER BY dt_report) AS sum_volume_2020_08
        , MIN(close_time) OVER (PARTITION BY login_hash, server_hash, symbol ORDER BY dt_report) AS date_first_trade
    FROM AllTradeData
),
RankedData AS (
    SELECT
          *
        , DENSE_RANK() OVER (PARTITION BY symbol ORDER BY sum_volume_prev_7d DESC) AS rank_volume_symbol_prev_7d
        , DENSE_RANK() OVER (PARTITION BY login_hash ORDER BY trade_count) AS rank_count_prev_7d
    FROM TradeCalculations
)
SELECT
      ROW_NUMBER() OVER (ORDER BY dt_report, login_hash, server_hash, symbol) AS row_number
    , dt_report
    , login_hash
    , server_hash
    , symbol
    , currency
    , sum_volume_prev_7d
    , sum_volume_prev_all
    , rank_volume_symbol_prev_7d
    , rank_count_prev_7d
    , sum_volume_2020_08
    , date_first_trade
FROM RankedData
ORDER BY row_number DESC;