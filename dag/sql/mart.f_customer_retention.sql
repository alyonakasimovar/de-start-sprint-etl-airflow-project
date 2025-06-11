INSERT INTO mart.f_customer_retention
    WITH weekly_orders AS (
        SELECT 
            dc.week_of_year as week_num,
            item_id,
            customer_id,
            COUNT(uniq_id) AS orders_count,
            SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunds_count,
            SUM(CASE WHEN status != 'refunded' THEN payment_amount ELSE 0 END) AS revenue
        FROM staging.user_order_log uol
        left join mart.d_calendar as dc on uol.date_time::Date = dc.date_actual
        WHERE dc.date_actual BETWEEN 
            DATE_TRUNC('week', '{{ds}}'::DATE) AND
            DATE_TRUNC('week', '{{ds}}'::DATE) + INTERVAL '6 days'
        GROUP BY dc.week_of_year, item_id, customer_id
    )
    SELECT
        'weekly' AS period_name,
        week_num AS period_id,
        item_id,
        COUNT(DISTINCT CASE WHEN orders_count = 1 THEN customer_id END) AS new_customers_count,
        COUNT(DISTINCT CASE WHEN orders_count > 1 THEN customer_id END) AS returning_customers_count,
        COUNT(DISTINCT CASE WHEN refunds_count > 0 THEN customer_id END) AS refunded_customer_count,
        SUM(CASE WHEN orders_count = 1 THEN revenue ELSE 0 END) AS new_customers_revenue,
        SUM(CASE WHEN orders_count > 1 THEN revenue ELSE 0 END) AS returning_customers_revenue,
        SUM(refunds_count) AS customers_refunded
    FROM weekly_orders
    GROUP BY week_num, item_id
    -- Обновляем, если данные за текущую неделю уже существуют
    ON CONFLICT (period_id, item_id) DO UPDATE SET
        new_customers_count = EXCLUDED.new_customers_count,
        returning_customers_count = EXCLUDED.returning_customers_count,
        refunded_customer_count = EXCLUDED.refunded_customer_count,
        new_customers_revenue = EXCLUDED.new_customers_revenue,
        returning_customers_revenue = EXCLUDED.returning_customers_revenue,
        customers_refunded = EXCLUDED.customers_refunded;