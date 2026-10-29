# Data Lineage & Directed Acyclic Graph (DAG)

This document visualizes and describes the complete data lineage of the Medallion Data Platform from source systems to analytical exposures.

---

## 1. End-to-End Lineage Flow

```mermaid
flowchart LR
    subgraph Sources["Raw External Sources"]
        SRC_API["Open-Meteo REST API"]
        SRC_CSV["Supplier CSV Drops"]
        SRC_PG["Postgres OLTP DB"]
    end

    subgraph Bronze["Bronze Layer (Append-Only Raw)"]
        B_W["bronze_weather_api"]
        B_S["bronze_supplier_inventory_csv"]
        B_O["bronze_orders_cdc"]
        B_C["bronze_customers_cdc"]
        B_P["bronze_products_cdc"]
        B_I["bronze_order_items_cdc"]
    end

    subgraph Silver["Silver Layer (Conformed & Deduplicated)"]
        S_W["silver_weather"]
        S_P["silver_products"]
        S_O["silver_orders"]
        S_C["silver_customers"]
        S_I["silver_order_items"]
    end

    subgraph Gold["Gold Layer (Star Schema Dimensions & Facts)"]
        D_DATE["dim_date"]
        D_CUST["dim_customer (SCD2)"]
        D_PROD["dim_product"]
        F_ORD["fact_orders"]
    end

    subgraph Marts["Analytical Business Marts"]
        M_REV["mart_daily_revenue"]
        M_LTV["mart_customer_ltv"]
        M_PERF["mart_product_performance"]
    end

    subgraph Exposures["Downstream BI & Analytics"]
        E_EXEC["Executive KPI Dashboard"]
        E_RET["Customer Retention Report"]
        E_MERCH["Merchandise Velocity Analytics"]
    end

    SRC_API --> B_W
    SRC_CSV --> B_S
    SRC_PG -.->|"Debezium CDC"| B_O & B_C & B_P & B_I

    B_W --> S_W
    B_S & B_P --> S_P
    B_O --> S_O
    B_C --> S_C
    B_I --> S_I

    S_C --> D_CUST
    S_P --> D_PROD
    S_O & S_I & D_CUST & D_PROD & S_W --> F_ORD

    F_ORD & D_DATE --> M_REV
    F_ORD & D_CUST --> M_LTV
    F_ORD & D_PROD --> M_PERF

    M_REV --> E_EXEC
    M_LTV --> E_RET
    M_PERF --> E_MERCH
```

---

## 2. Transformation Logic by Node

1. **`silver_orders`**: Consumes `bronze_orders_cdc`. Deduplicates mutations via `ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _cdc_ts_ms DESC)`. Filters CDC deletes (`_cdc_op = 'd'`).
2. **`silver_customers`**: Consumes `bronze_customers_cdc`. Trims strings, normalizes emails to lowercase, standardizes loyalty tiers, and prepares historical updates for SCD2 windowing.
3. **`silver_products`**: Joins `bronze_products_cdc` with the latest supplier inventory drop from `bronze_supplier_inventory_csv` to reconcile stock levels and wholesale unit costs.
4. **`silver_weather`**: Cleans weather observations from `bronze_weather_api`, calculates comfort/condition categories (`PLEASANT`, `RAINY`, `HOT`, `COLD`), and deduplicates by `(metric_date, city)`.
5. **`dim_customer`**: Implements Slowly Changing Dimension Type 2 (SCD2). Generates surrogate key `customer_sk = MD5(customer_id || valid_from)`, calculating validity intervals `valid_from` to `valid_to` and `is_current` active flags.
6. **`fact_orders`**: Primary transactional fact table. Enriches order line items with point-in-time SCD2 customer dimensions (`order_timestamp >= valid_from AND order_timestamp < valid_to`) and order-date weather observations.
7. **Business Marts**: Aggregations for daily revenue, customer lifetime value, and SKU performance.
