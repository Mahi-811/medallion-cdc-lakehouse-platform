# Enterprise Data Dictionary (Gold Layer & Marts)

This document provides column definitions, data types, nullability, primary/foreign key relationships, and business logic for all Gold dimensional star schema tables and analytical business marts.

---

## 1. Dimensional Model

### 1.1 `gold.dim_date`
**Description**: Calendar date dimension covering 2024 through 2028.  
**Materialization**: Table  
**Update Frequency**: Static / Annual regeneration  

| Column Name | Data Type | Nullable | Key | Description / Calculation |
|---|---|---|---|---|
| `date_key` | DATE | No | PK | Calendar date in `YYYY-MM-DD` ISO format. |
| `year` | INTEGER | No | - | 4-digit calendar year (e.g. 2026). |
| `quarter` | INTEGER | No | - | Calendar quarter (1, 2, 3, 4). |
| `month` | INTEGER | No | - | Calendar month (1 - 12). |
| `month_name` | VARCHAR | No | - | Full English month name (e.g. 'October'). |
| `month_short` | VARCHAR | No | - | 3-letter month abbreviation (e.g. 'Oct'). |
| `day_of_month` | INTEGER | No | - | Day of the month (1 - 31). |
| `day_of_week_num` | INTEGER | No | - | ISO day of the week (0 = Sunday ... 6 = Saturday). |
| `day_name` | VARCHAR | No | - | Full day name (e.g. 'Wednesday'). |
| `is_weekend` | BOOLEAN | No | - | True if day is Saturday or Sunday. |
| `is_holiday` | BOOLEAN | No | - | True if standard US federal holiday. |
| `fiscal_year` | INTEGER | No | - | Corporate fiscal year (Feb 1 - Jan 31). |
| `fiscal_quarter` | INTEGER | No | - | Corporate fiscal quarter (1, 2, 3, 4). |

---

### 1.2 `gold.dim_customer`
**Description**: Slowly Changing Dimension Type 2 (SCD2) tracking historical customer profile evolutions.  
**Materialization**: Table  
**Update Frequency**: Micro-batch CDC sync  

| Column Name | Data Type | Nullable | Key | Description / Calculation |
|---|---|---|---|---|
| `customer_sk` | VARCHAR | No | PK | Surrogate key: `MD5(customer_id \|\| valid_from)`. |
| `customer_id` | BIGINT | No | Natural Key | Source transactional customer ID. |
| `first_name` | VARCHAR | No | - | Customer given name. |
| `last_name` | VARCHAR | No | - | Customer surname. |
| `full_name` | VARCHAR | No | - | Concatenated full name. |
| `email` | VARCHAR | No | - | Lowercased, trimmed primary contact email. |
| `phone` | VARCHAR | Yes | - | Standardized contact telephone number. |
| `address` | VARCHAR | Yes | - | Primary delivery street address. |
| `city` | VARCHAR | Yes | - | Delivery city. |
| `loyalty_tier` | VARCHAR | No | - | Loyalty membership level: `BRONZE`, `SILVER`, `GOLD`, `PLATINUM`. |
| `version_num` | INTEGER | No | - | Sequential version number for this customer record (1, 2, ...). |
| `valid_from` | TIMESTAMP | No | - | Timestamp when this profile version became effective. |
| `valid_to` | TIMESTAMP | No | - | Timestamp when this profile version was superseded (or `9999-12-31`). |
| `is_current` | BOOLEAN | No | - | True if record is the current active customer state. |

---

### 1.3 `gold.dim_product`
**Description**: Conformed dimensional catalog with unit costs, margins, and active inventory levels.  
**Materialization**: Table  

| Column Name | Data Type | Nullable | Key | Description / Calculation |
|---|---|---|---|---|
| `product_id` | BIGINT | No | PK | Unique product identifier (`0` = Default/Unknown). |
| `sku` | VARCHAR | No | - | Global stock keeping unit code. |
| `product_name` | VARCHAR | No | - | Commercial catalog item title. |
| `category` | VARCHAR | No | - | Merchandise category (e.g. 'Electronics'). |
| `unit_cost` | DECIMAL(10, 2) | No | - | Cost of goods per unit. |
| `list_price` | DECIMAL(10, 2) | No | - | Standard retail listing price. |
| `gross_margin_amount` | DECIMAL(10, 2) | No | - | `list_price - unit_cost`. |
| `gross_margin_pct` | DOUBLE | No | - | `((list_price - unit_cost) / list_price) * 100`. |
| `available_stock` | BIGINT | No | - | Current inventory available in fulfillment centers. |
| `supplier_id` | BIGINT | Yes | - | Supplier company identifier. |
| `supplier_name` | VARCHAR | Yes | - | Supplier business name. |
| `replenishment_lead_days` | BIGINT | No | - | Expected days for restocking lead time. |
| `latest_restock_date` | DATE | Yes | - | Date of most recent inventory arrival. |
| `is_active` | BOOLEAN | No | - | Product active listing flag. |
| `last_catalog_sync_at` | TIMESTAMP | No | - | Timestamp of latest catalog sync. |

---

### 1.4 `gold.fact_orders`
**Description**: Transactional fact table at the order line grain, enriched with SCD2 customer context and weather indicators.  
**Materialization**: Incremental (Partitioned by `order_date_key`, clustered by `customer_id`, `product_id`)  

| Column Name | Data Type | Nullable | Key | Description / Calculation |
|---|---|---|---|---|
| `order_sk` | VARCHAR | No | PK | Surrogate key: `MD5(order_id \|\| item_id)`. |
| `order_id` | BIGINT | No | Natural Key | Transactional order number. |
| `item_id` | BIGINT | No | - | Transactional order item line sequence. |
| `customer_sk` | VARCHAR | No | FK | Join to `dim_customer.customer_sk` (SCD2 match). |
| `customer_id` | BIGINT | No | - | Natural customer key. |
| `product_id` | BIGINT | No | FK | Join to `dim_product.product_id`. |
| `order_date_key` | DATE | No | FK | Join to `dim_date.date_key`. |
| `order_timestamp` | TIMESTAMP | No | - | Precise UTC order placement timestamp. |
| `order_status` | VARCHAR | No | - | Status: `PENDING`, `PROCESSING`, `SHIPPED`, `DELIVERED`, `CANCELLED`. |
| `customer_loyalty_tier_at_order` | VARCHAR | No | - | Customer's loyalty tier at the time order occurred. |
| `shipping_city` | VARCHAR | No | - | Shipping destination city. |
| `quantity` | INTEGER | No | - | Number of units purchased. |
| `unit_price` | DECIMAL(10, 2) | No | - | Price charged per unit. |
| `gross_amount` | DECIMAL(12, 2) | No | - | Total line gross revenue before discounts. |
| `discount_amount` | DECIMAL(12, 2) | No | - | Promotional or loyalty discount applied. |
| `tax_amount` | DECIMAL(12, 2) | No | - | Sales tax charged. |
| `net_amount` | DECIMAL(12, 2) | No | - | Net revenue: `gross - discount + tax`. |
| `weather_temp_c` | DECIMAL(5, 2) | Yes | - | Local mean temperature (°C) on order date. |
| `weather_precipitation_mm` | DECIMAL(5, 2) | Yes | - | Local precipitation (mm) on order date. |
| `weather_condition` | VARCHAR | No | - | Meteorological flag: `PLEASANT`, `RAINY`, `HOT`, `COLD`. |

---

## 2. Business Marts

### 2.1 `marts.mart_daily_revenue`
**Description**: Daily executive rollup of order counts, gross/net revenues, AOV, and cancellation rates.

| Column Name | Data Type | Description |
|---|---|---|
| `report_date` | DATE (PK) | Calendar reporting day. |
| `total_orders` | BIGINT | Total orders placed on this day. |
| `distinct_customers` | BIGINT | Unique paying buyers. |
| `cancelled_orders` | BIGINT | Number of orders cancelled. |
| `delivered_orders` | BIGINT | Number of successfully delivered orders. |
| `gross_revenue` | DECIMAL(12, 2) | Sum of non-cancelled gross amounts. |
| `total_discounts` | DECIMAL(12, 2) | Sum of discounts redeemed. |
| `total_tax` | DECIMAL(12, 2) | Sum of sales tax collected. |
| `net_revenue` | DECIMAL(12, 2) | Realized net revenue. |
| `average_order_value` | DOUBLE | Average basket size (`net_revenue / fulfilled_orders`). |
| `cancellation_rate_pct` | DOUBLE | Percentage of orders cancelled on this date. |

### 2.2 `marts.mart_customer_ltv`
**Description**: Customer lifetime value, tenure, RFM segmentation, and churn risk classification.

| Column Name | Data Type | Description |
|---|---|---|
| `customer_id` | BIGINT (PK) | Natural customer identifier. |
| `full_name` | VARCHAR | Customer contact name. |
| `email` | VARCHAR | Contact email address. |
| `current_loyalty_tier` | VARCHAR | Current loyalty membership tier. |
| `city` | VARCHAR | Primary customer location. |
| `first_order_at` | TIMESTAMP | First historical transaction timestamp. |
| `latest_order_at` | TIMESTAMP | Most recent transaction timestamp. |
| `tenure_days` | BIGINT | Days between first order and current date. |
| `days_since_last_order` | BIGINT | Recency metric (days since latest order). |
| `lifetime_order_count` | BIGINT | Total historical orders placed. |
| `lifetime_spend` | DECIMAL(12, 2) | Total cumulative net spend. |
| `average_basket_size` | DOUBLE | Average net spend per order. |
| `rfm_segment` | VARCHAR | Segment: `Champions`, `Loyal Customers`, `Recent Active`, `Needs Attention`, `At Risk / Churned`. |
| `churn_risk_band` | VARCHAR | Risk classification: `LOW`, `MEDIUM`, `HIGH`. |

### 2.3 `marts.mart_product_performance`
**Description**: SKU sales velocity, realized margins, return/cancellation rates, and category rankings.

| Column Name | Data Type | Description |
|---|---|---|
| `product_id` | BIGINT (PK) | Product dimension key. |
| `sku` | VARCHAR | Stock keeping unit. |
| `product_name` | VARCHAR | Catalog item name. |
| `category` | VARCHAR | Merchandise category. |
| `unit_cost` | DECIMAL(10, 2) | Cost of goods. |
| `list_price` | DECIMAL(10, 2) | Standard price. |
| `available_stock` | BIGINT | On-hand fulfillment stock. |
| `supplier_name` | VARCHAR | Supplying vendor name. |
| `total_order_appearances` | BIGINT | Number of distinct orders containing this SKU. |
| `total_units_sold` | BIGINT | Total volume of units sold. |
| `units_cancelled` | BIGINT | Units cancelled prior to delivery. |
| `total_gross_revenue` | DECIMAL(12, 2) | Gross revenue generated. |
| `total_net_revenue` | DECIMAL(12, 2) | Net revenue realized. |
| `total_gross_margin` | DECIMAL(12, 2) | Total margin: `net_revenue - COGS`. |
| `cancellation_rate_pct` | DOUBLE | Unit cancellation rate percentage. |
| `revenue_rank_overall` | BIGINT | Dense rank across entire catalog by net revenue. |
| `revenue_rank_within_category`| BIGINT | Dense rank within category by net revenue. |
