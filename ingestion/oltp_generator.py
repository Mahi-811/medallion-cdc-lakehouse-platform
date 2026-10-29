"""
Postgres OLTP Data Generator and Continuous Mutation Simulator.
Generates realistic e-commerce transactions and mutations:
- Customers (with tier and address updates to drive SCD Type 2)
- Products (with price and inventory adjustments)
- Orders & Order Items (with lifecycle status transitions)
- Soft/Hard deletes to test CDC tombstone handling
- Debezium-compliant CDC envelope serialization
"""
from __future__ import annotations
import json
import time
import random
import logging
from datetime import datetime, date, timedelta
from typing import Any
from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("OLTPGenerator")

fake = Faker()
Faker.seed(42)
random.seed(42)

LOYALTY_TIERS = ["BRONZE", "SILVER", "GOLD", "PLATINUM"]
ORDER_STATUSES = ["PENDING", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]
PRODUCT_CATEGORIES = ["Electronics", "Home & Kitchen", "Apparel", "Beauty", "Sports & Outdoors"]

class OLTPDataSimulator:
    """Simulates an OLTP transactional database and generates CDC event envelopes."""
    def __init__(self):
        self.customers: dict[int, dict[str, Any]] = {}
        self.products: dict[int, dict[str, Any]] = {}
        self.orders: dict[int, dict[str, Any]] = {}
        self.order_items: dict[int, dict[str, Any]] = {}
        self.customer_seq = 1000
        self.product_seq = 200
        self.order_seq = 5000
        self.item_seq = 10000

    def seed(self, num_customers: int = 50, num_products: int = 25, num_orders: int = 200) -> list[dict[str, Any]]:
        """Initial baseline database seed producing 'r' (read/snapshot) CDC events."""
        events: list[dict[str, Any]] = []
        now_ts = int(time.time() * 1000)

        # 1. Seed Products
        for i in range(1, num_products + 1):
            self.product_seq += 1
            cost = round(random.uniform(10.0, 300.0), 2)
            margin = random.uniform(1.2, 1.8)
            price = round(cost * margin, 2)
            prod = {
                "product_id": self.product_seq,
                "sku": f"SKU-{fake.bothify(text='???-####').upper()}",
                "product_name": f"{fake.word().capitalize()} {fake.word().capitalize()}",
                "category": random.choice(PRODUCT_CATEGORIES),
                "unit_cost": cost,
                "list_price": price,
                "stock_quantity": random.randint(20, 500),
                "supplier_id": random.choice([101, 102, 103, 104]),
                "is_active": True,
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            self.products[self.product_seq] = prod
            events.append(self._create_cdc_envelope("products", before=None, after=prod, op="r", ts_ms=now_ts))

        # 2. Seed Customers
        for i in range(1, num_customers + 1):
            self.customer_seq += 1
            cust = {
                "customer_id": self.customer_seq,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.unique.email(),
                "phone": fake.phone_number(),
                "address": fake.street_address(),
                "city": random.choice(["New York", "Los Angeles", "Chicago", "Austin", "Seattle"]),
                "loyalty_tier": random.choice(LOYALTY_TIERS),
                "created_at": datetime.utcnow().isoformat(),
                "updated_at": datetime.utcnow().isoformat()
            }
            self.customers[self.customer_seq] = cust
            events.append(self._create_cdc_envelope("customers", before=None, after=cust, op="r", ts_ms=now_ts))

        # 3. Seed Orders & Order Items across the last 30 days
        base_date = date.today() - timedelta(days=30)
        for i in range(1, num_orders + 1):
            self.order_seq += 1
            cust_id = random.choice(list(self.customers.keys()))
            order_days_ago = random.randint(0, 30)
            order_datetime = datetime.combine(base_date + timedelta(days=order_days_ago), datetime.min.time()) + timedelta(minutes=random.randint(0, 1440))
            status = random.choice(ORDER_STATUSES)

            # Generate 1-4 items per order
            num_items = random.randint(1, 4)
            chosen_prods = random.sample(list(self.products.keys()), num_items)
            total_gross = 0.0

            for pid in chosen_prods:
                self.item_seq += 1
                prod = self.products[pid]
                qty = random.randint(1, 3)
                unit_price = prod["list_price"]
                line_total = round(qty * unit_price, 2)
                total_gross += line_total

                item = {
                    "item_id": self.item_seq,
                    "order_id": self.order_seq,
                    "product_id": pid,
                    "quantity": qty,
                    "unit_price": unit_price,
                    "line_total": line_total,
                    "created_at": order_datetime.isoformat()
                }
                self.order_items[self.item_seq] = item
                events.append(self._create_cdc_envelope("order_items", before=None, after=item, op="r", ts_ms=now_ts))

            discount = round(total_gross * random.choice([0.0, 0.05, 0.1, 0.15]), 2)
            taxable = total_gross - discount
            tax = round(taxable * 0.0825, 2)
            net_amt = round(taxable + tax, 2)

            ord_record = {
                "order_id": self.order_seq,
                "customer_id": cust_id,
                "order_date": order_datetime.date().isoformat(),
                "order_timestamp": order_datetime.isoformat(),
                "order_status": status,
                "gross_amount": round(total_gross, 2),
                "discount_amount": discount,
                "tax_amount": tax,
                "net_amount": net_amt,
                "created_at": order_datetime.isoformat(),
                "updated_at": order_datetime.isoformat()
            }
            self.orders[self.order_seq] = ord_record
            events.append(self._create_cdc_envelope("orders", before=None, after=ord_record, op="r", ts_ms=now_ts))

        logger.info("Database seeded: %d customers, %d products, %d orders. Produced %d CDC snapshot events.",
                    len(self.customers), len(self.products), len(self.orders), len(events))
        return events

    def simulate_mutations(self, cycle_count: int = 1) -> list[dict[str, Any]]:
        """
        Simulates live operational mutations:
        - New orders ('c')
        - Order status updates ('u')
        - Customer profile updates ('u') -> triggers SCD2 change in Gold
        - Soft or Hard deletes ('d') -> verifies CDC tombstone handling
        """
        events: list[dict[str, Any]] = []
        now_ts = int(time.time() * 1000)
        now_iso = datetime.utcnow().isoformat()

        for _ in range(cycle_count):
            # 1. Customer Mutation (Address change or Tier upgrade)
            if self.customers:
                cust_id = random.choice(list(self.customers.keys()))
                old_cust = dict(self.customers[cust_id])
                updated_cust = dict(old_cust)

                if random.choice([True, False]):
                    curr_tier_idx = LOYALTY_TIERS.index(old_cust["loyalty_tier"])
                    new_tier = LOYALTY_TIERS[min(curr_tier_idx + 1, len(LOYALTY_TIERS) - 1)]
                    updated_cust["loyalty_tier"] = new_tier
                else:
                    updated_cust["address"] = fake.street_address()
                    updated_cust["city"] = random.choice(["Austin", "Seattle", "Chicago", "Boston", "Denver"])

                updated_cust["updated_at"] = now_iso
                self.customers[cust_id] = updated_cust
                events.append(self._create_cdc_envelope("customers", before=old_cust, after=updated_cust, op="u", ts_ms=now_ts))

            # 2. New Order Creation ('c')
            self.order_seq += 1
            cust_id = random.choice(list(self.customers.keys()))
            pid = random.choice(list(self.products.keys()))
            prod = self.products[pid]
            qty = random.randint(1, 2)
            gross = round(prod["list_price"] * qty, 2)
            disc = round(gross * 0.05, 2)
            tax = round((gross - disc) * 0.0825, 2)
            net = round(gross - disc + tax, 2)

            new_order = {
                "order_id": self.order_seq,
                "customer_id": cust_id,
                "order_date": date.today().isoformat(),
                "order_timestamp": now_iso,
                "order_status": "PENDING",
                "gross_amount": gross,
                "discount_amount": disc,
                "tax_amount": tax,
                "net_amount": net,
                "created_at": now_iso,
                "updated_at": now_iso
            }
            self.orders[self.order_seq] = new_order
            events.append(self._create_cdc_envelope("orders", before=None, after=new_order, op="c", ts_ms=now_ts))

            # 3. Existing Order Status Advancement ('u')
            active_orders = [oid for oid, o in self.orders.items() if o["order_status"] in ("PENDING", "PROCESSING", "SHIPPED")]
            if active_orders:
                target_oid = random.choice(active_orders)
                old_order = dict(self.orders[target_oid])
                updated_order = dict(old_order)
                status_flow = {"PENDING": "PROCESSING", "PROCESSING": "SHIPPED", "SHIPPED": "DELIVERED"}
                updated_order["order_status"] = status_flow.get(old_order["order_status"], "DELIVERED")
                updated_order["updated_at"] = now_iso
                self.orders[target_oid] = updated_order
                events.append(self._create_cdc_envelope("orders", before=old_order, after=updated_order, op="u", ts_ms=now_ts))

            # 4. Simulated Delete ('d')
            # Soft or hard cancel an order to test CDC delete handling
            cancellable_orders = [oid for oid, o in self.orders.items() if o["order_status"] == "PENDING"]
            if cancellable_orders and random.random() < 0.3:
                del_oid = random.choice(cancellable_orders)
                del_order = self.orders.pop(del_oid)
                events.append(self._create_cdc_envelope("orders", before=del_order, after=None, op="d", ts_ms=now_ts))

        logger.info("Simulated %d mutation cycles. Generated %d CDC update/insert/delete events.", cycle_count, len(events))
        return events

    def _create_cdc_envelope(
        self,
        table: str,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        op: str,
        ts_ms: int
    ) -> dict[str, Any]:
        """Creates a standard Debezium PostgreSQL CDC JSON envelope."""
        return {
            "source": {
                "version": "2.5.0.Final",
                "connector": "postgresql",
                "name": "oltp",
                "ts_ms": ts_ms,
                "db": "ecommerce_oltp",
                "schema": "public",
                "table": table
            },
            "op": op,  # 'r' = read/snapshot, 'c' = create, 'u' = update, 'd' = delete
            "ts_ms": ts_ms,
            "before": before,
            "after": after
        }

if __name__ == "__main__":
    simulator = OLTPDataSimulator()
    seed_events = simulator.seed(num_customers=10, num_products=5, num_orders=20)
    mutation_events = simulator.simulate_mutations(cycle_count=5)
    print(f"Total simulated events: {len(seed_events) + len(mutation_events)}")
