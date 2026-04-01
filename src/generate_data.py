import random
from datetime import datetime, timedelta
from faker import Faker
import psycopg2

fake = Faker("en_IN")

DB_CONFIG = {
    "host": "host.docker.internal",
    "port": 5432,
    "dbname": "retail_ops_db",
    "user": "sanskarnamdeo",
    "password": "Sans2208"   # apna actual password
}

NUM_CUSTOMERS = 10000
NUM_PRODUCTS = 2000
NUM_ORDERS = 50000


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def clear_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            TRUNCATE TABLE shipments, payments, order_items, orders, products, customers
            RESTART IDENTITY CASCADE;
        """)
    conn.commit()


def generate_customers(conn):
    rows = []
    for customer_id in range(1, NUM_CUSTOMERS + 1):
        rows.append((
            customer_id,
            fake.first_name(),
            fake.last_name(),
            fake.email().lower(),
            fake.msisdn()[:10],
            fake.city(),
            fake.state(),
            "India",
            fake.date_time_between(start_date="-2y", end_date="now"),
            datetime.now()
        ))

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO customers
            (customer_id, first_name, last_name, email, phone, city, state, country, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)
    conn.commit()
    print(f"Inserted {len(rows)} customers")


def generate_products(conn):
    categories = {
        "Electronics": ["Mobile", "Laptop", "TV", "Accessories"],
        "Fashion": ["Footwear", "Clothing", "Watches"],
        "Home": ["Kitchen", "Furniture", "Decor"],
        "Beauty": ["Skincare", "Haircare", "Makeup"],
        "Sports": ["Fitness", "Outdoor", "Shoes"]
    }

    brands = ["Apple", "Samsung", "Nike", "Adidas", "Sony", "LG", "Boat", "Puma", "Mamaearth", "Ikea"]

    rows = []
    product_id = 1

    for _ in range(NUM_PRODUCTS):
        category = random.choice(list(categories.keys()))
        sub_category = random.choice(categories[category])
        brand = random.choice(brands)
        product_name = f"{brand} {sub_category} Item {product_id}"
        unit_price = round(random.uniform(200, 100000), 2)

        rows.append((
            product_id,
            product_name,
            category,
            sub_category,
            brand,
            unit_price,
            random.choice([True, True, True, False]),
            fake.date_time_between(start_date="-2y", end_date="now"),
            datetime.now()
        ))
        product_id += 1

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO products
            (product_id, product_name, category, sub_category, brand, unit_price, is_active, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, rows)
    conn.commit()
    print(f"Inserted {len(rows)} products")


def generate_orders_and_related(conn):
    order_rows = []
    order_item_rows = []
    payment_rows = []
    shipment_rows = []

    order_statuses = ["completed", "pending", "cancelled"]
    channels = ["online", "store", "app"]
    payment_methods = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash"]
    payment_statuses = ["completed", "pending", "failed"]

    order_item_id = 1
    payment_id = 1
    shipment_id = 1

    for order_id in range(1, NUM_ORDERS + 1):
        customer_id = random.randint(1, NUM_CUSTOMERS)
        order_date = fake.date_time_between(start_date="-1y", end_date="now")
        order_status = random.choices(order_statuses, weights=[75, 15, 10])[0]
        channel = random.choice(channels)

        order_rows.append((
            order_id,
            customer_id,
            order_date,
            order_status,
            channel,
            fake.address(),
            fake.address(),
            order_date,
            datetime.now()
        ))

        num_items = random.randint(1, 3)
        order_total = 0

        for _ in range(num_items):
            product_id = random.randint(1, NUM_PRODUCTS)
            quantity = random.randint(1, 5)
            unit_price = round(random.uniform(200, 100000), 2)
            discount_amount = round(random.uniform(0, unit_price * 0.2), 2)
            tax_amount = round((quantity * unit_price - discount_amount) * 0.18, 2)
            order_total += (quantity * unit_price - discount_amount + tax_amount)

            order_item_rows.append((
                order_item_id,
                order_id,
                product_id,
                quantity,
                unit_price,
                discount_amount,
                tax_amount
            ))
            order_item_id += 1

        pay_status = random.choices(payment_statuses, weights=[80, 15, 5])[0]
        payment_rows.append((
            payment_id,
            order_id,
            order_date + timedelta(minutes=random.randint(5, 60)),
            random.choice(payment_methods),
            pay_status,
            round(order_total, 2)
        ))
        payment_id += 1

        shipment_date = order_date + timedelta(hours=random.randint(4, 48))
        delivered = random.choice([True, True, True, False])
        delivery_date = shipment_date + timedelta(days=random.randint(1, 7)) if delivered else None
        shipment_status = "delivered" if delivered else random.choice(["pending", "in_transit", "shipped"])

        shipment_rows.append((
            shipment_id,
            order_id,
            shipment_date,
            delivery_date,
            random.choice(["BlueDart", "Delhivery", "Ecom", "XpressBees"]),
            shipment_status,
            f"TRK{100000 + shipment_id}"
        ))
        shipment_id += 1

    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO orders
            (order_id, customer_id, order_date, order_status, channel, shipping_address, billing_address, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, order_rows)

        cur.executemany("""
            INSERT INTO order_items
            (order_item_id, order_id, product_id, quantity, unit_price, discount_amount, tax_amount)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, order_item_rows)

        cur.executemany("""
            INSERT INTO payments
            (payment_id, order_id, payment_date, payment_method, payment_status, amount)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, payment_rows)

        cur.executemany("""
            INSERT INTO shipments
            (shipment_id, order_id, shipment_date, delivery_date, carrier, shipment_status, tracking_number)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, shipment_rows)

    conn.commit()
    print(f"Inserted {len(order_rows)} orders")
    print(f"Inserted {len(order_item_rows)} order_items")
    print(f"Inserted {len(payment_rows)} payments")
    print(f"Inserted {len(shipment_rows)} shipments")


def generate_all_data():
    conn = get_connection()
    try:
        clear_tables(conn)
        generate_customers(conn)
        generate_products(conn)
        generate_orders_and_related(conn)
        print("Synthetic data generation completed successfully")
    finally:
        conn.close()