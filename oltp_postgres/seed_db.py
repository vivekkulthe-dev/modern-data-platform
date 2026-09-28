import os
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
from dotenv import load_dotenv

load_dotenv()
fake = Faker()
Faker.seed(42)
random.seed(42)

def get_connection():
    return psycopg2.connect(
        host=os.getenv("NEON_HOST"),
        port=os.getenv("NEON_PORT"),
        dbname=os.getenv("NEON_DATABASE"),
        user=os.getenv("NEON_USER"),
        password=os.getenv("NEON_PASSWORD"),
        sslmode=os.getenv("NEON_SSLMODE", "require")
    )

def apply_schema(cursor):
    print("Applying 3NF DDL schema from oltp_postgres/schema.sql...")
    with open("oltp_postgres/schema.sql", "r") as f:
        ddl = f.read()
    cursor.execute(ddl)
    print("Schema applied successfully.")

def seed_categories(cursor):
    print("Seeding product categories...")
    categories_data = [
        ("Smartphones & Accessories", "Electronics"),
        ("Laptops & Computers", "Electronics"),
        ("Audio & Headphones", "Electronics"),
        ("Men's Apparel", "Clothing"),
        ("Women's Apparel", "Clothing"),
        ("Footwear", "Clothing"),
        ("Home & Kitchen", "Home Goods"),
        ("Fitness & Exercise", "Sports"),
        ("Books & Media", "Entertainment"),
        ("Personal Care & Grooming", "Beauty")
    ]
    query = """
        INSERT INTO categories (category_name, department)
        VALUES %s RETURNING category_id;
    """
    category_ids = execute_values(cursor, query, categories_data, fetch=True)
    return [c[0] for c in category_ids]

def seed_customers(cursor, count=500):
    print(f"Seeding {count} customers...")
    customers = []
    customer_ids = []
    now = datetime.now(timezone.utc)
    
    for _ in range(count):
        c_id = str(uuid.uuid4())
        customer_ids.append(c_id)
        created = now - timedelta(days=random.randint(30, 730))
        customers.append((
            c_id,
            fake.first_name(),
            fake.last_name(),
            fake.unique.email(),
            fake.phone_number()[:30],
            fake.street_address(),
            fake.city(),
            fake.state_abbr(),
            fake.postcode(),
            'United States',
            created,
            created + timedelta(days=random.randint(0, 10))
        ))
    
    query = """
        INSERT INTO customers (
            customer_id, first_name, last_name, email, phone,
            street_address, city, state, postal_code, country,
            created_at, updated_at
        ) VALUES %s;
    """
    execute_values(cursor, query, customers)
    return customer_ids

def seed_products(cursor, category_ids, count=150):
    print(f"Seeding {count} products...")
    products = []
    product_catalog = []  # tuple of (product_id, price)
    
    tech_nouns = ["Pro", "Ultra", "Max", "Lite", "Elite", "Smart", "Wireless", "Eco", "Prime", "Studio"]
    categories_lookup = {
        "Electronics": ["Earbuds", "Charger", "Cable", "Monitor", "Keyboard", "Power Bank"],
        "Clothing": ["T-Shirt", "Jeans", "Jacket", "Hoodie", "Sneakers", "Socks"],
        "Home Goods": ["Blender", "Air Fryer", "Cookware Set", "Throw Pillow", "Desk Lamp"],
        "Sports": ["Yoga Mat", "Dumbbells", "Resistance Bands", "Water Bottle"],
        "Entertainment": ["Hardcover", "Paperback", "Collector Edition"],
        "Beauty": ["Cleanser", "Moisturizer", "Serum", "Trimmer"]
    }
    
    for _ in range(count):
        p_id = str(uuid.uuid4())
        cat_id = random.choice(category_ids)
        modifier = random.choice(tech_nouns)
        item_type = random.choice(sum(categories_lookup.values(), []))
        name = f"{modifier} {item_type} Gen-{random.randint(1, 5)}"
        sku = f"SKU-{cat_id:02d}-{random.randint(10000, 99999)}"
        
        cost = Decimal(str(round(random.uniform(5.00, 250.00), 2)))
        margin = Decimal(str(round(random.uniform(1.25, 2.50), 2)))
        price = round(cost * margin, 2)
        stock = random.randint(10, 500)
        
        products.append((
            p_id, cat_id, name, sku, price, cost, stock, True
        ))
        product_catalog.append((p_id, price))
        
    query = """
        INSERT INTO products (
            product_id, category_id, product_name, sku, price, cost,
            stock_quantity, is_active
        ) VALUES %s;
    """
    execute_values(cursor, query, products)
    return product_catalog

def seed_orders_and_items(cursor, customer_ids, product_catalog, order_count=2000):
    print(f"Seeding {order_count} orders and associated line items...")
    orders = []
    order_items = []
    statuses = ['PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED', 'RETURNED']
    status_weights = [0.05, 0.10, 0.25, 0.50, 0.05, 0.05]
    carriers = ['FedEx', 'UPS', 'USPS', 'DHL Express']
    payments = ['CREDIT_CARD', 'PAYPAL', 'APPLE_PAY', 'BANK_TRANSFER']
    
    now = datetime.now(timezone.utc)
    
    for _ in range(order_count):
        o_id = str(uuid.uuid4())
        c_id = random.choice(customer_ids)
        order_date = now - timedelta(days=random.randint(1, 365), hours=random.randint(0, 23))
        status = random.choices(statuses, weights=status_weights)[0]
        payment = random.choice(payments)
        carrier = random.choice(carriers) if status in ['SHIPPED', 'DELIVERED', 'RETURNED'] else None
        
        num_items = random.randint(1, 5)
        selected_products = random.sample(product_catalog, num_items)
        
        items_subtotal = Decimal("0.00")
        for prod_id, unit_price in selected_products:
            qty = random.randint(1, 3)
            subtotal = round(unit_price * qty, 2)
            items_subtotal += subtotal
            order_items.append((
                str(uuid.uuid4()),
                o_id,
                prod_id,
                qty,
                unit_price,
                subtotal
            ))
            
        shipping = Decimal("0.00") if items_subtotal > Decimal("100.00") else Decimal("9.99")
        tax = round(items_subtotal * Decimal("0.0825"), 2)  # Standard 8.25% tax
        total = items_subtotal + shipping + tax
        
        orders.append((
            o_id,
            c_id,
            order_date,
            status,
            shipping,
            tax,
            total,
            payment,
            carrier,
            order_date
        ))
        
    orders_query = """
        INSERT INTO orders (
            order_id, customer_id, order_date, order_status,
            shipping_amount, tax_amount, total_amount, payment_method,
            shipping_carrier, created_at
        ) VALUES %s;
    """
    execute_values(cursor, orders_query, orders)
    
    items_query = """
        INSERT INTO order_items (
            order_item_id, order_id, product_id, quantity,
            unit_price, subtotal
        ) VALUES %s;
    """
    execute_values(cursor, items_query, order_items)
    print(f"Generated {len(order_items)} line items across {order_count} orders.")

def main():
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            apply_schema(cur)
            category_ids = seed_categories(cur)
            customer_ids = seed_customers(cur, count=500)
            product_catalog = seed_products(cur, category_ids, count=150)
            seed_orders_and_items(cur, customer_ids, product_catalog, order_count=2000)
            
            print("\nRow count verification:")
            for table in ['categories', 'customers', 'products', 'orders', 'order_items']:
                cur.execute(f"SELECT COUNT(*) FROM {table};")
                count = cur.fetchone()[0]
                print(f"  - {table}: {count:,} rows")
                
        conn.commit()
        print("\nDatabase seeded and committed successfully!")
    except Exception as e:
        conn.rollback()
        print(f"\nExecution failed! Rolled back transaction. Error: {e}")
        raise e
    finally:
        conn.close()

if __name__ == "__main__":
    main()