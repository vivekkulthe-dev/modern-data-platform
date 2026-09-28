import psycopg2, os
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('NEON_HOST'),
    port=os.getenv('NEON_PORT'),
    dbname=os.getenv('NEON_DATABASE'),
    user=os.getenv('NEON_USER'),
    password=os.getenv('NEON_PASSWORD'),
    sslmode=os.getenv('NEON_SSLMODE', 'require')
)
cur = conn.cursor()

# Test 1: Check for total balance mismatches
cur.execute('''
    SELECT 
        o.order_id,
        o.total_amount,
        (SUM(oi.subtotal) + o.tax_amount + o.shipping_amount) AS calculated
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY o.order_id, o.total_amount, o.tax_amount, o.shipping_amount
    HAVING ABS(o.total_amount - (SUM(oi.subtotal) + o.tax_amount + o.shipping_amount)) > 0.01;
''')
balance_errors = cur.fetchall()

# Test 2: Check for orphaned foreign keys
cur.execute('''
    SELECT COUNT(*) FROM orders WHERE customer_id NOT IN (SELECT customer_id FROM customers);
''')
orphan_orders = cur.fetchone()[0]

cur.execute('''
    SELECT COUNT(*) FROM order_items WHERE product_id NOT IN (SELECT product_id FROM products);
''')
orphan_items = cur.fetchone()[0]

print('=' * 60)
print('OLTP INTEGRITY AUDIT REPORT')
print('=' * 60)
print(f'Mismatched Order Totals : {len(balance_errors)}')
print(f'Orphaned Orders         : {orphan_orders}')
print(f'Orphaned Line Items     : {orphan_items}')

if len(balance_errors) == 0 and orphan_orders == 0 and orphan_items == 0:
    print('RESULT: PASSED. Referential integrity confirmed.')
else:
    print('RESULT: FAILED. Anomalies detected.')
conn.close()