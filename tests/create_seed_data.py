"""
Creates a small test dataset for CI/CD.
Used by GitHub Actions to test dbt without needing the full CSV.
"""

import duckdb
import os

os.makedirs("data/warehouse", exist_ok=True)
con = duckdb.connect("data/warehouse/supply_chain.duckdb")
con.execute("CREATE SCHEMA IF NOT EXISTS raw")
con.execute("""
    CREATE OR REPLACE TABLE raw.supply_chain AS
    SELECT * FROM (
        VALUES
        ('Debit', 3, 4, 10.5, 150.0, 'Late delivery', 1, 1, 'Fitness', 'Boston',
         'Estados Unidos', 'test@test.com', 'John', 1, 'Doe', 'pass', 'Consumer',
         'Massachusetts', '123 Main St', 02101.0, 1, 'Fitness', 42.36, -71.06,
         'US', 'Boston', 'Estados Unidos', 1, '1/15/2018 10:00', 1, 1, 5.0, 0.05,
         1, 50.0, 0.20, 2, 100.0, 95.0, 10.5, 'East', 'Massachusetts', 'Complete',
         02101.0, 1, 1, NULL, 'img.jpg', 'Treadmill', 50.0, 0, '1/18/2018 10:00',
         'Standard Class'),
        ('Debit', 5, 4, 8.0, 200.0, 'Advance shipping', 0, 2, 'Electronics', 'Miami',
         'Estados Unidos', 'jane@test.com', 'Jane', 2, 'Smith', 'pass', 'Corporate',
         'Florida', '456 Oak Ave', 33101.0, 2, 'Technology', 25.76, -80.19,
         'US', 'Miami', 'Estados Unidos', 2, '2/20/2018 14:30', 2, 2, 3.0, 0.03,
         2, 75.0, 0.15, 1, 200.0, 197.0, 8.0, 'South', 'Florida', 'Complete',
         33101.0, 2, 2, NULL, 'img2.jpg', 'Laptop', 75.0, 0, '2/23/2018 14:30',
         'First Class')
    ) AS t("Type", "Days for shipping (real)", "Days for shipment (scheduled)",
           "Benefit per order", "Sales per customer", "Delivery Status",
           "Late_delivery_risk", "Category Id", "Category Name", "Customer City",
           "Customer Country", "Customer Email", "Customer Fname", "Customer Id",
           "Customer Lname", "Customer Password", "Customer Segment", "Customer State",
           "Customer Street", "Customer Zipcode", "Department Id", "Department Name",
           "Latitude", "Longitude", "Market", "Order City", "Order Country",
           "Order Customer Id", "order date (DateOrders)", "Order Id",
           "Order Item Cardprod Id", "Order Item Discount", "Order Item Discount Rate",
           "Order Item Id", "Order Item Product Price", "Order Item Profit Ratio",
           "Order Item Quantity", "Sales", "Order Item Total", "Order Profit Per Order",
           "Order Region", "Order State", "Order Status", "Order Zipcode",
           "Product Card Id", "Product Category Id", "Product Description",
           "Product Image", "Product Name", "Product Price", "Product Status",
           "shipping date (DateOrders)", "Shipping Mode")
""")

count = con.execute("SELECT COUNT(*) FROM raw.supply_chain").fetchone()[0]
print(f"Seed data loaded: {count} rows")
con.close()