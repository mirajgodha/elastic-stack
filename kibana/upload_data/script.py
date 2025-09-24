import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import json

# Generate comprehensive datasets for Kibana dashboards

print("🚀 CREATING KIBANA-READY CSV DATASETS")
print("="*50)

# Set random seed for reproducible data
np.random.seed(42)
random.seed(42)

# 1. WEB ANALYTICS DATA
print("1. Creating Web Analytics Dataset...")

# Generate web analytics data for last 90 days
start_date = datetime.now() - timedelta(days=90)
web_data = []

user_agents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36", 
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)",
    "Mozilla/5.0 (iPad; CPU OS 14_7_1 like Mac OS X)",
    "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0"
]

pages = ["/home", "/products", "/about", "/contact", "/blog", "/pricing", "/login", "/signup", "/checkout", "/profile"]
referrers = ["google.com", "facebook.com", "twitter.com", "linkedin.com", "direct", "email", "ads.google.com"]
countries = ["United States", "India", "United Kingdom", "Germany", "France", "Canada", "Australia", "Japan"]
devices = ["Desktop", "Mobile", "Tablet"]

for i in range(50000):  # 50k records
    timestamp = start_date + timedelta(
        days=random.randint(0, 89),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    
    web_data.append({
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "user_id": f"user_{random.randint(1000, 9999)}",
        "session_id": f"session_{random.randint(100000, 999999)}",
        "page": random.choice(pages),
        "referrer": random.choice(referrers),
        "user_agent": random.choice(user_agents),
        "country": random.choice(countries),
        "device_type": random.choice(devices),
        "page_load_time": round(random.uniform(0.5, 5.0), 2),
        "bounce_rate": random.choice([True, False]),
        "conversion": random.choice([True, False]) if random.random() < 0.1 else False,
        "revenue": round(random.uniform(10, 500), 2) if random.random() < 0.05 else 0,
        "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
    })

web_df = pd.DataFrame(web_data)
web_df.to_csv('web_analytics.csv', index=False)
print("   ✅ web_analytics.csv created (50K records)")

# 2. E-COMMERCE SALES DATA
print("2. Creating E-commerce Sales Dataset...")

categories = ["Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Beauty", "Automotive"]
products = ["Smartphone", "Laptop", "T-Shirt", "Jeans", "Novel", "Cookbook", "Sofa", "Plant", 
           "Football", "Basketball", "Lipstick", "Shampoo", "Car Parts", "Tires"]
payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Google Pay", "Bank Transfer"]
order_status = ["Completed", "Processing", "Shipped", "Delivered", "Cancelled", "Refunded"]

ecommerce_data = []
for i in range(30000):  # 30k records
    timestamp = start_date + timedelta(
        days=random.randint(0, 89),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    quantity = random.randint(1, 5)
    unit_price = round(random.uniform(10, 1000), 2)
    
    ecommerce_data.append({
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "order_id": f"ORD_{random.randint(100000, 999999)}",
        "customer_id": f"CUST_{random.randint(1000, 9999)}",
        "product": random.choice(products),
        "category": random.choice(categories),
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(quantity * unit_price, 2),
        "discount": round(random.uniform(0, 50), 2) if random.random() < 0.3 else 0,
        "payment_method": random.choice(payment_methods),
        "order_status": random.choice(order_status),
        "shipping_cost": round(random.uniform(5, 25), 2),
        "customer_country": random.choice(countries),
        "sales_rep": f"Rep_{random.randint(1, 20)}"
    })

ecommerce_df = pd.DataFrame(ecommerce_data)
ecommerce_df.to_csv('ecommerce_sales.csv', index=False)
print("   ✅ ecommerce_sales.csv created (30K records)")

# 3. APPLICATION LOGS DATA
print("3. Creating Application Logs Dataset...")

log_levels = ["INFO", "WARN", "ERROR", "DEBUG", "FATAL"]
services = ["auth-service", "payment-service", "user-service", "product-service", "notification-service"]
environments = ["production", "staging", "development"]
error_codes = [200, 201, 400, 401, 403, 404, 500, 502, 503, 504]

logs_data = []
for i in range(100000):  # 100k records
    timestamp = start_date + timedelta(
        days=random.randint(0, 89),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59),
        microseconds=random.randint(0, 999999)
    )
    
    level = random.choice(log_levels)
    response_time = round(random.uniform(10, 2000), 1)
    
    logs_data.append({
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "level": level,
        "service": random.choice(services),
        "environment": random.choice(environments),
        "message": f"Processing request {random.randint(1000, 9999)}",
        "response_time_ms": response_time,
        "status_code": random.choice(error_codes),
        "user_id": f"user_{random.randint(1, 1000)}",
        "request_id": f"req_{random.randint(100000, 999999)}",
        "memory_usage_mb": round(random.uniform(100, 2000), 1),
        "cpu_usage_percent": round(random.uniform(10, 95), 1),
        "thread_count": random.randint(5, 50)
    })

logs_df = pd.DataFrame(logs_data)
logs_df.to_csv('application_logs.csv', index=False)
print("   ✅ application_logs.csv created (100K records)")

# 4. IOT SENSOR DATA
print("4. Creating IoT Sensor Dataset...")

sensor_types = ["temperature", "humidity", "pressure", "light", "motion", "air_quality"]
locations = ["Factory_Floor_1", "Factory_Floor_2", "Warehouse_A", "Warehouse_B", "Office_Building", "Data_Center"]
device_status = ["online", "offline", "maintenance"]

iot_data = []
for i in range(75000):  # 75k records
    timestamp = start_date + timedelta(
        days=random.randint(0, 89),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    
    sensor_type = random.choice(sensor_types)
    
    # Generate realistic values based on sensor type
    if sensor_type == "temperature":
        value = round(random.uniform(18, 35), 2)
        unit = "°C"
    elif sensor_type == "humidity":
        value = round(random.uniform(30, 90), 2)
        unit = "%"
    elif sensor_type == "pressure":
        value = round(random.uniform(990, 1030), 2)
        unit = "hPa"
    elif sensor_type == "light":
        value = round(random.uniform(0, 1000), 2)
        unit = "lux"
    elif sensor_type == "motion":
        value = random.choice([0, 1])
        unit = "detected"
    else:  # air_quality
        value = round(random.uniform(50, 300), 2)
        unit = "AQI"
    
    iot_data.append({
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "sensor_id": f"SENSOR_{random.randint(1000, 9999)}",
        "sensor_type": sensor_type,
        "location": random.choice(locations),
        "value": value,
        "unit": unit,
        "battery_level": round(random.uniform(10, 100), 1),
        "signal_strength": random.randint(-90, -30),
        "device_status": random.choice(device_status),
        "alert_triggered": random.choice([True, False]) if random.random() < 0.1 else False,
        "maintenance_due": random.choice([True, False]) if random.random() < 0.05 else False
    })

iot_df = pd.DataFrame(iot_data)
iot_df.to_csv('iot_sensors.csv', index=False)
print("   ✅ iot_sensors.csv created (75K records)")

# 5. FINANCIAL TRANSACTIONS DATA
print("5. Creating Financial Transactions Dataset...")

transaction_types = ["Purchase", "Refund", "Transfer", "Deposit", "Withdrawal", "Payment"]
currencies = ["USD", "EUR", "GBP", "INR", "JPY", "CAD", "AUD"]
merchant_categories = ["Grocery", "Gas Station", "Restaurant", "Online Shopping", "ATM", "Travel", "Entertainment"]

financial_data = []
for i in range(40000):  # 40k records
    timestamp = start_date + timedelta(
        days=random.randint(0, 89),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    
    amount = round(random.uniform(1, 5000), 2)
    currency = random.choice(currencies)
    
    financial_data.append({
        "timestamp": timestamp.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "transaction_id": f"TXN_{random.randint(100000, 999999)}",
        "account_id": f"ACC_{random.randint(10000, 99999)}",
        "transaction_type": random.choice(transaction_types),
        "amount": amount,
        "currency": currency,
        "merchant": f"Merchant_{random.randint(1, 500)}",
        "merchant_category": random.choice(merchant_categories),
        "card_type": random.choice(["Credit", "Debit", "Prepaid"]),
        "is_fraud": random.choice([True, False]) if random.random() < 0.02 else False,
        "processing_fee": round(amount * 0.025, 2),
        "country_code": random.choice(["US", "IN", "UK", "DE", "FR", "CA", "AU", "JP"]),
        "risk_score": round(random.uniform(0, 100), 1)
    })

financial_df = pd.DataFrame(financial_data)
financial_df.to_csv('financial_transactions.csv', index=False)
print("   ✅ financial_transactions.csv created (40K records)")

print("\n" + "="*50)
print("📊 SUMMARY OF CREATED DATASETS:")
print("="*50)

datasets = [
    ("web_analytics.csv", "50,000", "Web traffic, user behavior, conversions"),
    ("ecommerce_sales.csv", "30,000", "Sales transactions, products, customers"),
    ("application_logs.csv", "100,000", "System logs, performance metrics, errors"),
    ("iot_sensors.csv", "75,000", "IoT device data, sensor readings, alerts"),
    ("financial_transactions.csv", "40,000", "Financial data, payments, fraud detection")
]

for filename, records, description in datasets:
    print(f"📁 {filename:25} | {records:>8} records | {description}")

print("\n🎯 KIBANA DASHBOARD IDEAS:")
print("-" * 30)
dashboard_ideas = [
    "📈 Web Analytics: Traffic trends, user journeys, conversion funnels",
    "🛒 E-commerce: Sales performance, product analytics, customer insights", 
    "🔍 Application Monitoring: Error rates, response times, service health",
    "🌡️  IoT Dashboard: Sensor monitoring, anomaly detection, facility management",
    "💰 Financial Analytics: Transaction monitoring, fraud detection, spending patterns"
]

for idea in dashboard_ideas:
    print(f"   {idea}")

print(f"\n📋 KIBANA IMPORT INSTRUCTIONS:")
print("-" * 35)
instructions = [
    "1. Open Kibana → Stack Management → Data Views",
    "2. Upload CSV files using 'Upload a file' feature",
    "3. Configure timestamp field during import",
    "4. Create index patterns for each dataset",
    "5. Build visualizations and dashboards",
    "6. Use suggested time ranges: Last 90 days"
]

for instruction in instructions:
    print(f"   {instruction}")

print(f"\n✨ READY FOR KIBANA! All CSV files created successfully.")
print("Files are optimized for time-series analysis and dashboard creation.")