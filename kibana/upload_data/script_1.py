# Let's create a sample preview of each dataset to show the structure
import pandas as pd

print("📋 DATASET PREVIEWS - FIRST 3 ROWS OF EACH CSV")
print("="*60)

datasets = [
    ("web_analytics.csv", "🌐 WEB ANALYTICS DATA"),
    ("ecommerce_sales.csv", "🛒 E-COMMERCE SALES DATA"),
    ("application_logs.csv", "📊 APPLICATION LOGS DATA"),
    ("iot_sensors.csv", "🌡️ IOT SENSOR DATA"),
    ("financial_transactions.csv", "💰 FINANCIAL TRANSACTIONS DATA")
]

for filename, title in datasets:
    print(f"\n{title}")
    print("-" * len(title))
    df = pd.read_csv(filename)
    print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
    print("Columns:", ", ".join(df.columns.tolist()))
    print("\nSample Data (first 3 rows):")
    print(df.head(3).to_string(index=False))
    print()

# Create a dashboard configuration guide
print("\n🎨 KIBANA DASHBOARD CONFIGURATION GUIDE")
print("="*50)

dashboard_configs = {
    "🌐 Web Analytics Dashboard": {
        "visualizations": [
            "📈 Time Series: Page views over time",
            "🗺️ Map: Visitor locations by country", 
            "📊 Bar Chart: Top pages by visits",
            "🥧 Pie Chart: Device type distribution",
            "📉 Line Chart: Bounce rate trends",
            "💰 Metric: Total revenue"
        ],
        "filters": ["timestamp", "country", "device_type", "referrer"],
        "time_field": "timestamp"
    },
    
    "🛒 E-commerce Dashboard": {
        "visualizations": [
            "💰 Metric Cards: Total sales, orders, customers",
            "📈 Time Series: Sales trends over time",
            "📊 Horizontal Bar: Top products by revenue",
            "🗺️ Map: Sales by country",
            "🥧 Donut Chart: Payment method distribution",
            "📉 Line Chart: Order status trends"
        ],
        "filters": ["timestamp", "category", "customer_country", "order_status"],
        "time_field": "timestamp"
    },
    
    "📊 Application Monitoring": {
        "visualizations": [
            "🚨 Metric: Error rate percentage",
            "📈 Time Series: Response time trends",
            "📊 Vertical Bar: Log levels distribution",
            "🌡️ Gauge: CPU usage",
            "📉 Area Chart: Memory usage over time",
            "📋 Data Table: Recent error logs"
        ],
        "filters": ["timestamp", "level", "service", "environment"],
        "time_field": "timestamp"
    },
    
    "🌡️ IoT Sensor Dashboard": {
        "visualizations": [
            "🌡️ Line Chart: Temperature trends",
            "💧 Area Chart: Humidity levels",
            "🔋 Gauge: Battery levels",
            "📍 Map: Sensor locations",
            "🚨 Metric: Active alerts",
            "📊 Heat Map: Sensor readings by location"
        ],
        "filters": ["timestamp", "sensor_type", "location", "device_status"],
        "time_field": "timestamp"
    },
    
    "💰 Financial Analytics": {
        "visualizations": [
            "💰 Metric Cards: Total transactions, volume",
            "📈 Time Series: Transaction volume trends",
            "🥧 Pie Chart: Transaction types",
            "🔍 Scatter Plot: Amount vs risk score",
            "🚨 Metric: Fraud detection rate",
            "📊 Bar Chart: Top merchant categories"
        ],
        "filters": ["timestamp", "transaction_type", "currency", "country_code"],
        "time_field": "timestamp"
    }
}

for dashboard, config in dashboard_configs.items():
    print(f"\n{dashboard}")
    print("-" * len(dashboard))
    print("🎯 Recommended Visualizations:")
    for viz in config["visualizations"]:
        print(f"   • {viz}")
    print(f"🔍 Key Filters: {', '.join(config['filters'])}")
    print(f"⏰ Time Field: {config['time_field']}")

print(f"\n🛠️ ADVANCED KIBANA FEATURES TO TRY:")
print("-" * 40)

advanced_features = [
    "🔍 Discover: Explore raw data with KQL queries",
    "📊 Lens: Drag-and-drop visualization builder",
    "🚨 Watcher: Set up alerts for anomalies",
    "📈 TSVB: Time series visual builder for advanced charts",
    "🗺️ Maps: Geographic visualizations with layers",
    "📋 Canvas: Pixel-perfect infographic dashboards",
    "🔄 Transforms: Aggregate data for better performance",
    "📱 Mobile: Responsive dashboards for mobile viewing"
]

for feature in advanced_features:
    print(f"   {feature}")

print(f"\n📊 SAMPLE KQL QUERIES TO TRY:")
print("-" * 30)

sample_queries = [
    ('Web Analytics', 'device_type:"Mobile" AND conversion:true'),
    ('E-commerce', 'order_status:"Completed" AND total_amount > 100'),
    ('App Logs', 'level:"ERROR" AND service:"payment-service"'),
    ('IoT Sensors', 'sensor_type:"temperature" AND value > 30'),
    ('Financial', 'is_fraud:true OR risk_score > 80')
]

for dataset, query in sample_queries:
    print(f"   {dataset}: {query}")

print(f"\n✨ READY TO BUILD AMAZING DASHBOARDS!")
print("These datasets provide rich, realistic data for comprehensive Kibana practice.")