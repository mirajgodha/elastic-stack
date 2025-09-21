# =============================================================================
# ELASTICSEARCH 8.X DEMO - SAMPLE DATA SETUP
# =============================================================================
# This file contains sample data for demonstrating Elasticsearch 8.x features
# Run this first to create the required indices and data for all demos

# Create product catalog index with mapping (Elasticsearch 8.x style)
PUT /ecommerce
{
  "mappings": {
    "properties": {
      "product_id": { "type": "keyword" },
      "name": { "type": "text", "analyzer": "standard" },
      "description": { "type": "text" },
      "category": { "type": "keyword" },
      "brand": { "type": "keyword" },
      "price": { "type": "double" },
      "rating": { "type": "float" },
      "tags": { "type": "keyword" },
      "created_date": { "type": "date" },
      "location": { "type": "geo_point" },
      "in_stock": { "type": "boolean" },
      "sales_count": { "type": "integer" },
      "discount_percentage": { "type": "float" },
      "reviews": {
        "type": "nested",
        "properties": {
          "user": { "type": "keyword" },
          "rating": { "type": "integer" },
          "comment": { "type": "text" }
        }
      },
      "suggest": {
        "type": "completion",
        "analyzer": "simple",
        "preserve_separators": true,
        "preserve_position_increments": true,
        "max_input_length": 50,
        "contexts": [
          {
            "name": "category",
            "type": "category"
          }
        ]
      }
    }
  },
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": {
      "analyzer": {
        "custom_analyzer": {
          "type": "standard",
          "stopwords": "_english_"
        }
      }
    }
  }
}

# Create user activity index for more complex aggregations
PUT /user_activity
{
  "mappings": {
    "properties": {
      "user_id": { "type": "keyword" },
      "session_id": { "type": "keyword" },
      "activity_type": { "type": "keyword" },
      "timestamp": { "type": "date" },
      "duration_minutes": { "type": "integer" },
      "page_views": { "type": "integer" },
      "user_agent": { "type": "text" },
      "ip_address": { "type": "ip" },
      "location": { "type": "geo_point" },
      "device_type": { "type": "keyword" },
      "os": { "type": "keyword" },
      "referrer": { "type": "text" }
    }
  }
}

# Load sample ecommerce data
POST /_bulk
{ "index": { "_index": "ecommerce", "_id": "1" }}
{ "product_id": "LAPTOP001", "name": "MacBook Pro 16", "description": "Apple MacBook Pro with M2 chip, 16GB RAM, 512GB SSD", "category": "laptops", "brand": "Apple", "price": 2499.99, "rating": 4.8, "tags": ["premium", "professional", "apple"], "created_date": "2024-01-15", "location": { "lat": 37.7749, "lon": -122.4194 }, "in_stock": true, "sales_count": 150, "discount_percentage": 5.0, "reviews": [{"user": "tech_reviewer", "rating": 5, "comment": "Excellent performance and build quality"}], "suggest": { "input": ["MacBook Pro", "Apple laptop"], "contexts": { "category": ["laptops"] }}}
{ "index": { "_index": "ecommerce", "_id": "2" }}
{ "product_id": "PHONE001", "name": "iPhone 15 Pro", "description": "Latest iPhone with A17 Pro chip, advanced camera system", "category": "smartphones", "brand": "Apple", "price": 999.99, "rating": 4.7, "tags": ["smartphone", "premium", "ios"], "created_date": "2024-02-10", "location": { "lat": 40.7128, "lon": -74.0060 }, "in_stock": true, "sales_count": 300, "discount_percentage": 10.0, "reviews": [{"user": "mobile_expert", "rating": 5, "comment": "Best camera quality in smartphones"}], "suggest": { "input": ["iPhone", "Apple phone"], "contexts": { "category": ["smartphones"] }}}
{ "index": { "_index": "ecommerce", "_id": "3" }}
{ "product_id": "LAPTOP002", "name": "Dell XPS 13", "description": "Ultra-thin laptop with Intel i7 processor, 16GB RAM", "category": "laptops", "brand": "Dell", "price": 1299.99, "rating": 4.5, "tags": ["ultrabook", "business", "portable"], "created_date": "2024-01-20", "location": { "lat": 34.0522, "lon": -118.2437 }, "in_stock": true, "sales_count": 89, "discount_percentage": 15.0, "reviews": [{"user": "business_user", "rating": 4, "comment": "Great for business use, very portable"}], "suggest": { "input": ["Dell XPS", "Dell laptop"], "contexts": { "category": ["laptops"] }}}
{ "index": { "_index": "ecommerce", "_id": "4" }}
{ "product_id": "TABLET001", "name": "iPad Air", "description": "Apple iPad Air with M1 chip, 256GB storage, WiFi", "category": "tablets", "brand": "Apple", "price": 749.99, "rating": 4.6, "tags": ["tablet", "creative", "productivity"], "created_date": "2024-03-05", "location": { "lat": 41.8781, "lon": -87.6298 }, "in_stock": false, "sales_count": 45, "discount_percentage": 0.0, "reviews": [{"user": "artist", "rating": 5, "comment": "Perfect for digital art and design work"}], "suggest": { "input": ["iPad Air", "Apple tablet"], "contexts": { "category": ["tablets"] }}}
{ "index": { "_index": "ecommerce", "_id": "5" }}
{ "product_id": "PHONE002", "name": "Samsung Galaxy S24", "description": "Samsung flagship with advanced AI features and great camera", "category": "smartphones", "brand": "Samsung", "price": 899.99, "rating": 4.4, "tags": ["android", "flagship", "ai"], "created_date": "2024-02-15", "location": { "lat": 25.7617, "lon": -80.1918 }, "in_stock": true, "sales_count": 200, "discount_percentage": 20.0, "reviews": [{"user": "android_fan", "rating": 4, "comment": "Great Android phone with excellent features"}], "suggest": { "input": ["Galaxy S24", "Samsung phone"], "contexts": { "category": ["smartphones"] }}}
{ "index": { "_index": "ecommerce", "_id": "6" }}
{ "product_id": "LAPTOP003", "name": "Lenovo ThinkPad X1", "description": "Business laptop with enterprise security and reliability", "category": "laptops", "brand": "Lenovo", "price": 1599.99, "rating": 4.3, "tags": ["business", "enterprise", "security"], "created_date": "2024-01-25", "location": { "lat": 39.7392, "lon": -104.9903 }, "in_stock": true, "sales_count": 67, "discount_percentage": 8.0, "reviews": [{"user": "it_manager", "rating": 4, "comment": "Reliable for enterprise use"}], "suggest": { "input": ["ThinkPad", "Lenovo laptop"], "contexts": { "category": ["laptops"] }}}
{ "index": { "_index": "ecommerce", "_id": "7" }}
{ "product_id": "HEADPHONES001", "name": "Sony WH-1000XM5", "description": "Wireless noise cancelling headphones with premium sound", "category": "audio", "brand": "Sony", "price": 399.99, "rating": 4.9, "tags": ["wireless", "noise-cancelling", "premium"], "created_date": "2024-03-10", "location": { "lat": 47.6062, "lon": -122.3321 }, "in_stock": true, "sales_count": 120, "discount_percentage": 12.0, "reviews": [{"user": "music_lover", "rating": 5, "comment": "Best noise cancellation and sound quality"}], "suggest": { "input": ["Sony headphones", "WH-1000XM5"], "contexts": { "category": ["audio"] }}}
{ "index": { "_index": "ecommerce", "_id": "8" }}
{ "product_id": "WATCH001", "name": "Apple Watch Series 9", "description": "Advanced smartwatch with health monitoring and fitness tracking", "category": "wearables", "brand": "Apple", "price": 429.99, "rating": 4.5, "tags": ["smartwatch", "health", "fitness"], "created_date": "2024-02-20", "location": { "lat": 32.7767, "lon": -96.7970 }, "in_stock": true, "sales_count": 180, "discount_percentage": 7.0, "reviews": [{"user": "fitness_enthusiast", "rating": 4, "comment": "Great for fitness tracking and health monitoring"}], "suggest": { "input": ["Apple Watch", "smartwatch"], "contexts": { "category": ["wearables"] }}}

# Load user activity data for aggregation demos
POST /_bulk
{ "index": { "_index": "user_activity", "_id": "1" }}
{ "user_id": "user001", "session_id": "sess001", "activity_type": "page_view", "timestamp": "2024-03-01T10:30:00", "duration_minutes": 5, "page_views": 3, "user_agent": "Mozilla/5.0 Chrome", "ip_address": "192.168.1.100", "location": { "lat": 37.7749, "lon": -122.4194 }, "device_type": "desktop", "os": "Windows", "referrer": "google.com" }
{ "index": { "_index": "user_activity", "_id": "2" }}
{ "user_id": "user002", "session_id": "sess002", "activity_type": "purchase", "timestamp": "2024-03-01T14:15:00", "duration_minutes": 15, "page_views": 8, "user_agent": "Mozilla/5.0 Safari", "ip_address": "10.0.0.50", "location": { "lat": 40.7128, "lon": -74.0060 }, "device_type": "mobile", "os": "iOS", "referrer": "facebook.com" }
{ "index": { "_index": "user_activity", "_id": "3" }}
{ "user_id": "user003", "session_id": "sess003", "activity_type": "search", "timestamp": "2024-03-02T09:20:00", "duration_minutes": 3, "page_views": 2, "user_agent": "Mozilla/5.0 Firefox", "ip_address": "172.16.0.25", "location": { "lat": 34.0522, "lon": -118.2437 }, "device_type": "desktop", "os": "macOS", "referrer": "direct" }
{ "index": { "_index": "user_activity", "_id": "4" }}
{ "user_id": "user001", "session_id": "sess004", "activity_type": "page_view", "timestamp": "2024-03-02T16:45:00", "duration_minutes": 7, "page_views": 5, "user_agent": "Mozilla/5.0 Chrome", "ip_address": "192.168.1.100", "location": { "lat": 37.7749, "lon": -122.4194 }, "device_type": "desktop", "os": "Windows", "referrer": "twitter.com" }
{ "index": { "_index": "user_activity", "_id": "5" }}
{ "user_id": "user004", "session_id": "sess005", "activity_type": "purchase", "timestamp": "2024-03-03T11:30:00", "duration_minutes": 20, "page_views": 12, "user_agent": "Mozilla/5.0 Edge", "ip_address": "203.0.113.45", "location": { "lat": 41.8781, "lon": -87.6298 }, "device_type": "tablet", "os": "Android", "referrer": "instagram.com" }
{ "index": { "_index": "user_activity", "_id": "6" }}
{ "user_id": "user002", "session_id": "sess006", "activity_type": "search", "timestamp": "2024-03-03T08:00:00", "duration_minutes": 2, "page_views": 1, "user_agent": "Mozilla/5.0 Safari", "ip_address": "10.0.0.50", "location": { "lat": 40.7128, "lon": -74.0060 }, "device_type": "mobile", "os": "iOS", "referrer": "bing.com" }

# Verify data is loaded correctly
GET /ecommerce/_count
GET /user_activity/_count

# Check mapping
GET /ecommerce/_mapping
GET /user_activity/_mapping