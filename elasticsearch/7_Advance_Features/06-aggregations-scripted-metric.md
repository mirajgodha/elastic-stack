# =============================================================================
# AGGREGATIONS - SCRIPTED METRIC
# =============================================================================
# Scripted metric allows you to write custom aggregation logic using scripts
# Useful for complex calculations that can't be done with built-in aggregations

# Example 1: Calculate custom score for products (rating * sales_count / price)
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "value_score": {
      "scripted_metric": {
        "init_script": "state.values = []",
        "map_script": "state.values.add(doc['rating'].value * doc['sales_count'].value / doc['price'].value)",
        "combine_script": "double sum = 0; for (v in state.values) sum += v; return sum",
        "reduce_script": "double total = 0; for (s in states) total += s; return total"
      }
    }
  }
}

# Example 2: Calculate weighted average rating (weighted by sales_count)
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "weighted_avg_rating": {
      "scripted_metric": {
        "init_script": "state.ratings = []; state.weights = []",
        "map_script": "state.ratings.add(doc['rating'].value); state.weights.add(doc['sales_count'].value)",
        "combine_script": "double totalWeighted = 0; double totalWeights = 0; for (int i = 0; i < state.ratings.size(); i++) { totalWeighted += state.ratings[i] * state.weights[i]; totalWeights += state.weights[i]; } return ['weighted': totalWeighted, 'total_weights': totalWeights]",
        "reduce_script": "double totalWeighted = 0; double totalWeights = 0; for (s in states) { totalWeighted += s.weighted; totalWeights += s.total_weights; } return totalWeights > 0 ? totalWeighted / totalWeights : 0"
      }
    }
  }
}

# Example 3: Count products in different price ranges
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "price_distribution": {
      "scripted_metric": {
        "init_script": "state.low = 0; state.medium = 0; state.high = 0; state.premium = 0",
        "map_script": "double price = doc['price'].value; if (price < 500) state.low++; else if (price < 1000) state.medium++; else if (price < 2000) state.high++; else state.premium++",
        "combine_script": "return ['low': state.low, 'medium': state.medium, 'high': state.high, 'premium': state.premium]",
        "reduce_script": "int low = 0; int medium = 0; int high = 0; int premium = 0; for (s in states) { low += s.low; medium += s.medium; high += s.high; premium += s.premium; } return ['low': low, 'medium': medium, 'high': high, 'premium': premium]"
      }
    }
  }
}

# Example 4: Calculate revenue per brand
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "revenue_by_brand": {
      "scripted_metric": {
        "init_script": "state.brandRevenue = [:]",
        "map_script": "String brand = doc['brand'].value; double revenue = doc['price'].value * doc['sales_count'].value; if (state.brandRevenue.containsKey(brand)) { state.brandRevenue[brand] += revenue; } else { state.brandRevenue[brand] = revenue; }",
        "combine_script": "return state.brandRevenue",
        "reduce_script": "Map totalRevenue = [:]; for (brandMap in states) { for (entry in brandMap.entrySet()) { String brand = entry.getKey(); double revenue = entry.getValue(); if (totalRevenue.containsKey(brand)) { totalRevenue[brand] += revenue; } else { totalRevenue[brand] = revenue; } } } return totalRevenue"
      }
    }
  }
}

# Example 5: Calculate discount impact on sales
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "discount_analysis": {
      "scripted_metric": {
        "init_script": "state.discountedSales = 0; state.fullPriceSales = 0",
        "map_script": "if (doc['discount_percentage'].value > 0) { state.discountedSales += doc['sales_count'].value; } else { state.fullPriceSales += doc['sales_count'].value; }",
        "combine_script": "return ['discounted': state.discountedSales, 'full_price': state.fullPriceSales]",
        "reduce_script": "int totalDiscounted = 0; int totalFullPrice = 0; for (s in states) { totalDiscounted += s.discounted; totalFullPrice += s.full_price; } return ['discounted_sales': totalDiscounted, 'full_price_sales': totalFullPrice, 'discount_percentage_of_sales': totalDiscounted + totalFullPrice > 0 ? (totalDiscounted * 100.0) / (totalDiscounted + totalFullPrice) : 0]"
      }
    }
  }
}

# Example 6: User activity pattern analysis
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "activity_patterns": {
      "scripted_metric": {
        "init_script": "state.hourlyActivity = [:]",
        "map_script": "long timestamp = doc['timestamp'].value.millis; int hour = (int)((timestamp / (1000 * 60 * 60)) % 24); if (state.hourlyActivity.containsKey(hour)) { state.hourlyActivity[hour]++; } else { state.hourlyActivity[hour] = 1; }",
        "combine_script": "return state.hourlyActivity",
        "reduce_script": "Map totalActivity = [:]; for (hourMap in states) { for (entry in hourMap.entrySet()) { int hour = entry.getKey(); int count = entry.getValue(); if (totalActivity.containsKey(hour)) { totalActivity[hour] += count; } else { totalActivity[hour] = count; } } } return totalActivity"
      }
    }
  }
}

# Example 7: Complex stock analysis with parameters
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "stock_analysis": {
      "scripted_metric": {
        "params": {
          "low_stock_threshold": 50,
          "high_sales_threshold": 100
        },
        "init_script": "state.lowStock = 0; state.highSales = 0; state.criticalItems = []",
        "map_script": "int sales = doc['sales_count'].value; boolean inStock = doc['in_stock'].value; String productName = doc['name'].value; if (sales < params.low_stock_threshold && inStock) { state.lowStock++; state.criticalItems.add(productName); } if (sales > params.high_sales_threshold) { state.highSales++; }",
        "combine_script": "return ['low_stock': state.lowStock, 'high_sales': state.highSales, 'critical_items': state.criticalItems]",
        "reduce_script": "int totalLowStock = 0; int totalHighSales = 0; List allCriticalItems = []; for (s in states) { totalLowStock += s.low_stock; totalHighSales += s.high_sales; allCriticalItems.addAll(s.critical_items); } return ['low_stock_count': totalLowStock, 'high_sales_count': totalHighSales, 'critical_items': allCriticalItems]"
      }
    }
  }
}

# Example 8: Performance metrics with time-based calculations
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "performance_metrics": {
      "scripted_metric": {
        "init_script": "state.sessions = [:]; state.totalDuration = 0; state.totalPageViews = 0",
        "map_script": "String userId = doc['user_id'].value; int duration = doc['duration_minutes'].value; int pageViews = doc['page_views'].value; state.totalDuration += duration; state.totalPageViews += pageViews; if (!state.sessions.containsKey(userId)) { state.sessions[userId] = ['duration': 0, 'pageViews': 0, 'sessions': 0]; } state.sessions[userId].duration += duration; state.sessions[userId].pageViews += pageViews; state.sessions[userId].sessions++",
        "combine_script": "return ['sessions': state.sessions, 'totalDuration': state.totalDuration, 'totalPageViews': state.totalPageViews]",
        "reduce_script": "Map allSessions = [:]; int grandTotalDuration = 0; int grandTotalPageViews = 0; for (s in states) { grandTotalDuration += s.totalDuration; grandTotalPageViews += s.totalPageViews; for (entry in s.sessions.entrySet()) { String userId = entry.getKey(); Map userStats = entry.getValue(); if (!allSessions.containsKey(userId)) { allSessions[userId] = ['duration': 0, 'pageViews': 0, 'sessions': 0]; } allSessions[userId].duration += userStats.duration; allSessions[userId].pageViews += userStats.pageViews; allSessions[userId].sessions += userStats.sessions; } } return ['user_sessions': allSessions, 'total_duration': grandTotalDuration, 'total_page_views': grandTotalPageViews, 'unique_users': allSessions.size()]"
      }
    }
  }
}

# Key Points:
# - init_script: Initialize variables for each shard
# - map_script: Process each document
# - combine_script: Combine results within each shard
# - reduce_script: Combine results across all shards
# - params: Pass parameters to scripts
# - Scripts can access doc values, _source, and parameters
# - Use painless scripting language (secure and fast)