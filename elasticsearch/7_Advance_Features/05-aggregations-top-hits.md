# =============================================================================
# AGGREGATIONS - TOP HITS
# =============================================================================
# Top hits aggregation returns the most relevant documents for each bucket
# Useful for getting actual document samples from aggregated data

# Example 1: Get top 2 products by category
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "top_products": {
          "top_hits": {
            "size": 2,
            "_source": ["name", "price", "rating"],
            "sort": [
              {
                "rating": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}

# Example 2: Get highest-rated product per brand
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "brands": {
      "terms": {
        "field": "brand"
      },
      "aggs": {
        "best_product": {
          "top_hits": {
            "size": 1,
            "_source": ["name", "rating", "price", "description"],
            "sort": [
              {
                "rating": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}

# Example 3: Get cheapest and most expensive product per category
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "cheapest": {
          "top_hits": {
            "size": 1,
            "_source": ["name", "price"],
            "sort": [
              {
                "price": {
                  "order": "asc"
                }
              }
            ]
          }
        },
        "most_expensive": {
          "top_hits": {
            "size": 1,
            "_source": ["name", "price"],
            "sort": [
              {
                "price": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}

# Example 4: Top hits with multiple sort criteria
# Get top 3 products with best rating-to-price ratio per category
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "best_value": {
          "top_hits": {
            "size": 3,
            "_source": ["name", "price", "rating", "sales_count"],
            "sort": [
              {
                "_script": {
                  "type": "number",
                  "script": {
                    "source": "doc['rating'].value / (doc['price'].value / 1000)"
                  },
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}

# Example 5: Top hits with highlighting
GET /ecommerce/_search
{
  "size": 0,
  "query": {
    "match": {
      "description": "advanced"
    }
  },
  "aggs": {
    "brands": {
      "terms": {
        "field": "brand"
      },
      "aggs": {
        "top_matching_products": {
          "top_hits": {
            "size": 2,
            "_source": ["name", "price"],
            "highlight": {
              "fields": {
                "description": {}
              }
            }
          }
        }
      }
    }
  }
}

# Example 6: User activity - Latest activity per user
GET /user_activity/_search
{
  "size": 0,
  "aggs": {
    "users": {
      "terms": {
        "field": "user_id"
      },
      "aggs": {
        "latest_activity": {
          "top_hits": {
            "size": 1,
            "_source": ["activity_type", "timestamp", "device_type"],
            "sort": [
              {
                "timestamp": {
                  "order": "desc"
                }
              }
            ]
          }
        },
        "total_page_views": {
          "sum": {
            "field": "page_views"
          }
        }
      }
    }
  }
}

# Example 7: Top hits with script fields
# Add calculated fields to the top hits results
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "categories": {
      "terms": {
        "field": "category"
      },
      "aggs": {
        "top_products_with_discount_price": {
          "top_hits": {
            "size": 2,
            "_source": ["name", "price", "discount_percentage"],
            "script_fields": {
              "discounted_price": {
                "script": {
                  "source": "doc['price'].value * (1 - doc['discount_percentage'].value / 100)"
                }
              }
            },
            "sort": [
              {
                "rating": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}

# Example 8: Top hits with stored fields (if you have stored fields)
GET /ecommerce/_search
{
  "size": 0,
  "aggs": {
    "in_stock_status": {
      "terms": {
        "field": "in_stock"
      },
      "aggs": {
        "sample_products": {
          "top_hits": {
            "size": 3,
            "_source": false,
            "stored_fields": ["name", "price"],
            "sort": [
              {
                "sales_count": {
                  "order": "desc"
                }
              }
            ]
          }
        }
      }
    }
  }
}

# Key Points:
# - Top hits returns actual documents, not just aggregated statistics
# - Use _source to specify which fields to include
# - Sort parameter determines which documents are returned as "top"
# - Size controls how many documents per bucket
# - Can combine with highlighting, script fields, and stored fields