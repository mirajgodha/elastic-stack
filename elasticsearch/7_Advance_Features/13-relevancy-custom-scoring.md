# =============================================================================
# RELEVANCY & CUSTOM SCORING - BOOSTING AND ADVANCED SCORING
# =============================================================================
# Advanced techniques to control document scoring and relevance

# BOOSTING - Simple query-time boosting
# =====================================

# Example 1: Boost specific fields in multi_match
GET /ecommerce/_search
{
  "query": {
    "multi_match": {
      "query": "Apple laptop",
      "fields": [
        "name^3",
        "brand^2",
        "description"
      ]
    }
  }
}

# Example 2: Bool query with boosted clauses
GET /ecommerce/_search
{
  "query": {
    "bool": {
      "must": [
        {"match": {"category": "laptops"}}
      ],
      "should": [
        {"match": {"brand": {"query": "Apple", "boost": 2.0}}},
        {"range": {"rating": {"gte": 4.5, "boost": 1.5}}},
        {"range": {"price": {"lte": 2000, "boost": 0.5}}}
      ]
    }
  }
}

# Example 3: Boosting query (boost positive, reduce negative)
GET /ecommerce/_search
{
  "query": {
    "boosting": {
      "positive": {
        "match": {
          "description": "professional laptop"
        }
      },
      "negative": {
        "match": {
          "description": "gaming"
        }
      },
      "negative_boost": 0.3
    }
  }
}

# FUNCTION SCORE - Advanced custom scoring
# ========================================

# Example 4: Random score function
GET /ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "match": {"category": "smartphones"}
      },
      "functions": [
        {
          "random_score": {
            "seed": 42
          }
        }
      ],
      "score_mode": "sum",
      "boost_mode": "replace"
    }
  }
}

# Example 5: Field value factor scoring
GET /ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "match_all": {}
      },
      "functions": [
        {
          "field_value_factor": {
            "field": "rating",
            "factor": 1.5,
            "modifier": "log1p"
          }
        },
        {
          "field_value_factor": {
            "field": "sales_count",
            "factor": 0.01,
            "modifier": "sqrt"
          }
        }
      ],
      "score_mode": "multiply",
      "boost_mode": "multiply"
    }
  }
}

# Example 6: Decay function scoring (gaussian decay)
GET /ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "match": {"description": "smartphone"}
      },
      "functions": [
        {
          "gauss": {
            "price": {
              "origin": 800,
              "scale": 200,
              "decay": 0.5
            }
          }
        },
        {
          "linear": {
            "rating": {
              "origin": 5.0,
              "scale": 1.0
            }
          }
        }
      ],
      "score_mode": "sum"
    }
  }
}

# Example 7: Geographic decay function
GET /ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "match_all": {}
      },
      "functions": [
        {
          "exp": {
            "location": {
              "origin": {
                "lat": 37.7749,
                "lon": -122.4194
              },
              "scale": "100km",
              "decay": 0.33
            }
          }
        }
      ]
    }
  }
}

# Example 8: Script score for complex calculations
GET /ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "match": {"in_stock": true}
      },
      "functions": [
        {
          "script_score": {
            "script": {
              "source": "Math.log(2 + doc['sales_count'].value) * doc['rating'].value / Math.log(doc['price'].value / 100 + 1)"
            }
          }
        }
      ]
    }
  }
}

# Example 9: Weight functions with filters
GET /ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "match": {"category": "laptops"}
      },
      "functions": [
        {
          "filter": {"term": {"brand": "Apple"}},
          "weight": 2.0
        },
        {
          "filter": {"range": {"rating": {"gte": 4.5}}},
          "weight": 1.5
        },
        {
          "filter": {"range": {"discount_percentage": {"gt": 10}}},
          "weight": 1.2
        }
      ],
      "score_mode": "sum",
      "boost_mode": "multiply"
    }
  }
}

# Example 10: Complex scoring with multiple functions
GET /ecommerce/_search
{
  "query": {
    "function_score": {
      "query": {
        "multi_match": {
          "query": "professional laptop",
          "fields": ["name^2", "description"]
        }
      },
      "functions": [
        {
          "filter": {"term": {"brand": "Apple"}},
          "weight": 1.5
        },
        {
          "field_value_factor": {
            "field": "rating",
            "factor": 1.0,
            "modifier": "ln1p",
            "missing": 1
          }
        },
        {
          "gauss": {
            "price": {
              "origin": 1500,
              "scale": 500,
              "decay": 0.6
            }
          }
        },
        {
          "script_score": {
            "script": {
              "source": "doc['sales_count'].value > 100 ? 1.2 : 1.0"
            }
          }
        }
      ],
      "score_mode": "multiply",
      "boost_mode": "sum",
      "max_boost": 2.0,
      "min_score": 1.0
    }
  }
}

# SCRIPT SCORE QUERY - Pure script-based scoring
# ==============================================

# Example 11: Simple script score
GET /ecommerce/_search
{
  "query": {
    "script_score": {
      "query": {
        "match": {"category": "smartphones"}
      },
      "script": {
        "source": "_score * doc['rating'].value * Math.log(doc['sales_count'].value + 1)"
      }
    }
  }
}

# Example 12: Script score with parameters
GET /ecommerce/_search
{
  "query": {
    "script_score": {
      "query": {
        "match_all": {}
      },
      "script": {
        "source": "params.rating_weight * doc['rating'].value + params.sales_weight * Math.log(doc['sales_count'].value + 1)",
        "params": {
          "rating_weight": 2.0,
          "sales_weight": 0.5
        }
      }
    }
  }
}

# Key Points:
# - Field boosting (^2): Simple way to increase field importance
# - Bool query boosting: Boost individual clauses
# - Function score: Advanced scoring with multiple functions
# - Random score: Randomize results for testing or variety
# - Field value factor: Use field values in scoring calculation
# - Decay functions: Gaussian, linear, exponential decay
# - Script score: Custom scoring logic with Painless scripts
# - score_mode: How to combine function scores (multiply, sum, avg, first, max, min)
# - boost_mode: How to combine with query score (multiply, replace, sum, avg, max, min)