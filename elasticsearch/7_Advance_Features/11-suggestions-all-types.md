# =============================================================================
# SUGGESTIONS - TERM, PHRASE, COMPLETION, CONTEXT SUGGESTIONS
# =============================================================================
# Elasticsearch provides various suggestion types for search assistance

# TERM SUGGESTION - Suggests corrections for individual terms
# ==========================================================

# Example 1: Basic term suggestion for misspelled words
GET /ecommerce/_search
{
  "suggest": {
    "product_name_suggestion": {
      "text": "mackbook",
      "term": {
        "field": "name"
      }
    }
  }
}

# Example 2: Term suggestion with custom parameters
GET /ecommerce/_search
{
  "suggest": {
    "brand_suggestion": {
      "text": "appel",
      "term": {
        "field": "brand",
        "size": 3,
        "max_edits": 2,
        "min_word_length": 3,
        "prefix_length": 1
      }
    }
  }
}

# PHRASE SUGGESTION - Suggests corrections for entire phrases
# ==========================================================

# Example 3: Basic phrase suggestion
GET /ecommerce/_search
{
  "suggest": {
    "phrase_suggestion": {
      "text": "macbok pro laprop",
      "phrase": {
        "field": "name",
        "size": 2,
        "gram_size": 3,
        "direct_generator": [
          {
            "field": "name",
            "suggest_mode": "always"
          }
        ]
      }
    }
  }
}

# Example 4: Advanced phrase suggestion with smoothing
GET /ecommerce/_search
{
  "suggest": {
    "advanced_phrase": {
      "text": "smart fone with good camara",
      "phrase": {
        "field": "description",
        "size": 3,
        "gram_size": 2,
        "confidence": 1.0,
        "smoothing": {
          "laplace": {
            "alpha": 0.7
          }
        },
        "direct_generator": [
          {
            "field": "description",
            "suggest_mode": "popular",
            "min_word_length": 3
          }
        ]
      }
    }
  }
}

# COMPLETION SUGGESTION - Fast auto-complete functionality
# ========================================================

# Example 5: Basic completion suggestion
GET /ecommerce/_search
{
  "suggest": {
    "product_completion": {
      "prefix": "mac",
      "completion": {
        "field": "suggest",
        "size": 5
      }
    }
  }
}

# Example 6: Completion suggestion with fuzzy matching
GET /ecommerce/_search
{
  "suggest": {
    "fuzzy_completion": {
      "prefix": "ipone",
      "completion": {
        "field": "suggest",
        "size": 3,
        "fuzzy": {
          "fuzziness": "AUTO"
        }
      }
    }
  }
}

# Example 7: Completion with skip duplicates
GET /ecommerce/_search
{
  "suggest": {
    "unique_completion": {
      "prefix": "app",
      "completion": {
        "field": "suggest",
        "size": 5,
        "skip_duplicates": true
      }
    }
  }
}

# CONTEXT SUGGESTION - Context-aware completion
# =============================================

# Example 8: Context suggestion by category
GET /ecommerce/_search
{
  "suggest": {
    "category_completion": {
      "prefix": "app",
      "completion": {
        "field": "suggest",
        "size": 3,
        "contexts": {
          "category": ["laptops"]
        }
      }
    }
  }
}

# Example 9: Multiple context completion
GET /ecommerce/_search
{
  "suggest": {
    "multi_context_completion": {
      "prefix": "sam",
      "completion": {
        "field": "suggest",
        "size": 5,
        "contexts": {
          "category": ["smartphones", "tablets"]
        }
      }
    }
  }
}

# Example 10: Combined suggestions in one request
GET /ecommerce/_search
{
  "suggest": {
    "term_suggest": {
      "text": "macbok",
      "term": {
        "field": "name"
      }
    },
    "phrase_suggest": {
      "text": "macbok pro laprop",
      "phrase": {
        "field": "name",
        "size": 2
      }
    },
    "completion_suggest": {
      "prefix": "mac",
      "completion": {
        "field": "suggest",
        "size": 3
      }
    }
  }
}

# Example 11: Suggestion with query context
GET /ecommerce/_search
{
  "query": {
    "match": {
      "category": "laptops"
    }
  },
  "suggest": {
    "laptop_suggestions": {
      "text": "mackbook",
      "term": {
        "field": "name"
      }
    }
  }
}

# Example 12: Complex completion with multiple options
GET /ecommerce/_search
{
  "suggest": {
    "smart_completion": {
      "prefix": "ip",
      "completion": {
        "field": "suggest",
        "size": 5,
        "fuzzy": {
          "fuzziness": 1,
          "transpositions": true,
          "min_length": 3,
          "prefix_length": 1
        },
        "contexts": {
          "category": ["smartphones", "tablets"]
        }
      }
    }
  }
}

# Key Points:
# - Term suggestions: Fix individual misspelled words
# - Phrase suggestions: Fix entire phrases considering context
# - Completion suggestions: Fast auto-complete for user input
# - Context suggestions: Category or context-aware completions
# - Use fuzzy matching for typo tolerance
# - Combine multiple suggestion types in one request
# - Configure fuzziness, confidence, and other parameters for better results