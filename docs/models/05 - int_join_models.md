# Intermediate Join Models

## 1. Introduction

The `int_join_models` is a powerful model type for combining data from multiple models through SQL joins. It enables you to create enriched datasets by bringing together related information from different staging or intermediate models, forming the backbone of dimensional modeling and analytical data preparation.

## 2. Creating with Visual Editor

You can create this model type using the visual canvas:

1. Run `DJ: Create Model` from Command Palette, or select "Create Model" under Actions in tree view
2. Step 1 - Basic Information: Select model type `int_join_models`, enter group, topic, and name
3. Step 2 - Data Modeling: Use the visual editor
4. Add Select nodes for models, add Join node, configure join types and conditions (see join workflow example in Visual Editor guide)

See [Visual Editor Guide](../VISUAL_EDITOR.md) for complete canvas documentation and workflow examples.

---

## 3. Purpose

The primary purposes of the `int_join_models` are:

- **Data Enrichment:** Combine data from different models to add context and detail
- **Relationship Building:** Create explicit relationships between different business entities
- **Dimensional Modeling:** Build fact tables enriched with dimensional attributes
- **Data Consolidation:** Merge data from disparate sources into unified analytical views
- **Business Logic Implementation:** Apply complex business rules that require data from multiple sources

## 4. Model Configuration

An `int_join_models` requires the following configuration parameters:

- **`type`:** `int_join_models`
- **`group`:** A logical grouping for the model (e.g., `jaffle_shop`, `ecommerce`, `finance`)
- **`name`:** A unique name for the model following naming conventions
- **`materialized`:** Either `ephemeral` or `incremental`
- **`from`:** Specifies the base model and join configurations:
  - **`model`:** The base model (string identifier)
  - **`join`:** Array of join configurations
- **`select`:** Array of column definitions (supports model-level and column-level selection)
- **`where`:** Optional filtering conditions
- **`group_by`:** Optional grouping for aggregations
- **`having`:** Optional post-aggregation filtering
- **`order_by`:** Optional result ordering
- **`limit`:** Optional row limit
- **`offset`:** Optional row offset

**Note:** `topic` is optional for `int_join_models` (unlike other model types where it's required).

## 5. Join Configuration

Each join in the `join` array requires:

- **`model`:** The model to join (string identifier)
- **`type`:** Join type - `inner`, `left`, `right`, `full`, or `cross` (defaults to `inner`)
- **`on`:** Join condition (required for all join types except `cross`)
  - **`and`:** Array of conditions combined with AND logic
    - **`expr`:** SQL expression for the join condition
- **`override_alias`:** Optional alias for the joined model

## 6. Basic Example: Orders Enriched with Customer and Store Data

Let's create an enriched orders model by joining order data with customer and store information.

**Prerequisites:** We have `stg__sales__orders__standardized`, `stg__customers__profiles__clean`, and `stg__sales__stores__locations` from our staging examples.

```json
{
  "type": "int_join_models",
  "group": "sales",
  "topic": "orders",
  "name": "enriched",
  "from": {
    "model": "stg__sales__orders__standardized",
    "join": [
      {
        "model": "stg__customers__profiles__clean",
        "type": "left",
        "on": {
          "and": [
            {
              "expr": "stg__sales__orders__standardized.customer_id = stg__customers__profiles__clean.customer_id"
            }
          ]
        }
      },
      {
        "model": "stg__sales__stores__locations",
        "type": "left",
        "on": {
          "and": [
            {
              "expr": "stg__sales__orders__standardized.store_id = stg__sales__stores__locations.store_id"
            }
          ]
        }
      }
    ]
  },
  "select": [
    {
      "model": "stg__sales__orders__standardized",
      "type": "all_from_model"
    },
    {
      "name": "customer_name",
      "expr": "stg__customers__profiles__clean.customer_name",
      "type": "dim",
      "description": "Customer full name"
    },
    {
      "name": "customer_first_name",
      "expr": "stg__customers__profiles__clean.first_name",
      "type": "dim",
      "description": "Customer first name"
    },
    {
      "name": "store_name",
      "expr": "stg__sales__stores__locations.store_name",
      "type": "dim",
      "description": "Store location name"
    },
    {
      "name": "store_tax_rate",
      "expr": "stg__sales__stores__locations.tax_rate",
      "type": "fct",
      "description": "Store tax rate"
    },
    {
      "name": "order_total_dollars",
      "expr": "CAST(stg__sales__orders__standardized.order_total_cents AS DECIMAL(10,2)) / 100.0",
      "type": "fct",
      "description": "Order total in dollars"
    }
  ]
}
```

This example demonstrates:

- **Multiple left joins:** Enriching orders with both customer and store data
- **Model-level selection:** Include all columns from the base orders model using `all_from_model`
- **Column-level selection:** Add specific columns from joined models
- **Data type conversion:** Converting cents to dollars with proper decimal casting
- **Comprehensive documentation:** Each column includes business context

## 7. Advanced Example: Supply Chain Cost Analysis

Let's create a supply chain cost analysis model by joining supply inventory with product catalog data.

**Prerequisites:** We have `stg__supply_chain__supplies__inventory` and `stg__products__catalog__catalog` from our staging examples.

```json
{
  "type": "int_join_models",
  "group": "supply_chain",
  "topic": "supplies",
  "name": "cost_analysis",
  "from": {
    "model": "stg__supply_chain__supplies__inventory",
    "join": [
      {
        "model": "stg__products__catalog__catalog",
        "type": "left",
        "on": {
          "and": [
            {
              "expr": "stg__supply_chain__supplies__inventory.product_sku = stg__products__catalog__catalog.product_sku"
            }
          ]
        }
      }
    ]
  },
  "select": [
    {
      "model": "stg__supply_chain__supplies__inventory",
      "type": "all_from_model"
    },
    {
      "name": "product_name",
      "expr": "stg__products__catalog__catalog.product_name",
      "type": "dim",
      "description": "Product name that uses this supply"
    },
    {
      "name": "product_type",
      "expr": "stg__products__catalog__catalog.product_type",
      "type": "dim",
      "description": "Product category (jaffle, beverage)"
    },
    {
      "name": "product_price_dollars",
      "expr": "stg__products__catalog__catalog.price_dollars",
      "type": "fct",
      "description": "Product selling price in dollars"
    },
    {
      "name": "supply_to_price_ratio",
      "expr": "CASE WHEN stg__products__catalog__catalog.price_dollars > 0 THEN (CAST(stg__supply_chain__supplies__inventory.cost_cents AS DECIMAL(10,2)) / 100.0) / stg__products__catalog__catalog.price_dollars ELSE NULL END",
      "type": "fct",
      "description": "Supply cost as percentage of product price"
    },
    {
      "name": "cost_tier",
      "expr": "CASE WHEN stg__supply_chain__supplies__inventory.cost_dollars >= 0.40 THEN 'High Cost' WHEN stg__supply_chain__supplies__inventory.cost_dollars >= 0.20 THEN 'Medium Cost' ELSE 'Low Cost' END",
      "type": "dim",
      "description": "Supply cost tier classification"
    }
  ]
}
```

This example demonstrates:

- **Business logic in joins:** Connecting supply chain data with product information
- **Complex calculations:** Supply cost to price ratio with null handling
- **Categorical classification:** Cost tier assignment based on business rules
- **Cross-domain analysis:** Linking supply chain and product domains
- **Risk assessment:** Understanding cost efficiency across products

## 8. Join Types Reference

### Inner Join

Use when you only want records that exist in both models:

```json
{
  "model": "stg__customers__profiles__clean",
  "type": "inner",
  "on": {
    "and": [
      {
        "expr": "base_model.customer_id = stg__customers__profiles__clean.customer_id"
      }
    ]
  }
}
```

### Left Join

Use when you want all records from the base model, with optional data from the joined model:

```json
{
  "model": "stg__sales__stores__locations",
  "type": "left",
  "on": {
    "and": [
      {
        "expr": "base_model.store_id = stg__sales__stores__locations.store_id"
      }
    ]
  }
}
```

## 9. Best Practices

### Performance Optimization

- **Use appropriate join types:** Inner joins are faster than outer joins
- **Join on indexed columns:** Ensure join keys are properly indexed
- **Filter early:** Apply WHERE conditions before joins when possible
- **Limit result sets:** Use LIMIT for development and testing

### Data Quality

- **Handle null values:** Consider how joins affect null handling
- **Validate join keys:** Ensure referential integrity between models
- **Document relationships:** Clearly describe business relationships
- **Test join results:** Verify expected row counts and data completeness

### Model Design

- **Start with base model:** Choose the most important model as your base
- **Logical join order:** Join related models in business-logical sequence
- **Consistent naming:** Use clear, descriptive column names
- **Comprehensive documentation:** Document each join's business purpose

## 10. Integration with Data Pipeline

The `int_join_models` serves as a crucial bridge in your data pipeline:

1. **Upstream Dependencies:** Staging models provide clean, standardized data
2. **Join Processing:** Combines related data from multiple domains
3. **Downstream Usage:** Feeds mart models and analytics dashboards
4. **Business Value:** Creates enriched datasets for analysis and reporting

**Example Pipeline Flow:**

```text
stg__customers__profiles__clean + stg__sales__orders__standardized
                    ↓
            int__sales__orders__enriched
                    ↓
        mart__customers__dashboard__analytics
```

---

_This documentation provides comprehensive coverage of `int_join_models` with real-world examples from the jaffle shop business scenario. Each example demonstrates practical applications and follows established naming conventions._
