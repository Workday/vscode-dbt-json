# Mart Join Models

## 1. Introduction

The `mart_join_models` is the most comprehensive mart model type, designed for creating sophisticated analytics datasets by joining multiple intermediate models. This model type serves as the ultimate presentation layer, combining data from various sources to create complete, business-ready datasets that provide 360-degree views of business entities like customers, products, or orders. These models are optimized for complex business intelligence, executive reporting, and advanced analytics.

## 2. Creating with Visual Editor

You can create this model type using the visual canvas:

1. Run `DJ: Create Model` from Command Palette, or select "Create Model" under Actions in tree view
2. Step 1 - Basic Information: Select model type `mart_join_models`, enter group, topic, and name
3. Step 2 - Data Modeling: Use the visual editor
4. Add Join node for comprehensive 360-degree view, configure all relationships

See [Visual Editor Guide](../VISUAL_EDITOR.md) for complete canvas documentation and workflow examples.

---

## 3. Purpose

The primary purposes of the `mart_join_models` are:

- **Comprehensive Data Integration:** Combine multiple intermediate models to create complete business views
- **360-Degree Analytics:** Provide holistic perspectives on customers, products, orders, and business operations
- **Executive Reporting:** Create high-level datasets for strategic decision-making and executive dashboards
- **Advanced Business Intelligence:** Enable complex analytical queries across multiple business dimensions
- **Data Consolidation:** Centralize related business metrics and dimensions in single, authoritative datasets
- **Performance Optimization:** Pre-join related data to eliminate complex joins in downstream applications

## 4. Model Configuration

A `mart_join_models` requires the following configuration parameters:

- **`type`:** `mart_join_models`
- **`group`:** A logical grouping for the model (e.g., `jaffle_shop`, `enterprise`, `analytics`)
- **`topic`:** A more specific categorization within the group (e.g., `customers`, `orders`, `products`)
- **`name`:** A unique name for the model following naming conventions
- **`from`:** Specifies the base model and join operations:
  - **`model`:** The primary model for the join (string identifier)
  - **`join`:** Array of join configurations for additional models
- **`select`:** Array of column definitions from joined models
- **`where`:** Optional filtering conditions applied to the joined result
- **`group_by`:** Optional grouping for aggregations across joined data

## 5. Join Process

The `mart_join_models` performs the following transformation:

1. **Base Model Selection:** Establishes the primary model as the foundation
2. **Join Execution:** Performs specified joins with additional models using defined conditions
3. **Column Selection:** Selects and transforms columns from all joined models
4. **Business Logic Application:** Applies complex business rules across multiple data sources
5. **Data Quality Assurance:** Implements validations and quality checks across joined data
6. **Performance Optimization:** Structures the final dataset for optimal analytical performance

## 6. Basic Example: Comprehensive Product Analytics

Let's create a comprehensive product analytics dataset that combines menu performance with cost efficiency data.

**Scenario:** Create a complete product analytics dataset for executive reporting and strategic decision-making.

**Prerequisites:** We have `mart__products__reporting__menu_analytics` and `mart__products__reporting__cost_efficiency` from our mart examples.

```json
{
  "type": "mart_join_models",
  "group": "analytics",
  "topic": "dashboard",
  "name": "comprehensive_analytics",
  "from": {
    "model": "mart__products__reporting__menu_analytics",
    "join": [
      {
        "model": "mart__products__reporting__cost_efficiency",
        "type": "left",
        "on": {
          "and": [
            {
              "expr": "mart__products__reporting__menu_analytics.product_sku = mart__products__reporting__cost_efficiency.product_sku"
            }
          ]
        }
      }
    ]
  },
  "select": [
    {
      "name": "product_sku",
      "expr": "mart__products__reporting__menu_analytics.product_sku",
      "type": "dim",
      "description": "Product SKU identifier"
    },
    {
      "name": "product_type",
      "expr": "mart__products__reporting__menu_analytics.product_type",
      "type": "dim",
      "description": "Product category (jaffle/beverage)"
    },
    {
      "name": "total_orders",
      "expr": "mart__products__reporting__menu_analytics.total_orders",
      "type": "fct",
      "description": "Number of orders containing this product"
    },
    {
      "name": "total_items_sold",
      "expr": "mart__products__reporting__menu_analytics.total_items_sold",
      "type": "fct",
      "description": "Total quantity sold"
    },
    {
      "name": "supply_cost_dollars",
      "expr": "mart__products__reporting__cost_efficiency.supply_cost_dollars",
      "type": "fct",
      "description": "Individual supply cost in dollars"
    },
    {
      "name": "cost_efficiency_category",
      "expr": "mart__products__reporting__cost_efficiency.cost_efficiency_category",
      "type": "dim",
      "description": "Cost efficiency classification"
    },
    {
      "name": "perishable_risk_level",
      "expr": "mart__products__reporting__cost_efficiency.perishable_risk_level",
      "type": "dim",
      "description": "Risk level based on perishable supplies"
    },
    {
      "name": "business_impact_score",
      "expr": "CASE WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Highly Efficient' THEN 100 WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Efficient' THEN 85 WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Moderate' THEN 70 ELSE 50 END",
      "type": "fct",
      "description": "Business impact score based on cost efficiency (0-100)"
    },
    {
      "name": "strategic_recommendation",
      "expr": "CASE WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Highly Efficient' THEN 'Promote & Expand' WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Efficient' THEN 'Monitor Performance' WHEN mart__products__reporting__cost_efficiency.cost_efficiency_category = 'Moderate' THEN 'Optimize Costs' ELSE 'Review Strategy' END",
      "type": "dim",
      "description": "Strategic business recommendation"
    }
  ]
}
```

This example demonstrates:

- **Mart-to-mart joins:** Combining multiple mart models for comprehensive analytics
- **Strategic business logic:** Calculating business impact scores and recommendations
- **Multi-dimensional analysis:** Combining sales performance with cost efficiency
- **Executive reporting:** Creating datasets optimized for strategic decision-making
- **Complex transformations:** Advanced CASE statements for business intelligence

## 7. Advanced Example: Customer 360 Analytics

Let's create a realistic customer 360 view that demonstrates more complex join patterns and business logic.

```json
{
  "type": "mart_join_models",
  "group": "customers",
  "topic": "analytics",
  "name": "customer_360_view",
  "from": {
    "model": "mart__customers__dashboard__analytics",
    "join": [
      {
        "model": "mart__sales__reporting__revenue",
        "type": "left",
        "on": {
          "and": [
            {
              "expr": "mart__customers__dashboard__analytics.customer_id = mart__sales__reporting__revenue.customer_id"
            }
          ]
        }
      }
    ]
  },
  "select": [
    {
      "name": "customer_id",
      "expr": "mart__customers__dashboard__analytics.customer_id",
      "type": "dim",
      "description": "Unique customer identifier"
    },
    {
      "name": "customer_name",
      "expr": "mart__customers__dashboard__analytics.customer_name",
      "type": "dim",
      "description": "Customer full name"
    },
    {
      "name": "customer_segment",
      "expr": "mart__customers__dashboard__analytics.customer_segment",
      "type": "dim",
      "description": "Customer segment classification"
    },
    {
      "name": "total_revenue_dollars",
      "expr": "SUM(mart__sales__reporting__revenue.total_dollars)",
      "type": "fct",
      "description": "Total customer revenue in dollars"
    },
    {
      "name": "total_orders",
      "expr": "COUNT(DISTINCT mart__sales__reporting__revenue.order_id)",
      "type": "fct",
      "description": "Total number of orders"
    },
    {
      "name": "avg_order_value_dollars",
      "expr": "AVG(mart__sales__reporting__revenue.total_dollars)",
      "type": "fct",
      "description": "Average order value in dollars"
    },
    {
      "name": "customer_value_tier",
      "expr": "CASE WHEN SUM(mart__sales__reporting__revenue.total_dollars) >= 500 THEN 'High Value' WHEN SUM(mart__sales__reporting__revenue.total_dollars) >= 200 THEN 'Medium Value' ELSE 'Standard Value' END",
      "type": "dim",
      "description": "Customer value tier based on total spending"
    },
    {
      "name": "is_weekend_shopper",
      "expr": "COUNT(CASE WHEN mart__sales__reporting__revenue.is_weekend_order THEN 1 END) > COUNT(*) * 0.5",
      "type": "dim",
      "description": "Whether customer primarily shops on weekends"
    }
  ],
  "group_by": [
    {
      "expr": "mart__customers__dashboard__analytics.customer_id"
    },
    {
      "expr": "mart__customers__dashboard__analytics.customer_name"
    },
    {
      "expr": "mart__customers__dashboard__analytics.customer_segment"
    }
  ]
}
```

This example demonstrates:

- **Customer aggregation:** Grouping revenue data by customer for 360-degree view
- **Cross-mart analysis:** Combining customer profiles with sales revenue data
- **Advanced aggregations:** SUM, COUNT DISTINCT, AVG across joined data
- **Behavioral insights:** Weekend shopping pattern analysis
- **Value-based segmentation:** Customer tiers based on spending behavior

## 8. Best Practices

### Join Strategy

- **Start with the most comprehensive model:** Choose the mart model with the richest data as your base
- **Use appropriate join types:** Left joins for optional enrichment, inner joins for required relationships
- **Consider data volume:** Be mindful of cartesian products and data explosion
- **Optimize join conditions:** Use indexed columns and efficient join predicates

### Business Logic

- **Centralize complex calculations:** Implement sophisticated business rules in mart joins
- **Create strategic metrics:** Develop KPIs that span multiple business domains
- **Add executive insights:** Include high-level categorizations and recommendations
- **Implement data quality checks:** Validate joined data for completeness and accuracy

### Performance Optimization

- **Pre-aggregate when possible:** Reduce computation in downstream applications
- **Choose appropriate materialization:** Table materialization for frequently accessed datasets
- **Monitor query performance:** Track execution times and optimize as needed
- **Consider incremental processing:** For large datasets with time-based updates

## 9. Integration with Data Pipeline

The `mart_join_models` serves as the ultimate presentation layer:

1. **Upstream Dependencies:** Multiple mart models provide specialized business views
2. **Comprehensive Integration:** Combines related business domains into unified datasets
3. **Downstream Usage:** Feeds executive dashboards, strategic reporting, and advanced analytics
4. **Business Value:** Enables holistic business intelligence and data-driven decision making

**Example Pipeline Flow:**

```text
mart__products__reporting__menu_analytics + mart__products__reporting__cost_efficiency
                                    ↓
                    mart__analytics__dashboard__comprehensive_analytics
                                    ↓
                        Executive Dashboards & Strategic Reports
```

---

_This documentation provides comprehensive coverage of `mart_join_models` with real-world examples from the jaffle shop business scenario. Each example demonstrates practical applications for creating comprehensive business intelligence datasets._
