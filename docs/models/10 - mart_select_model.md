# Mart Select Model

## 1. Introduction

The `mart_select_model` is designed for creating analytics-ready datasets that serve as the final layer in the data pipeline. These models select and transform data from intermediate models to create business-focused tables optimized for reporting, dashboards, and business intelligence applications. Mart models represent the "presentation layer" of your data warehouse, providing clean, well-structured data for end-users.

## 2. Creating with Visual Editor

You can create this model type using the visual canvas:

1. Run `DJ: Create Model` from Command Palette, or select "Create Model" under Actions in tree view
2. Step 1 - Basic Information: Select model type `mart_select_model`, enter group, topic, and name
3. Step 2 - Data Modeling: Use the visual editor
4. Add Select node, configure final dimensions and metrics

See [Visual Editor Guide](../VISUAL_EDITOR.md) for complete canvas documentation and workflow examples.

---

## 3. Purpose

The primary purposes of the `mart_select_model` are:

- **Business Intelligence:** Create analytics-ready datasets for BI tools and dashboards
- **Reporting Optimization:** Provide pre-calculated, business-focused data for reports
- **User-Friendly Interface:** Present data in business terms that stakeholders understand
- **Performance Enhancement:** Pre-aggregate and optimize data for analytical queries
- **Data Governance:** Implement consistent business logic and definitions across the organization
- **Self-Service Analytics:** Enable business users to access clean, reliable data independently

## 4. Model Configuration

A `mart_select_model` requires the following configuration parameters:

- **`type`:** `mart_select_model`
- **`group`:** A logical grouping for the model (e.g., `customer_experience`, `finance`, `sales_operations`)
- **`topic`:** A more specific categorization within the group (e.g., `customers`, `orders`, `products`)
- **`name`:** A unique name for the model following naming conventions
- **`from`:** Specifies the source model:
  - **`model`:** The intermediate model to select from (string identifier)
- **`select`:** Array of column definitions specifying which data to include and how to present it
- **`group_by`:** Optional grouping for aggregations

## 5. Mart Process

The `mart_select_model` performs the following transformation:

1. **Data Selection:** Selects relevant columns from intermediate models
2. **Business Logic Application:** Applies business rules, calculations, and transformations
3. **Data Presentation:** Formats data using business-friendly names and structures
4. **Quality Assurance:** Implements data quality checks and business validations
5. **Optimization:** Structures data for optimal analytical performance
6. **Documentation:** Provides clear business context and definitions

## 6. Basic Example: Customer Dashboard Analytics

Let's create a customer analytics mart for dashboard reporting using our jaffle shop customer data.

**Scenario:** Create a business-ready customer analytics table for customer experience teams and dashboards.

**Prerequisites:** We have `int__customers__profiles__summary` from our intermediate examples.

```json
{
  "type": "mart_select_model",
  "group": "customers",
  "topic": "dashboard",
  "name": "analytics",
  "from": {
    "model": "int__customers__profiles__summary"
  },
  "select": [
    {
      "name": "customer_id",
      "expr": "customer_id",
      "type": "dim",
      "description": "Unique customer identifier"
    },
    {
      "name": "customer_name",
      "expr": "customer_name",
      "type": "dim",
      "description": "Customer full name"
    },
    {
      "name": "first_name",
      "expr": "first_name",
      "type": "dim",
      "description": "Customer first name"
    },
    {
      "name": "last_name",
      "expr": "last_name",
      "type": "dim",
      "description": "Customer last name"
    },
    {
      "name": "customer_segment",
      "expr": "CASE WHEN total_dollars >= 50.00 THEN 'VIP' WHEN total_dollars >= 15.00 THEN 'Regular' ELSE 'New' END",
      "type": "dim",
      "description": "Customer segment classification"
    },
    {
      "name": "has_last_name",
      "expr": "last_name IS NOT NULL",
      "type": "dim",
      "description": "Whether customer provided a last name"
    }
  ]
}
```

This example demonstrates:

- **Business-friendly presentation:** Clean customer data for dashboard consumption
- **Customer segmentation:** Business logic for categorizing customers
- **Boolean flags:** Easy-to-use indicators for filtering and analysis
- **Comprehensive documentation:** Each field includes business context
- **Dashboard optimization:** Simple, clean structure for reporting tools

## 7. Advanced Example: Sales Revenue Reporting

Let's create a comprehensive sales revenue mart with time dimensions and business classifications.

**Prerequisites:** We have `int__sales__orders__enriched` from our intermediate examples.

```json
{
  "type": "mart_select_model",
  "group": "sales",
  "topic": "reporting",
  "name": "revenue",
  "from": {
    "model": "int__sales__orders__enriched"
  },
  "select": [
    {
      "name": "order_id",
      "expr": "order_id",
      "type": "dim",
      "description": "Unique order identifier"
    },
    {
      "name": "customer_id",
      "expr": "customer_id",
      "type": "dim",
      "description": "Customer identifier"
    },
    {
      "name": "customer_name",
      "expr": "customer_name",
      "type": "dim",
      "description": "Customer name"
    },
    {
      "name": "store_id",
      "expr": "store_id",
      "type": "dim",
      "description": "Store identifier"
    },
    {
      "name": "store_name",
      "expr": "store_name",
      "type": "dim",
      "description": "Store location name"
    },
    {
      "name": "order_date",
      "expr": "order_date",
      "type": "dim",
      "description": "Order date"
    },
    {
      "name": "order_year",
      "expr": "EXTRACT(YEAR FROM order_date)",
      "type": "dim",
      "description": "Order year"
    },
    {
      "name": "order_month",
      "expr": "EXTRACT(MONTH FROM order_date)",
      "type": "dim",
      "description": "Order month"
    },
    {
      "name": "order_quarter",
      "expr": "EXTRACT(QUARTER FROM order_date)",
      "type": "dim",
      "description": "Order quarter"
    },
    {
      "name": "subtotal_dollars",
      "expr": "CAST(subtotal_cents AS DECIMAL(10,2)) / 100.0",
      "type": "fct",
      "description": "Order subtotal in dollars"
    },
    {
      "name": "tax_dollars",
      "expr": "CAST(tax_paid_cents AS DECIMAL(10,2)) / 100.0",
      "type": "fct",
      "description": "Tax amount in dollars"
    },
    {
      "name": "total_dollars",
      "expr": "order_total_dollars",
      "type": "fct",
      "description": "Total order amount in dollars"
    },
    {
      "name": "revenue_tier",
      "expr": "CASE WHEN order_total_dollars >= 15.00 THEN 'High' WHEN order_total_dollars >= 8.00 THEN 'Medium' ELSE 'Low' END",
      "type": "dim",
      "description": "Revenue tier classification"
    },
    {
      "name": "is_weekend_order",
      "expr": "day_of_week(order_date) IN (6, 7)",
      "type": "dim",
      "description": "Boolean flag for weekend orders"
    }
  ]
}
```

This example demonstrates:

- **Time dimensions:** Year, month, quarter extractions for reporting
- **Unit conversion:** Converting cents to dollars for business users
- **Revenue classification:** Business logic for categorizing order values
- **Weekend analysis:** Boolean flags for temporal analysis
- **Comprehensive reporting:** All key dimensions and facts for sales analysis

## 8. Best Practices

### Business-Friendly Design

- **Use clear naming:** Column names should be understandable by business users
- **Convert units:** Present data in business-friendly units (dollars vs cents)
- **Add classifications:** Create categorical fields for easy filtering and grouping
- **Include documentation:** Every column should have a business description

### Performance Optimization

- **Pre-aggregate when possible:** Reduce computation for downstream users
- **Choose appropriate materialization:** Table for frequently accessed data
- **Limit column selection:** Only include necessary fields for the use case
- **Consider indexing:** Structure data for optimal query performance

### Data Quality

- **Implement business rules:** Apply consistent logic across all marts
- **Handle edge cases:** Consider null values and data quality issues
- **Validate calculations:** Ensure derived metrics are accurate
- **Test thoroughly:** Verify data accuracy and completeness

## 9. Integration with Data Pipeline

The `mart_select_model` serves as the final presentation layer:

1. **Upstream Dependencies:** Intermediate models provide processed business data
2. **Business Transformation:** Apply final business logic and formatting
3. **Downstream Usage:** Feeds BI tools, dashboards, and analytics applications
4. **User Access:** Provides clean, documented data for business users

**Example Pipeline Flow:**

```text
int__customers__profiles__summary
            ↓
mart__customers__dashboard__analytics
            ↓
    Customer Dashboard & Reports
```

---

_This documentation provides comprehensive coverage of `mart_select_model` with real-world examples from the jaffle shop business scenario. Each example demonstrates practical applications for creating analytics-ready datasets._
