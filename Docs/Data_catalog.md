# **Data Catalog**

## Overview
The gold layer consist of views, in which data is represented and is structured ready for bussines analysis and reporting.
These data objects consist on two dimension tables and one fact table.

## **Data Objects**
- [Customers (`gold.dim_customers`)](#customers)
- [Products (`gold.dim_products`)](#products)
- [Sales (`gold.fact_sales`)](#sales)

## *Customers*
[Back to Data objects](#data-objects)
### **`gold.dim_customers`** 
* **Purpose:** Contains customer details.

| Column name | Type | Values | Description |
|----|----|----|----|
| `customer_key`  | `bigint`  | $\mathbb{Z}^{+}$  | Surrogate key unique for each customer record.  |
| `customer_id` | `integer` | - | Unique numerical identifier of customers. |
| `customer_number` | `varchar(50)` | - | Unique alphanumerical indentifier of customers, it appends the `customer_id`. It is used for tracking and referencing. |
| `first_name` | `varchar(50)` | - | Customer's first name.   |
| `last_name`  | `varchar(50)` | - | Customer's last or family name.   |
| `country`  | `varchar(50)`  | - | Customer's country of residence.   |
| `marital_status` | `varchar(50)` | 'Married', 'Single' | Customer's marital status. 'n/a' is possible. |
| `gender` | `varchar(50)` | 'Male', 'Female'  | Customer's gender. 'n/a' is possible. |
| `birthdate`  | `date` | YYYY-MM-DD | Customer's date of birth.   |
| `create_day` | `date` | YYYY-MM-DD | Date of creation of the customer record into the system. |
---

## *Products* 
[Back to Data objects](#data-objects)
### **`gold.dim_products`**
* **Purpose:** Contains products details.

| Column name | Type | Values | Description |
|---|---|---|---|
| `product_key` | `bigint` | $\mathbb{Z}^{+}$ | Surrogate key unique for each product record. |
| `product_id` | `integer` | - | Numerical identifier of products.  |
| `product_number` | `varchar(50)` | - | Alphanumerical identifier of products. Coded and used for categorization.  |
| `product_name` | `varchar(50)` | - | Descriptive name of the products, includes details such as size or color. |
| `category_id` | `varchar(50)` | - | Identifier of the product category. Used for classification, indicates both the `category` and `subcategory`. |
| `category` | `varchar(50)` | - | Name assigned to the product's category. |
| `subcategory` | `varchar(50)` | - | Name assigned to the product's subcategory.  |
| `maintainace` | `varchar(50)` | 'Yes', 'No' | Indicates wheter the product requries maintainace. 'n/a' is possible. |
| `cost` | `integer` | $\mathbb{N}$ | Cost or base price of the product.  |
| `product_line` | `varchar(50)` | - | The line or series to which the product belongs (e.g., Road, Mountain). |
| `start_date` | `date` | YYYY-MM-DD  | The date the product became available in the system.  |
---

[Back to Data objects](#data-objects)

## *Sales*
### **`gold.fact_sales`**
* **Purpose:** Contains details about transactios (sales) for analysis.

| Column name | Type | Values | Description |
|---|---|---|---|
| `order_number` | `varchar(50)` | - | Alphanumeric identifier of a transaction/sales order.  |
| `product_key` | `bigint` | $\mathbb{Z}^{+}$ | Surrogate key linked to the `gold.dim_product` view. |
| `customer_key` | `bigint` | $\mathbb{Z}^{+}$ | Surrogate key linked to the `gold.dim_customer` view. |
| `order_date` | `date` | YYYY-MM-DD | Date the sale order was placed. |
| `shipping_date` | `date` | YYYY-MM-DD | Date the sale order was shipped.  |
| `due_date` | `date` | YYYY-MM-DD | Expected date of arrival of the sale, or when the payment is due. |
| `sales_amount` | `integer` | $\mathbb{N}$ | The value of sale, calculated by `quantity`*`price` |
| `quantity` | `integer` | $\mathbb{Z}^{+}$ | Number of units of product in the sales orded. |
| `price` | `integer` | $\mathbb{Z}^{+}$ | Value/price per unit of product.  |
---







