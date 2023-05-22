-- https://www.w3schools.com/sql/trysqlserver.asp?filename=trysql_select_all

-- Problem : Who is the second best employee (by Sales Amount) each month ?
WITH
  base AS (
  SELECT
    DISTINCT orders.EmployeeId,
    (firstname + ' ' + lastname) AS Employee_Name,
    LEFT(orderDate,7) AS period,
    COUNT(DISTINCT orderdetails.orderId) order_number,
    SUM(quantity * price) AS Sales_Amount
  FROM
    Orders
  LEFT JOIN
    employees
  ON
    employees.employeeId = orders.employeeId
  LEFT JOIN
    orderDetails
  ON
    orderdetails.orderId = orders.orderid
  LEFT JOIN
    products
  ON
    products.productid = orderdetails.productId
  GROUP BY
    orders.EmployeeId,
    (firstname + ' ' + lastname),
    LEFT(orderDate,7) ),


  ##For Selecting top 2 each month  
  ranking AS (
  SELECT
    EmployeeId,
    Employee_Name,
    period,
    Sales_Amount,
    Order_Number,
    DENSE_RANK() OVER(PARTITION BY period ORDER BY sales_amount DESC) AS dr
  FROM
    base)
SELECT
  employeeId,
  Employee_Name,
  Period,
  Sales_Amount,
  Order_Number
FROM
  ranking
WHERE
  dr = 2
ORDER BY
  Period asc

##We use dense_rank instead of order_by to prevent the possibilities of having same sales_amount