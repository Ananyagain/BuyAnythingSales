CREATE TABLE SalesOrders (
    OrderID INT,
    OrderDate DATE,
    CustomerID INT,
    CustomerName VARCHAR(255),
    Country VARCHAR(100),
    ProductID INT,
    ProductCategory VARCHAR(100),
    ProductName VARCHAR(255),
    Quantity INT,
    UnitPrice DECIMAL(10,2),
    TotalPrice DECIMAL(10,2),
    SalesRegion VARCHAR(100)
);

CREATE TABLE DateHolder(
  LastDate Date
  );

INSERT INTO DateHolder values('2025-06-22') --updating the first value to support incremental load
