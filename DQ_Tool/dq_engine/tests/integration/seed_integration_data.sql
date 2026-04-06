SET NOCOUNT ON;

IF SCHEMA_ID('dbo') IS NULL
BEGIN
    EXEC('CREATE SCHEMA dbo');
END
GO

IF OBJECT_ID('dbo.Customers', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Customers
    (
        CustomerID INT NOT NULL,
        Email NVARCHAR(255) NULL,
        Phone NVARCHAR(50) NULL,
        Status NVARCHAR(20) NULL
    );
END
GO

IF OBJECT_ID('dbo.Orders', 'U') IS NULL
BEGIN
    CREATE TABLE dbo.Orders
    (
        OrderID INT NOT NULL,
        CustomerID INT NULL,
        Amount DECIMAL(18,2) NULL,
        OrderDate DATE NULL,
        ShipDate DATE NULL,
        Status NVARCHAR(20) NULL
    );
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.Customers)
BEGIN
    INSERT INTO dbo.Customers (CustomerID, Email, Phone, Status)
    VALUES
        (1, 'alice@example.com', '2100000001', 'ACTIVE'),
        (2, NULL, '2100000002', 'ACTIVE'),
        (3, 'bob.example.com', '2', 'INACTIVE'),
        (3, 'bob.example.com', '2', 'INACTIVE');
END
GO

IF NOT EXISTS (SELECT 1 FROM dbo.Orders)
BEGIN
    INSERT INTO dbo.Orders (OrderID, CustomerID, Amount, OrderDate, ShipDate, Status)
    VALUES
        (1001, 1, 120.00, '2024-01-10', '2024-01-11', 'SHIPPED'),
        (1002, 2, -10.00, '2024-01-11', NULL, 'SHIPPED'),
        (1003, 9999, 20.00, '2024-01-12', '2024-01-10', 'NEW');
END
GO
