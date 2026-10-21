IF DB_ID(N'billing_source') IS NULL
BEGIN
    CREATE DATABASE billing_source;
END;
GO

DECLARE @login_name SYSNAME = N'$(BillingUser)';
DECLARE @login_password NVARCHAR(128) = N'$(BillingPassword)';
DECLARE @login_sql NVARCHAR(MAX);

IF SUSER_ID(@login_name) IS NULL
BEGIN
    SET @login_sql = N'CREATE LOGIN ' + QUOTENAME(@login_name)
        + N' WITH PASSWORD = ' + QUOTENAME(@login_password, '''')
        + N', CHECK_POLICY = ON, CHECK_EXPIRATION = OFF;';
    EXEC sys.sp_executesql @login_sql;
END
ELSE
BEGIN
    SET @login_sql = N'ALTER LOGIN ' + QUOTENAME(@login_name)
        + N' WITH PASSWORD = ' + QUOTENAME(@login_password, '''');';
    EXEC sys.sp_executesql @login_sql;
END;
GO

USE billing_source;
GO

DECLARE @database_user SYSNAME = N'$(BillingUser)';
DECLARE @user_sql NVARCHAR(MAX);

IF USER_ID(@database_user) IS NULL
BEGIN
    SET @user_sql = N'CREATE USER ' + QUOTENAME(@database_user)
        + N' FOR LOGIN ' + QUOTENAME(@database_user) + N';';
    EXEC sys.sp_executesql @user_sql;
END;

IF IS_ROLEMEMBER(N'db_datareader', @database_user) <> 1
BEGIN
    SET @user_sql = N'ALTER ROLE db_datareader ADD MEMBER ' + QUOTENAME(@database_user) + N';';
    EXEC sys.sp_executesql @user_sql;
END;
GO

IF OBJECT_ID(N'dbo.transactions', N'U') IS NULL
BEGIN
    CREATE TABLE dbo.transactions (
        source_row_id BIGINT IDENTITY(1, 1) NOT NULL,
        transaction_id NVARCHAR(50) NULL,
        customer_id NVARCHAR(50) NULL,
        transaction_timestamp DATETIMEOFFSET NULL,
        amount DECIMAL(18, 2) NULL,
        currency_code NVARCHAR(10) NULL,
        transaction_status NVARCHAR(20) NULL,
        payment_method NVARCHAR(30) NULL,
        updated_at DATETIMEOFFSET NULL,
        CONSTRAINT pk_billing_transactions PRIMARY KEY (source_row_id)
    );
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'ix_billing_transactions_updated_at'
      AND object_id = OBJECT_ID(N'dbo.transactions')
)
BEGIN
    CREATE INDEX ix_billing_transactions_updated_at
        ON dbo.transactions (updated_at, source_row_id);
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.indexes
    WHERE name = N'ix_billing_transactions_transaction_id'
      AND object_id = OBJECT_ID(N'dbo.transactions')
)
BEGIN
    CREATE INDEX ix_billing_transactions_transaction_id
        ON dbo.transactions (transaction_id, updated_at DESC);
END;
GO
