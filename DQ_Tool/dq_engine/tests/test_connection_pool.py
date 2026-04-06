
from dq_engine.db_connection import SqlConnectionPool

def test_pool():
    print("Creating pool...")

    pool = SqlConnectionPool(
        server=r"SANDBOX-SQL\MSSQL2022",  
        database="CSBDATA_DEV",
        auth_type="Windows Authentication"
    )

    print("Pool created successfully.")

    # Acquire a connection
    with pool.acquire() as conn:
        print("Connection acquired:", conn)
        cursor = conn.cursor()
        cursor.execute("SELECT DB_NAME()")
        db = cursor.fetchone()[0]
        print("Connected to database:", db)

    print("Connection returned to pool.")

    # Acquire again to test reuse
    with pool.acquire() as conn:
        print("Re-acquired connection:", conn)
        cursor = conn.cursor()
        cursor.execute("SELECT SYSTEM_USER")
        user = cursor.fetchone()[0]
        print("SYSTEM_USER:", user)

    print("Connection pool test completed successfully.")


if __name__ == "__main__":
    test_pool()