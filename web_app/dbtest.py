# db_test.py - Test database connection
import os
from sqlalchemy import create_engine, text

# Option 1: Use postgres user (simplest)
#connection_string = 'postgresql://postgres@localhost:5432/dnc_processor'

# Option 2: Use your user with password
username = "anakin0"
password = "dejameacuerdo"
connection_string = f'postgresql://{username}:{password}@localhost:5432/dnc_processor'

print(f"Testing connection: {connection_string}")

try:
    engine = create_engine(connection_string)
    
    with engine.connect() as conn:
        # Test basic connection
        result = conn.execute(text("SELECT version();"))
        version = result.fetchone()[0]
        print(f"✅ PostgreSQL connected: {version}")
        
        # Check if records table exists
        result = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'records'
            );
        """))
        table_exists = result.fetchone()[0]
        print(f"✅ Records table exists: {table_exists}")
        
        if table_exists:
            # Count records
            result = conn.execute(text("SELECT COUNT(*) FROM records;"))
            count = result.fetchone()[0]
            print(f"✅ Total records: {count}")
    
    print("\n🚀 Database is ready for testing!")
    
except Exception as e:
    print(f"❌ Database error: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure PostgreSQL is running: sudo systemctl start postgresql")
    print("2. Create database as postgres: sudo -u postgres createdb dnc_processor")
    print("3. Test connection: psql -U postgres -d dnc_processor")

