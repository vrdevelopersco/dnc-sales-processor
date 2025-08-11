# scripts/manage_database.py
import argparse
from sqlalchemy import create_engine, text

def get_engine():
    return create_engine('postgresql://anakin0:dejameacuerdo@localhost:5432/dnc_processor')

def clear_all_records():
    """Clear all records from the database"""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Get count before deletion
        result = conn.execute(text("SELECT COUNT(*) FROM records"))
        before_count = result.fetchone()[0]
        
        if before_count == 0:
            print("No records to delete.")
            return
        
        # Confirm deletion
        response = input(f"This will delete {before_count} records. Are you sure? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled.")
            return
        
        # Clear all records
        conn.execute(text("DELETE FROM records"))
        conn.commit()
        
        print(f"✅ Deleted {before_count} records from database.")

def clear_by_state(state_code):
    """Clear records for a specific state"""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Get count before deletion
        result = conn.execute(text("SELECT COUNT(*) FROM records WHERE state = :state"), {'state': state_code})
        before_count = result.fetchone()[0]
        
        if before_count == 0:
            print(f"No records found for state {state_code}.")
            return
        
        # Confirm deletion
        response = input(f"This will delete {before_count} records for state {state_code}. Are you sure? (yes/no): ")
        if response.lower() != 'yes':
            print("Operation cancelled.")
            return
        
        # Clear records for state
        conn.execute(text("DELETE FROM records WHERE state = :state"), {'state': state_code})
        conn.commit()
        
        print(f"✅ Deleted {before_count} records for state {state_code}.")

def show_stats():
    """Show database statistics"""
    engine = get_engine()
    
    with engine.connect() as conn:
        # Total records
        result = conn.execute(text("SELECT COUNT(*) FROM records"))
        total_records = result.fetchone()[0]
        print(f"Total records: {total_records:,}")
        
        # Records by state
        result = conn.execute(text("SELECT state, COUNT(*) FROM records GROUP BY state ORDER BY COUNT(*) DESC"))
        state_counts = result.fetchall()
        
        print("\nRecords by state:")
        for state, count in state_counts:
            print(f"  {state}: {count:,}")
        
        # DNC vs Non-DNC
        result = conn.execute(text("SELECT is_dnc, COUNT(*) FROM records GROUP BY is_dnc"))
        dnc_counts = result.fetchall()
        
        print("\nDNC Status:")
        for is_dnc, count in dnc_counts:
            status = "On DNC List" if is_dnc else "Not on DNC List"
            print(f"  {status}: {count:,}")

def drop_table():
    """Drop the entire records table"""
    engine = get_engine()
    
    response = input("This will DROP the entire records table. Are you ABSOLUTELY sure? (yes/no): ")
    if response.lower() != 'yes':
        print("Operation cancelled.")
        return
    
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS records"))
        conn.commit()
        print("✅ Records table dropped.")

def main():
    parser = argparse.ArgumentParser(description='Manage DNC database')
    parser.add_argument('action', choices=['clear-all', 'clear-state', 'stats', 'drop-table'], 
                        help='Action to perform')
    parser.add_argument('--state', help='State code for clear-state action')
    
    args = parser.parse_args()
    
    if args.action == 'clear-all':
        clear_all_records()
    elif args.action == 'clear-state':
        if not args.state:
            print("Error: --state is required for clear-state action")
            return
        clear_by_state(args.state.upper())
    elif args.action == 'stats':
        show_stats()
    elif args.action == 'drop-table':
        drop_table()

if __name__ == '__main__':
    main()