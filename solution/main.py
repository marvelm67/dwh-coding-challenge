#!/usr/bin/env python3
"""
DWH Coding Challenge Solution
Mengerjakan 3 task sesuai README.md dengan event sourcing pattern
"""

import json
import os
import pandas as pd
from datetime import datetime
import glob

class EventLogProcessor:
    """
    Process event logs dan buat historical table views
    """

    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.accounts = {}
        self.cards = {}
        self.savings = {}

    def load_and_process_table(self, table_name):
        """
        Load JSON files untuk satu table dan process jadi final state
        """
        table_path = os.path.join(self.data_dir, table_name)
        
        # Load JSON files dan sort by timestamp
        json_files = glob.glob(os.path.join(table_path, "*.json"))
        json_files.sort(key=lambda x: int(os.path.basename(x).replace('.json', '')))
        
        # Load events
        events = []
        for file_path in json_files:
            with open(file_path, 'r') as f:
                events.append(json.load(f))
        
        # Process events jadi final state
        records = {}
        for event in events:
            record_id = event.get('id')
            if not record_id:
                continue
                
            if event.get('op') == 'c':  # Create
                records[record_id] = {
                    'created_at': event.get('ts'),
                    'last_updated': event.get('ts'),
                    **event.get('data', {})
                }
            elif event.get('op') == 'u':  # Update
                if record_id in records:
                    records[record_id]['last_updated'] = event.get('ts')
                    records[record_id].update(event.get('set', {}))
        
        return records

    def process_all_tables(self):
        """Process semua tables sekaligus"""
        print("🔄 Processing accounts table...")
        self.accounts = self.load_and_process_table('accounts')
        
        print("🔄 Processing cards table...")
        self.cards = self.load_and_process_table('cards')
        
        print("🔄 Processing savings_accounts table...")
        self.savings = self.load_and_process_table('savings_accounts')

    def create_dataframe(self, records):
        """Convert records jadi DataFrame untuk ditampilin"""
        if not records:
            return pd.DataFrame()
        
        data = []
        for record_id, record_data in records.items():
            row = {'id': record_id, **record_data}
            
            # Convert timestamp jadi format yang readable
            for ts_key in ['created_at', 'last_updated']:
                if ts_key in row and isinstance(row[ts_key], (int, float)):
                    row[ts_key] = datetime.fromtimestamp(row[ts_key] / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            data.append(row)
        
        return pd.DataFrame(data)

    # Task 1: Get DataFrames
    def get_accounts_df(self):
        return self.create_dataframe(self.accounts)

    def get_cards_df(self):
        return self.create_dataframe(self.cards)

    def get_savings_accounts_df(self):
        return self.create_dataframe(self.savings)

    # ============================================================================
    # TASK 2: DENORMALIZED JOINED TABLE
    # ============================================================================

    def create_denormalized_view(self) -> pd.DataFrame:
        """
        Creates a denormalized view by joining accounts, cards, and
        savings_accounts based on the inferred ID pattern (aX -> cX, aX -> saX).
        """
        accounts_df = self.get_accounts_df().rename(columns={'id': 'internal_id'})
        cards_df = self.get_cards_df().rename(columns={'id': 'internal_id'})
        savings_df = self.get_savings_accounts_df().rename(columns={'id': 'internal_id'})
        
        if accounts_df.empty:
            return pd.DataFrame()
            
        # 1. Prepare Cards and Savings DFs for merging
        # Create a linkage column 'account_id' by inferring from card_id/savings_account_id
        
        # Link Cards to Accounts (c1 -> a1)
        if not cards_df.empty:
            cards_df['account_id'] = cards_df['card_id'].str.replace('c', 'a')
            card_cols = ['account_id', 'card_id', 'card_number', 'credit_used', 'monthly_limit', 'status']
            cards_for_merge = cards_df[card_cols].rename(columns={'status': 'card_status'})
        else:
            cards_for_merge = pd.DataFrame()
            
        # Link Savings to Accounts (sa1 -> a1)
        if not savings_df.empty:
            savings_df['account_id'] = savings_df['savings_account_id'].str.replace('sa', 'a')
            savings_cols = ['account_id', 'savings_account_id', 'balance', 'interest_rate_percent', 'status']
            savings_for_merge = savings_df[savings_cols].rename(columns={'status': 'savings_status'})
        else:
            savings_for_merge = pd.DataFrame()

        # 2. Perform Merges starting from Accounts
        result = accounts_df.copy()
        
        if not cards_for_merge.empty:
            result = result.merge(cards_for_merge, on='account_id', how='left')
        
        if not savings_for_merge.empty:
            result = result.merge(savings_for_merge, on='account_id', how='left')
            
        # Clean up and reorder
        result.drop(columns=['internal_id'], errors='ignore', inplace=True)
        
        return result

    # ============================================================================
    # TASK 3: TRANSACTION ANALYSIS
    # ============================================================================

    def analyze_transactions(self):
        """Analisa transactions berdasarkan perubahan balance dan credit_used"""
        transactions = []
        
        # Analisa card transactions (credit_used changes)
        cards_events = self.load_and_process_table_events('cards')
        for event in cards_events:
            if event.get('op') == 'u' and 'credit_used' in event.get('set', {}):
                transactions.append({
                    'timestamp': event['ts'],
                    'datetime': datetime.fromtimestamp(event['ts'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'type': 'card_transaction',
                    'card_id': self.get_record_field(event['id'], 'cards', 'card_id'),
                    'value': event['set']['credit_used'],
                    'description': f"Credit used updated to {event['set']['credit_used']}"
                })
        
        # Analisa savings account transactions (balance changes)
        savings_events = self.load_and_process_table_events('savings_accounts')
        for event in savings_events:
            if event.get('op') == 'u' and 'balance' in event.get('set', {}):
                transactions.append({
                    'timestamp': event['ts'],
                    'datetime': datetime.fromtimestamp(event['ts'] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                    'type': 'savings_transaction',
                    'savings_account_id': self.get_record_field(event['id'], 'savings_accounts', 'savings_account_id'),
                    'value': event['set']['balance'],
                    'description': f"Balance updated to {event['set']['balance']}"
                })
        
        # Sort transactions by timestamp
        transactions.sort(key=lambda x: x['timestamp'])
        return transactions
    
    def load_and_process_table_events(self, table_name):
        """Helper function untuk load events aja tanpa processing"""
        table_path = os.path.join(self.data_dir, table_name)
        json_files = glob.glob(os.path.join(table_path, "*.json"))
        json_files.sort(key=lambda x: int(os.path.basename(x).replace('.json', '')))
        
        events = []
        for file_path in json_files:
            with open(file_path, 'r') as f:
                events.append(json.load(f))
        return events
    
    def get_record_field(self, record_id, table, field):
        """Get specific field value for a record"""
        if table == 'cards' and record_id in self.cards:
            return self.cards[record_id].get(field, 'Unknown')
        elif table == 'savings_accounts' and record_id in self.savings:
            return self.savings[record_id].get(field, 'Unknown')
        elif table == 'accounts' and record_id in self.accounts:
            return self.accounts[record_id].get(field, 'Unknown')
        return 'Unknown'


def main():
    """Main function untuk execute semua tasks sesuai README.md"""
    print("=" * 80)
    print("DWH CODING CHALLENGE SOLUTION")
    print("=" * 80)
    
    # Initialize processor dengan data directory
    # Coba current directory dulu (untuk Docker), terus parent directory (untuk local)
    current_dir_data = os.path.join(os.path.dirname(__file__), 'data')
    parent_dir_data = os.path.join(os.path.dirname(__file__), '..', 'data')
    
    if os.path.exists(current_dir_data):
        data_dir = current_dir_data
        print("📁 Using data directory:", current_dir_data)
    elif os.path.exists(parent_dir_data):
        data_dir = parent_dir_data
        print("📁 Using data directory:", parent_dir_data)
    else:
        data_dir = 'data'  # Fallback ke relative path
        print("📁 Using fallback data directory:", data_dir)
    
    processor = EventLogProcessor(data_dir)
    
    # Load dan process semua event logs dari JSON files
    processor.process_all_tables()
    
    # ============================================================================
    # TASK 1: VISUALIZE COMPLETE HISTORICAL TABLE VIEW OF EACH TABLE
    # Requirement: "Visualize the complete historical table view of each tables 
    # in tabular format in stdout (hint: print your table)"
    # ============================================================================
    
    print("\n" + "=" * 80)
    print("TASK 1: HISTORICAL TABLE VIEWS")
    print("=" * 80)
    
    # Set pandas display options untuk better alignment
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', 20)
    
    # Task 1.1: Show accounts table
    print("\n1.1 ACCOUNTS TABLE (Complete Historical View):")
    print("=" * 120)
    accounts_df = processor.get_accounts_df()
    if not accounts_df.empty:
        print(accounts_df.to_string(index=False, justify='left'))
    else:
        print("   No accounts found.")
    print("=" * 120)
    
    # Task 1.2: Show cards table
    print("\n1.2 CARDS TABLE (Complete Historical View):")
    print("=" * 120)
    cards_df = processor.get_cards_df()
    if not cards_df.empty:
        print(cards_df.to_string(index=False, justify='left'))
    else:
        print("   No cards found.")
    print("=" * 120)
    
    # Task 1.3: Show savings accounts table
    print("\n1.3 SAVINGS ACCOUNTS TABLE (Complete Historical View):")
    print("=" * 120)
    savings_df = processor.get_savings_accounts_df()
    if not savings_df.empty:
        print(savings_df.to_string(index=False, justify='left'))
    else:
        print("   No savings accounts found.")
    print("=" * 120)
    
    # ============================================================================
    # TASK 2: VISUALIZE DENORMALIZED JOINED TABLE
    # Requirement: "Visualize the complete historical table view of the 
    # denormalized joined table in stdout by joining these three tables"
    # Join key: account can have up to one card and one savings_account
    # ============================================================================
    
    print("\n" + "=" * 80)
    print("TASK 2: DENORMALIZED JOINED TABLE")
    print("=" * 80)
    
    # Task 2: Create dan show denormalized joined table
    print("\nDENORMALIZED VIEW (Accounts + Cards + Savings Accounts):")
    print("=" * 150)
    denormalized_df = processor.create_denormalized_view()
    if not denormalized_df.empty:
        # Set wider display untuk denormalized table
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', 15)
        print(denormalized_df.to_string(index=False, justify='left'))
    else:
        print("   No joined records found.")
    print("=" * 150)
    
    # ============================================================================
    # TASK 3: TRANSACTION ANALYSIS
    # Requirement: "discuss how many transactions has been made, when did each 
    # of them occur, and how much the value of each transaction?"
    # Transaction definition: activity which change the balance of savings 
    # account or credit used of the card
    # ============================================================================
    
    print("\n" + "=" * 80)
    print("TASK 3: TRANSACTION ANALYSIS")
    print("=" * 80)
    
    # Task 3: Analyze semua transactions
    transactions = processor.analyze_transactions()
    
    print(f"\n📊 TRANSACTION SUMMARY:")
    print(f"Total number of transactions detected: {len(transactions)}")
    print(f"Transaction definition: Changes to balance (savings) or credit_used (cards)")
    
    print(f"\n📋 DETAILED TRANSACTION LIST:")
    print("-" * 120)
    
    if transactions:
        for i, transaction in enumerate(transactions, 1):
            print(f"{i}. {transaction['datetime']} - {transaction['type'].upper()}")
            print(f"   💰 Value: {transaction['value']}")
            print(f"   📝 Description: {transaction['description']}")
            if 'card_id' in transaction:
                print(f"   💳 Card ID: {transaction['card_id']}")
            if 'savings_account_id' in transaction:
                print(f"   🏦 Savings Account ID: {transaction['savings_account_id']}")
            print()
    else:
        print("   No transactions found in the event logs.")
    
    # Summary of findings
    print("\n" + "=" * 80)
    print("📈 ANALYSIS SUMMARY:")
    print("=" * 80)
    print(f"✅ Task 1: Processed {len(accounts_df)} accounts, {len(cards_df)} cards, {len(savings_df)} savings accounts")
    print(f"✅ Task 2: Created denormalized view with {len(denormalized_df)} joined records")
    print(f"✅ Task 3: Identified {len(transactions)} transactions with timestamps and values")
    print("=" * 80)
    print("🎉 ANALYSIS COMPLETE - ALL TASKS EXECUTED SUCCESSFULLY")
    print("=" * 80)


if __name__ == "__main__":
    main()