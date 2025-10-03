# DWH Coding Challenge Solution

## Overview

This solution processes event logs from three tables (`accounts`, `cards`, and `savings_accounts`) to reconstruct historical table views and analyze transactions. The solution is implemented in Python using pandas for data processing.

## Solution Architecture

### Core Components

1. **EventLogProcessor Class**: Main class that handles:

   - Loading and parsing JSON event logs
   - Processing create ('c') and update ('u') operations
   - Reconstructing current state of each table
   - Creating denormalized joined views
   - Analyzing transactions

2. **Key Features**:
   - Chronological processing of events based on timestamps
   - Proper handling of create and update operations
   - Automatic relationship inference between tables
   - Transaction detection based on balance and credit_used changes

### Data Processing Logic

#### 1. Event Log Processing

- Events are sorted by timestamp (filename)
- Create operations (`op: 'c'`) initialize records with `data` field
- Update operations (`op: 'u'`) modify existing records with `set` field
- Each record tracks creation and last update timestamps

#### 2. Table Relationships

Based on the data pattern analysis, the relationships are inferred as:

- Account ID pattern: `a1`, `a2`, etc.
- Card ID pattern: `c1`, `c2`, etc. (corresponding to accounts)
- Savings Account ID pattern: `sa1`, `sa2`, etc. (corresponding to accounts)

#### 3. Transaction Analysis

Transactions are identified as:

- **Card Transactions**: Changes to `credit_used` field in cards table
- **Savings Transactions**: Changes to `balance` field in savings_accounts table

## Tasks Implementation

### Task 1: Historical Table Views

- Processes all event logs for each table
- Reconstructs current state of all records
- Displays complete historical table view in tabular format
- Converts timestamps to human-readable dates

### Task 2: Denormalized Joined Table

- Joins accounts, cards, and savings_accounts tables
- Uses account_id as the primary key for joining
- Infers relationships based on ID patterns (a1 → c1 → sa1)
- Creates comprehensive view with all related information

### Task 3: Transaction Analysis

- Identifies all transactions by monitoring balance and credit_used changes
- Provides transaction count, timestamps, and values
- Categorizes transactions by type (card vs savings)
- Shows chronological order of all transactions

## File Structure

```
solution/
├── main.py              # Main solution script with detailed comments for each task
├── requirements.txt     # Python dependencies (pandas)
├── Dockerfile          # Docker container configuration
├── run.sh              # Build and run automation script (Docker)
├── run_local.sh        # Local execution script (without Docker)
└── README.md           # This documentation
```

## How to Run

### Prerequisites

- Docker installed on your system
- Basic knowledge of command line operations

### Option 1: Using Docker (Recommended)

1. **Navigate to the solution directory**:

   ```bash
   cd solution
   ```

2. **Make the run script executable**:

   ```bash
   chmod +x run.sh
   ```

3. **Run the automated build and execution script**:
   ```bash
   ./run.sh
   ```

This will:

- Build the Docker image with all dependencies
- Copy the data files into the container
- Execute the main script
- Display all results in the terminal

### Option 2: Manual Docker Commands

1. **Navigate to the parent directory**:

   ```bash
   cd ..  # Go to dwh-coding-challenge directory
   ```

2. **Build the Docker image**:

   ```bash
   docker build -f solution/Dockerfile -t dwh-challenge .
   ```

3. **Run the container**:
   ```bash
   docker run --rm dwh-challenge
   ```

### Option 3: Local Python Execution (Automated)

1. **Navigate to solution directory**:

   ```bash
   cd solution
   ```

2. **Make the local run script executable**:

   ```bash
   chmod +x run_local.sh
   ```

3. **Run the automated local script**:
   ```bash
   ./run_local.sh
   ```

### Option 4: Manual Local Python Execution

1. **Install dependencies**:

   ```bash
   pip install -r requirements.txt
   ```

2. **Run the script**:
   ```bash
   python main.py
   ```

## Expected Output

The solution will display:

1. **Task 1 Results**: Complete historical table views for:

   - Accounts table with all account information
   - Cards table with card details and status changes
   - Savings accounts table with balance and interest information

2. **Task 2 Results**: A denormalized joined table showing:

   - Account holder information
   - Associated card details (if any)
   - Associated savings account details (if any)

3. **Task 3 Results**: Transaction analysis including:
   - Total number of transactions
   - Detailed list of each transaction with timestamp and value
   - Transaction type classification

## Technical Decisions

### Why Python and Pandas?

- **Pandas**: Excellent for data manipulation and tabular display
- **Python**: Simple syntax and great JSON handling capabilities
- **Minimal Dependencies**: Only pandas is required, keeping the solution lightweight

### Data Processing Approach

- **Event Sourcing**: Reconstruct current state from event log history
- **Chronological Processing**: Ensure events are processed in the correct order
- **State Tracking**: Maintain both creation and last update timestamps

### Docker Implementation

- **Multi-stage Approach**: Efficient caching of dependencies
- **Minimal Base Image**: Python 3.11-slim for smaller container size
- **Automated Deployment**: Single script execution for complete workflow

## Assumptions Made

1. **ID Relationships**: Account `a1` corresponds to card `c1` and savings account `sa1`
2. **Transaction Definition**: Only balance changes in savings accounts and credit_used changes in cards constitute transactions
3. **Data Integrity**: Event logs are complete and properly formatted
4. **Timestamp Format**: All timestamps are in milliseconds since epoch

## Future Enhancements

1. **Error Handling**: Add more robust error handling for malformed data
2. **Configuration**: Make table relationships configurable
3. **Performance**: Add indexing for large datasets
4. **Validation**: Add data validation and consistency checks
5. **Logging**: Add detailed logging for debugging purposes

## Sample Output

Below is a screenshot of the application running in Docker container, showing the complete execution of all three tasks:

![DWH Challenge Output](solution/output-screenshot.png)

The output demonstrates:

- **Task 1**: Historical table views for accounts, cards, and savings accounts tables
- **Task 2**: Denormalized joined table combining all three tables
- **Task 3**: Comprehensive transaction analysis with 8 detected transactions

_Note: The screenshot shows the actual Docker container execution with all formatting and tabular displays as specified in the requirements._
