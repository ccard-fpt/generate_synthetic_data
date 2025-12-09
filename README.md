# Synthetic Data Generator for MySQL

A powerful Python tool for generating synthetic test data that respects complex database constraints including foreign keys, composite UNIQUE constraints, and polymorphic relationships.

## Overview

### What It Does

This tool generates realistic synthetic SQL INSERT statements for MySQL databases by:
- Introspecting database schemas to understand table structures
- Respecting foreign key relationships and dependency order
- Satisfying complex UNIQUE constraints (single-column, composite, and overlapping)
- Supporting polymorphic relationships via conditional foreign keys
- Generating data with realistic distributions and patterns

### Key Features

- **Intelligent Constraint Handling**: Automatically detects and satisfies composite UNIQUE constraints, including overlapping constraints that share columns
- **Foreign Key Support**: Physical FKs (from schema) and logical FKs (from config), with conditional FKs for polymorphic relationships
- **Static FK Sampling**: Sample values from existing production tables instead of generating parent data
- **Cartesian Product Generation**: Guarantees zero duplicates for composite UNIQUE constraints with all FK columns
- **Stratified Sampling**: Balanced distribution when satisfying multiple overlapping UNIQUE constraints
- **Multi-threaded Generation**: Parallel processing for large datasets
- **Reproducible Results**: Seed-based random generation for consistent test data
- **Batch INSERT Statements**: Configurable batch sizes for optimized database loading

### When to Use It

- **Testing**: Generate large volumes of test data that respects referential integrity
- **Development**: Populate development databases with realistic data patterns
- **Performance Testing**: Create datasets of specific sizes to test query performance
- **Data Migration**: Generate synthetic data matching production schema constraints
- **Privacy Compliance**: Replace sensitive production data with synthetic equivalents

## Installation

### Requirements

- **Python**: 3.7 or higher
- **PyMySQL**: Python MySQL client library
- **MySQL Server**: Any version (read-only access needed for schema introspection)

### Installation Steps

1. **Clone the repository**:
```bash
git clone https://github.com/ChrisCGH/generate_synthetic_data.git
cd generate_synthetic_data
```

2. **Install dependencies**:
```bash
pip install PyMySQL
```

3. **Verify installation**:
```bash
python generate_synthetic_data.py --help
```

### Quick Start Example

```bash
# Create a simple configuration file
cat > config.json << 'EOF'
[
  {
    "schema": "testdb",
    "table": "users",
    "rows": 1000
  }
]
EOF

# Generate SQL
python generate_synthetic_data.py \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql output.sql

# Load into database
mysql -u root testdb < output.sql
```

## Basic Usage

### Command-Line Syntax

```bash
python generate_synthetic_data.py \
  --config <config.json> \
  --src-host <hostname> \
  --src-user <username> \
  --out-sql <output.sql> \
  [OPTIONS]
```

### Required Parameters

- `--config`: Path to JSON configuration file
- `--src-host`: MySQL server hostname or IP address
- `--src-user`: MySQL username
- `--out-sql`: Output file path for INSERT statements

### Common Options

```bash
--src-password PASSWORD    # MySQL password (or use --ask-pass)
--ask-pass                 # Prompt for password securely
--rows 10000              # Default rows per table
--threads 8               # Number of parallel threads
--batch-size 1000         # Rows per INSERT statement
--seed 42                 # Random seed for reproducibility
--debug                   # Enable verbose debug output
```

### Simple Example

Generate 5000 users with related orders:

```bash
python generate_synthetic_data.py \
  --config users_orders.json \
  --src-host localhost \
  --src-user root \
  --ask-pass \
  --out-sql test_data.sql \
  --rows 5000 \
  --threads 4
```

## Configuration File Format

The configuration is a JSON array of table definitions:

```json
[
  {
    "schema": "database_name",
    "table": "table_name",
    "rows": 1000,
    "populate_columns": [...],
    "logical_fks": [...],
    "static_fks": [...],
    "fk_ratios": {...}
  }
]
```

### Table Configuration Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `schema` | string | Yes | Database schema name |
| `table` | string | Yes | Table name |
| `rows` | integer | No | Number of rows to generate (overrides `--rows`) |
| `populate_columns` | array | No | Column value specifications |
| `logical_fks` | array | No | Foreign key definitions not in schema |
| `static_fks` | array | No | Sample FK values from existing tables |
| `fk_ratios` | object | No | FK reuse ratios (e.g., `{"user_id": 2}`) |

### Foreign Key Definitions

#### Physical FKs

Automatically detected from database schema. No configuration needed.

#### Logical FKs

Define foreign keys not enforced in the database:

```json
{
  "column": "user_id",
  "referenced_schema": "mydb",
  "referenced_table": "users",
  "referenced_column": "id"
}
```

#### Composite Foreign Keys

Multiple columns referencing a composite key:

```json
{
  "table_schema": "mydb",
  "table_name": "order_items",
  "columns": ["order_id", "order_version"],
  "referenced_table_schema": "mydb",
  "referenced_table_name": "orders",
  "referenced_columns": ["id", "version"]
}
```

#### Static FKs

Sample values from existing production tables:

```json
{
  "column": "country_code",
  "static_schema": "production",
  "static_table": "countries",
  "static_column": "code"
}
```

## Advanced Features

### 5.1 Populate Columns

Control how specific columns are populated.

#### Simple Format (Column Names Only)

```json
{
  "populate_columns": ["status", "priority"]
}
```

Uses default value generation based on column data type.

#### Extended Format with Explicit Values

```json
{
  "populate_columns": [
    {
      "column": "status",
      "values": ["active", "inactive", "pending", "deleted"]
    },
    {
      "column": "priority",
      "values": [1, 2, 3, 4, 5]
    }
  ]
}
```

Randomly selects from provided values.

#### Min/Max Ranges for Numeric Types

```json
{
  "populate_columns": [
    {
      "column": "age",
      "min": 18,
      "max": 65
    },
    {
      "column": "salary",
      "min": 30000,
      "max": 150000
    }
  ]
}
```

Generates values within the specified range.

#### UNIQUE Column Handling

For columns with UNIQUE constraints, the system automatically:
- Generates unique values within the specified range or value list
- Appends suffixes to strings if needed
- Uses sequential integers when range allows

### 5.2 Foreign Key Management

#### Physical vs Logical FKs

**Physical FKs**: Defined in database schema via `FOREIGN KEY` constraints
- Automatically detected via `information_schema`
- No configuration needed

**Logical FKs**: Relationships not enforced in schema
- Must be explicitly defined in configuration
- Useful for legacy databases or optional relationships

#### Conditional FKs (Polymorphic Relationships)

Define FKs that apply only when a condition is met:

```json
{
  "column": "commentable_id",
  "referenced_schema": "mydb",
  "referenced_table": "posts",
  "referenced_column": "id",
  "condition": "commentable_type = 'Post'"
}
```

**Use Case**: Polymorphic associations common in Rails/Laravel applications.

#### FK Population Rates

Control how often FK values are reused:

```json
{
  "fk_ratios": {
    "user_id": 3
  }
}
```

Each unique `user_id` value appears in approximately 3 rows.

#### Static FK Sampling from Existing Tables

Instead of generating parent data, sample from existing production tables:

```json
{
  "static_fks": [
    {
      "column": "country_code",
      "static_schema": "production",
      "static_table": "countries",
      "static_column": "code"
    }
  ]
}
```

Useful when parent tables are:
- Large and expensive to generate
- Already populated in production
- Reference data (countries, currencies, etc.)

#### Composite FKs

Multi-column foreign keys:

```json
{
  "table_schema": "mydb",
  "table_name": "order_items",
  "columns": ["order_id", "order_line"],
  "referenced_table_schema": "mydb",
  "referenced_table_name": "order_lines",
  "referenced_columns": ["order_id", "line_number"]
}
```

### 5.3 UNIQUE Constraint Handling

The tool intelligently handles various UNIQUE constraint scenarios.

#### Single-Column UNIQUE Constraints

Automatically generates unique values:
- **Strings**: Appends sequential suffixes
- **Integers**: Sequential values within range
- **With explicit values**: Cycles through provided list

#### Composite UNIQUE Constraints (All FKs)

When a UNIQUE constraint contains only foreign key columns:

```sql
UNIQUE KEY (user_id, product_id)
```

**Strategy**: Cartesian product of parent values
- Guarantees zero duplicates
- Efficient for large combination spaces

**Example**:
- 1000 users × 50 products = 50,000 unique combinations
- Request 10,000 rows → Sample 10,000 from 50,000

#### Mixed UNIQUE Constraints (FK + Non-FK Columns)

When UNIQUE constraints mix FK and non-FK columns:

```sql
UNIQUE KEY (user_id, status)
```

**Configuration**:
```json
{
  "populate_columns": [
    {
      "column": "status",
      "values": ["active", "inactive", "pending"]
    }
  ]
}
```

**Strategy**: Cartesian product of FK values × explicit values
- 1000 users × 3 statuses = 3,000 combinations

#### Overlapping UNIQUE Constraints with Cartesian Product

When multiple UNIQUE constraints share columns:

```sql
UNIQUE KEY ACS (A_ID, C_ID)
UNIQUE KEY APR (A_ID, PR)
```

Both share column `A_ID`.

**Strategy**: Multi-constraint Cartesian product
1. Detect shared columns: `{A_ID}`
2. Collect non-shared column values: `PR: [0, 1]`, `C_ID: [1..10]`
3. Generate full Cartesian product: `A_ID × (PR × C_ID)`
4. Result: Satisfies both constraints simultaneously

**Example**:
- 3000 A_ID values
- 2 PR values (0, 1)
- 10 C_ID values
- Cartesian product: 3000 × (2 × 10) = **60,000 combinations**
- Request 6000 rows → Zero duplicates in both constraints

#### Stratified Sampling for Balanced Distribution

When sampling from Cartesian products, uses stratified sampling to ensure:
- Even distribution across shared column values
- Maximum diversity in constraint combinations
- Minimizes clustering of specific value patterns

**Algorithm**: Smart diversity selection
1. Group by primary shared column
2. For each group, select candidates that minimize conflicts
3. Ensures balanced representation across all constraint dimensions

### 5.4 Primary Key Configuration

#### Auto-Increment PKs

Automatically handled:
```sql
CREATE TABLE users (
  id INT PRIMARY KEY AUTO_INCREMENT,
  ...
);
```

Generated SQL omits the PK column, letting MySQL assign values.

#### Explicit PK Values

For non-auto-increment PKs, generates sequential values starting from 1.

#### Composite PKs with FK Overlap

When composite PK shares columns with foreign keys:

```sql
CREATE TABLE order_items (
  order_id INT,
  item_id INT,
  PRIMARY KEY (order_id, item_id),
  FOREIGN KEY (order_id) REFERENCES orders(id)
);
```

**Strategy**: Hybrid Cartesian product
- Ensures PK uniqueness while respecting FK relationships
- Coordinates PK generation with FK value assignment

#### Hybrid Cartesian Product for Complex Scenarios

Handles cases where:
- Composite PK includes FK columns
- Composite UNIQUE constraints overlap with PK
- Multiple constraints interact

The system automatically selects the tightest constraint to optimize.

### 5.5 Performance Features

#### Multi-Threaded Generation

```bash
--threads 8
```

Parallelizes row generation across multiple threads:
- Each thread generates rows for assigned tables
- Thread-safe value tracking for UNIQUE constraints
- Significant speedup for large datasets

#### Batch INSERT Statements

```bash
--batch-size 1000
```

Groups rows into multi-value INSERT statements:
- Reduces SQL file size
- Faster database loading
- Adjustable based on MySQL's `max_allowed_packet`

Default: 100 rows per INSERT

#### Seed for Reproducibility

```bash
--seed 42
```

Enables reproducible data generation:
- Same seed → same data
- Useful for debugging and consistent test environments
- Different seeds → different data distributions

#### Debug Mode

```bash
--debug
```

Enables verbose logging:
- Constraint detection details
- Cartesian product generation steps
- FK resolution logic
- UNIQUE value allocation
- Combination counts and distribution

## Configuration Examples

### Example 1: Basic Table with FKs

Simple user/order relationship:

```json
[
  {
    "schema": "mydb",
    "table": "users",
    "rows": 1000
  },
  {
    "schema": "mydb",
    "table": "orders",
    "rows": 5000,
    "logical_fks": [
      {
        "column": "user_id",
        "referenced_schema": "mydb",
        "referenced_table": "users",
        "referenced_column": "id"
      }
    ]
  }
]
```

**Result**: 1000 users, 5000 orders (average 5 orders per user)

### Example 2: UNIQUE Constraints with Explicit Values

User preferences with controlled values:

```json
[
  {
    "schema": "mydb",
    "table": "users",
    "rows": 5000
  },
  {
    "schema": "mydb",
    "table": "user_preferences",
    "rows": 10000,
    "populate_columns": [
      {
        "column": "theme",
        "values": ["light", "dark", "auto"]
      },
      {
        "column": "language",
        "values": ["en", "es", "fr", "de"]
      },
      {
        "column": "notifications_enabled",
        "values": [0, 1]
      }
    ],
    "logical_fks": [
      {
        "column": "user_id",
        "referenced_schema": "mydb",
        "referenced_table": "users",
        "referenced_column": "id"
      }
    ]
  }
]
```

**Schema Assumption**:
```sql
CREATE TABLE user_preferences (
  id INT PRIMARY KEY AUTO_INCREMENT,
  user_id INT NOT NULL,
  theme VARCHAR(10),
  language VARCHAR(5),
  notifications_enabled TINYINT,
  FOREIGN KEY (user_id) REFERENCES users(id)
);
```

**Result**: 10,000 preference records with controlled enum-like values

### Example 3: Overlapping UNIQUE Constraints

Complex constraint satisfaction:

```json
[
  {
    "schema": "db",
    "table": "A",
    "rows": 3000
  },
  {
    "schema": "db",
    "table": "C",
    "rows": 10
  },
  {
    "schema": "db",
    "table": "AC",
    "rows": 6000,
    "populate_columns": [
      {
        "column": "PR",
        "values": [0, 1]
      }
    ],
    "logical_fks": [
      {
        "column": "A_ID",
        "referenced_schema": "db",
        "referenced_table": "A",
        "referenced_column": "ID"
      },
      {
        "column": "C_ID",
        "referenced_schema": "db",
        "referenced_table": "C",
        "referenced_column": "ID"
      }
    ]
  }
]
```

**Schema**:
```sql
CREATE TABLE AC (
  A_ID INT NOT NULL,
  C_ID INT NOT NULL,
  PR TINYINT NOT NULL,
  UNIQUE KEY ACS (A_ID, C_ID),
  UNIQUE KEY APR (A_ID, PR),
  FOREIGN KEY (A_ID) REFERENCES A(ID),
  FOREIGN KEY (C_ID) REFERENCES C(ID)
);
```

**How It Works**:
- Both constraints share column `A_ID`
- System detects overlapping constraints
- Generates Cartesian product: 3000 A_IDs × (2 PR values × 10 C_IDs) = 60,000 combinations
- Samples 6000 rows using stratified sampling
- **Result**: Zero duplicates in both `UNIQUE(A_ID, C_ID)` and `UNIQUE(A_ID, PR)`

**Debug Output**:
```
[DEBUG] db.AC: Found overlapping UNIQUE constraints: ['ACS', 'APR']
[DEBUG] db.AC: Shared columns: ['A_ID']
[DEBUG] db.AC: Generated 60000 total valid combinations
[DEBUG] db.AC: Pre-allocated 6000 rows satisfying 2 constraints
```

### Example 4: Conditional FKs (Polymorphic Relationships)

Rails/Laravel-style polymorphic associations:

```json
[
  {
    "schema": "mydb",
    "table": "posts",
    "rows": 1000
  },
  {
    "schema": "mydb",
    "table": "photos",
    "rows": 500
  },
  {
    "schema": "mydb",
    "table": "videos",
    "rows": 200
  },
  {
    "schema": "mydb",
    "table": "comments",
    "rows": 10000,
    "populate_columns": [
      {
        "column": "commentable_type",
        "values": ["Post", "Photo", "Video"]
      }
    ],
    "logical_fks": [
      {
        "column": "commentable_id",
        "referenced_schema": "mydb",
        "referenced_table": "posts",
        "referenced_column": "id",
        "condition": "commentable_type = 'Post'"
      },
      {
        "column": "commentable_id",
        "referenced_schema": "mydb",
        "referenced_table": "photos",
        "referenced_column": "id",
        "condition": "commentable_type = 'Photo'"
      },
      {
        "column": "commentable_id",
        "referenced_schema": "mydb",
        "referenced_table": "videos",
        "referenced_column": "id",
        "condition": "commentable_type = 'Video'"
      }
    ]
  }
]
```

**Schema**:
```sql
CREATE TABLE comments (
  id INT PRIMARY KEY AUTO_INCREMENT,
  commentable_type VARCHAR(50),
  commentable_id INT,
  content TEXT
);
```

**How It Works**:
- System evaluates `commentable_type` for each row
- Selects appropriate FK based on matching condition
- Each comment references valid ID from correct parent table

**Result**: 10,000 comments distributed across posts, photos, and videos

### Example 5: Static FKs (Sampling from Production)

Reuse existing reference data:

```json
[
  {
    "schema": "mydb",
    "table": "test_orders",
    "rows": 1000,
    "static_fks": [
      {
        "column": "country_code",
        "static_schema": "production",
        "static_table": "countries",
        "static_column": "code"
      },
      {
        "column": "currency_code",
        "static_schema": "production",
        "static_table": "currencies",
        "static_column": "code"
      }
    ],
    "logical_fks": [
      {
        "column": "user_id",
        "referenced_schema": "mydb",
        "referenced_table": "test_users",
        "referenced_column": "id"
      }
    ]
  }
]
```

**Use Case**: Avoid generating large reference tables (countries, currencies) by sampling from production.

**Performance**: Faster than generating hundreds of parent rows, especially with `--sample-size`.

## Command-Line Options Reference

| Option | Description | Default | Example |
|--------|-------------|---------|---------|
| `--config` | Path to JSON configuration file | Required | `--config tables.json` |
| `--src-host` | MySQL server hostname | Required | `--src-host localhost` |
| `--src-user` | MySQL username | Required | `--src-user root` |
| `--out-sql` | Output INSERT SQL file path | Required | `--out-sql data.sql` |
| `--src-port` | MySQL server port | 3306 | `--src-port 3307` |
| `--src-password` | MySQL password | None | `--src-password secret` |
| `--ask-pass` | Prompt for password securely | False | `--ask-pass` |
| `--out-delete` | Output DELETE SQL file | None | `--out-delete cleanup.sql` |
| `--rows` | Default rows per table | 100 | `--rows 10000` |
| `--scale` | Scale rows by factor | None | `--scale 1.5` |
| `--sample-size` | Static FK sample size | 1000 | `--sample-size 5000` |
| `--seed` | Random seed for reproducibility | 42 | `--seed 12345` |
| `--hmac-key` | HMAC key for pseudonymization | None | `--hmac-key mykey` |
| `--threads` | Number of parallel threads | 4 | `--threads 8` |
| `--batch-size` | Rows per INSERT statement | 100 | `--batch-size 1000` |
| `--debug` | Enable debug output | False | `--debug` |

## How It Works

### Data Generation Pipeline

1. **Introspection Phase**
   - Connect to MySQL server
   - Load table schemas from `information_schema`
   - Detect primary keys, UNIQUE constraints, and foreign keys
   - Parse configuration file for logical FKs and column specifications

2. **Validation Phase**
   - Check for missing parent tables referenced by FKs
   - Validate configuration syntax and references
   - Detect circular dependencies
   - Warn about potential constraint violations

3. **Dependency Resolution**
   - Build dependency graph from FK relationships
   - Perform topological sort to determine generation order
   - Parent tables generated before child tables
   - Handles complex multi-level dependencies

4. **Parallel Generation Phase**
   - Distribute tables across worker threads
   - Each thread generates rows for assigned tables
   - Thread-safe tracking of:
     - Generated values for UNIQUE constraints
     - Parent values for FK resolution
     - Composite PK combinations

5. **FK Resolution**
   - **Pre-allocated UNIQUE FK tuples**: Assigned first (from Cartesian products)
   - **Conditional FKs**: Evaluate conditions, assign matching FK
   - **Unconditional FKs**: Random selection from parent values
   - **Composite FKs**: Coordinate multi-column assignments

6. **Constraint Satisfaction**
   - **Cartesian product**: For composite UNIQUE with all controlled columns
   - **Stratified sampling**: For overlapping UNIQUE constraints
   - **Sequential generation**: For single-column UNIQUE
   - **Conditional evaluation**: For polymorphic relationships

7. **SQL Generation**
   - Format rows as multi-value INSERT statements
   - Batch rows according to `--batch-size`
   - Handle auto-increment columns (omit from INSERT)
   - Escape special characters and NULL values
   - Generate optional DELETE statements for cleanup

8. **Output**
   - Write INSERT SQL to `--out-sql` file
   - Optionally write DELETE SQL to `--out-delete` file
   - Print summary statistics

### UNIQUE Constraint Strategy

#### Single-Column UNIQUE

**Detection**: Column has UNIQUE constraint or is part of single-column UNIQUE index

**Strategy**:
- **Strings**: Generate base value + sequential suffix
  - Example: `"user_1"`, `"user_2"`, `"user_3"`
- **Integers**: Sequential values within min/max range
  - Example: `1, 2, 3, ..., N`
- **With explicit values**: Cycle through provided list
  - Config: `{"column": "status", "values": ["A", "B", "C"]}`
  - Result: `A, B, C, A, B, C, ...`

#### Composite UNIQUE (All FKs)

**Detection**: UNIQUE constraint on 2+ columns, all are foreign keys

**Strategy**: Cartesian product
- Load parent values for each FK column
- Generate all combinations using `itertools.product()`
- Sample or repeat as needed
- **Guarantees**: Zero duplicates

**Example**:
```
UNIQUE (user_id, product_id)
Users: [1, 2, 3]
Products: [A, B]
Combinations: (1,A), (1,B), (2,A), (2,B), (3,A), (3,B) = 6 unique
```

#### Mixed UNIQUE (FK + Non-FK with Explicit Values)

**Detection**: UNIQUE constraint with both FK and non-FK columns, where non-FK has `populate_columns` config

**Strategy**: Extended Cartesian product
- FK columns: Load parent values
- Non-FK columns: Use explicit values from `populate_columns`
- Generate Cartesian product of all columns
- **Guarantees**: Zero duplicates if sufficient combinations exist

**Example**:
```
UNIQUE (user_id, status)
Config: {"column": "status", "values": ["active", "inactive"]}
Users: [1, 2, 3]
Combinations: (1,active), (1,inactive), (2,active), (2,inactive), (3,active), (3,inactive) = 6 unique
```

#### Overlapping UNIQUE Constraints

**Detection**: Multiple UNIQUE constraints share one or more columns

**Strategy**: Multi-constraint Cartesian product with stratified sampling

**Steps**:
1. Identify shared columns (intersection of constraint columns)
2. Identify non-shared columns (unique to each constraint)
3. For each shared column value:
   - Generate Cartesian product of all non-shared column values
   - Creates combinations satisfying all constraints simultaneously
4. Apply stratified sampling to ensure balanced distribution

**Example**:
```
UNIQUE (A_ID, C_ID)  -- Constraint 1
UNIQUE (A_ID, PR)    -- Constraint 2
Shared: A_ID
Non-shared: C_ID (from constraint 1), PR (from constraint 2)

For each A_ID value:
  Generate combinations of (PR, C_ID)
  A_ID=1: (1, 0, 1), (1, 0, 2), ..., (1, 1, 10) = 20 combinations
  
Total: N_A_ID * (N_PR * N_C_ID) combinations
```

**Stratified Sampling**: 
- Groups by shared column value
- Within each group, selects diverse non-shared value combinations
- Ensures even distribution across all constraint dimensions
- Minimizes clustering and maximizes coverage

## Troubleshooting

### Duplicates in UNIQUE Constraints

**Symptom**: 
```
ERROR 1062 (23000): Duplicate entry '123-456' for key 'unique_user_product'
```

**Causes**:
1. Non-FK column in UNIQUE constraint without explicit values
2. Insufficient unique value range (range too small for requested rows)
3. Multiple UNIQUE constraints not properly coordinated

**Solutions**:
1. **Add explicit values** for non-FK columns:
   ```json
   {
     "populate_columns": [
       {
         "column": "status",
         "values": ["active", "inactive", "pending"]
       }
     ]
   }
   ```

2. **Increase range** for numeric columns:
   ```json
   {
     "populate_columns": [
       {
         "column": "code",
         "min": 1,
         "max": 100000
       }
     ]
   }
   ```

3. **Reduce row count** to fit within available combinations:
   - Check debug output for "X unique combinations" message
   - Set `rows` ≤ X in configuration

### Missing Parent Values

**Symptom**:
```
WARNING: No parent values available for FK user_id -> mydb.users.id
```

**Causes**:
1. Parent table not included in configuration
2. Parent table generated after child table (dependency order issue)
3. Parent table has 0 rows

**Solutions**:
1. **Add parent table** to configuration:
   ```json
   [
     {
       "schema": "mydb",
       "table": "users",
       "rows": 1000
     },
     {
       "schema": "mydb",
       "table": "orders",
       "rows": 5000,
       "logical_fks": [...]
     }
   ]
   ```

2. **Check dependency order**: Parent tables are automatically generated first, but verify configuration is correct

3. **Use static FKs** if parent data exists in database:
   ```json
   {
     "static_fks": [
       {
         "column": "user_id",
         "static_schema": "mydb",
         "static_table": "users",
         "static_column": "id"
       }
     ]
   }
   ```

### Insufficient Combinations

**Symptom**:
```
WARNING: db.AC only has 25 unique FK combinations but 100 rows requested. Will generate duplicates.
```

**Cause**: Not enough unique value combinations to satisfy row count without duplicates

**Example**:
- UNIQUE(user_id, product_id)
- 5 users × 5 products = 25 unique combinations
- Requesting 100 rows → Need 75 duplicates

**Solutions**:
1. **Reduce row count**:
   ```json
   {
     "rows": 25  // Match or stay below combination count
   }
   ```

2. **Increase parent table rows**:
   ```json
   [
     {
       "schema": "mydb",
       "table": "users",
       "rows": 50  // Increased from 5
     },
     {
       "schema": "mydb",
       "table": "products",
       "rows": 20  // Increased from 5
     }
   ]
   // Now: 50 × 20 = 1000 combinations (sufficient for 100 rows)
   ```

3. **Add more non-FK columns** to UNIQUE constraint (if schema allows modification)

### Connection Errors

**Symptom**:
```
Error: Failed to connect to MySQL: (2003, "Can't connect to MySQL server on 'localhost'")
```

**Solutions**:
1. Verify MySQL server is running
2. Check hostname and port: `--src-host localhost --src-port 3306`
3. Verify user credentials: `--src-user root --ask-pass`
4. Check firewall rules allowing connections
5. Test connection manually: `mysql -h localhost -u root -p`

### Permission Errors

**Symptom**:
```
Error: (1142, "SELECT command denied to user 'readonly'@'localhost' for table 'COLUMNS'")
```

**Cause**: User lacks privileges to query `information_schema`

**Solution**: Grant necessary privileges:
```sql
GRANT SELECT ON information_schema.* TO 'readonly'@'localhost';
GRANT SELECT ON mydb.* TO 'readonly'@'localhost';
```

### Memory Issues

**Symptom**: Python process killed or out-of-memory errors with large datasets

**Solutions**:
1. **Reduce threads**: `--threads 2` (reduces parallel memory usage)
2. **Reduce batch size**: `--batch-size 50` (smaller in-memory batches)
3. **Process tables separately**: Split configuration into smaller files
4. **Increase system memory** or use machine with more RAM

### Slow Generation

**Symptom**: Generation takes too long for large datasets

**Solutions**:
1. **Increase threads**: `--threads 8` or `--threads 16`
2. **Increase batch size**: `--batch-size 1000` or `--batch-size 5000`
3. **Use static FKs**: Avoid generating large parent tables
4. **Reduce sample size**: `--sample-size 100` for static FK sampling
5. **Profile with debug**: `--debug` to identify bottlenecks

## Performance Tips

1. **Optimize Thread Count**
   ```bash
   --threads 8
   ```
   - Recommended: Number of CPU cores or 2× cores
   - More threads = faster generation (up to a point)
   - Watch memory usage with high thread counts

2. **Increase Batch Size**
   ```bash
   --batch-size 1000
   ```
   - Larger batches = fewer INSERT statements
   - Faster database loading
   - Limited by MySQL's `max_allowed_packet` (default 64MB)
   - If loading fails, reduce batch size

3. **Use Static FKs for Reference Data**
   ```json
   {
     "static_fks": [
       {
         "column": "country_code",
         "static_schema": "production",
         "static_table": "countries",
         "static_column": "code"
       }
     ]
   }
   ```
   - Avoids generating large reference tables
   - Much faster for tables with hundreds or thousands of reference rows
   - Useful for countries, currencies, categories, etc.

4. **Reduce Sample Size for Static FKs**
   ```bash
   --sample-size 100
   ```
   - Default is 1000 rows sampled per static FK
   - Reduce if parent table has many duplicates or is already small
   - Increase for better diversity from large parent tables

5. **Use Seed for Reproducible Testing**
   ```bash
   --seed 42
   ```
   - Same seed produces identical data across runs
   - Useful for debugging and consistent test environments
   - Change seed to generate different data distributions

6. **Process Large Datasets in Chunks**
   - Split configuration into multiple files
   - Generate data in phases (e.g., parent tables first, then children)
   - Combine SQL files with `cat parent.sql child.sql > full.sql`

7. **Monitor with Debug Mode**
   ```bash
   --debug
   ```
   - Identify which tables take longest to generate
   - Check for inefficient FK resolution
   - Verify Cartesian product is being used where expected
   - Watch for constraint violation warnings

8. **Optimize Database Loading**
   ```sql
   -- Disable foreign key checks during load
   SET FOREIGN_KEY_CHECKS = 0;
   SOURCE data.sql;
   SET FOREIGN_KEY_CHECKS = 1;
   
   -- Disable indexes during load, rebuild after
   ALTER TABLE large_table DISABLE KEYS;
   SOURCE data.sql;
   ALTER TABLE large_table ENABLE KEYS;
   ```

## Development

### Requirements

- **Python**: 3.7 or higher
- **PyMySQL**: `pip install PyMySQL`
- **MySQL Server**: For testing (can use Docker)

### Project Structure

```
generate_synthetic_data/
├── generate_synthetic_data.py       # Main script
├── generate_synthetic_data_utils.py # Utility functions and data structures
├── test_*.py                        # Unit and integration tests
├── CARTESIAN_UNIQUE_FK_FEATURE.md   # Feature documentation
├── MULTI_CONSTRAINT_CARTESIAN_FEATURE.md
└── README.md                        # This file
```

### Running Tests

The project includes comprehensive test coverage:

```bash
# Run all tests
python -m pytest test_*.py -v

# Run specific test file
python test_cartesian_unique_fks.py
python test_overlapping_constraints.py
python test_integration_overlapping.py

# Run with coverage
python -m pytest --cov=generate_synthetic_data --cov-report=html
```

**Note**: Some tests require a running MySQL instance and will be skipped if PyMySQL connection fails.

### Manual Testing

Create a test configuration:

```bash
cat > test_config.json << 'EOF'
[
  {
    "schema": "testdb",
    "table": "test_table",
    "rows": 100
  }
]
EOF

# Run with debug mode
python generate_synthetic_data.py \
  --config test_config.json \
  --src-host localhost \
  --src-user root \
  --ask-pass \
  --out-sql test_output.sql \
  --debug
```

### Debug Mode

Enable verbose logging to understand generation process:

```bash
--debug
```

**Debug Output Includes**:
- Table dependency order
- Constraint detection (UNIQUE, FK, PK)
- Cartesian product generation:
  - Parent value counts
  - Total combinations
  - Sampling strategy
- FK resolution steps:
  - Conditional FK evaluation
  - Parent value selection
  - Pre-allocated tuple assignment
- UNIQUE value allocation:
  - Value pool sizes
  - Sequential generation
  - Conflict detection
- Performance metrics:
  - Time per table
  - Rows generated per second

**Example Debug Output**:
```
[DEBUG] db.AC: Found overlapping UNIQUE constraints: ['ACS', 'APR']
[DEBUG] db.AC: Shared columns: ['A_ID']
[DEBUG] db.AC: Loaded 3000 parent values for A_ID
[DEBUG] db.AC: Loaded 10 parent values for C_ID
[DEBUG] db.AC: Loaded 2 explicit values for PR
[DEBUG] db.AC: Generated 60000 total valid combinations
[DEBUG] db.AC: Pre-allocated 6000 rows satisfying 2 constraints
[DEBUG] db.AC: Conditional FK column commentable_id has 3 conditional FKs
[DEBUG] db.AC: Conditional FK fk_posts matched (condition: commentable_type = 'Post'), assigned commentable_id=42
```

## Contributing

Contributions are welcome! Please follow these guidelines:

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature/my-feature`
3. **Write tests** for new functionality
4. **Ensure tests pass**: `python -m pytest test_*.py`
5. **Follow Python style**: PEP 8 conventions
6. **Submit a pull request** with clear description

### Areas for Contribution

- Additional constraint types (CHECK constraints, computed columns)
- More data generators (dates, emails, phone numbers)
- Performance optimizations for very large datasets
- Additional database support (PostgreSQL, SQLite)
- Web UI for configuration management
- Better error messages and validation

## Quick Reference

### Generate 10,000 Test Users

```bash
cat > users_config.json << 'EOF'
[
  {
    "schema": "testdb",
    "table": "users",
    "rows": 10000
  }
]
EOF

python generate_synthetic_data.py \
  --config users_config.json \
  --src-host localhost \
  --src-user root \
  --ask-pass \
  --out-sql users_test_data.sql \
  --threads 4 \
  --batch-size 500 \
  --debug
```

### With DELETE Statements for Cleanup

```bash
python generate_synthetic_data.py \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --ask-pass \
  --out-sql inserts.sql \
  --out-delete deletes.sql \
  --rows 1000

# Load data
mysql -u root testdb < inserts.sql

# Clean up later
mysql -u root testdb < deletes.sql
```

### From Existing Production Data

```bash
python generate_synthetic_data.py \
  --config config.json \
  --src-host prod-replica.example.com \
  --src-port 3306 \
  --src-user readonly \
  --ask-pass \
  --out-sql test_data.sql \
  --sample-size 5000 \
  --seed 12345
```

### Scale Existing Configuration

```bash
# Generate 1.5× the rows specified in config
python generate_synthetic_data.py \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql scaled_data.sql \
  --scale 1.5
```

### Reproducible Test Data

```bash
# Same seed = same data every time
python generate_synthetic_data.py \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql test_data.sql \
  --seed 42 \
  --threads 4
```

### Maximum Performance

```bash
python generate_synthetic_data.py \
  --config config.json \
  --src-host localhost \
  --src-user root \
  --out-sql data.sql \
  --threads 16 \
  --batch-size 5000 \
  --sample-size 100
```

---

## Support

For issues, questions, or contributions:
- **Issues**: [GitHub Issues](https://github.com/ChrisCGH/generate_synthetic_data/issues)
- **Discussions**: [GitHub Discussions](https://github.com/ChrisCGH/generate_synthetic_data/discussions)
- **Documentation**: This README and feature documentation in repository

---

**Last Updated**: 2025-12-09
