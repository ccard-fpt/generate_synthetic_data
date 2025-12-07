# Cartesian Product for Composite UNIQUE Constraints with FK Columns

## Overview

This feature implements support for composite UNIQUE constraints where all columns are foreign keys. It uses Cartesian product generation to guarantee uniqueness of FK combinations, preventing duplicate violations.

## Problem Statement

When a table has:
1. A composite UNIQUE constraint on FK columns (e.g., `UNIQUE (A_ID, C_ID)`)
2. Both columns are FKs to parent tables
3. A separate PRIMARY KEY (e.g., auto-increment `ID`)

The previous FK resolution randomly assigned values from parent tables, causing **duplicate violations** of the UNIQUE constraint.

### Example Scenario

```sql
CREATE TABLE AC (
  ID INT PRIMARY KEY AUTO_INCREMENT,
  A_ID INT NOT NULL,
  C_ID INT NOT NULL,
  PR TINYINT,
  UNIQUE KEY (A_ID, C_ID),
  FOREIGN KEY (A_ID) REFERENCES A(ID),
  FOREIGN KEY (C_ID) REFERENCES C(ID)
);
```

**Parent tables:**
- Table A: 3000 rows (3000 unique IDs)
- Table C: 10 rows (10 unique IDs)

**Requested:** 6000 rows in AC

**Expected:** 6000 unique (A_ID, C_ID) combinations (out of 30,000 possible)

**Previous behavior:** Random duplicates like:
```
Row 1:   A_ID=123, C_ID=1
Row 500: A_ID=123, C_ID=1  ← DUPLICATE!
```

**New behavior:** 6000 unique combinations guaranteed by Cartesian product

## Solution

### Detection Logic

The system detects composite UNIQUE constraints where ALL columns are foreign keys:

```python
# Check for composite UNIQUE constraints where all columns are FKs
if not pk_fk_columns and tmeta.unique_constraints:
    for uc in tmeta.unique_constraints:
        if len(uc.columns) >= 2:  # Multi-column UNIQUE
            all_fks = all(col in fk_map for col in uc.columns)
            if all_fks:
                # Use Cartesian product for this constraint
```

### Cartesian Product Generation

1. Load parent values for each FK column
2. Generate all possible combinations using `itertools.product`
3. Sample or repeat combinations as needed
4. Pre-allocate combinations to rows

```python
import itertools

# Load parent values
parent_a_values = [1, 2, ..., 3000]
parent_c_values = [1, 2, ..., 10]

# Generate Cartesian product
all_combinations = list(itertools.product(parent_a_values, parent_c_values))
# Result: 30,000 combinations

# Sample 6000 combinations
rng.shuffle(all_combinations)
selected = all_combinations[:6000]

# Pre-allocate to rows
pre_allocated_unique_fk_tuples = {
    "A_ID": {0: 1479, 1: 709, 2: 432, ...},
    "C_ID": {0: 6, 1: 9, 2: 5, ...}
}
```

### FK Resolution

During FK resolution, columns that are pre-allocated via UNIQUE constraint Cartesian product are skipped:

```python
# Assign pre-allocated UNIQUE FK values first
if pre_allocated_unique_fk_tuples:
    for col_name, value_map in pre_allocated_unique_fk_tuples.items():
        if row_idx in value_map:
            temp_row[col_name] = value_map[row_idx]
            assigned_by_conditional_fk.add(col_name)  # Mark as assigned

# Skip pre-allocated columns in normal FK resolution
if fk_col in assigned_by_conditional_fk:
    continue
```

## Usage Examples

### Basic Configuration

```json
{
  "schema": "db",
  "table": "AC",
  "rows": 6000,
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
```

**Debug output:**
```
[DEBUG] db.AC: Composite UNIQUE idx_unique has all FK columns: ['A_ID', 'C_ID']. Using Cartesian product.
[DEBUG] db.AC: Loaded 3000 parent values for A_ID from db.A.ID
[DEBUG] db.AC: Loaded 10 parent values for C_ID from db.C.ID
[DEBUG] db.AC: Generated 30000 total combinations from Cartesian product
[DEBUG] db.AC: Pre-allocated 6000 unique FK tuples for UNIQUE constraint idx_unique
```

### Three-Column UNIQUE Constraint

```json
{
  "schema": "db",
  "table": "ABC",
  "rows": 1000,
  "logical_fks": [
    {"column": "A_ID", "referenced_schema": "db", "referenced_table": "A", "referenced_column": "ID"},
    {"column": "B_ID", "referenced_schema": "db", "referenced_table": "B", "referenced_column": "ID"},
    {"column": "C_ID", "referenced_schema": "db", "referenced_table": "C", "referenced_column": "ID"}
  ]
}
```

With `UNIQUE (A_ID, B_ID, C_ID)`, generates 3-way Cartesian product.

### Insufficient Combinations

```json
{
  "schema": "db",
  "table": "XY",
  "rows": 100,
  "logical_fks": [
    {"column": "X_ID", "referenced_schema": "db", "referenced_table": "X", "referenced_column": "ID"},
    {"column": "Y_ID", "referenced_schema": "db", "referenced_table": "Y", "referenced_column": "ID"}
  ]
}
```

With X having 5 rows and Y having 5 rows (25 combinations total), requesting 100 rows:

**Output:**
```
WARNING: db.XY only has 25 unique FK combinations but 100 rows requested. Will generate duplicates.
```

The system will repeat combinations to fill 100 rows, but only 25 will be unique.

## Behavior Details

### When Cartesian Product Activates

The Cartesian product logic activates when:
1. The table has at least one composite UNIQUE constraint (2+ columns)
2. ALL columns in the constraint are foreign keys
3. The PK is NOT using Cartesian product (PK takes precedence)

### Precedence Rules

1. **PK Cartesian Product** (highest): If the PK is entirely composed of FKs
2. **UNIQUE Cartesian Product**: If a composite UNIQUE has all FKs (and PK doesn't use Cartesian)
3. **Sequential Generation**: For uncontrolled columns in composite UNIQUE
4. **Random Selection** (default): Normal FK resolution

### Interaction with fk_ratios

When Cartesian product is used for UNIQUE constraints, `fk_ratios` is **ignored** for those FK columns:

```json
{
  "fk_ratios": {
    "A_ID": 2  // This will be ignored
  }
}
```

**Warning:**
```
WARNING: db.AC: fk_ratios for column A_ID will be ignored because it's in a composite UNIQUE constraint with Cartesian product
```

### Multiple UNIQUE Constraints

If multiple composite UNIQUE constraints have all FK columns, the **first one is used**:

```
WARNING: db.T: Multiple composite UNIQUE constraints with all FKs found. Using: unique_a_c
```

### Single-Column UNIQUE Constraints

Single-column UNIQUE constraints are NOT handled by this feature. They continue to use existing unique value pool logic.

## Testing

### Unit Tests

`test_cartesian_unique_fks.py` contains 15 tests:

```bash
$ python3 test_cartesian_unique_fks.py -v
```

**Test coverage:**
- Detection of composite UNIQUE constraints with all FK columns
- Cartesian product generation (basic, three-column, sampling)
- Pre-allocation structure and assignment
- Skip logic for FK resolution
- Uniqueness guarantees
- FK ratios conflict detection

### Integration Test

`test_integration_cartesian_unique.py` demonstrates real-world scenarios:

```bash
$ python3 test_integration_cartesian_unique.py
```

**Test scenarios:**
1. Main scenario: 6000 rows from 30,000 combinations
2. Insufficient combinations: 20 rows from 6 combinations
3. Three-column UNIQUE constraint: 100 rows from 150 combinations

## Performance Considerations

### Memory Efficiency

When repeating combinations (insufficient parent values), the implementation uses modulo indexing instead of list multiplication:

```python
# Memory efficient
extended_combinations = []
for i in range(requested_rows):
    extended_combinations.append(all_combinations[i % len(all_combinations)])

# Instead of:
# all_combinations * repetitions  # Creates large temporary list
```

### Large Cartesian Products

For large parent tables (e.g., 10,000 × 10,000 = 100M combinations), the system:
1. Generates the full Cartesian product
2. Shuffles for randomness
3. Samples the requested subset

**Memory usage:** O(total_combinations) during generation, then O(requested_rows) after sampling.

## Backward Compatibility

The feature is fully backward compatible:

1. **Existing configs continue to work unchanged**
2. **No changes to PK Cartesian product logic**
3. **Single-column UNIQUE handling unchanged**
4. **Only activates for composite UNIQUE with all FK columns**

## Error Handling

### Missing Parent Values

If parent values cannot be loaded for any FK column:

```
ERROR: No parent values found for FK A_ID -> db.A.ID
```

The Cartesian product is skipped, and normal FK resolution is used.

### Partial Parent Load Prevention

The system uses `all_parents_loaded` flag to ensure either all parent values load successfully or none are used:

```python
all_parents_loaded = True
for col_name in uc.columns:
    if not parent_vals:
        all_parents_loaded = False
        break

if all_parents_loaded and len(parent_value_lists) == len(uc.columns):
    # Proceed with Cartesian product
```

This prevents inconsistent state where some FK columns have values but others don't.

## Limitations

1. **Single-column UNIQUE**: Not handled by this feature (uses existing logic)
2. **Mixed UNIQUE**: UNIQUE constraints with both FK and non-FK columns are not handled
3. **Multiple constraints**: Only the first composite UNIQUE with all FKs is used
4. **Memory**: Large Cartesian products require sufficient memory for generation

## Future Enhancements

Potential improvements:

1. Support multiple composite UNIQUE constraints simultaneously
2. Optimize memory usage for very large Cartesian products
3. Add configuration to prefer specific UNIQUE constraint when multiple exist
4. Support partial FK Cartesian products (e.g., 2 of 3 columns are FKs)

## Implementation Files

- `generate_synthetic_data.py`: Core implementation (lines ~1139-1242)
- `test_cartesian_unique_fks.py`: Unit tests (15 tests)
- `test_integration_cartesian_unique.py`: Integration tests (3 scenarios)

## References

- Issue: Support Cartesian Product for Composite UNIQUE Constraints with FK Columns
- PR: [Link to PR]
- Tests: 138/138 passing (excluding pymysql-dependent tests)
