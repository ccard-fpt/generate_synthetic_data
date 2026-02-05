# Multiple Format Placeholders Feature

## Overview

The synthetic data generator now supports **multiple placeholders** in format strings, allowing you to avoid duplicates when generating values for columns with UNIQUE constraints or when you need more variation than a single range can provide.

## Problem Statement

Previously, when using a format string with a single placeholder like:

```json
{
  "column": "FileName",
  "min": 1754928466,
  "max": 1764928466,
  "format": "hosted_agg01lgw_1_192.168.0.1_{:10d}_1.gz.enc"
}
```

If you needed to generate more values than the range size (max - min + 1), you would encounter duplicate errors:

```
CRITICAL ERROR: Duplicate in blackspider.LogData.FileName: 
  ('hosted_agg01lgw_1_192.168.0.1_1764115773_1.gz.enc', 'hss01lgw') at batch_idx=3298418
```

## Solution

You can now use **multiple placeholders** in your format string:

```json
{
  "column": "FileName",
  "min": 1754928466,
  "max": 1764928466,
  "format": "hosted_agg01lgw_1_192.168.0.1_{:10d}_{:1d}.gz.enc"
}
```

The second placeholder `{:1d}` will be automatically filled with variation values (0-9), providing 10 times more unique values.

## How It Works

### Value Generation Strategy

1. **First placeholder**: Uses the value from your `min`/`max` range
2. **Additional placeholders**: Automatically filled with variation values (0-9)

This provides **10^(n-1) variations** for **n placeholders**.

### Examples

#### Two Placeholders (10x more values)

```json
{
  "column": "transaction_id",
  "min": 1000,
  "max": 2000,
  "format": "TXN_{:04d}_{:1d}"
}
```

Generates values like:
- `TXN_1523_0`
- `TXN_1523_7`
- `TXN_1847_3`

**Capacity**: (2000 - 1000 + 1) × 10 = **10,010 unique values**

#### Three Placeholders (100x more values)

```json
{
  "column": "order_code",
  "min": 100,
  "max": 999,
  "format": "ORD_{:03d}_{:02d}_{:01d}"
}
```

Generates values like:
- `ORD_352_05_3`
- `ORD_781_91_7`
- `ORD_142_23_0`

**Capacity**: (999 - 100 + 1) × 100 = **90,000 unique values**

#### Real-World Example: Log Files

```json
{
  "column": "FileName",
  "min": 1754928466,
  "max": 1764928466,
  "format": "hosted_agg01lgw_1_192.168.0.1_{:10d}_{:1d}.gz.enc"
}
```

Generates values like:
- `hosted_agg01lgw_1_192.168.0.1_1756796291_0.gz.enc`
- `hosted_agg01lgw_1_192.168.0.1_1759542692_3.gz.enc`
- `hosted_agg01lgw_1_192.168.0.1_1758673320_2.gz.enc`

**Capacity**: 10 billion × 10 = **100 billion unique values**

## Format Placeholder Specifications

### Supported Format Specifiers

All standard Python format specifiers are supported:

- `{:d}` - Decimal integer (no padding)
- `{:03d}` - Decimal with zero-padding (3 digits)
- `{:08d}` - Decimal with zero-padding (8 digits)
- `{:02x}` - Hexadecimal lowercase (2 digits)
- `{:04X}` - Hexadecimal uppercase (4 digits)

### Custom Ranges for Placeholders

You can now specify custom ranges for each placeholder using the `format_ranges` parameter:

```json
{
  "column": "code",
  "min": 100,
  "max": 200,
  "format": "CODE_{:d}_{:d}",
  "format_ranges": [[100, 200], [50, 99]]
}
```

This generates values like:
- `CODE_150_75`
- `CODE_125_82`
- `CODE_189_67`

**How it works:**
- First element in `format_ranges` corresponds to first placeholder (uses `min`/`max` if omitted)
- Additional elements specify ranges for additional placeholders
- If `format_ranges` is not specified, additional placeholders default to [0, 9]

#### Example: Wide Range for Additional Placeholder

```json
{
  "column": "transaction_id",
  "min": 1000,
  "max": 2000,
  "format": "TXN_{:d}_{:d}",
  "format_ranges": [[1000, 2000], [0, 999]]
}
```

Generates:
- `TXN_1523_456`
- `TXN_1847_123`
- `TXN_1256_789`

**Capacity**: 1,001 base values × 1,000 variations = **1,001,000 unique values**

#### Example: Multiple Custom Ranges

```json
{
  "column": "part_number",
  "min": 100,
  "max": 999,
  "format": "PART_{:d}_{:d}_{:d}",
  "format_ranges": [[100, 999], [1, 50], [1, 10]]
}
```

Generates:
- `PART_523_25_7`
- `PART_847_12_3`
- `PART_256_48_9`

**Capacity**: 900 × 50 × 10 = **450,000 unique values**

### Default Placeholder Behavior

When `format_ranges` is not specified:

- **First placeholder**: Uses `min`/`max` range from configuration
- **Additional placeholders**: Default to [0, 9] range
- **Distribution**: Randomly selected (for individual values) or sequentially enumerated (for unique pools)

## Backward Compatibility

Single placeholder formats continue to work exactly as before:

```json
{
  "column": "user_code",
  "min": 1,
  "max": 1000,
  "format": "USER_{:08d}"
}
```

No changes needed to existing configurations!

## Configuration Examples

### Minimal Configuration

```json
{
  "populate_columns": [
    {
      "column": "id",
      "min": 1,
      "max": 100,
      "format": "ID_{:03d}_{:01d}"
    }
  ]
}
```

### Full Table Configuration

```json
{
  "schema": "mydb",
  "table": "log_files",
  "rows": 100000,
  "populate_columns": [
    {
      "column": "file_id",
      "min": 1,
      "max": 50000,
      "format": "FILE_{:06d}_{:01d}"
    },
    {
      "column": "batch_code",
      "min": 2024001,
      "max": 2024365,
      "format": "BATCH_{:07d}_{:02d}_{:01d}"
    }
  ]
}
```

## UNIQUE Constraints

Multiple placeholders work seamlessly with UNIQUE constraints:

```sql
CREATE TABLE files (
  id INT PRIMARY KEY AUTO_INCREMENT,
  filename VARCHAR(255) UNIQUE,
  created_date DATE
);
```

```json
{
  "populate_columns": [
    {
      "column": "filename",
      "min": 1754928466,
      "max": 1764928466,
      "format": "file_{:10d}_{:1d}.dat"
    }
  ]
}
```

The generator will:
1. Create a pool of unique values using sequential counters for additional placeholders
2. Guarantee no duplicates in the generated data
3. Efficiently handle large datasets

## Best Practices

### Choosing Number of Placeholders

1. **Single placeholder**: When your range is large enough for all rows
   ```json
   "min": 1, "max": 1000000, "format": "CODE_{:08d}"
   ```

2. **Two placeholders**: When you need 10x more capacity
   ```json
   "min": 1, "max": 100000, "format": "CODE_{:06d}_{:1d}"
   ```

3. **Three+ placeholders**: For maximum uniqueness with small ranges
   ```json
   "min": 1, "max": 1000, "format": "CODE_{:04d}_{:02d}_{:01d}"
   ```

### Placeholder Width and Padding

Python format specifiers control padding:

```json
// {:d} - No padding (variable width)
"format": "CODE_{:d}_{:d}"
// Generates: CODE_5_123, CODE_42_7, CODE_999_1

// {:03d} - Zero-padded to 3 digits (fixed width)
"format": "CODE_{:03d}_{:03d}"
// Generates: CODE_005_123, CODE_042_007, CODE_999_001

// {:5d} - Space-padded to 5 digits (usually avoid)
"format": "CODE_{:5d}_{:5d}"
// Generates: CODE_    5_  123, CODE_   42_    7
```

**Recommendation**: Use `{:d}` for no padding or `{:0Nd}` for zero-padding to N digits.

### Custom Ranges for Placeholders

When you need specific ranges for each placeholder:

```json
{
  "column": "code",
  "min": 100,
  "max": 200,
  "format": "CODE_{:d}_{:d}",
  "format_ranges": [[100, 200], [50, 99]]
}
```

- First placeholder: 100-200
- Second placeholder: 50-99
- Generates: `CODE_150_75`, `CODE_125_82`

**Without `format_ranges`:**
- First placeholder: uses min/max (100-200)
- Additional placeholders: default to 0-9
- Generates: `CODE_150_5`, `CODE_125_2`

### Performance Considerations

- Multiple placeholders add minimal overhead
- Unique pool generation is efficient even with millions of values
- Random variation ensures good distribution
- Custom ranges are fully supported in both individual and pool generation

## Troubleshooting

### Error: "Format string failed"

**Cause**: Mismatched number of placeholders
```json
// Wrong: 2 placeholders, but format expects 1
"format": "CODE_{:03d}_{:02d}"  
```

**Solution**: Ensure format string matches your intention:
```json
// Correct
"format": "CODE_{:03d}_{:02d}"  // Uses auto-variation for second
```

### Warning: "range has only X values but Y rows requested"

**Cause**: Range too small even with multiple placeholders

**Solution**: Either:
1. Increase the range (min/max)
2. Add more placeholders
3. Reduce the number of rows

### Still Getting Duplicates?

Check:
1. Is there a UNIQUE constraint defined in your schema?
2. Are you using `generate_unique_value_pool()` for UNIQUE columns?
3. Is your total capacity (range × 10^(placeholders-1)) >= rows needed?

## Technical Details

### Implementation

- **Function**: `count_format_placeholders(format_str)`
  - Counts `{...}` placeholders using regex
  
- **Value Generation**: `generate_value_with_config(rng, col, config)`
  - First placeholder: `rng.randint(min, max)`
  - Additional: `rng.randint(0, 9)`
  
- **Pool Generation**: `generate_unique_value_pool(col_meta, config, needed_count, rng)`
  - First placeholder: Values from range
  - Additional: Sequential counter (mod 10)

### Testing

Run tests to verify the feature:

```bash
python3 test_extended_populate_columns.py
python3 test_unique_constraints.py
```

## See Also

- [README.md](README.md) - General documentation
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) - Configuration reference
- [test_extended_populate_columns.py](test_extended_populate_columns.py) - Example usage in tests
