#!/usr/bin/env python3
"""Integration test for multi-constraint Cartesian product.

This tests the full scenario described in the issue where a table has:
- UNIQUE KEY ACS (A_ID, C_ID) - 3000 × 10 = 30,000 combinations
- UNIQUE KEY APR (A_ID, PR) - 3000 × 2 = 6,000 combinations
- Both share column A_ID
- Requesting 6000 rows

Both constraints should be satisfied with zero duplicates.
"""
import random
import sys


def simulate_multi_constraint_cartesian():
    """Simulate the multi-constraint Cartesian product logic."""
    
    print("=" * 70)
    print("Integration Test: Multi-Constraint Cartesian Product")
    print("=" * 70)
    
    # Simulate parent tables
    print("\nSetting up parent tables...")
    table_a_rows = [{"ID": i} for i in range(1, 3001)]  # 3000 rows (A_ID values)
    table_c_rows = [{"ID": i} for i in range(1, 11)]    # 10 rows (C_ID values)
    pr_values = [0, 1]  # 2 PR values from populate_columns config
    
    print(f"  Table A: {len(table_a_rows)} rows")
    print(f"  Table C: {len(table_c_rows)} rows")
    print(f"  PR values: {pr_values}")
    
    # Calculate theoretical combinations
    acs_combos = len(table_a_rows) * len(table_c_rows)  # 3000 * 10 = 30,000
    apr_combos = len(table_a_rows) * len(pr_values)     # 3000 * 2 = 6,000
    
    print(f"\nTheoretical combinations:")
    print(f"  ACS (A_ID, C_ID): {acs_combos:,} combinations")
    print(f"  APR (A_ID, PR): {apr_combos:,} combinations")
    
    # Simulate multi-constraint generation using true Cartesian product
    print("\nSimulating multi-constraint Cartesian product...")
    print("  Shared column: A_ID")
    print("  Non-shared columns: PR, C_ID")
    
    # Generate combinations using true Cartesian product
    import itertools
    all_combinations = []
    
    a_id_values = [r["ID"] for r in table_a_rows]
    c_id_values = [r["ID"] for r in table_c_rows]
    
    # Build value lists for non-shared columns
    non_shared_value_lists = {
        'PR': pr_values,
        'C_ID': c_id_values
    }
    
    non_shared_cols = list(non_shared_value_lists.keys())
    value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
    
    # Generate Cartesian product for each A_ID
    for a_id in a_id_values:
        for combo in itertools.product(*value_lists):
            combination = {"A_ID": a_id}
            for col_name, value in zip(non_shared_cols, combo):
                combination[col_name] = value
            all_combinations.append(combination)
    
    print(f"  Generated {len(all_combinations):,} total valid combinations")
    print(f"  (3,000 A_IDs × 2 PR values × 10 C_ID values = {3000 * 2 * 10:,})")
    
    # Use stratified sampling instead of random shuffle
    from collections import defaultdict
    
    primary_shared_col = 'A_ID'
    shared_values = a_id_values
    requested_rows = 6000
    
    # Group combinations by shared value (A_ID)
    combos_by_shared_val = defaultdict(list)
    for combo in all_combinations:
        shared_val = combo[primary_shared_col]
        combos_by_shared_val[shared_val].append(combo)
    
    # Calculate rows per shared value
    rows_per_shared_val = requested_rows // len(shared_values)
    remainder = requested_rows % len(shared_values)
    
    print(f"  Using stratified sampling: {rows_per_shared_val} rows per shared value, {remainder} remainder")
    
    random.seed(42)
    selected = []
    shared_values_list = list(shared_values)
    random.shuffle(shared_values_list)  # Randomize order
    
    for idx, shared_val in enumerate(shared_values_list):
        available = combos_by_shared_val[shared_val]
        num_rows_for_this_val = rows_per_shared_val + (1 if idx < remainder else 0)
        
        random.shuffle(available)
        selected_for_this_val = available[:num_rows_for_this_val]
        selected.extend(selected_for_this_val)
    
    # Shuffle final selection
    random.shuffle(selected)
    
    print(f"  Selected {len(selected):,} rows for table AC")
    
    # Verify uniqueness
    print("\nVerifying constraint satisfaction...")
    
    # Check APR (A_ID, PR) uniqueness
    apr_pairs = set()
    apr_duplicates = []
    for row in selected:
        pair = (row["A_ID"], row["PR"])
        if pair in apr_pairs:
            apr_duplicates.append(pair)
        apr_pairs.add(pair)
    
    print(f"  APR (A_ID, PR): {len(apr_pairs):,} unique pairs")
    if apr_duplicates:
        print(f"    ✗ FAILED: {len(apr_duplicates)} duplicates found")
        return False
    else:
        print(f"    ✓ PASSED: No duplicates")
    
    # Check ACS (A_ID, C_ID) uniqueness
    acs_pairs = set()
    acs_duplicates = []
    for row in selected:
        pair = (row["A_ID"], row["C_ID"])
        if pair in acs_pairs:
            acs_duplicates.append(pair)
        acs_pairs.add(pair)
    
    print(f"  ACS (A_ID, C_ID): {len(acs_pairs):,} unique pairs")
    if acs_duplicates:
        print(f"    ✗ FAILED: {len(acs_duplicates)} duplicates found")
        return False
    else:
        print(f"    ✓ PASSED: No duplicates")
    
    print("\n" + "=" * 70)
    print("✓ SUCCESS: Both constraints satisfied with zero duplicates!")
    print("=" * 70)
    
    return True


if __name__ == "__main__":
    success = simulate_multi_constraint_cartesian()
    sys.exit(0 if success else 1)
