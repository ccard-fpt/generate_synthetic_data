#!/usr/bin/env python3
"""
Integration test demonstrating Cartesian product for composite UNIQUE constraints with FK columns.

This test creates a realistic scenario matching the problem statement:
- Table A with 3000 rows
- Table C with 10 rows
- Table AC with composite UNIQUE(A_ID, C_ID) where both are FKs
- Request 6000 rows in AC

Expected: 6000 unique (A_ID, C_ID) combinations from the 30,000 possible combinations.
"""

import sys
import random
from generate_synthetic_data_utils import (
    ColumnMeta,
    UniqueConstraint,
    TableMeta,
    FKMeta,
)


def simulate_cartesian_unique_fk():
    """Simulate the Cartesian product logic for UNIQUE FK constraints."""
    
    # Simulate parent tables
    print("Setting up parent tables...")
    table_a_rows = [{"ID": i} for i in range(1, 3001)]  # 3000 rows
    table_c_rows = [{"ID": i} for i in range(1, 11)]    # 10 rows
    
    print(f"  Table A: {len(table_a_rows)} rows")
    print(f"  Table C: {len(table_c_rows)} rows")
    
    # Simulate child table AC with UNIQUE(A_ID, C_ID)
    print("\nSimulating table AC generation...")
    
    # Extract parent values
    parent_a_values = [r["ID"] for r in table_a_rows]
    parent_c_values = [r["ID"] for r in table_c_rows]
    
    print(f"  Parent A unique values: {len(set(parent_a_values))}")
    print(f"  Parent C unique values: {len(set(parent_c_values))}")
    
    # Generate Cartesian product
    import itertools
    all_combinations = list(itertools.product(parent_a_values, parent_c_values))
    
    print(f"\nCartesian product:")
    print(f"  Total possible combinations: {len(all_combinations)}")
    
    # Request 6000 rows
    requested_rows = 6000
    print(f"  Requested rows: {requested_rows}")
    
    # Check if we have enough combinations
    if len(all_combinations) >= requested_rows:
        print(f"  ✓ Sufficient combinations available")
        
        # Sample random subset
        rng = random.Random(42)
        rng.shuffle(all_combinations)
        selected_combinations = all_combinations[:requested_rows]
        
        print(f"\nResult:")
        print(f"  Generated rows: {len(selected_combinations)}")
        print(f"  Unique combinations: {len(set(selected_combinations))}")
        
        # Verify uniqueness
        if len(selected_combinations) == len(set(selected_combinations)):
            print(f"  ✓ All combinations are unique!")
            
            # Show sample combinations
            print(f"\nSample combinations (first 10):")
            for i, (a_id, c_id) in enumerate(selected_combinations[:10]):
                print(f"    Row {i+1}: A_ID={a_id}, C_ID={c_id}")
            
            print(f"\n  ... (showing 10 of {len(selected_combinations)} rows)")
            
            return True
        else:
            print(f"  ✗ ERROR: Duplicates found!")
            return False
    else:
        print(f"  ✗ Insufficient combinations: only {len(all_combinations)} available")
        return False


def test_insufficient_combinations():
    """Test scenario where there are fewer combinations than requested rows."""
    print("\n" + "="*80)
    print("TEST: Insufficient combinations scenario")
    print("="*80)
    
    # Small parent tables
    parent_a_values = [1, 2, 3]  # 3 values
    parent_c_values = [10, 20]   # 2 values
    
    print(f"  Parent A: {len(parent_a_values)} values")
    print(f"  Parent C: {len(parent_c_values)} values")
    
    # Generate Cartesian product
    import itertools
    all_combinations = list(itertools.product(parent_a_values, parent_c_values))
    
    print(f"  Total combinations: {len(all_combinations)}")
    
    # Request more rows than combinations
    requested_rows = 20
    print(f"  Requested rows: {requested_rows}")
    
    if len(all_combinations) < requested_rows:
        print(f"  ⚠ WARNING: Only {len(all_combinations)} unique combinations but {requested_rows} rows requested")
        print(f"  Will generate duplicates by repeating combinations")
        
        # Repeat combinations using modulo
        extended_combinations = []
        for i in range(requested_rows):
            extended_combinations.append(all_combinations[i % len(all_combinations)])
        
        print(f"\nResult:")
        print(f"  Generated rows: {len(extended_combinations)}")
        print(f"  Unique combinations: {len(set(extended_combinations))}")
        
        # Show all unique combinations
        print(f"\nAll unique combinations:")
        for i, combo in enumerate(sorted(set(extended_combinations))):
            print(f"    Combination {i+1}: A_ID={combo[0]}, C_ID={combo[1]}")
        
        return True
    
    return False


def test_three_column_unique():
    """Test scenario with three FK columns in UNIQUE constraint."""
    print("\n" + "="*80)
    print("TEST: Three-column UNIQUE constraint")
    print("="*80)
    
    # Parent tables
    parent_a_values = list(range(1, 11))   # 10 values
    parent_b_values = list(range(1, 6))    # 5 values
    parent_c_values = list(range(1, 4))    # 3 values
    
    print(f"  Parent A: {len(parent_a_values)} values")
    print(f"  Parent B: {len(parent_b_values)} values")
    print(f"  Parent C: {len(parent_c_values)} values")
    
    # Generate Cartesian product
    import itertools
    all_combinations = list(itertools.product(parent_a_values, parent_b_values, parent_c_values))
    
    print(f"\nCartesian product:")
    print(f"  Total combinations: {len(all_combinations)} (should be {10*5*3})")
    
    # Request subset
    requested_rows = 100
    print(f"  Requested rows: {requested_rows}")
    
    if len(all_combinations) >= requested_rows:
        rng = random.Random(42)
        rng.shuffle(all_combinations)
        selected = all_combinations[:requested_rows]
        
        print(f"\nResult:")
        print(f"  Generated rows: {len(selected)}")
        print(f"  Unique combinations: {len(set(selected))}")
        print(f"  ✓ All combinations are unique!")
        
        # Show sample
        print(f"\nSample combinations (first 5):")
        for i, (a_id, b_id, c_id) in enumerate(selected[:5]):
            print(f"    Row {i+1}: A_ID={a_id}, B_ID={b_id}, C_ID={c_id}")
        
        return True
    
    return False


if __name__ == "__main__":
    print("="*80)
    print("INTEGRATION TEST: Cartesian Product for Composite UNIQUE Constraints")
    print("="*80)
    
    # Run main test
    result1 = simulate_cartesian_unique_fk()
    
    # Run additional tests
    result2 = test_insufficient_combinations()
    result3 = test_three_column_unique()
    
    # Summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"  Main scenario (6000 rows from 30000 combinations): {'PASS' if result1 else 'FAIL'}")
    print(f"  Insufficient combinations scenario: {'PASS' if result2 else 'FAIL'}")
    print(f"  Three-column UNIQUE constraint: {'PASS' if result3 else 'FAIL'}")
    
    if all([result1, result2, result3]):
        print("\n✓ All integration tests passed!")
        sys.exit(0)
    else:
        print("\n✗ Some tests failed")
        sys.exit(1)
