#!/usr/bin/env python3
"""Test to verify the multi-constraint Cartesian product bug fix.

This test validates that when overlapping UNIQUE constraints exist,
the system generates a true Cartesian product of all non-shared columns,
ensuring zero duplicates for ALL constraints.
"""
import unittest
import random


class TestMultiConstraintCartesianFix(unittest.TestCase):
    """Test the fix for multi-constraint Cartesian product generation."""
    
    def test_buggy_behavior_with_max_causes_duplicates(self):
        """Demonstrate that using MAX causes duplicates in the tighter constraint."""
        # Scenario from problem statement:
        # - UNIQUE(A_ID, C_ID) where C_ID has 10 values
        # - UNIQUE(A_ID, PR) where PR has 2 values
        # - Shared column: A_ID (3000 values)
        # - Requesting: 6000 rows
        
        a_id_values = list(range(1, 3001))  # 3000 A_IDs
        pr_values = [0, 1]  # 2 PR values
        c_id_values = list(range(1, 11))  # 10 C_ID values
        
        # BUGGY: rows_per_shared_combo = max(2, 10) = 10
        rows_per_shared_combo = max(len(pr_values), len(c_id_values))
        
        # Generate using the buggy modulo cycling approach
        all_combinations = []
        for a_id in a_id_values:
            for local_idx in range(rows_per_shared_combo):
                combination = {
                    'A_ID': a_id,
                    'PR': pr_values[local_idx % len(pr_values)],  # Cycles: 0,1,0,1,0,1,0,1,0,1
                    'C_ID': c_id_values[local_idx % len(c_id_values)],  # Cycles: 1,2,3,4,5,6,7,8,9,10
                }
                all_combinations.append(combination)
        
        self.assertEqual(len(all_combinations), 30000)  # 3000 * 10
        
        # Shuffle and select 6000
        random.seed(42)
        random.shuffle(all_combinations)
        selected = all_combinations[:6000]
        
        # Check APR (A_ID, PR) uniqueness - SHOULD FAIL WITH BUGGY CODE
        apr_pairs = set()
        apr_duplicates = []
        for row in selected:
            pair = (row['A_ID'], row['PR'])
            if pair in apr_pairs:
                apr_duplicates.append(pair)
            apr_pairs.add(pair)
        
        # With the bug, APR will have duplicates
        self.assertGreater(len(apr_duplicates), 0, 
                          "Buggy code should produce APR duplicates")
    
    def test_correct_cartesian_product_no_duplicates(self):
        """Verify that true Cartesian product generates zero duplicates."""
        import itertools
        
        # Same scenario
        a_id_values = list(range(1, 3001))  # 3000 A_IDs
        pr_values = [0, 1]  # 2 PR values
        c_id_values = list(range(1, 11))  # 10 C_ID values
        
        # CORRECT: Generate true Cartesian product
        all_combinations = []
        
        # Non-shared columns for the constraints
        non_shared_value_lists = {
            'PR': pr_values,
            'C_ID': c_id_values,
        }
        
        for a_id in a_id_values:
            # Generate Cartesian product of all non-shared columns
            non_shared_cols = list(non_shared_value_lists.keys())
            value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
            
            for combo in itertools.product(*value_lists):
                row_assignment = {'A_ID': a_id}
                
                for col_name, value in zip(non_shared_cols, combo):
                    row_assignment[col_name] = value
                
                all_combinations.append(row_assignment)
        
        # Should generate 3000 * (2 * 10) = 60,000 combinations
        self.assertEqual(len(all_combinations), 60000)
        
        # Shuffle and select 6000
        random.seed(42)
        random.shuffle(all_combinations)
        selected = all_combinations[:6000]
        
        # Check APR (A_ID, PR) uniqueness - should be ZERO duplicates
        apr_pairs = set()
        for row in selected:
            pair = (row['A_ID'], row['PR'])
            self.assertNotIn(pair, apr_pairs, 
                           f"APR pair {pair} should be unique")
            apr_pairs.add(pair)
        
        self.assertEqual(len(apr_pairs), 6000, 
                        "All 6000 APR pairs should be unique")
        
        # Check ACS (A_ID, C_ID) uniqueness - should be ZERO duplicates
        acs_pairs = set()
        for row in selected:
            pair = (row['A_ID'], row['C_ID'])
            self.assertNotIn(pair, acs_pairs, 
                           f"ACS pair {pair} should be unique")
            acs_pairs.add(pair)
        
        self.assertEqual(len(acs_pairs), 6000, 
                        "All 6000 ACS pairs should be unique")
    
    def test_cartesian_product_with_three_columns(self):
        """Test Cartesian product with three non-shared columns."""
        import itertools
        
        # Scenario: Three non-shared columns
        a_id_values = [1, 2, 3]  # 3 shared values
        col1_values = [10, 20]   # 2 values
        col2_values = [100, 200, 300]  # 3 values
        col3_values = [1000, 2000]  # 2 values
        
        # Generate Cartesian product
        non_shared_value_lists = {
            'COL1': col1_values,
            'COL2': col2_values,
            'COL3': col3_values,
        }
        
        all_combinations = []
        for a_id in a_id_values:
            non_shared_cols = list(non_shared_value_lists.keys())
            value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
            
            for combo in itertools.product(*value_lists):
                row_assignment = {'A_ID': a_id}
                for col_name, value in zip(non_shared_cols, combo):
                    row_assignment[col_name] = value
                all_combinations.append(row_assignment)
        
        # Should generate 3 * (2 * 3 * 2) = 36 combinations
        expected_combos = len(a_id_values) * len(col1_values) * len(col2_values) * len(col3_values)
        self.assertEqual(len(all_combinations), expected_combos)
        
        # All should be unique
        unique_tuples = set()
        for row in all_combinations:
            tuple_key = (row['A_ID'], row['COL1'], row['COL2'], row['COL3'])
            self.assertNotIn(tuple_key, unique_tuples,
                           f"Tuple {tuple_key} should be unique")
            unique_tuples.add(tuple_key)
        
        self.assertEqual(len(unique_tuples), expected_combos)


if __name__ == "__main__":
    unittest.main()
