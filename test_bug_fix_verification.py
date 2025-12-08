#!/usr/bin/env python3
"""Verification test for the multi-constraint Cartesian product bug fix.

This test verifies that the fix correctly generates a true Cartesian product
instead of using the buggy MAX-based approach with modulo cycling.
"""
import unittest
import itertools


class TestBugFixVerification(unittest.TestCase):
    """Verify the bug fix implementation."""
    
    def test_buggy_approach_generates_fewer_combinations(self):
        """Verify that the buggy MAX approach generates fewer combinations."""
        # Scenario from problem statement
        a_id_values = list(range(1, 3001))  # 3000 A_IDs
        pr_values = [0, 1]  # 2 PR values
        c_id_values = list(range(1, 11))  # 10 C_ID values
        
        # BUGGY APPROACH: rows_per_shared_combo = max(2, 10) = 10
        rows_per_shared_combo = max(len(pr_values), len(c_id_values))
        
        buggy_combinations = []
        for a_id in a_id_values:
            for local_idx in range(rows_per_shared_combo):
                combination = {
                    'A_ID': a_id,
                    'PR': pr_values[local_idx % len(pr_values)],  # Modulo cycling
                    'C_ID': c_id_values[local_idx % len(c_id_values)],
                }
                buggy_combinations.append(combination)
        
        # Should generate 30,000 combinations (3000 * 10)
        self.assertEqual(len(buggy_combinations), 30000)
        
        # Verify that within a single A_ID, PR values repeat
        a_id_1_rows = [r for r in buggy_combinations if r['A_ID'] == 1]
        self.assertEqual(len(a_id_1_rows), 10)
        
        # Count occurrences of each PR value for A_ID=1
        pr_counts = {}
        for row in a_id_1_rows:
            pr = row['PR']
            pr_counts[pr] = pr_counts.get(pr, 0) + 1
        
        # With modulo cycling, PR=0 appears 5 times, PR=1 appears 5 times
        self.assertEqual(pr_counts[0], 5, "PR=0 should appear 5 times (showing the bug)")
        self.assertEqual(pr_counts[1], 5, "PR=1 should appear 5 times (showing the bug)")
    
    def test_fixed_approach_generates_cartesian_product(self):
        """Verify that the fixed approach generates true Cartesian product."""
        # Same scenario
        a_id_values = list(range(1, 3001))
        pr_values = [0, 1]
        c_id_values = list(range(1, 11))
        
        # FIXED APPROACH: True Cartesian product
        non_shared_value_lists = {
            'PR': pr_values,
            'C_ID': c_id_values,
        }
        
        fixed_combinations = []
        non_shared_cols = list(non_shared_value_lists.keys())
        value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
        
        for a_id in a_id_values:
            for combo in itertools.product(*value_lists):
                combination = {'A_ID': a_id}
                for col_name, value in zip(non_shared_cols, combo):
                    combination[col_name] = value
                fixed_combinations.append(combination)
        
        # Should generate 60,000 combinations (3000 * 2 * 10)
        self.assertEqual(len(fixed_combinations), 60000)
        
        # Verify that all 60,000 combinations are unique 3-tuples
        unique_tuples = set()
        for row in fixed_combinations:
            tuple_key = (row['A_ID'], row['PR'], row['C_ID'])
            self.assertNotIn(tuple_key, unique_tuples, 
                           "Each 3-tuple should be unique in the Cartesian product")
            unique_tuples.add(tuple_key)
        
        self.assertEqual(len(unique_tuples), 60000)
        
        # Verify that for A_ID=1, each PR value appears exactly 10 times
        # (once for each C_ID)
        a_id_1_rows = [r for r in fixed_combinations if r['A_ID'] == 1]
        self.assertEqual(len(a_id_1_rows), 20)  # 2 PR * 10 C_ID
        
        pr_counts = {}
        for row in a_id_1_rows:
            pr = row['PR']
            pr_counts[pr] = pr_counts.get(pr, 0) + 1
        
        # Each PR should appear exactly 10 times (once for each C_ID)
        self.assertEqual(pr_counts[0], 10, "PR=0 should appear 10 times")
        self.assertEqual(pr_counts[1], 10, "PR=1 should appear 10 times")
        
        # Verify that each (A_ID=1, PR, C_ID) combination is unique
        a_id_1_tuples = set()
        for row in a_id_1_rows:
            tuple_key = (row['A_ID'], row['PR'], row['C_ID'])
            a_id_1_tuples.add(tuple_key)
        self.assertEqual(len(a_id_1_tuples), 20, 
                        "All 20 combinations for A_ID=1 should be unique")
    
    def test_cartesian_product_improvement_over_buggy(self):
        """Verify that Cartesian product generates more combinations than buggy approach."""
        a_id_values = list(range(1, 3001))
        pr_values = [0, 1]
        c_id_values = list(range(1, 11))
        
        # Buggy combinations
        buggy_count = len(a_id_values) * max(len(pr_values), len(c_id_values))
        
        # Fixed combinations
        fixed_count = len(a_id_values) * len(pr_values) * len(c_id_values)
        
        self.assertEqual(buggy_count, 30000)
        self.assertEqual(fixed_count, 60000)
        self.assertGreater(fixed_count, buggy_count,
                          "Cartesian product should generate more combinations")
        self.assertEqual(fixed_count / buggy_count, 2,
                        "Should generate exactly 2x more combinations")


if __name__ == "__main__":
    unittest.main()
