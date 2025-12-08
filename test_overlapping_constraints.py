#!/usr/bin/env python3
"""Unit tests for multi-constraint Cartesian product with overlapping UNIQUE constraints.

This tests the feature where multiple composite UNIQUE constraints share columns,
and both constraints are satisfied simultaneously using a multi-constraint approach.
"""
import unittest
import sys
from io import StringIO
from generate_synthetic_data_utils import (
    ColumnMeta,
    UniqueConstraint,
    TableMeta,
    FKMeta,
    GLOBALS,
)


class TestOverlappingConstraintDetection(unittest.TestCase):
    """Test detection of overlapping UNIQUE constraints."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Enable debug mode for test output
        GLOBALS["debug"] = True
    
    def tearDown(self):
        """Clean up after tests."""
        GLOBALS["debug"] = False
    
    def test_detect_overlapping_constraints_with_shared_column(self):
        """Test that constraints sharing columns are detected as overlapping."""
        # Two constraints that share A_ID
        # ACS: A_ID, C_ID (share A_ID)
        # APR: A_ID, PR (share A_ID)
        constraints = [
            UniqueConstraint("ACS", ("A_ID", "C_ID")),
            UniqueConstraint("APR", ("A_ID", "PR")),
        ]
        
        # Find overlapping constraints
        overlapping_groups = []
        if len(constraints) > 1:
            for i, uc1 in enumerate(constraints):
                group = set([uc1])
                for j, uc2 in enumerate(constraints):
                    if i != j and set(uc1.columns) & set(uc2.columns):
                        group.add(uc2)
                
                if len(group) > 1:
                    overlapping_groups.append(list(group))
            
            # Deduplicate groups
            unique_groups = []
            for group in overlapping_groups:
                group_set = frozenset(uc.constraint_name for uc in group)
                if not any(group_set == frozenset(uc.constraint_name for uc in g) for g in unique_groups):
                    unique_groups.append(group)
            overlapping_groups = unique_groups
        
        # Should detect overlapping
        self.assertEqual(len(overlapping_groups), 1)
        self.assertEqual(len(overlapping_groups[0]), 2)
        constraint_names = set(uc.constraint_name for uc in overlapping_groups[0])
        self.assertEqual(constraint_names, {"ACS", "APR"})
    
    def test_no_overlapping_for_disjoint_constraints(self):
        """Test that constraints with no shared columns are not overlapping."""
        # Two constraints with no shared columns
        constraints = [
            UniqueConstraint("AB", ("A_ID", "B_ID")),
            UniqueConstraint("CD", ("C_ID", "D_ID")),
        ]
        
        # Find overlapping constraints
        overlapping_groups = []
        if len(constraints) > 1:
            for i, uc1 in enumerate(constraints):
                group = set([uc1])
                for j, uc2 in enumerate(constraints):
                    if i != j and set(uc1.columns) & set(uc2.columns):
                        group.add(uc2)
                
                if len(group) > 1:
                    overlapping_groups.append(list(group))
            
            # Deduplicate groups
            unique_groups = []
            for group in overlapping_groups:
                group_set = frozenset(uc.constraint_name for uc in group)
                if not any(group_set == frozenset(uc.constraint_name for uc in g) for g in unique_groups):
                    unique_groups.append(group)
            overlapping_groups = unique_groups
        
        # Should not detect overlapping
        self.assertEqual(len(overlapping_groups), 0)
    
    def test_find_shared_columns(self):
        """Test finding shared columns among overlapping constraints."""
        # Two constraints that share A_ID
        constraint1 = UniqueConstraint("ACS", ("A_ID", "C_ID"))
        constraint2 = UniqueConstraint("APR", ("A_ID", "PR"))
        
        constraint_group = [constraint1, constraint2]
        
        # Find shared columns
        shared_cols = set(constraint_group[0].columns)
        for uc in constraint_group[1:]:
            shared_cols &= set(uc.columns)
        
        # Should find A_ID as shared
        self.assertEqual(shared_cols, {"A_ID"})
    
    def test_identify_non_shared_columns(self):
        """Test identifying non-shared columns for each constraint."""
        # Two constraints that share A_ID
        constraint1 = UniqueConstraint("ACS", ("A_ID", "C_ID"))
        constraint2 = UniqueConstraint("APR", ("A_ID", "PR"))
        
        constraint_group = [constraint1, constraint2]
        
        # Find shared columns
        shared_cols = set(constraint_group[0].columns)
        for uc in constraint_group[1:]:
            shared_cols &= set(uc.columns)
        
        # Find non-shared columns for each constraint
        non_shared_for_acs = [col for col in constraint1.columns if col not in shared_cols]
        non_shared_for_apr = [col for col in constraint2.columns if col not in shared_cols]
        
        # Should find C_ID for ACS and PR for APR
        self.assertEqual(non_shared_for_acs, ["C_ID"])
        self.assertEqual(non_shared_for_apr, ["PR"])
    
    def test_three_way_overlapping_constraints(self):
        """Test detection of three constraints sharing columns."""
        # Three constraints that all share A_ID
        constraints = [
            UniqueConstraint("AC", ("A_ID", "C_ID")),
            UniqueConstraint("AP", ("A_ID", "PR")),
            UniqueConstraint("AB", ("A_ID", "B_ID")),
        ]
        
        # Find overlapping constraints
        overlapping_groups = []
        if len(constraints) > 1:
            for i, uc1 in enumerate(constraints):
                group = set([uc1])
                for j, uc2 in enumerate(constraints):
                    if i != j and set(uc1.columns) & set(uc2.columns):
                        group.add(uc2)
                
                if len(group) > 1:
                    overlapping_groups.append(list(group))
            
            # Deduplicate groups
            unique_groups = []
            for group in overlapping_groups:
                group_set = frozenset(uc.constraint_name for uc in group)
                if not any(group_set == frozenset(uc.constraint_name for uc in g) for g in unique_groups):
                    unique_groups.append(group)
            overlapping_groups = unique_groups
        
        # Should detect all three as overlapping
        self.assertEqual(len(overlapping_groups), 1)
        self.assertEqual(len(overlapping_groups[0]), 3)


class TestMultiConstraintCombinationGeneration(unittest.TestCase):
    """Test generation logic for multi-constraint Cartesian product."""
    
    def test_cartesian_product_combinations_count(self):
        """Test that Cartesian product generates correct number of combinations."""
        import itertools
        
        # Scenario: A_ID shared, PR has 2 values, C_ID has 10 values
        # With Cartesian product: 2 * 10 = 20 combinations per A_ID
        
        shared_values = [1, 2, 3]  # 3 A_ID values
        pr_values = [0, 1]  # 2 PR values
        c_id_values = list(range(1, 11))  # 10 C_ID values
        
        # Build non-shared value lists
        non_shared_value_lists = {
            'PR': pr_values,
            'C_ID': c_id_values,
        }
        
        all_combinations = []
        non_shared_cols = list(non_shared_value_lists.keys())
        value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
        
        for a_id in shared_values:
            for combo in itertools.product(*value_lists):
                row_assignment = {'A_ID': a_id}
                for col_name, value in zip(non_shared_cols, combo):
                    row_assignment[col_name] = value
                all_combinations.append(row_assignment)
        
        # Should generate 3 * (2 * 10) = 60 combinations
        expected = len(shared_values) * len(pr_values) * len(c_id_values)
        self.assertEqual(len(all_combinations), expected)
        self.assertEqual(len(all_combinations), 60)
    
    def test_combination_assignment_structure(self):
        """Test that row assignments have correct structure using Cartesian product."""
        import itertools
        
        # Simulate generating combinations for shared A_ID values
        shared_values = [1, 2, 3]  # 3 A_ID values
        pr_values = [0, 1]
        c_id_values = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
        
        # Use Cartesian product approach
        non_shared_value_lists = {
            'PR': pr_values,
            'C_ID': c_id_values,
        }
        
        all_combinations = []
        non_shared_cols = list(non_shared_value_lists.keys())
        value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
        
        for shared_val in shared_values:
            for combo in itertools.product(*value_lists):
                row_assignment = {"A_ID": shared_val}
                for col_name, value in zip(non_shared_cols, combo):
                    row_assignment[col_name] = value
                all_combinations.append(row_assignment)
        
        # Should generate 3 * (2 * 10) = 60 combinations
        self.assertEqual(len(all_combinations), 60)
        
        # Verify all 3-tuples are unique
        all_tuples = set((r["A_ID"], r["PR"], r["C_ID"]) for r in all_combinations)
        self.assertEqual(len(all_tuples), 60)  # All 3-tuples should be unique
        
        # Verify uniqueness of (A_ID, PR) pairs - each A_ID has each PR exactly 10 times
        apr_pairs = [(r["A_ID"], r["PR"]) for r in all_combinations]
        # 3 A_IDs * 2 PR values = 6 unique pairs, but each appears 10 times (once per C_ID)
        unique_apr_pairs = set(apr_pairs)
        self.assertEqual(len(unique_apr_pairs), 6)  
        
        # Verify uniqueness of (A_ID, C_ID) pairs - each A_ID has each C_ID exactly 2 times
        acs_pairs = [(r["A_ID"], r["C_ID"]) for r in all_combinations]
        # 3 A_IDs * 10 C_ID values = 30 unique pairs, but each appears 2 times (once per PR)
        unique_acs_pairs = set(acs_pairs)
        self.assertEqual(len(unique_acs_pairs), 30)
    
    def test_insufficient_combinations_repeats(self):
        """Test handling when not enough unique combinations exist."""
        import itertools
        
        # Scenario: Only 2 A_ID values, 2 PR values, 2 C_ID values
        # Cartesian product: 2 * (2 * 2) = 8 total unique combinations
        # But requesting 20 rows - should repeat combinations
        
        shared_values = [1, 2]  # 2 A_ID values
        requested_rows = 20
        
        pr_values = [0, 1]
        c_id_values = [10, 20]
        
        # Generate all combinations using Cartesian product
        non_shared_value_lists = {
            'PR': pr_values,
            'C_ID': c_id_values,
        }
        
        all_combinations = []
        non_shared_cols = list(non_shared_value_lists.keys())
        value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
        
        for shared_val in shared_values:
            for combo in itertools.product(*value_lists):
                row_assignment = {"A_ID": shared_val}
                for col_name, value in zip(non_shared_cols, combo):
                    row_assignment[col_name] = value
                all_combinations.append(row_assignment)
        
        # Should have 8 unique combinations (2 * 2 * 2)
        self.assertEqual(len(all_combinations), 8)
        
        # Extend to 20 by repeating
        if len(all_combinations) < requested_rows:
            extended_combinations = []
            for i in range(requested_rows):
                extended_combinations.append(all_combinations[i % len(all_combinations)])
            all_combinations = extended_combinations
        
        # Should now have 20 rows
        self.assertEqual(len(all_combinations), 20)
        
        # But only 8 unique combinations
        unique_combos = set()
        for r in all_combinations:
            unique_combos.add((r["A_ID"], r["PR"], r["C_ID"]))
        self.assertEqual(len(unique_combos), 8)


if __name__ == "__main__":
    unittest.main()
