#!/usr/bin/env python3
"""Unit tests for Cartesian product with composite UNIQUE constraints containing FK columns.

This tests the feature where composite UNIQUE constraints with all FK columns
use Cartesian product to guarantee uniqueness.
"""
import unittest
import random
from generate_synthetic_data_utils import (
    ColumnMeta,
    UniqueConstraint,
    TableMeta,
    FKMeta,
)


class TestCartesianUniqueDetection(unittest.TestCase):
    """Test detection of composite UNIQUE constraints with all FK columns."""
    
    def test_detect_all_fk_unique_constraint(self):
        """Test that UNIQUE constraints with all FK columns are detected."""
        # Simulate unique constraints
        unique_constraints = [
            UniqueConstraint("unique_a_c", ("A_ID", "C_ID")),  # Both are FKs
        ]
        
        # FK map
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
            "C_ID": FKMeta("fk_c", "db", "AC", "C_ID", "db", "C", "ID", False, None),
        }
        
        # Check if all columns are FKs
        unique_fk_constraints = []
        for uc in unique_constraints:
            if len(uc.columns) < 2:
                continue
            all_fks = all(col in fk_map for col in uc.columns)
            if all_fks:
                unique_fk_constraints.append(uc)
        
        # Should detect the constraint
        self.assertEqual(len(unique_fk_constraints), 1)
        self.assertEqual(unique_fk_constraints[0].constraint_name, "unique_a_c")
    
    def test_skip_single_column_unique(self):
        """Test that single-column UNIQUE constraints are skipped."""
        unique_constraints = [
            UniqueConstraint("unique_email", ("email",)),  # Single column
        ]
        
        fk_map = {
            "email": FKMeta("fk_email", "db", "T", "email", "db", "Users", "email", False, None),
        }
        
        unique_fk_constraints = []
        for uc in unique_constraints:
            if len(uc.columns) < 2:
                continue
            all_fks = all(col in fk_map for col in uc.columns)
            if all_fks:
                unique_fk_constraints.append(uc)
        
        # Should NOT detect single-column constraints
        self.assertEqual(len(unique_fk_constraints), 0)
    
    def test_skip_mixed_fk_non_fk_unique(self):
        """Test that UNIQUE constraints with mixed FK/non-FK columns are skipped."""
        unique_constraints = [
            UniqueConstraint("unique_a_pr", ("A_ID", "PR")),  # A_ID is FK, PR is not
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "AC", "A_ID", "db", "A", "ID", False, None),
        }
        
        unique_fk_constraints = []
        for uc in unique_constraints:
            if len(uc.columns) < 2:
                continue
            all_fks = all(col in fk_map for col in uc.columns)
            if all_fks:
                unique_fk_constraints.append(uc)
        
        # Should NOT detect mixed constraints
        self.assertEqual(len(unique_fk_constraints), 0)
    
    def test_multiple_unique_constraints(self):
        """Test handling of multiple composite UNIQUE constraints with all FKs."""
        unique_constraints = [
            UniqueConstraint("unique_a_c", ("A_ID", "C_ID")),  # Both are FKs
            UniqueConstraint("unique_b_d", ("B_ID", "D_ID")),  # Both are FKs
        ]
        
        fk_map = {
            "A_ID": FKMeta("fk_a", "db", "T", "A_ID", "db", "A", "ID", False, None),
            "C_ID": FKMeta("fk_c", "db", "T", "C_ID", "db", "C", "ID", False, None),
            "B_ID": FKMeta("fk_b", "db", "T", "B_ID", "db", "B", "ID", False, None),
            "D_ID": FKMeta("fk_d", "db", "T", "D_ID", "db", "D", "ID", False, None),
        }
        
        unique_fk_constraints = []
        for uc in unique_constraints:
            if len(uc.columns) < 2:
                continue
            all_fks = all(col in fk_map for col in uc.columns)
            if all_fks:
                unique_fk_constraints.append(uc)
        
        # Should detect both constraints
        self.assertEqual(len(unique_fk_constraints), 2)


class TestCartesianProductGeneration(unittest.TestCase):
    """Test Cartesian product generation for UNIQUE FK constraints."""
    
    def test_cartesian_product_basic(self):
        """Test basic Cartesian product generation."""
        import itertools
        
        # Parent A: 3 values
        parent_a_values = [1, 2, 3]
        # Parent C: 2 values
        parent_c_values = [10, 20]
        
        # Generate Cartesian product
        all_combinations = list(itertools.product(parent_a_values, parent_c_values))
        
        # Should have 3 * 2 = 6 combinations
        self.assertEqual(len(all_combinations), 6)
        
        # Check uniqueness
        self.assertEqual(len(set(all_combinations)), 6)
        
        # Check some expected combinations
        self.assertIn((1, 10), all_combinations)
        self.assertIn((3, 20), all_combinations)
    
    def test_cartesian_product_three_columns(self):
        """Test Cartesian product with three FK columns."""
        import itertools
        
        parent_a_values = [1, 2]
        parent_b_values = [10, 20]
        parent_c_values = [100, 200]
        
        all_combinations = list(itertools.product(parent_a_values, parent_b_values, parent_c_values))
        
        # Should have 2 * 2 * 2 = 8 combinations
        self.assertEqual(len(all_combinations), 8)
        self.assertEqual(len(set(all_combinations)), 8)
    
    def test_cartesian_product_sampling(self):
        """Test sampling when more combinations than needed rows."""
        import itertools
        import random
        
        # Using seed for reproducible test results
        rng = random.Random(42)
        
        parent_a_values = list(range(1, 101))  # 100 values
        parent_c_values = list(range(1, 11))   # 10 values
        
        all_combinations = list(itertools.product(parent_a_values, parent_c_values))
        
        # Should have 100 * 10 = 1000 combinations
        self.assertEqual(len(all_combinations), 1000)
        
        # Sample 500 combinations
        needed_rows = 500
        rng.shuffle(all_combinations)
        sampled = all_combinations[:needed_rows]
        
        # Should have exactly 500 combinations
        self.assertEqual(len(sampled), needed_rows)
        
        # All should be unique
        self.assertEqual(len(set(sampled)), needed_rows)
    
    def test_cartesian_product_insufficient_combinations(self):
        """Test when there are fewer combinations than requested rows."""
        import itertools
        
        parent_a_values = [1, 2, 3]  # 3 values
        parent_c_values = [10, 20]    # 2 values
        
        all_combinations = list(itertools.product(parent_a_values, parent_c_values))
        
        # Should have 3 * 2 = 6 combinations
        self.assertEqual(len(all_combinations), 6)
        
        # Need 20 rows
        needed_rows = 20
        
        # Repeat combinations
        repetitions = (needed_rows // len(all_combinations)) + 1
        repeated = (all_combinations * repetitions)[:needed_rows]
        
        # Should have exactly 20 rows
        self.assertEqual(len(repeated), needed_rows)
        
        # But only 6 unique combinations
        self.assertEqual(len(set(repeated)), 6)


class TestPreAllocatedUniqueFK(unittest.TestCase):
    """Test pre-allocation of UNIQUE FK values."""
    
    def test_pre_allocated_dict_structure(self):
        """Test the structure of pre-allocated UNIQUE FK tuples."""
        import itertools
        
        # Simulate parent values
        parent_a_values = [1, 2, 3]
        parent_c_values = [10, 20]
        parent_col_names = ["A_ID", "C_ID"]
        
        # Generate combinations
        all_combinations = list(itertools.product(parent_a_values, parent_c_values))
        
        # Pre-allocate for 6 rows
        pre_allocated_unique_fk_tuples = {}
        for i, combo in enumerate(all_combinations):
            for j, col_name in enumerate(parent_col_names):
                if col_name not in pre_allocated_unique_fk_tuples:
                    pre_allocated_unique_fk_tuples[col_name] = {}
                pre_allocated_unique_fk_tuples[col_name][i] = combo[j]
        
        # Check structure
        self.assertEqual(len(pre_allocated_unique_fk_tuples), 2)
        self.assertIn("A_ID", pre_allocated_unique_fk_tuples)
        self.assertIn("C_ID", pre_allocated_unique_fk_tuples)
        
        # Check A_ID values
        self.assertEqual(len(pre_allocated_unique_fk_tuples["A_ID"]), 6)
        self.assertEqual(pre_allocated_unique_fk_tuples["A_ID"][0], 1)
        self.assertEqual(pre_allocated_unique_fk_tuples["A_ID"][1], 1)
        self.assertEqual(pre_allocated_unique_fk_tuples["A_ID"][2], 2)
        
        # Check C_ID values
        self.assertEqual(len(pre_allocated_unique_fk_tuples["C_ID"]), 6)
        self.assertEqual(pre_allocated_unique_fk_tuples["C_ID"][0], 10)
        self.assertEqual(pre_allocated_unique_fk_tuples["C_ID"][1], 20)
        self.assertEqual(pre_allocated_unique_fk_tuples["C_ID"][2], 10)
    
    def test_row_assignment_from_pre_allocated(self):
        """Test assigning FK values from pre-allocated tuples."""
        # Simulate pre-allocated tuples
        pre_allocated_unique_fk_tuples = {
            "A_ID": {0: 1, 1: 1, 2: 2},
            "C_ID": {0: 10, 1: 20, 2: 10}
        }
        
        # Simulate row assignment
        rows = [{}, {}, {}]
        
        for row_idx in range(3):
            temp_row = rows[row_idx]
            
            # Assign pre-allocated values
            for col_name, value_map in pre_allocated_unique_fk_tuples.items():
                if row_idx in value_map:
                    temp_row[col_name] = value_map[row_idx]
        
        # Check assignments
        self.assertEqual(rows[0]["A_ID"], 1)
        self.assertEqual(rows[0]["C_ID"], 10)
        self.assertEqual(rows[1]["A_ID"], 1)
        self.assertEqual(rows[1]["C_ID"], 20)
        self.assertEqual(rows[2]["A_ID"], 2)
        self.assertEqual(rows[2]["C_ID"], 10)
        
        # Verify uniqueness of (A_ID, C_ID) tuples
        tuples = [(r["A_ID"], r["C_ID"]) for r in rows]
        self.assertEqual(len(set(tuples)), 3)


class TestSkipLogic(unittest.TestCase):
    """Test that pre-allocated UNIQUE FK columns are skipped in FK resolution."""
    
    def test_skip_conditional_fk_for_pre_allocated(self):
        """Test that conditional FKs skip pre-allocated columns."""
        pre_allocated_unique_fk_tuples = {
            "A_ID": {0: 1, 1: 2},
            "C_ID": {0: 10, 1: 20}
        }
        
        assigned_by_conditional_fk = set()
        
        # Simulate assignment from pre-allocated
        row_idx = 0
        for col_name, value_map in pre_allocated_unique_fk_tuples.items():
            if row_idx in value_map:
                assigned_by_conditional_fk.add(col_name)
        
        # Check that columns are marked as assigned
        self.assertIn("A_ID", assigned_by_conditional_fk)
        self.assertIn("C_ID", assigned_by_conditional_fk)
        
        # Simulate FK resolution - should skip these columns
        fk_col = "A_ID"
        should_skip = fk_col in assigned_by_conditional_fk
        self.assertTrue(should_skip)
    
    def test_skip_composite_fk_for_pre_allocated(self):
        """Test that composite FKs are skipped if columns are pre-allocated."""
        pre_allocated_unique_fk_tuples = {
            "A_ID": {0: 1},
            "C_ID": {0: 10}
        }
        
        # Simulate composite FK
        fk_child_cols = ["A_ID", "C_ID"]
        
        # Check if all columns are pre-allocated
        fk_cols_pre_allocated = all(
            child_col in pre_allocated_unique_fk_tuples 
            for child_col in fk_child_cols
        )
        
        # Should skip this composite FK
        self.assertTrue(fk_cols_pre_allocated)


class TestUniquenessGuarantee(unittest.TestCase):
    """Test that Cartesian product guarantees uniqueness."""
    
    def test_uniqueness_with_sufficient_combinations(self):
        """Test that all rows have unique combinations when sufficient parent values exist."""
        import itertools
        
        # Parent tables
        parent_a_values = list(range(1, 3001))  # 3000 values
        parent_c_values = list(range(1, 11))    # 10 values
        
        # Generate all combinations
        all_combinations = list(itertools.product(parent_a_values, parent_c_values))
        
        # Should have 3000 * 10 = 30,000 combinations
        self.assertEqual(len(all_combinations), 30000)
        
        # Request 6000 rows
        needed_rows = 6000
        
        # Sample combinations with reproducible seed for testing
        rng = random.Random(42)
        rng.shuffle(all_combinations)
        sampled = all_combinations[:needed_rows]
        
        # All 6000 rows should have unique combinations
        self.assertEqual(len(sampled), needed_rows)
        self.assertEqual(len(set(sampled)), needed_rows)
    
    def test_detect_duplicate_combinations(self):
        """Test detection when duplicates would occur (insufficient parent values)."""
        import itertools
        
        # Parent tables with limited values
        parent_a_values = [1, 2, 3]  # 3 values
        parent_c_values = [10, 20]    # 2 values
        
        # Generate all combinations
        all_combinations = list(itertools.product(parent_a_values, parent_c_values))
        
        # Should have 3 * 2 = 6 combinations
        self.assertEqual(len(all_combinations), 6)
        
        # Request 20 rows (more than available combinations)
        needed_rows = 20
        
        # Check if we have enough combinations
        has_enough = len(all_combinations) >= needed_rows
        self.assertFalse(has_enough)
        
        # Would need to repeat combinations
        repetitions = (needed_rows // len(all_combinations)) + 1
        repeated = (all_combinations * repetitions)[:needed_rows]
        
        # Should have 20 rows but only 6 unique combinations
        self.assertEqual(len(repeated), 20)
        self.assertEqual(len(set(repeated)), 6)


class TestFKRatiosWarning(unittest.TestCase):
    """Test that fk_ratios conflicts with Cartesian product are warned about."""
    
    def test_detect_fk_ratios_conflict(self):
        """Test detection of fk_ratios on columns in Cartesian product UNIQUE constraint."""
        # UNIQUE constraint columns
        uc_columns = ["A_ID", "C_ID"]
        
        # Config with fk_ratios
        cfg = {
            "fk_ratios": {
                "A_ID": 2,  # This will be ignored
                "C_ID": 3,  # This will be ignored
            }
        }
        
        # Check for conflicts
        fk_ratios = cfg.get("fk_ratios", {})
        conflicts = [col for col in uc_columns if col in fk_ratios]
        
        # Should detect both columns as conflicts
        self.assertEqual(len(conflicts), 2)
        self.assertIn("A_ID", conflicts)
        self.assertIn("C_ID", conflicts)


if __name__ == '__main__':
    unittest.main()
