#!/usr/bin/env python3
"""Test for stratified sampling fix in multi-constraint Cartesian product.

This test validates that the stratified sampling approach ensures all shared values
(e.g., A_IDs) appear the correct number of times, eliminating duplicates in
overlapping UNIQUE constraints.
"""
import unittest
import random
from collections import defaultdict


class TestStratifiedSampling(unittest.TestCase):
    """Test stratified sampling for multi-constraint Cartesian product."""
    
    def test_random_sampling_causes_imbalance(self):
        """Demonstrate that random sampling causes uneven distribution of shared values."""
        # Scenario from problem statement:
        # - 3000 A_ID values (shared column)
        # - 2 PR values (non-shared)
        # - 10 C_ID values (non-shared)
        # - 60,000 total combinations
        # - Requesting 6,000 rows
        
        import itertools
        
        a_id_values = list(range(1, 3001))  # 3000 A_IDs
        pr_values = [0, 1]  # 2 PR values
        c_id_values = list(range(1, 11))  # 10 C_ID values
        
        # Generate all combinations using Cartesian product
        all_combinations = []
        non_shared_value_lists = {'PR': pr_values, 'C_ID': c_id_values}
        non_shared_cols = list(non_shared_value_lists.keys())
        value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
        
        for a_id in a_id_values:
            for combo in itertools.product(*value_lists):
                combination = {'A_ID': a_id}
                for col_name, value in zip(non_shared_cols, combo):
                    combination[col_name] = value
                all_combinations.append(combination)
        
        self.assertEqual(len(all_combinations), 60000)
        
        # BUGGY APPROACH: Random shuffle and take first 6000
        random.seed(42)
        random.shuffle(all_combinations)
        selected = all_combinations[:6000]
        
        # Count how many times each A_ID appears
        a_id_counts = defaultdict(int)
        for row in selected:
            a_id_counts[row['A_ID']] += 1
        
        # With random sampling, distribution is uneven
        unique_a_ids = len(a_id_counts)
        
        # Should be 3000 A_IDs, but will be less due to random sampling
        self.assertLess(unique_a_ids, 3000, 
                       "Random sampling should result in missing A_IDs")
        
        # Check for duplicates in APR constraint
        apr_pairs = set()
        apr_duplicates = 0
        for row in selected:
            pair = (row['A_ID'], row['PR'])
            if pair in apr_pairs:
                apr_duplicates += 1
            apr_pairs.add(pair)
        
        # Random sampling causes duplicates
        self.assertGreater(apr_duplicates, 0,
                          "Random sampling should cause APR duplicates")
        
        print(f"\nRandom sampling results:")
        print(f"  Unique A_IDs: {unique_a_ids} (should be 3000)")
        print(f"  APR duplicates: {apr_duplicates} (should be 0)")
    
    def test_stratified_sampling_ensures_balance(self):
        """Verify that stratified sampling with smart diversity ensures all shared values appear correctly."""
        import itertools
        
        a_id_values = list(range(1, 3001))  # 3000 A_IDs
        pr_values = [0, 1]  # 2 PR values
        c_id_values = list(range(1, 11))  # 10 C_ID values
        
        # Generate all combinations using Cartesian product
        all_combinations = []
        non_shared_value_lists = {'PR': pr_values, 'C_ID': c_id_values}
        non_shared_cols = list(non_shared_value_lists.keys())
        value_lists = [non_shared_value_lists[col] for col in non_shared_cols]
        
        for a_id in a_id_values:
            for combo in itertools.product(*value_lists):
                combination = {'A_ID': a_id}
                for col_name, value in zip(non_shared_cols, combo):
                    combination[col_name] = value
                all_combinations.append(combination)
        
        self.assertEqual(len(all_combinations), 60000)
        
        # FIXED APPROACH: Smart Stratified sampling with diversity
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
        
        self.assertEqual(rows_per_shared_val, 2, "Should be 2 rows per A_ID")
        self.assertEqual(remainder, 0, "Should divide evenly")
        
        # Select stratified with smart diversity
        random.seed(42)
        selected_combinations = []
        shared_values_list = list(shared_values)
        random.shuffle(shared_values_list)  # Randomize order
        
        constraint_non_shared_cols = ['PR', 'C_ID']
        
        for idx, shared_val in enumerate(shared_values_list):
            available = combos_by_shared_val[shared_val]
            num_rows_for_this_val = rows_per_shared_val + (1 if idx < remainder else 0)
            
            # SMART SELECTION: Ensure diversity in all constraint columns
            selected = []
            smart_selection_succeeded = False
            
            if num_rows_for_this_val > 1 and num_rows_for_this_val <= 10 and len(constraint_non_shared_cols) >= 1:
                # Try to ensure diversity in the first constraint column
                first_col = constraint_non_shared_cols[0]
                by_first_col = defaultdict(list)
                for combo in available:
                    by_first_col[combo[first_col]].append(combo)
                
                first_col_values = list(by_first_col.keys())
                
                # If we have enough distinct values in first column, select one from each
                if len(first_col_values) >= num_rows_for_this_val:
                    random.shuffle(first_col_values)
                    
                    # Now ensure diversity in other constraint columns too
                    used_values = defaultdict(set)  # Track used values for each column
                    
                    for first_val in first_col_values[:num_rows_for_this_val]:
                        candidates = by_first_col[first_val]
                        
                        # Filter candidates to maximize diversity in other columns
                        best_candidate = None
                        for candidate in candidates:
                            # Check if this candidate adds diversity
                            conflicts = 0
                            for col in constraint_non_shared_cols[1:]:
                                if candidate[col] in used_values[col]:
                                    conflicts += 1
                            
                            if conflicts == 0 or best_candidate is None:
                                best_candidate = candidate
                                if conflicts == 0:
                                    break  # Found a perfect candidate
                        
                        if best_candidate is None:
                            best_candidate = candidates[random.randint(0, len(candidates) - 1)]
                        
                        selected.append(best_candidate)
                        
                        # Mark values as used
                        for col in constraint_non_shared_cols:
                            used_values[col].add(best_candidate[col])
                    
                    smart_selection_succeeded = True
            
            # If smart selection didn't work, fall back to random selection
            if not smart_selection_succeeded:
                random.shuffle(available)
                selected = available[:num_rows_for_this_val]
            
            selected_combinations.extend(selected)
        
        # Shuffle final selection
        random.shuffle(selected_combinations)
        
        self.assertEqual(len(selected_combinations), 6000)
        
        # Verify all A_IDs present
        a_id_counts = defaultdict(int)
        for row in selected_combinations:
            a_id_counts[row['A_ID']] += 1
        
        unique_a_ids = len(a_id_counts)
        self.assertEqual(unique_a_ids, 3000,
                        "All 3000 A_IDs should be present")
        
        # Verify each A_ID appears exactly 2 times
        for a_id, count in a_id_counts.items():
            self.assertEqual(count, 2,
                           f"A_ID {a_id} should appear exactly 2 times, got {count}")
        
        # Verify NO duplicates in APR constraint
        apr_pairs = set()
        apr_duplicates = 0
        for row in selected_combinations:
            pair = (row['A_ID'], row['PR'])
            if pair in apr_pairs:
                apr_duplicates += 1
            apr_pairs.add(pair)
        
        self.assertEqual(apr_duplicates, 0,
                        "Smart stratified sampling should eliminate APR duplicates")
        
        # Verify NO duplicates in ACS constraint
        acs_pairs = set()
        acs_duplicates = 0
        for row in selected_combinations:
            pair = (row['A_ID'], row['C_ID'])
            if pair in acs_pairs:
                acs_duplicates += 1
            acs_pairs.add(pair)
        
        self.assertEqual(acs_duplicates, 0,
                        "Smart stratified sampling should eliminate ACS duplicates")
        
        print(f"\nSmart stratified sampling results:")
        print(f"  Unique A_IDs: {unique_a_ids} (target: 3000) ✓")
        print(f"  APR duplicates: {apr_duplicates} (target: 0) ✓")
        print(f"  ACS duplicates: {acs_duplicates} (target: 0) ✓")
    
    def test_stratified_sampling_with_remainder(self):
        """Test stratified sampling when rows don't divide evenly."""
        import itertools
        
        # Scenario: 100 shared values, 10 non-shared values, 101 rows requested
        # rows_per_shared_val = 101 // 100 = 1
        # remainder = 101 % 100 = 1
        # First 1 shared value gets 2 rows, rest get 1 row
        
        shared_values = list(range(1, 101))  # 100 shared values
        non_shared_values = list(range(1, 11))  # 10 non-shared values
        requested_rows = 101
        
        # Generate all combinations
        all_combinations = []
        for shared_val in shared_values:
            for non_shared_val in non_shared_values:
                all_combinations.append({
                    'SHARED': shared_val,
                    'NON_SHARED': non_shared_val
                })
        
        self.assertEqual(len(all_combinations), 1000)  # 100 * 10
        
        # Stratified sampling
        primary_shared_col = 'SHARED'
        combos_by_shared_val = defaultdict(list)
        for combo in all_combinations:
            shared_val = combo[primary_shared_col]
            combos_by_shared_val[shared_val].append(combo)
        
        rows_per_shared_val = requested_rows // len(shared_values)
        remainder = requested_rows % len(shared_values)
        
        self.assertEqual(rows_per_shared_val, 1)
        self.assertEqual(remainder, 1)
        
        random.seed(42)
        selected_combinations = []
        shared_values_list = list(shared_values)
        random.shuffle(shared_values_list)
        
        for idx, shared_val in enumerate(shared_values_list):
            available = combos_by_shared_val[shared_val]
            num_rows_for_this_val = rows_per_shared_val + (1 if idx < remainder else 0)
            
            random.shuffle(available)
            selected = available[:num_rows_for_this_val]
            selected_combinations.extend(selected)
        
        self.assertEqual(len(selected_combinations), 101)
        
        # Count distribution
        shared_counts = defaultdict(int)
        for row in selected_combinations:
            shared_counts[row['SHARED']] += 1
        
        # Verify: 1 shared value has 2 rows, 99 have 1 row
        values_with_2 = sum(1 for count in shared_counts.values() if count == 2)
        values_with_1 = sum(1 for count in shared_counts.values() if count == 1)
        
        self.assertEqual(values_with_2, 1, "Exactly 1 shared value should have 2 rows")
        self.assertEqual(values_with_1, 99, "99 shared values should have 1 row")
        self.assertEqual(len(shared_counts), 100, "All 100 shared values should be present")
    
    def test_stratified_sampling_more_rows_than_shared_values(self):
        """Test when requested rows > shared values."""
        import itertools
        
        # Scenario: 10 shared values, 5 non-shared values, 100 rows requested
        # rows_per_shared_val = 100 // 10 = 10
        # Each shared value should get 10 rows
        
        shared_values = list(range(1, 11))  # 10 shared values
        non_shared_values = list(range(1, 6))  # 5 non-shared values
        requested_rows = 100
        
        # Generate all combinations (50 total: 10 * 5)
        all_combinations = []
        for shared_val in shared_values:
            for non_shared_val in non_shared_values:
                all_combinations.append({
                    'SHARED': shared_val,
                    'NON_SHARED': non_shared_val
                })
        
        self.assertEqual(len(all_combinations), 50)
        
        # But we're requesting 100 rows - not enough combinations!
        # This should be handled by the "insufficient combinations" logic
        # which extends the list by repeating
        
        # Simulate extension
        if len(all_combinations) < requested_rows:
            extended = []
            for i in range(requested_rows):
                extended.append(all_combinations[i % len(all_combinations)])
            all_combinations = extended
        
        self.assertEqual(len(all_combinations), 100)
        
        # Now stratified sampling should work
        primary_shared_col = 'SHARED'
        combos_by_shared_val = defaultdict(list)
        for combo in all_combinations:
            shared_val = combo[primary_shared_col]
            combos_by_shared_val[shared_val].append(combo)
        
        rows_per_shared_val = requested_rows // len(shared_values)
        remainder = requested_rows % len(shared_values)
        
        self.assertEqual(rows_per_shared_val, 10)
        self.assertEqual(remainder, 0)
        
        # Each shared value should have 10 combinations available
        for shared_val in shared_values:
            self.assertEqual(len(combos_by_shared_val[shared_val]), 10)


if __name__ == "__main__":
    unittest.main()
