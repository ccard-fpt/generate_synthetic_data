#!/usr/bin/env python3
"""Constraint resolution module for UNIQUE constraints and FK relationships"""
import sys
import itertools
from collections import defaultdict
from generate_synthetic_data_utils import debug_print


class ConstraintResolver(object):
    """
    Responsible for analyzing and resolving database constraints.
    
    Handles:
    - Identifying constraint types (single vs composite UNIQUE)
    - Detecting overlapping constraints that share columns
    - Selecting optimal constraint resolution strategies
    - Building Cartesian products for all-FK UNIQUE constraints
    """
    
    def __init__(self, metadata, unique_constraints, fk_columns):
        """
        Initialize constraint resolver.
        
        Args:
            metadata: Dict of TableMeta objects indexed by "schema.table"
            unique_constraints: Dict of UniqueConstraint lists indexed by "schema.table"
            fk_columns: Dict of FK column sets indexed by "schema.table"
        """
        self.metadata = metadata
        self.unique_constraints = unique_constraints
        self.fk_columns = fk_columns
    
    def classify_unique_constraints(self, table_key):
        """
        Classify UNIQUE constraints by type for a table.
        
        Args:
            table_key: "schema.table" string
        
        Returns:
            Tuple of (single_unique_cols, composite_constraints, composite_cols)
            where:
            - single_unique_cols: set of column names in single-column UNIQUE
            - composite_constraints: list of UniqueConstraint with 2+ columns
            - composite_cols: set of all columns in composite UNIQUE constraints
        """
        constraints = self.unique_constraints.get(table_key, [])
        
        single_unique_cols = set()
        composite_constraints = []
        composite_cols = set()
        
        for uc in constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
            else:
                composite_constraints.append(uc)
                composite_cols.update(uc.columns)
        
        return single_unique_cols, composite_constraints, composite_cols
    
    def find_overlapping_constraints(self, composite_constraints):
        """
        Find groups of UNIQUE constraints that share columns.
        
        Args:
            composite_constraints: List of UniqueConstraint objects
        
        Returns:
            List of constraint groups (each group is a list of UniqueConstraint)
        """
        if len(composite_constraints) < 2:
            return []
        
        overlapping_groups = []
        
        for i, uc1 in enumerate(composite_constraints):
            group = set([uc1])
            
            for j, uc2 in enumerate(composite_constraints):
                if i != j and set(uc1.columns) & set(uc2.columns):
                    group.add(uc2)
            
            if len(group) > 1:
                overlapping_groups.append(list(group))
        
        # Deduplicate groups
        unique_groups = []
        for group in overlapping_groups:
            group_set = frozenset(uc.constraint_name for uc in group)
            
            if not any(group_set == frozenset(uc.constraint_name for uc in g) 
                      for g in unique_groups):
                unique_groups.append(group)
        
        return unique_groups
    
    def identify_shared_columns(self, constraint_group):
        """
        Find columns shared by all constraints in a group.
        
        Args:
            constraint_group: List of UniqueConstraint objects
        
        Returns:
            Set of column names present in all constraints
        """
        if not constraint_group:
            return set()
        
        shared = set(constraint_group[0].columns)
        
        for uc in constraint_group[1:]:
            shared &= set(uc.columns)
        
        return shared
    
    def find_non_shared_columns(self, constraint_group, shared_cols):
        """
        Find columns unique to each constraint (not shared).
        
        Args:
            constraint_group: List of UniqueConstraint objects
            shared_cols: Set of shared column names
        
        Returns:
            Dict mapping column_name -> constraint_name for non-shared columns
        """
        non_shared = {}
        
        for uc in constraint_group:
            for col in uc.columns:
                if col not in shared_cols:
                    non_shared[col] = uc.constraint_name
        
        return non_shared
    
    def select_tightest_constraint(self, table_key, composite_constraints,
                                   fk_map, populate_config, generated_rows):
        """
        Select the UNIQUE constraint with fewest combinations.
        
        When multiple composite UNIQUE constraints exist, choosing the
        tightest one minimizes duplicate risk.
        
        Args:
            table_key: "schema.table" string
            composite_constraints: List of UniqueConstraint objects
            fk_map: Dict mapping column_name -> FKMeta
            populate_config: Dict of column configs
            generated_rows: Dict of generated rows for parent tables
        
        Returns:
            Tuple of (selected_constraint, estimated_combinations)
        """
        if len(composite_constraints) == 1:
            return composite_constraints[0], None
        
        constraint_combos = []
        
        for uc in composite_constraints:
            combo_count = 1
            
            for col_name in uc.columns:
                # Check if FK column
                if col_name in fk_map:
                    fk = fk_map[col_name]
                    parent_node = "{0}.{1}".format(
                        fk.referenced_table_schema,
                        fk.referenced_table_name
                    )
                    
                    if parent_node in generated_rows:
                        parent_rows = generated_rows[parent_node]
                        parent_col = fk.referenced_column_name
                        parent_vals = [r.get(parent_col) for r in parent_rows 
                                      if r and r.get(parent_col) is not None]
                        unique_vals = len(set(parent_vals))
                        combo_count *= unique_vals
                    else:
                        # Parent not generated yet
                        combo_count = float('inf')
                        break
                else:
                    # Non-FK column with explicit config
                    col_config = populate_config.get(col_name, {})
                    
                    if "values" in col_config:
                        combo_count *= len(col_config["values"])
                    elif "min" in col_config:
                        min_val = col_config.get("min", 0)
                        max_val = col_config.get("max", 100)
                        combo_count *= (max_val - min_val + 1)
                    else:
                        # Can't determine size
                        combo_count = float('inf')
                        break
            
            constraint_combos.append((uc, combo_count))
        
        # Sort by combo count (ascending) and pick tightest
        constraint_combos.sort(key=lambda x: x[1])
        selected_uc, min_combos = constraint_combos[0]
        
        # Log decision
        debug_print("{0}: Multiple composite UNIQUE constraints found:".format(table_key))
        
        for uc, combo_count in constraint_combos:
            marker = "✓ SELECTED" if uc == selected_uc else ""
            
            if combo_count == float('inf'):
                debug_print("  - {0} {1}: unknown combinations {2}".format(
                    uc.constraint_name, uc.columns, marker))
            else:
                debug_print("  - {0} {1}: {2} combinations {3}".format(
                    uc.constraint_name, uc.columns, combo_count, marker))
        
        return selected_uc, min_combos
    
    def build_cartesian_product(self, value_lists):
        """
        Generate Cartesian product of value lists.
        
        Args:
            value_lists: List of lists of values
        
        Returns:
            List of tuples representing all combinations
        """
        if not value_lists:
            return []
        
        return list(itertools.product(*value_lists))
    
    def stratified_sample(self, combinations, primary_shared_col, shared_values,
                         constraint_non_shared_cols, target_size, rng):
        """
        Sample combinations ensuring balanced distribution across shared values.
        
        Uses stratified sampling to guarantee each shared value appears
        proportionally in the final sample.
        
        Args:
            combinations: List of dicts with column->value mappings
            primary_shared_col: Name of primary shared column
            shared_values: List of unique shared column values
            constraint_non_shared_cols: List of non-shared column names
            target_size: Total number of combinations to sample
            rng: Random number generator
        
        Returns:
            List of sampled combination dicts
        """
        # Group combinations by shared value
        combos_by_shared_val = defaultdict(list)
        
        for combo in combinations:
            shared_val = combo[primary_shared_col]
            combos_by_shared_val[shared_val].append(combo)
        
        # Calculate rows per shared value
        rows_per_shared_val = target_size // len(shared_values)
        remainder = target_size % len(shared_values)
        
        debug_print("Stratified sampling: {0} rows per shared value, {1} remainder".format(
            rows_per_shared_val, remainder))
        
        selected_combinations = []
        shared_values_list = list(shared_values)
        rng.shuffle(shared_values_list)
        
        for idx, shared_val in enumerate(shared_values_list):
            available = combos_by_shared_val[shared_val]
            
            if not available:
                continue
            
            # First 'remainder' shared values get one extra row
            num_rows_for_this_val = rows_per_shared_val + (1 if idx < remainder else 0)
            
            # Smart selection for diversity
            selected = self._select_diverse_combinations(
                available, constraint_non_shared_cols, num_rows_for_this_val, rng)
            
            selected_combinations.extend(selected)
        
        # Shuffle final selection
        rng.shuffle(selected_combinations)
        
        return selected_combinations
    
    def _select_diverse_combinations(self, available, constraint_cols, 
                                    num_needed, rng):
        """
        Select combinations maximizing diversity in constraint columns.
        
        Args:
            available: List of available combination dicts
            constraint_cols: List of column names to maximize diversity for
            num_needed: Number of combinations to select
            rng: Random number generator
        
        Returns:
            List of selected combination dicts
        """
        if num_needed <= 1 or num_needed > 10 or not constraint_cols:
            # Fall back to random selection
            rng.shuffle(available)
            return available[:num_needed]
        
        # Try smart selection for small numbers
        first_col = constraint_cols[0]
        by_first_col = defaultdict(list)
        
        for combo in available:
            by_first_col[combo[first_col]].append(combo)
        
        first_col_values = list(by_first_col.keys())
        
        if len(first_col_values) >= num_needed:
            rng.shuffle(first_col_values)
            selected = []
            used_values = defaultdict(set)
            
            for first_val in first_col_values[:num_needed]:
                candidates = by_first_col[first_val]
                
                # Find candidate with minimum conflicts
                best_candidate = None
                
                for candidate in candidates:
                    conflicts = 0
                    
                    for col in constraint_cols[1:]:
                        if candidate[col] in used_values[col]:
                            conflicts += 1
                    
                    if conflicts == 0 or best_candidate is None:
                        best_candidate = candidate
                        if conflicts == 0:
                            break
                
                if best_candidate is None:
                    best_candidate = candidates[rng.randint(0, len(candidates) - 1)]
                
                selected.append(best_candidate)
                
                # Mark values as used
                for col in constraint_cols:
                    used_values[col].add(best_candidate[col])
            
            return selected
        
        # Fallback
        rng.shuffle(available)
        return available[:num_needed]
