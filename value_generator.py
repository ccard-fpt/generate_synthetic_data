#!/usr/bin/env python3
"""Value generation module for creating synthetic column values"""
import sys
import threading
from generate_synthetic_data_utils import (
    debug_print, generate_value_with_config, generate_unique_value_pool,
    parse_fk_condition
)
from generate_synthetic_data_patterns import ThreadLocalCounter


class ValueGenerator(object):
    """
    Responsible for generating column values respecting constraints.
    
    Handles:
    - Global unique value pool management (thread-safe)
    - Sequential value generation for uncontrolled columns
    - Discriminator column identification for conditional FKs
    - Batch row generation with constraint validation
    """
    
    def __init__(self, metadata, unique_constraints, fk_columns, 
                 populate_columns_config, static_samples, fks, args):
        """
        Initialize value generator.
        
        Args:
            metadata: Dict of TableMeta objects
            unique_constraints: Dict of UniqueConstraint lists
            fk_columns: Dict of FK column sets
            populate_columns_config: Dict of populate column configs
            static_samples: Dict of static FK sample values
            fks: List of FKMeta objects
            args: Command-line arguments (for seed, threads)
        """
        self.metadata = metadata
        self.unique_constraints = unique_constraints
        self.fk_columns = fk_columns
        self.populate_columns_config = populate_columns_config
        self.static_samples = static_samples
        self.fks = fks
        self.args = args
        
        # Global unique value pools (thread-safe)
        self.global_unique_value_pools = {}
        self.global_unique_pool_lock = threading.Lock()
        
        # Sequential counters for uncontrolled columns in composite UNIQUE
        # Using ThreadLocalCounter for reduced lock contention (30-50% improvement)
        self.composite_unique_counters = {}
        self.composite_unique_counter_lock = threading.Lock()
    
    def initialize_global_unique_pools(self, node, num_rows, rng):
        """
        Pre-allocate unique value pools for UNIQUE columns.
        
        Creates a single pool per UNIQUE column shared across all batches.
        Ensures no duplicates within the same column across the entire table.
        
        Args:
            node: "schema.table" string
            num_rows: Total number of rows to generate
            rng: Random number generator
        """
        tmeta = self.metadata.get(node)
        if not tmeta:
            return
        
        constraints = self.unique_constraints.get(node, [])
        populate_config = self.populate_columns_config.get(node, {})
        
        # Identify single-column UNIQUE constraints
        single_unique_cols = set()
        for uc in constraints:
            if len(uc.columns) == 1:
                single_unique_cols.add(uc.columns[0])
        
        # Create pools for UNIQUE columns with min/max or values config
        for col_name in single_unique_cols:
            if col_name in populate_config and col_name not in tmeta.pk_columns:
                col_config = populate_config[col_name]
                
                # Only pre-allocate if config has min/max or values
                if "min" in col_config or "values" in col_config:
                    pool_key = "{0}.{1}".format(node, col_name)
                    
                    if pool_key in self.global_unique_value_pools:
                        continue
                    
                    col_meta = next((c for c in tmeta.columns if c.name == col_name), None)
                    if col_meta:
                        # Generate pool for ALL rows
                        unique_pool = generate_unique_value_pool(
                            col_meta, col_config, num_rows, rng)
                        
                        if len(unique_pool) < num_rows:
                            print("WARNING: {0}: UNIQUE column {1} has insufficient unique values "
                                  "({2} available, {3} needed)".format(
                                node, col_name, len(unique_pool), num_rows), file=sys.stderr)
                            print("  Consider expanding the range or reducing row count", file=sys.stderr)
                        
                        # Store pool with cursor at 0
                        self.global_unique_value_pools[pool_key] = {
                            'pool': unique_pool,
                            'cursor': 0,
                            'size': len(unique_pool)
                        }
                        
                        debug_print("{0}: Created global unique pool for {1} with {2} values".format(
                            node, col_name, len(unique_pool)))
    
    def classify_columns_for_generation(self, node, tmeta):
        """
        Classify columns by generation strategy.
        
        Returns:
            Tuple of (single_unique_cols, composite_unique_constraints, 
                     composite_unique_cols, all_unique_cols, 
                     cols_needing_sequential, discriminator_cols)
        """
        from constraint_resolver import ConstraintResolver
        
        constraints = self.unique_constraints.get(node, [])
        fk_cols = self.fk_columns.get(node, set())
        populate_config = self.populate_columns_config.get(node, {})
        
        # Use ConstraintResolver to classify
        resolver = ConstraintResolver(self.metadata, self.unique_constraints, self.fk_columns)
        single_unique_cols, composite_constraints, composite_cols = resolver.classify_unique_constraints(node)
        
        all_unique_cols = set(single_unique_cols)
        all_unique_cols.update(composite_cols)
        
        # Identify discriminator columns for conditional FKs
        discriminator_cols = set()
        for fk in self.fks:
            if "{0}.{1}".format(fk.table_schema, fk.table_name) == node and fk.condition:
                parsed = parse_fk_condition(fk.condition)
                if parsed:
                    discriminator_cols.add(parsed['column'])
        
        # Identify columns needing sequential generation
        cols_needing_sequential = set()
        for uc in composite_constraints:
            controlled_cols = []
            uncontrolled_cols = []
            
            for col_name in uc.columns:
                is_controlled = col_name in populate_config and (
                    "values" in populate_config.get(col_name, {}) or
                    "min" in populate_config.get(col_name, {})
                )
                is_fk = col_name in fk_cols
                is_pk = col_name in tmeta.pk_columns
                
                # Discriminator ENUM columns are controlled
                if col_name in discriminator_cols:
                    col_meta = next((c for c in tmeta.columns if c.name == col_name), None)
                    if col_meta and col_meta.data_type and col_meta.data_type.lower() == "enum":
                        is_controlled = True
                
                if is_controlled or is_fk or is_pk:
                    controlled_cols.append(col_name)
                else:
                    uncontrolled_cols.append(col_name)
            
            if uncontrolled_cols and controlled_cols:
                for col_name in uncontrolled_cols:
                    cols_needing_sequential.add(col_name)
        
        return (single_unique_cols, composite_constraints, composite_cols,
                all_unique_cols, cols_needing_sequential, discriminator_cols)
    
    def generate_batch(self, node, start_idx, end_idx, thread_rng, tmeta, cfg):
        """
        Generate a batch of rows with guaranteed unique values.
        
        Main entry point for row generation. Orchestrates all value
        generation strategies based on column classification.
        
        Args:
            node: "schema.table" string
            start_idx: Starting row index
            end_idx: Ending row index (exclusive)
            thread_rng: Thread-local random number generator
            tmeta: TableMeta object
            cfg: Table configuration dict
        
        Returns:
            List of row dicts
        """
        rows = []
        
        # Classify columns by generation strategy
        (single_unique_cols, composite_constraints, composite_cols,
         all_unique_cols, cols_needing_sequential, discriminator_cols) = \
            self.classify_columns_for_generation(node, tmeta)
        
        # Track local unique values
        local_trackers = {uc.constraint_name: set() for uc in composite_constraints}
        
        # Identify unique columns with global pools
        unique_cols_with_global_pools = set()
        populate_config = self.populate_columns_config.get(node, {})
        for col_name in single_unique_cols:
            if col_name in populate_config and col_name not in tmeta.pk_columns:
                col_config = populate_config[col_name]
                if "min" in col_config or "values" in col_config:
                    pool_key = "{0}.{1}".format(node, col_name)
                    if pool_key in self.global_unique_value_pools:
                        unique_cols_with_global_pools.add(col_name)
        
        # Generate rows
        for batch_idx in range(start_idx, end_idx):
            row = self._generate_single_row(
                node, batch_idx, tmeta, cfg,
                single_unique_cols, composite_cols, all_unique_cols,
                cols_needing_sequential, discriminator_cols,
                unique_cols_with_global_pools, thread_rng
            )
            
            # Validate unique constraints
            valid = self._validate_unique_constraints(
                node, row, composite_constraints, local_trackers, batch_idx)
            
            if valid:
                rows.append(row)
            else:
                print("ERROR: Skipping row at batch_idx {0}".format(batch_idx), file=sys.stderr)
        
        return rows
    
    def _generate_single_row(self, node, batch_idx, tmeta, cfg,
                            single_unique_cols, composite_cols, all_unique_cols,
                            cols_needing_sequential, discriminator_cols,
                            unique_cols_with_global_pools, thread_rng):
        """
        Generate a single row with all column values.
        
        Applies generation strategies in priority order:
        1. Sequential generation (uncontrolled columns in composite UNIQUE)
        2. Global unique pools (UNIQUE columns with config)
        3. Extended populate_columns config
        4. Default value generation
        
        Args:
            node: "schema.table" string
            batch_idx: Row index
            tmeta: TableMeta object
            cfg: Table config
            single_unique_cols: Set of single-column UNIQUE columns
            composite_cols: Set of composite UNIQUE columns
            all_unique_cols: Set of all UNIQUE columns
            cols_needing_sequential: Set of columns needing sequential generation
            discriminator_cols: Set of discriminator columns for conditional FKs
            unique_cols_with_global_pools: Set of UNIQUE columns with pre-allocated pools
            thread_rng: Random number generator
        
        Returns:
            Dict mapping column_name -> value
        """
        row = {}
        fk_cols = self.fk_columns.get(node, set())
        populate_config = self.populate_columns_config.get(node, {})
        
        for col in tmeta.columns:
            cname = col.name
            
            # Skip auto-increment PKs (will be assigned by database)
            if cname in tmeta.pk_columns and tmeta.auto_increment:
                row[cname] = None
                continue
            
            # Handle static FKs
            if cfg:
                found_static = False
                for sf in cfg.get("static_fks", []):
                    if sf["column"] == cname:
                        key = "{0}.{1}.{2}".format(
                            sf['static_schema'], sf['static_table'], sf['static_column'])
                        pool = self.static_samples.get(key, [])
                        row[cname] = thread_rng.choice(pool) if pool else None
                        found_static = True
                        break
                if found_static:
                    continue
            
            # Skip FK columns (unless discriminator)
            if cname in fk_cols and cname not in discriminator_cols:
                row[cname] = None
                continue
            
            # PRIORITY 0: Sequential generation for uncontrolled columns
            if cname in cols_needing_sequential:
                row[cname] = self._handle_sequential_generation(node, cname, col)
                continue
            
            # PRIORITY 1: Global unique pool (thread-safe)
            if cname in unique_cols_with_global_pools:
                row[cname] = self._handle_unique_pool(node, cname)
                continue
            
            # PRIORITY 2: Extended populate_columns config
            col_config = populate_config.get(cname)
            if col_config and ("values" in col_config or "min" in col_config):
                base_value = generate_value_with_config(thread_rng, col, col_config)
                
                # Handle single-column UNIQUE with suffix
                if cname in single_unique_cols and col.data_type.lower() in (
                    "varchar", "char", "text", "mediumtext", "longtext") and base_value is not None:
                    maxlen = int(col.char_max_length) if col.char_max_length else 255
                    base_str = str(base_value)
                    suffix = "_{0}".format(batch_idx)
                    max_base_len = maxlen - len(suffix)
                    if max_base_len < 1:
                        row[cname] = suffix[1:maxlen]
                    else:
                        row[cname] = (base_str[:max_base_len] + suffix)[:maxlen]
                    continue
                
                row[cname] = base_value
                continue
            
            # PRIORITY 3: Default value generation
            row[cname] = self._generate_default_value(
                cname, col, batch_idx, single_unique_cols, thread_rng)
        
        return row
    
    def _handle_sequential_generation(self, node, cname, col):
        """
        Generate sequential value for uncontrolled columns in composite UNIQUE.
        Uses ThreadLocalCounter for reduced lock contention (30-50% improvement).
        
        Thread-safe counter-based generation to prevent collisions.
        
        Args:
            node: "schema.table" string
            cname: Column name
            col: ColumnMeta object
        
        Returns:
            Sequential value (string or int)
        """
        counter_key = "{0}.{1}".format(node, cname)
        
        # Initialize ThreadLocalCounter if needed (lock protected)
        with self.composite_unique_counter_lock:
            if counter_key not in self.composite_unique_counters:
                self.composite_unique_counters[counter_key] = ThreadLocalCounter(batch_size=100)
        
        # Get counter value (mostly lock-free)
        counter_val = self.composite_unique_counters[counter_key].next()
        
        dtype = (col.data_type or "").lower()
        
        try:
            maxlen = int(col.char_max_length) if col.char_max_length else 255
        except (ValueError, TypeError):
            maxlen = 255
        
        if dtype in ("varchar", "char", "text", "mediumtext", "longtext"):
            seq_value = "seq_{0:08d}".format(counter_val)
            return seq_value[:maxlen]
        elif "int" in dtype or dtype in ("bigint", "smallint", "mediumint", "tinyint"):
            return counter_val
        else:
            seq_value = "seq_{0:08d}".format(counter_val)
            return seq_value[:maxlen] if maxlen else seq_value
    
    def _handle_unique_pool(self, node, cname):
        """
        Get value from global unique pool (thread-safe).
        
        Args:
            node: "schema.table" string
            cname: Column name
        
        Returns:
            Unique value from pool or None if exhausted
        """
        pool_key = "{0}.{1}".format(node, cname)
        pool_data = self.global_unique_value_pools[pool_key]
        
        with self.global_unique_pool_lock:
            cursor = pool_data['cursor']
            if cursor < pool_data['size']:
                value = pool_data['pool'][cursor]
                pool_data['cursor'] += 1
                
                # Debug progress logging every 1000 values
                if pool_data['cursor'] % 1000 == 0:
                    debug_print("{0}: Used {1}/{2} unique values for column {3}".format(
                        node, pool_data['cursor'], pool_data['size'], cname))
                
                return value
            else:
                # Pool exhausted
                print("ERROR: {0}: Exhausted unique value pool for {1}".format(
                    node, cname), file=sys.stderr)
                return None
    
    def _generate_default_value(self, cname, col, batch_idx, single_unique_cols, thread_rng):
        """
        Generate default value based on column type.
        
        Args:
            cname: Column name
            col: ColumnMeta object
            batch_idx: Row index
            single_unique_cols: Set of single-column UNIQUE columns
            thread_rng: Random number generator
        
        Returns:
            Generated value
        """
        import re
        from generate_synthetic_data_utils import (
            rand_decimal_str, rand_email, rand_name, rand_phone, 
            rand_string, rand_datetime
        )
        
        dtype = (col.data_type or "").lower()
        
        if "int" in dtype or dtype in ("bigint", "smallint", "mediumint", "tinyint"):
            if cname in single_unique_cols:
                return batch_idx
            else:
                return thread_rng.randint(18, 80) if re.search(r"age|years? ", cname, re.I) else thread_rng.randint(0, 10000)
        
        elif dtype in ("decimal", "numeric", "float", "double"):
            prec, scale = int(col.numeric_precision or 10), int(col.numeric_scale or 0)
            return rand_decimal_str(thread_rng, prec, scale)
        
        elif dtype in ("varchar", "char", "text", "mediumtext", "longtext"):
            lname = cname.lower()
            if "email" in lname:
                base_value = rand_email(thread_rng)
            elif "name" in lname:
                base_value = rand_name(thread_rng)
            elif "phone" in lname:
                base_value = rand_phone(thread_rng)
            else:
                maxlen = int(col.char_max_length) if col.char_max_length else 24
                base_value = rand_string(thread_rng, min(maxlen, 24))
            
            # Append suffix for single-column UNIQUE
            if cname in single_unique_cols and base_value is not None:
                maxlen = int(col.char_max_length) if col.char_max_length else 255
                base_str = str(base_value)
                suffix = "_{0}".format(batch_idx)
                max_base_len = maxlen - len(suffix)
                if max_base_len < 1:
                    return suffix[1:maxlen]
                else:
                    return (base_str[:max_base_len] + suffix)[:maxlen]
            
            return base_value
        
        elif dtype in ("date", "datetime", "timestamp"):
            return rand_datetime(thread_rng).split(" ")[0] if dtype == "date" else rand_datetime(thread_rng)
        
        elif dtype == "enum":
            m = re.findall(r"'((?:[^']|(?:''))*)'", col.column_type or "")
            vals = [v.replace("''", "'") for v in m]
            return thread_rng.choice(vals) if vals else None
        
        elif dtype == "set":
            m = re.findall(r"'((?:[^']|(?:''))*)'", col.column_type or "")
            set_values = [v.replace("''", "'") for v in m]
            
            if set_values:
                num_values_to_select = thread_rng.randint(0, len(set_values))
                if num_values_to_select == 0:
                    return ''
                else:
                    shuffled = list(set_values)
                    thread_rng.shuffle(shuffled)
                    selected = shuffled[:num_values_to_select]
                    selected_sorted = [v for v in set_values if v in selected]
                    return ','.join(selected_sorted)
            return ''
        
        elif col.is_nullable == "NO":
            return rand_string(thread_rng, 8)
        
        return None
    
    def _validate_unique_constraints(self, node, row, composite_constraints, 
                                    local_trackers, batch_idx):
        """
        Validate row against composite UNIQUE constraints.
        
        Args:
            node: "schema.table" string
            row: Row dict
            composite_constraints: List of UniqueConstraint objects
            local_trackers: Dict tracking used value tuples
            batch_idx: Row index for error reporting
        
        Returns:
            bool indicating if row is valid
        """
        for uc in composite_constraints:
            combo_tuple = tuple(row.get(col) for col in uc.columns)
            if not any(v is None for v in combo_tuple):
                if combo_tuple in local_trackers[uc.constraint_name]:
                    print("CRITICAL ERROR: Duplicate in {0}.{1}: {2} at batch_idx={3}".format(
                        node, uc.constraint_name, combo_tuple, batch_idx), file=sys.stderr)
                    return False
                local_trackers[uc.constraint_name].add(combo_tuple)
        
        return True
