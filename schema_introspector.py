#!/usr/bin/env python3
"""Schema introspection module for loading database metadata"""
import sys
from generate_synthetic_data_utils import (
    debug_print, TableMeta
)


def load_table_columns(conn, schema, table):
    """Load column metadata from information_schema"""
    from generate_synthetic_data_utils import ColumnMeta
    cur = conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_TYPE, "
        "COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, "
        "NUMERIC_SCALE, COLUMN_DEFAULT FROM information_schema.COLUMNS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s ORDER BY ORDINAL_POSITION",
        (schema, table)
    )
    return [ColumnMeta(*r) for r in cur.fetchall()]


def load_table_pk(conn, schema, table):
    """Load primary key column names"""
    cur = conn.cursor()
    cur.execute(
        "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND CONSTRAINT_NAME='PRIMARY' "
        "ORDER BY ORDINAL_POSITION",
        (schema, table)
    )
    return [r[0] for r in cur.fetchall()]


def load_table_engine_and_ai(conn, schema, table):
    """Load table engine and auto_increment status"""
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT ENGINE, AUTO_INCREMENT FROM information_schema.TABLES "
            "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
            (schema, table)
        )
        r = cur.fetchone()
        return (r[0], r[1]) if r else (None, None)
    except:
        return None, None


def load_unique_constraints(conn, schema, table):
    """Load UNIQUE constraints from information_schema"""
    from generate_synthetic_data_utils import UniqueConstraint
    cur = conn.cursor()
    cur.execute(
        "SELECT INDEX_NAME, COLUMN_NAME, SEQ_IN_INDEX FROM information_schema.STATISTICS "
        "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND NON_UNIQUE=0 "
        "ORDER BY INDEX_NAME, SEQ_IN_INDEX",
        (schema, table)
    )
    constraints = {}
    for idx_name, col_name, seq in cur.fetchall():
        if idx_name != "PRIMARY":
            constraints.setdefault(idx_name, []).append(col_name)
    return [UniqueConstraint(n, tuple(cols)) for n, cols in constraints.items()]


def sample_static_fk_values(conn, static_schema, static_table, static_column, sample_size, rng):
    """Sample distinct values from a static FK source table"""
    cur = conn.cursor()
    try:
        query = "SELECT DISTINCT `{0}` FROM `{1}`.`{2}` WHERE `{3}` IS NOT NULL".format(
            static_column, static_schema, static_table, static_column
        )
        if sample_size <= 500:
            query += " ORDER BY RAND()"
        query += " LIMIT %s"
        cur.execute(query, (sample_size,))
        return [r[0] for r in cur.fetchall()]
    except Exception as e:
        print("Error: Failed to sample from {0}.{1}.{2}: {3}".format(
            static_schema, static_table, static_column, e), file=sys.stderr)
        sys.exit(1)


class SchemaIntrospector(object):
    """
    Responsible for loading and analyzing database schema metadata.
    
    Handles:
    - Loading table columns, primary keys, and constraints
    - Sampling static FK values from production tables
    - Detecting forced explicit parent generation scenarios
    - Preparing PK sequences for non-auto-increment tables
    """
    
    def __init__(self, conn, config, rng, sample_size):
        """
        Initialize schema introspector.
        
        Args:
            conn: MySQL database connection
            config: List of table configuration dicts
            rng: Random number generator (for static sampling)
            sample_size: Number of values to sample for static FKs
        """
        self.conn = conn
        self.config = config
        self.rng = rng
        self.sample_size = sample_size
        
        # Table metadata indexed by "schema.table"
        self.metadata = {}
        
        # Unique constraints indexed by "schema.table"
        self.unique_constraints = {}
        
        # Static FK samples indexed by "schema.table.column"
        self.static_samples = {}
        
        # Tables that must explicitly generate PK values
        self.forced_explicit_parents = set()
        
        # Next PK value for tables with explicit PK generation
        self.pk_next_vals = {}
        
        # Track used PK values for uniqueness
        self.used_pk_values = {}
    
    def introspect_schemas(self, config_table_names):
        """
        Load metadata for all tables in config.
        
        Args:
            config_table_names: Set of "schema.table" strings to introspect
        
        Returns:
            Tuple of (metadata, unique_constraints) dicts
        """
        for key in config_table_names:
            schema, table = key.split(".", 1)
            
            try:
                cols = load_table_columns(self.conn, schema, table)
                pkcols = load_table_pk(self.conn, schema, table)
                engine, auto_inc = load_table_engine_and_ai(self.conn, schema, table)
                unique_cons = load_unique_constraints(self.conn, schema, table)
                
                if not cols:
                    print("Error: Table {0}.{1} not found".format(schema, table), file=sys.stderr)
                    sys.exit(1)
                
                self.metadata[key] = TableMeta(
                    schema, table, cols, pkcols,
                    auto_inc is not None, engine
                )
                
                self.unique_constraints[key] = unique_cons
                
                if pkcols:
                    self.used_pk_values[key] = set()
                
                # Debug log UNIQUE constraints
                if unique_cons:
                    single_unique_cols = set()
                    composite_unique_cols = set()
                    
                    for uc in unique_cons:
                        if len(uc.columns) == 1:
                            single_unique_cols.add(uc.columns[0])
                        else:
                            composite_unique_cols.update(uc.columns)
                    
                    debug_print("{0}: Single-column UNIQUE: {1}".format(
                        key, single_unique_cols))
                    
                    if composite_unique_cols:
                        debug_print("{0}: Composite UNIQUE columns: {1}".format(
                            key, composite_unique_cols))
            
            except Exception as e:
                print("Error: Failed to load {0}.{1}: {2}".format(
                    schema, table, e), file=sys.stderr)
                sys.exit(1)
        
        return self.metadata, self.unique_constraints
    
    def sample_static_fks(self):
        """
        Sample values from static FK definitions in config.
        
        Populates self.static_samples with sampled values indexed by
        "schema.table.column" keys.
        """
        for table_cfg in self.config:
            for static_fk in table_cfg.get("static_fks", []):
                key = "{0}.{1}.{2}".format(
                    static_fk['static_schema'],
                    static_fk['static_table'],
                    static_fk['static_column']
                )
                
                if key not in self.static_samples:
                    self.static_samples[key] = sample_static_fk_values(
                        self.conn,
                        static_fk["static_schema"],
                        static_fk["static_table"],
                        static_fk["static_column"],
                        self.sample_size,
                        self.rng
                    )
    
    def detect_forced_explicit_parents(self, fks, logical_composite_fks, table_map):
        """
        Identify tables that must explicitly generate PK values.
        
        This is needed when:
        1. Child tables have NOT NULL FKs to multiple parent tables
        2. Composite FKs reference parent PK columns
        3. Config explicitly sets explicit_pk=true
        
        Args:
            fks: List of FKMeta objects
            logical_composite_fks: List of composite FK dicts
            table_map: Dict mapping "schema.table" to config dicts
        """
        from collections import defaultdict
        
        # Find child tables with NOT NULL FKs to multiple parents
        child_to_parents = defaultdict(list)
        
        for fk in fks:
            child = "{0}.{1}".format(fk.table_schema, fk.table_name)
            parent = "{0}.{1}".format(
                fk.referenced_table_schema, fk.referenced_table_name)
            
            tmeta = self.metadata.get(child)
            if tmeta:
                for col in tmeta.columns:
                    if col.name == fk.column_name and col.is_nullable == "NO":
                        child_to_parents[child].append(parent)
                        break
        
        # Mark parents referenced by multiple NOT NULL FKs
        for child, parents in child_to_parents.items():
            unique_parents = set([p for p in parents if p in table_map])
            if len(unique_parents) > 1:
                for parent in unique_parents:
                    self.forced_explicit_parents.add(parent)
        
        # Mark parents referenced by composite FK PK columns
        for comp_fk in logical_composite_fks:
            parent_table = "{0}.{1}".format(
                comp_fk['referenced_table_schema'],
                comp_fk['referenced_table_name']
            )
            
            if parent_table in table_map:
                parent_meta = self.metadata.get(parent_table)
                
                if parent_meta and parent_meta.pk_columns:
                    for ref_col in comp_fk['referenced_columns']:
                        if ref_col in parent_meta.pk_columns:
                            self.forced_explicit_parents.add(parent_table)
                            break
        
        # Mark tables with explicit_pk config
        for table_cfg in self.config:
            if table_cfg.get("explicit_pk", False):
                table_key = "{0}.{1}".format(
                    table_cfg['schema'], table_cfg['table'])
                self.forced_explicit_parents.add(table_key)
    
    def prepare_pk_sequences(self):
        """
        Initialize PK sequences for tables needing explicit PK generation.
        
        Determines starting values by querying current MAX(pk) and
        AUTO_INCREMENT values from the database.
        """
        for table_name, tmeta in self.metadata.items():
            # Only handle single-column PKs
            if not tmeta.pk_columns or len(tmeta.pk_columns) != 1:
                continue
            
            pk_col = tmeta.pk_columns[0]
            
            # Skip auto-increment tables unless forced
            if tmeta.auto_increment and table_name not in self.forced_explicit_parents:
                continue
            
            # Query for AUTO_INCREMENT value
            cur = self.conn.cursor()
            cur.execute(
                "SELECT AUTO_INCREMENT FROM information_schema.TABLES "
                "WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s",
                (tmeta.schema, tmeta.name)
            )
            
            result = cur.fetchone()
            auto_inc_next = result[0] if result and result[0] else None
            
            # Query for current MAX(pk)
            try:
                cur.execute(
                    "SELECT MAX(`{0}`) FROM `{1}`.`{2}`".format(
                        pk_col, tmeta.schema, tmeta.name))
                result = cur.fetchone()
                current_max = result[0] if result and result[0] else 0
            except:
                current_max = 0
            
            # Determine starting value
            start = max(1, auto_inc_next if auto_inc_next and isinstance(auto_inc_next, int) else 1)
            
            if isinstance(current_max, int):
                start = max(start, current_max + 1)
            
            self.pk_next_vals[table_name] = start
    
    def get_next_pk_value(self, table_name):
        """
        Thread-safe retrieval of next PK value for a table.
        
        Args:
            table_name: "schema.table" string
        
        Returns:
            Next integer PK value
        """
        if table_name not in self.pk_next_vals:
            return None
        
        # Note: Caller must handle thread-safety with pk_counter_lock
        value = self.pk_next_vals[table_name]
        self.pk_next_vals[table_name] += 1
        return value
