#!/usr/bin/env python3
"""Unit tests for SchemaIntrospector class"""
import unittest
import random
from collections import namedtuple


# Mock data structures
ColumnMeta = namedtuple("ColumnMeta", [
    "name", "data_type", "is_nullable", "column_type", "column_key", 
    "extra", "char_max_length", "numeric_precision", "numeric_scale", "column_default"
])

TableMeta = namedtuple("TableMeta", [
    "schema", "name", "columns", "pk_columns", "auto_increment", "engine"
])

UniqueConstraint = namedtuple("UniqueConstraint", ["constraint_name", "columns"])

FKMeta = namedtuple("FKMeta", [
    "constraint_name", "table_schema", "table_name", "column_name",
    "referenced_table_schema", "referenced_table_name", "referenced_column_name",
    "is_logical", "condition"
])


class MockConnection:
    """Mock database connection for testing"""
    
    def __init__(self):
        self.tables = {}
    
    def cursor(self):
        return MockCursor(self.tables)


class MockCursor:
    """Mock database cursor for testing"""
    
    def __init__(self, tables):
        self.tables = tables
        self.result = []
    
    def execute(self, query, params=None):
        """Mock execute - return empty results"""
        self.result = []
    
    def fetchall(self):
        return self.result
    
    def fetchone(self):
        return self.result[0] if self.result else None


class TestSchemaIntrospector(unittest.TestCase):
    """Test cases for SchemaIntrospector functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.conn = MockConnection()
        self.config = []
        self.rng = random.Random(42)
        self.sample_size = 100
    
    def test_initialization(self):
        """Test SchemaIntrospector initialization"""
        from schema_introspector import SchemaIntrospector
        
        introspector = SchemaIntrospector(
            self.conn, self.config, self.rng, self.sample_size
        )
        
        self.assertEqual(len(introspector.metadata), 0)
        self.assertEqual(len(introspector.unique_constraints), 0)
        self.assertEqual(len(introspector.static_samples), 0)
        self.assertEqual(len(introspector.forced_explicit_parents), 0)
    
    def test_pk_sequence_initialization(self):
        """Test PK sequence value tracking"""
        from schema_introspector import SchemaIntrospector
        
        introspector = SchemaIntrospector(
            self.conn, self.config, self.rng, self.sample_size
        )
        
        table_key = "test.users"
        introspector.pk_next_vals[table_key] = 100
        
        # Get next value
        val = introspector.get_next_pk_value(table_key)
        self.assertEqual(val, 100)
        
        # Should increment
        val = introspector.get_next_pk_value(table_key)
        self.assertEqual(val, 101)
    
    def test_detect_forced_explicit_parents_multiple_fks(self):
        """Test detection of forced explicit parent generation"""
        from schema_introspector import SchemaIntrospector
        
        # Setup table metadata
        cols_child = [
            ColumnMeta("id", "int", "NO", "int", "PRI", "auto_increment", None, 10, 0, None),
            ColumnMeta("parent1_id", "int", "NO", "int", "MUL", "", None, 10, 0, None),
            ColumnMeta("parent2_id", "int", "NO", "int", "MUL", "", None, 10, 0, None)
        ]
        tmeta_child = TableMeta("test", "child", cols_child, ["id"], True, "InnoDB")
        
        cols_parent1 = [
            ColumnMeta("id", "int", "NO", "int", "PRI", "auto_increment", None, 10, 0, None)
        ]
        tmeta_parent1 = TableMeta("test", "parent1", cols_parent1, ["id"], True, "InnoDB")
        
        cols_parent2 = [
            ColumnMeta("id", "int", "NO", "int", "PRI", "auto_increment", None, 10, 0, None)
        ]
        tmeta_parent2 = TableMeta("test", "parent2", cols_parent2, ["id"], True, "InnoDB")
        
        self.config = [
            {"schema": "test", "table": "child", "rows": 10},
            {"schema": "test", "table": "parent1", "rows": 5},
            {"schema": "test", "table": "parent2", "rows": 5}
        ]
        
        introspector = SchemaIntrospector(
            self.conn, self.config, self.rng, self.sample_size
        )
        
        introspector.metadata = {
            "test.child": tmeta_child,
            "test.parent1": tmeta_parent1,
            "test.parent2": tmeta_parent2
        }
        
        fks = [
            FKMeta("fk1", "test", "child", "parent1_id", "test", "parent1", "id", False, None),
            FKMeta("fk2", "test", "child", "parent2_id", "test", "parent2", "id", False, None)
        ]
        
        table_map = {
            "test.child": self.config[0],
            "test.parent1": self.config[1],
            "test.parent2": self.config[2]
        }
        
        introspector.detect_forced_explicit_parents(fks, [], table_map)
        
        # Both parents should be marked for explicit PK generation
        self.assertIn("test.parent1", introspector.forced_explicit_parents)
        self.assertIn("test.parent2", introspector.forced_explicit_parents)


if __name__ == '__main__':
    unittest.main()
