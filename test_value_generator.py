#!/usr/bin/env python3
"""Unit tests for ValueGenerator class"""
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


class TestValueGenerator(unittest.TestCase):
    """Test cases for ValueGenerator functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        # Create minimal ValueGenerator setup
        self.metadata = {}
        self.unique_constraints = {}
        self.fk_columns = {}
        self.populate_columns_config = {}
        self.static_samples = {}
        self.fks = []
        
        # Mock args
        MockArgs = namedtuple("Args", ["seed", "threads"])
        self.args = MockArgs(seed=42, threads=1)
    
    def test_column_classification_basic(self):
        """Test basic column classification"""
        # Setup test table with various column types
        table_key = "test.orders"
        cols = [
            ColumnMeta("id", "int", "NO", "int", "PRI", "auto_increment", None, 10, 0, None),
            ColumnMeta("email", "varchar", "NO", "varchar(100)", "UNI", "", 100, None, None, None),
            ColumnMeta("user_id", "int", "NO", "int", "MUL", "", None, 10, 0, None),
            ColumnMeta("amount", "decimal", "NO", "decimal(10,2)", "", "", None, 10, 2, None)
        ]
        tmeta = TableMeta("test", "orders", cols, ["id"], True, "InnoDB")
        
        self.metadata[table_key] = tmeta
        self.unique_constraints[table_key] = [
            UniqueConstraint("uk_email", ("email",))
        ]
        self.fk_columns[table_key] = {"user_id"}
        
        # Verify table setup
        self.assertEqual(len(tmeta.columns), 4)
        self.assertEqual(tmeta.pk_columns, ["id"])
        self.assertTrue(tmeta.auto_increment)
    
    def test_unique_constraint_validation_pass(self):
        """Test unique constraint validation passes for unique values"""
        from value_generator import ValueGenerator
        
        generator = ValueGenerator(
            self.metadata, self.unique_constraints, self.fk_columns,
            self.populate_columns_config, self.static_samples, self.fks, self.args
        )
        
        composite_constraints = [
            UniqueConstraint("uk_user_org", ("user_id", "org_id"))
        ]
        
        row = {"user_id": 1, "org_id": 100}
        local_trackers = {"uk_user_org": set()}
        
        valid = generator._validate_unique_constraints(
            "test.table", row, composite_constraints, local_trackers, 0)
        
        self.assertTrue(valid)
        self.assertIn((1, 100), local_trackers["uk_user_org"])
    
    def test_unique_constraint_validation_fail(self):
        """Test unique constraint validation fails for duplicate values"""
        from value_generator import ValueGenerator
        
        generator = ValueGenerator(
            self.metadata, self.unique_constraints, self.fk_columns,
            self.populate_columns_config, self.static_samples, self.fks, self.args
        )
        
        composite_constraints = [
            UniqueConstraint("uk_user_org", ("user_id", "org_id"))
        ]
        
        row = {"user_id": 1, "org_id": 100}
        local_trackers = {"uk_user_org": {(1, 100)}}  # Already exists
        
        valid = generator._validate_unique_constraints(
            "test.table", row, composite_constraints, local_trackers, 1)
        
        self.assertFalse(valid)
    
    def test_unique_constraint_validation_with_nulls(self):
        """Test unique constraint validation with NULL values"""
        from value_generator import ValueGenerator
        
        generator = ValueGenerator(
            self.metadata, self.unique_constraints, self.fk_columns,
            self.populate_columns_config, self.static_samples, self.fks, self.args
        )
        
        composite_constraints = [
            UniqueConstraint("uk_user_org", ("user_id", "org_id"))
        ]
        
        row = {"user_id": None, "org_id": 100}
        local_trackers = {"uk_user_org": set()}
        
        valid = generator._validate_unique_constraints(
            "test.table", row, composite_constraints, local_trackers, 0)
        
        # NULL values should not be tracked in UNIQUE constraints
        self.assertTrue(valid)
        self.assertEqual(len(local_trackers["uk_user_org"]), 0)


class TestValueGeneratorEdgeCases(unittest.TestCase):
    """Test edge cases and error conditions"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.metadata = {}
        self.unique_constraints = {}
        self.fk_columns = {}
        self.populate_columns_config = {}
        self.static_samples = {}
        self.fks = []
        
        MockArgs = namedtuple("Args", ["seed", "threads"])
        self.args = MockArgs(seed=42, threads=1)
    
    def test_empty_table_metadata(self):
        """Test handling empty table metadata"""
        from value_generator import ValueGenerator
        
        generator = ValueGenerator(
            self.metadata, self.unique_constraints, self.fk_columns,
            self.populate_columns_config, self.static_samples, self.fks, self.args
        )
        
        # Should not crash with empty metadata
        self.assertEqual(len(generator.metadata), 0)
        self.assertEqual(len(generator.global_unique_value_pools), 0)
    
    def test_sequential_counter_thread_safety(self):
        """Test sequential counter initialization"""
        from value_generator import ValueGenerator
        
        generator = ValueGenerator(
            self.metadata, self.unique_constraints, self.fk_columns,
            self.populate_columns_config, self.static_samples, self.fks, self.args
        )
        
        col = ColumnMeta("counter", "int", "NO", "int", "", "", None, 10, 0, None)
        
        # First call should return 0
        val1 = generator._handle_sequential_generation("test.table", "counter", col)
        self.assertEqual(val1, 0)
        
        # Second call should increment
        val2 = generator._handle_sequential_generation("test.table", "counter", col)
        self.assertEqual(val2, 1)


if __name__ == '__main__':
    unittest.main()