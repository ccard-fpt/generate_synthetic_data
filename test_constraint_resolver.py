#!/usr/bin/env python3
"""Unit tests for ConstraintResolver class"""
import unittest
import random
from collections import namedtuple
from constraint_resolver import ConstraintResolver

# Mock data structures
UniqueConstraint = namedtuple("UniqueConstraint", ["constraint_name", "columns"])


class TestConstraintResolver(unittest.TestCase):
    """Test cases for ConstraintResolver functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.metadata = {}
        self.unique_constraints = {}
        self.fk_columns = {}
        self.resolver = ConstraintResolver(
            self.metadata, self.unique_constraints, self.fk_columns)
    
    def test_classify_single_column_unique(self):
        """Test classification of single-column UNIQUE constraints"""
        table_key = "test_schema.test_table"
        self.unique_constraints[table_key] = [
            UniqueConstraint("uk_email", ("email",)),
            UniqueConstraint("uk_username", ("username",))
        ]
        
        single, composite, comp_cols = self.resolver.classify_unique_constraints(table_key)
        
        self.assertEqual(single, {"email", "username"})
        self.assertEqual(len(composite), 0)
        self.assertEqual(len(comp_cols), 0)
    
    def test_classify_composite_unique(self):
        """Test classification of composite UNIQUE constraints"""
        table_key = "test_schema.test_table"
        self.unique_constraints[table_key] = [
            UniqueConstraint("uk_user_org", ("user_id", "org_id")),
            UniqueConstraint("uk_name_date", ("name", "created_date"))
        ]
        
        single, composite, comp_cols = self.resolver.classify_unique_constraints(table_key)
        
        self.assertEqual(len(single), 0)
        self.assertEqual(len(composite), 2)
        self.assertEqual(comp_cols, {"user_id", "org_id", "name", "created_date"})
    
    def test_build_cartesian_product(self):
        """Test building Cartesian product of value lists"""
        value_lists = [
            [1, 2],
            ['a', 'b'],
            [True, False]
        ]
        
        product = self.resolver.build_cartesian_product(value_lists)
        
        self.assertEqual(len(product), 8)
        self.assertIn((1, 'a', True), product)
        self.assertIn((2, 'b', False), product)


if __name__ == '__main__':
    unittest.main()
