import re
import random
import string
from datetime import datetime, timedelta
from collections import namedtuple
from generate_synthetic_data_patterns import CompiledPatterns

# Named tuple for column metadata
ColumnMetadata = namedtuple('ColumnMetadata', ['name', 'data_type', 'column_type', 'is_nullable'])

class SyntheticDataGenerator:
    """
    A utility class for generating synthetic data based on column metadata.
    """
    
    def __init__(self, seed=None):
        """
        Initialize the synthetic data generator.
        
        Args:
            seed (int, optional): Random seed for reproducibility
        """
        if seed is not None:
            random.seed(seed)
    
    @staticmethod
    def generate_string(length=10, nullable=False, null_probability=0.1):
        """
        Generate a random string.
        
        Args:
            length (int): Length of the string
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            
        Returns:
            str or None: Generated string or None
        """
        if nullable and random.random() < null_probability:
            return None
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    
    @staticmethod
    def generate_integer(min_val=0, max_val=1000, nullable=False, null_probability=0.1):
        """
        Generate a random integer.
        
        Args:
            min_val (int): Minimum value
            max_val (int): Maximum value
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            
        Returns:
            int or None: Generated integer or None
        """
        if nullable and random.random() < null_probability:
            return None
        return random.randint(min_val, max_val)
    
    @staticmethod
    def generate_float(min_val=0.0, max_val=1000.0, nullable=False, null_probability=0.1, precision=2):
        """
        Generate a random float.
        
        Args:
            min_val (float): Minimum value
            max_val (float): Maximum value
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            precision (int): Number of decimal places
            
        Returns:
            float or None: Generated float or None
        """
        if nullable and random.random() < null_probability:
            return None
        value = random.uniform(min_val, max_val)
        return round(value, precision)
    
    @staticmethod
    def generate_boolean(nullable=False, null_probability=0.1):
        """
        Generate a random boolean.
        
        Args:
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            
        Returns:
            bool or None: Generated boolean or None
        """
        if nullable and random.random() < null_probability:
            return None
        return random.choice([True, False])
    
    @staticmethod
    def generate_date(start_date=None, end_date=None, nullable=False, null_probability=0.1):
        """
        Generate a random date.
        
        Args:
            start_date (datetime): Start date range
            end_date (datetime): End date range
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            
        Returns:
            datetime or None: Generated date or None
        """
        if nullable and random.random() < null_probability:
            return None
        
        if start_date is None:
            start_date = datetime(2000, 1, 1)
        if end_date is None:
            end_date = datetime.now()
        
        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randint(0, days_between)
        
        return start_date + timedelta(days=random_days)
    
    @staticmethod
    def generate_email(domain="example.com", nullable=False, null_probability=0.1):
        """
        Generate a random email address.
        
        Args:
            domain (str): Email domain
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            
        Returns:
            str or None: Generated email or None
        """
        if nullable and random.random() < null_probability:
            return None
        
        username_length = random.randint(5, 15)
        username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=username_length))
        return f"{username}@{domain}"
    
    @staticmethod
    def generate_phone(format_pattern="###-###-####", nullable=False, null_probability=0.1):
        """
        Generate a random phone number.
        
        Args:
            format_pattern (str): Phone number format (# replaced with digit)
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            
        Returns:
            str or None: Generated phone number or None
        """
        if nullable and random.random() < null_probability:
            return None
        
        phone = ""
        for char in format_pattern:
            if char == '#':
                phone += str(random.randint(0, 9))
            else:
                phone += char
        return phone
    
    def generate_for_column(self, col):
        """
        Generate synthetic data based on column metadata.
        
        Args:
            col (ColumnMetadata): Column metadata
            
        Returns:
            Generated value appropriate for the column type
        """
        nullable = col.is_nullable
        
        # Check for age-related columns
        if CompiledPatterns.AGE_PATTERN.search(col.name):
            return self.generate_integer(min_val=18, max_val=100, nullable=nullable)
        
        # Check for email columns
        if re.search(r"email", col.name, re.I):
            return self.generate_email(nullable=nullable)
        
        # Check for phone columns
        if re.search(r"phone|tel", col.name, re.I):
            return self.generate_phone(nullable=nullable)
        
        # Check for date columns
        if re.search(r"date|time", col.name, re.I) or col.data_type in ['date', 'datetime', 'timestamp']:
            return self.generate_date(nullable=nullable)
        
        # Generate based on data type
        if col.data_type in ['int', 'integer', 'bigint', 'smallint']:
            return self.generate_integer(nullable=nullable)
        elif col.data_type in ['float', 'double', 'decimal', 'numeric']:
            return self.generate_float(nullable=nullable)
        elif col.data_type in ['bool', 'boolean']:
            return self.generate_boolean(nullable=nullable)
        elif col.data_type in ['varchar', 'char', 'text', 'string']:
            return self.generate_string(nullable=nullable)
        else:
            return self.generate_string(nullable=nullable)
    
    def generate_dataset(self, columns, num_rows=100):
        """
        Generate a complete synthetic dataset.
        
        Args:
            columns (list): List of ColumnMetadata objects
            num_rows (int): Number of rows to generate
            
        Returns:
            list: List of dictionaries representing rows
        """
        dataset = []
        for _ in range(num_rows):
            row = {}
            for col in columns:
                row[col.name] = self.generate_for_column(col)
            dataset.append(row)
        return dataset


class EnumDataGenerator:
    """
    Specialized generator for enum-type columns.
    """
    
    @staticmethod
    def extract_enum_values(column_type):
        """
        Extract enum values from column type definition.
        
        Args:
            column_type (str): Column type string (e.g., "enum('value1','value2')")
            
        Returns:
            list: List of enum values
        """
        if not column_type:
            return []
        
        m = CompiledPatterns.ENUM_PATTERN.findall(col.column_type or "")
        return [v.replace("''", "'") for v in m]
    
    @staticmethod
    def generate_enum_value(column_type, nullable=False, null_probability=0.1):
        """
        Generate a value from an enum column type.
        
        Args:
            column_type (str): Column type string
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            
        Returns:
            str or None: Random enum value or None
        """
        if nullable and random.random() < null_probability:
            return None
        
        m = CompiledPatterns.ENUM_PATTERN.findall(col.column_type or "")
        values = [v.replace("''", "'") for v in m]
        
        if not values:
            return None
        
        return random.choice(values)


class SetDataGenerator:
    """
    Specialized generator for set-type columns.
    """
    
    @staticmethod
    def extract_set_values(set_definition):
        """
        Extract set values from set type definition.
        
        Args:
            set_definition (str): Set type string (e.g., "set('value1','value2')")
            
        Returns:
            list: List of set values
        """
        if not set_definition:
            return []
        
        # Match single-quoted strings, handling escaped quotes
        pattern = r"'((?:[^']|(?:''))*)'"
        matches = re.findall(pattern, set_definition)
        return [m.replace("''", "'") for m in matches]
    
    @staticmethod
    def generate_set_value(set_definition, nullable=False, null_probability=0.1, 
                          min_values=1, max_values=None):
        """
        Generate a value from a set column type.
        
        Args:
            set_definition (str): Set type string
            nullable (bool): Whether the value can be null
            null_probability (float): Probability of generating null
            min_values (int): Minimum number of values to select
            max_values (int): Maximum number of values to select
            
        Returns:
            str or None: Comma-separated set values or None
        """
        if nullable and random.random() < null_probability:
            return None
        
        values = SetDataGenerator.extract_set_values(set_definition)
        
        if not values:
            return None
        
        if max_values is None:
            max_values = len(values)
        
        max_values = min(max_values, len(values))
        min_values = min(min_values, max_values)
        
        num_values = random.randint(min_values, max_values)
        selected = random.sample(values, num_values)
        
        return ','.join(selected)


class NameDataGenerator:
    """
    Specialized generator for name-related columns.
    """
    
    FIRST_NAMES = [
        'James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda',
        'William', 'Barbara', 'David', 'Elizabeth', 'Richard', 'Susan', 'Joseph', 'Jessica',
        'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
        'Matthew', 'Betty', 'Anthony', 'Margaret', 'Mark', 'Sandra', 'Donald', 'Ashley',
        'Steven', 'Kimberly', 'Paul', 'Emily', 'Andrew', 'Donna', 'Joshua', 'Michelle'
    ]
    
    LAST_NAMES = [
        'Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
        'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
        'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
        'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker',
        'Young', 'Allen', 'King', 'Wright', 'Scott', 'Torres', 'Nguyen', 'Hill', 'Flores'
    ]
    
    @staticmethod
    def generate_first_name(nullable=False, null_probability=0.1):
        """Generate a random first name."""
        if nullable and random.random() < null_probability:
            return None
        return random.choice(NameDataGenerator.FIRST_NAMES)
    
    @staticmethod
    def generate_last_name(nullable=False, null_probability=0.1):
        """Generate a random last name."""
        if nullable and random.random() < null_probability:
            return None
        return random.choice(NameDataGenerator.LAST_NAMES)
    
    @staticmethod
    def generate_full_name(nullable=False, null_probability=0.1):
        """Generate a random full name."""
        if nullable and random.random() < null_probability:
            return None
        first = random.choice(NameDataGenerator.FIRST_NAMES)
        last = random.choice(NameDataGenerator.LAST_NAMES)
        return f"{first} {last}"


class AddressDataGenerator:
    """
    Specialized generator for address-related columns.
    """
    
    STREET_NAMES = [
        'Main', 'Oak', 'Pine', 'Maple', 'Cedar', 'Elm', 'Washington', 'Lake',
        'Hill', 'Park', 'First', 'Second', 'Third', 'Fourth', 'Fifth'
    ]
    
    STREET_TYPES = ['St', 'Ave', 'Blvd', 'Dr', 'Ln', 'Rd', 'Way', 'Ct']
    
    CITIES = [
        'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia',
        'San Antonio', 'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville',
        'Fort Worth', 'Columbus', 'San Francisco', 'Charlotte', 'Indianapolis',
        'Seattle', 'Denver', 'Washington'
    ]
    
    STATES = [
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
    ]
    
    @staticmethod
    def generate_street_address(nullable=False, null_probability=0.1):
        """Generate a random street address."""
        if nullable and random.random() < null_probability:
            return None
        number = random.randint(1, 9999)
        street = random.choice(AddressDataGenerator.STREET_NAMES)
        street_type = random.choice(AddressDataGenerator.STREET_TYPES)
        return f"{number} {street} {street_type}"
    
    @staticmethod
    def generate_city(nullable=False, null_probability=0.1):
        """Generate a random city name."""
        if nullable and random.random() < null_probability:
            return None
        return random.choice(AddressDataGenerator.CITIES)
    
    @staticmethod
    def generate_state(nullable=False, null_probability=0.1):
        """Generate a random state code."""
        if nullable and random.random() < null_probability:
            return None
        return random.choice(AddressDataGenerator.STATES)
    
    @staticmethod
    def generate_zip_code(nullable=False, null_probability=0.1):
        """Generate a random ZIP code."""
        if nullable and random.random() < null_probability:
            return None
        return f"{random.randint(10000, 99999)}"
    
    @staticmethod
    def generate_full_address(nullable=False, null_probability=0.1):
        """Generate a complete random address."""
        if nullable and random.random() < null_probability:
            return None
        street = AddressDataGenerator.generate_street_address()
        city = AddressDataGenerator.generate_city()
        state = AddressDataGenerator.generate_state()
        zip_code = AddressDataGenerator.generate_zip_code()
        return f"{street}, {city}, {state} {zip_code}"


class CompanyDataGenerator:
    """
    Specialized generator for company-related data.
    """
    
    COMPANY_PREFIXES = [
        'Tech', 'Global', 'Digital', 'Smart', 'Dynamic', 'Innovative', 'Advanced',
        'Premier', 'Elite', 'Superior', 'Mega', 'Ultra', 'Prime', 'Alpha', 'Beta'
    ]
    
    COMPANY_SUFFIXES = [
        'Corp', 'Inc', 'LLC', 'Ltd', 'Group', 'Associates', 'Partners',
        'Solutions', 'Systems', 'Services', 'Technologies', 'Industries'
    ]
    
    @staticmethod
    def generate_company_name(nullable=False, null_probability=0.1):
        """Generate a random company name."""
        if nullable and random.random() < null_probability:
            return None
        prefix = random.choice(CompanyDataGenerator.COMPANY_PREFIXES)
        suffix = random.choice(CompanyDataGenerator.COMPANY_SUFFIXES)
        return f"{prefix} {suffix}"


def extract_set_values_from_definition(set_definition):
    """
    Utility function to extract values from a SET column definition.
    
    Args:
        set_definition (str): SET type definition string
        
    Returns:
        list: Extracted values
    """
    if not set_definition:
        return []
    
    m = CompiledPatterns.ENUM_PATTERN.findall(set_definition or "")
    return [v.replace("''", "'") for v in m]


def create_column_metadata(name, data_type, column_type=None, is_nullable=True):
    """
    Helper function to create ColumnMetadata objects.
    
    Args:
        name (str): Column name
        data_type (str): Data type
        column_type (str, optional): Full column type definition
        is_nullable (bool): Whether column accepts null values
        
    Returns:
        ColumnMetadata: Column metadata object
    """
    return ColumnMetadata(
        name=name,
        data_type=data_type,
        column_type=column_type,
        is_nullable=is_nullable
    )
