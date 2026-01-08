#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import hashlib, hmac, re, random, sys
import os, json
from typing import Optional, List, Dict, Any, Tuple
from generate_synthetic_data_patterns import CompiledPatterns
from decimal import Decimal, InvalidOperation

class ColumnConstraint:
    """Represents a constraint on a column (CHECK, DEFAULT, etc.)."""
    def __init__(self, constraint_type: str, definition: str):
        self.constraint_type = constraint_type.upper()
        self.definition = definition

    def __repr__(self):
        return f"ColumnConstraint(type={self.constraint_type}, def={self.definition})"

class ForeignKeyConstraint:
    """Represents a foreign key relationship."""
    def __init__(self, column_name: str, ref_table: str, ref_column: str, on_delete: str = "NO ACTION", on_update: str = "NO ACTION"):
        self.column_name = column_name
        self.ref_table = ref_table
        self.ref_column = ref_column
        self.on_delete = on_delete.upper()
        self.on_update = on_update.upper()

    def __repr__(self):
        return (f"ForeignKeyConstraint({self.column_name} -> "
                f"{self.ref_table}.{self.ref_column}, "
                f"ON DELETE {self.on_delete}, ON UPDATE {self.on_update})")

class ColumnDefinition:
    """Represents a column in a table."""
    def __init__(self, name: str, column_type: str, is_nullable: bool = True,
                 default_value: Optional[str] = None, is_primary_key: bool = False,
                 is_auto_increment: bool = False, character_maximum_length: Optional[int] = None,
                 numeric_precision: Optional[int] = None, numeric_scale: Optional[int] = None,
                 extra: Optional[str] = None):
        self.name = name
        self.column_type = column_type.upper()
        self.is_nullable = is_nullable
        self.default_value = default_value
        self.is_primary_key = is_primary_key
        self.is_auto_increment = is_auto_increment
        self.character_maximum_length = character_maximum_length
        self.numeric_precision = numeric_precision
        self.numeric_scale = numeric_scale
        self.extra = extra or ""
        self.constraints: List[ColumnConstraint] = []

    def add_constraint(self, constraint: ColumnConstraint):
        self.constraints.append(constraint)

    def __repr__(self):
        return (f"ColumnDefinition(name={self.name}, type={self.column_type}, "
                f"nullable={self.is_nullable}, pk={self.is_primary_key}, "
                f"auto_inc={self.is_auto_increment})")

class TableDefinition:
    """Represents a table schema."""
    def __init__(self, name: str, schema: str = "public"):
        self.name = name
        self.schema = schema
        self.columns: List[ColumnDefinition] = []
        self.foreign_keys: List[ForeignKeyConstraint] = []

    def add_column(self, column: ColumnDefinition):
        self.columns.append(column)

    def add_foreign_key(self, fk: ForeignKeyConstraint):
        self.foreign_keys.append(fk)

    def get_primary_key_columns(self) -> List[ColumnDefinition]:
        return [col for col in self.columns if col.is_primary_key]

    def get_column(self, name: str) -> Optional[ColumnDefinition]:
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None

    def __repr__(self):
        return f"TableDefinition(name={self.name}, columns={len(self.columns)}, fks={len(self.foreign_keys)})"

class DataGenerator:
    """Handles generation of synthetic data based on column types and constraints."""
    
    def __init__(self, seed: Optional[int] = None):
        if seed is not None:
            random.seed(seed)
        
        # Common data pools
        self.first_names = [
            "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
            "William", "Barbara", "David", "Elizabeth", "Richard", "Susan", "Joseph", "Jessica",
            "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
            "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
            "Steven", "Kimberly", "Paul", "Emily", "Andrew", "Donna", "Joshua", "Michelle",
            "Kenneth", "Dorothy", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
            "Edward", "Deborah", "Ronald", "Stephanie", "Timothy", "Rebecca", "Jason", "Sharon",
            "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
            "Nicholas", "Shirley", "Eric", "Angela", "Jonathan", "Helen", "Stephen", "Anna",
            "Larry", "Brenda", "Justin", "Pamela", "Scott", "Nicole", "Brandon", "Emma",
            "Benjamin", "Samantha", "Samuel", "Katherine", "Raymond", "Christine", "Gregory", "Debra",
            "Frank", "Rachel", "Alexander", "Catherine", "Patrick", "Carolyn", "Raymond", "Janet",
            "Jack", "Ruth", "Dennis", "Maria", "Jerry", "Heather", "Tyler", "Diane",
            "Aaron", "Virginia", "Jose", "Julie", "Adam", "Joyce", "Henry", "Victoria",
            "Nathan", "Olivia", "Douglas", "Kelly", "Zachary", "Christina", "Peter", "Lauren",
            "Kyle", "Joan", "Walter", "Evelyn", "Ethan", "Judith", "Jeremy", "Megan",
            "Harold", "Cheryl", "Keith", "Andrea", "Christian", "Hannah", "Roger", "Martha",
            "Noah", "Jacqueline", "Gerald", "Frances", "Carl", "Gloria", "Terry", "Ann",
            "Sean", "Teresa", "Austin", "Kathryn", "Arthur", "Sara", "Lawrence", "Janice"
        ]
        
        self.last_names = [
            "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
            "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
            "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White",
            "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker", "Young",
            "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
            "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
            "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
            "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
            "Cook", "Rogers", "Gutierrez", "Ortiz", "Morgan", "Cooper", "Peterson", "Bailey",
            "Reed", "Kelly", "Howard", "Ramos", "Kim", "Cox", "Ward", "Richardson",
            "Watson", "Brooks", "Chavez", "Wood", "James", "Bennett", "Gray", "Mendoza",
            "Ruiz", "Hughes", "Price", "Alvarez", "Castillo", "Sanders", "Patel", "Myers",
            "Long", "Ross", "Foster", "Jimenez", "Powell", "Jenkins", "Perry", "Russell",
            "Sullivan", "Bell", "Coleman", "Butler", "Henderson", "Barnes", "Gonzales", "Fisher",
            "Vasquez", "Simmons", "Romero", "Jordan", "Patterson", "Alexander", "Hamilton", "Graham",
            "Reynolds", "Griffin", "Wallace", "Moreno", "West", "Cole", "Hayes", "Bryant",
            "Herrera", "Gibson", "Ellis", "Tran", "Medina", "Aguilar", "Stevens", "Murray",
            "Ford", "Castro", "Marshall", "Owens", "Harrison", "Fernandez", "McDonald", "Woods",
            "Washington", "Kennedy", "Wells", "Vargas", "Henry", "Chen", "Freeman", "Webb"
        ]
        
        self.cities = [
            "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
            "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Fort Worth", "Columbus",
            "Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
            "Boston", "El Paso", "Nashville", "Detroit", "Oklahoma City", "Portland", "Las Vegas",
            "Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno",
            "Mesa", "Sacramento", "Atlanta", "Kansas City", "Colorado Springs", "Omaha",
            "Raleigh", "Miami", "Long Beach", "Virginia Beach", "Oakland", "Minneapolis", "Tulsa",
            "Tampa", "Arlington", "New Orleans", "Wichita", "Cleveland", "Bakersfield", "Aurora",
            "Anaheim", "Honolulu", "Santa Ana", "Riverside", "Corpus Christi", "Lexington"
        ]
        
        self.states = [
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"
        ]
        
        self.domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"]
        
        self.street_names = [
            "Main", "Oak", "Maple", "Cedar", "Elm", "Washington", "Lake", "Hill",
            "Park", "River", "Pine", "Church", "Spring", "Sunset", "Forest", "Broadway"
        ]
        
        self.street_types = ["St", "Ave", "Blvd", "Rd", "Dr", "Ln", "Ct", "Way"]

    def sanitize_identifier(self, s: Optional[str]) -> str:
        """Convert a string to a safe identifier (alphanumeric + underscore)."""
        return CompiledPatterns.NON_ALPHANUMERIC.sub("_", s or "")

    def generate_email(self, first_name: Optional[str] = None, last_name: Optional[str] = None) -> str:
        """Generate a synthetic email address."""
        if not first_name:
            first_name = random.choice(self.first_names)
        if not last_name:
            last_name = random.choice(self.last_names)
        
        domain = random.choice(self.domains)
        
        # Clean names for email
        digits = CompiledPatterns.NON_DIGIT.sub("", value)
        fname = first_name.lower().replace(" ", "")
        lname = last_name.lower().replace(" ", "")
        
        patterns = [
            f"{fname}.{lname}@{domain}",
            f"{fname}{lname}@{domain}",
            f"{fname[0]}{lname}@{domain}",
            f"{fname}_{lname}@{domain}",
            f"{fname}{random.randint(1, 999)}@{domain}"
        ]
        
        return random.choice(patterns)

    def generate_phone(self) -> str:
        """Generate a synthetic US phone number."""
        area_code = random.randint(200, 999)
        exchange = random.randint(200, 999)
        number = random.randint(1000, 9999)
        
        formats = [
            f"({area_code}) {exchange}-{number}",
            f"{area_code}-{exchange}-{number}",
            f"{area_code}.{exchange}.{number}",
            f"{area_code}{exchange}{number}"
        ]
        
        return random.choice(formats)

    def generate_username(self, name: Optional[str] = None) -> str:
        """Generate a synthetic username."""
        if not name:
            name = f"{random.choice(self.first_names)}{random.choice(self.last_names)}"
        
        uname = CompiledPatterns.NON_ALPHANUM_DOT.sub(".", name. lower()). strip(".")
        
        # Sometimes add a number suffix
        if random.random() < 0.3:
            uname += str(random.randint(1, 999))
        
        return uname

    def generate_address(self) -> str:
        """Generate a synthetic street address."""
        number = random.randint(1, 9999)
        street = random.choice(self.street_names)
        street_type = random.choice(self.street_types)
        
        return f"{number} {street} {street_type}"

    def generate_city(self) -> str:
        """Generate a city name."""
        return random.choice(self.cities)

    def generate_state(self) -> str:
        """Generate a US state code."""
        return random.choice(self.states)

    def generate_zip(self) -> str:
        """Generate a synthetic ZIP code."""
        if random.random() < 0.7:
            return f"{random.randint(10000, 99999)}"
        else:
            return f"{random.randint(10000, 99999)}-{random.randint(1000, 9999)}"

    def generate_value_for_column(self, col: ColumnDefinition, table_data: Dict[str, List[Dict]] = None) -> Any:
        """
        Generate a synthetic value for a given column based on its type and constraints.
        table_data is a dict: table_name -> list of row dicts (for FK lookups).
        """
        col_name_lower = col.name.lower()
        col_type = col.column_type.upper()
        
        # Handle auto-increment columns
        if col.is_auto_increment:
            return None  # Will be handled by the database
        
        # Handle default values
        if col.default_value and col.default_value.upper() not in ("NULL", "CURRENT_TIMESTAMP"):
            return col.default_value.strip("'\"")
        
        # Check for age-related columns
        if CompiledPatterns.AGE_PATTERN.search(col.name):
            return random.randint(18, 80)
        
        # Name-based heuristics
        if "email" in col_name_lower:
            return self.generate_email()
        elif "phone" in col_name_lower or "tel" in col_name_lower or "mobile" in col_name_lower:
            return self.generate_phone()
        elif "username" in col_name_lower or "user_name" in col_name_lower:
            return self.generate_username()
        elif "first_name" in col_name_lower or "firstname" in col_name_lower or "fname" in col_name_lower:
            return random.choice(self.first_names)
        elif "last_name" in col_name_lower or "lastname" in col_name_lower or "lname" in col_name_lower:
            return random.choice(self.last_names)
        elif "name" in col_name_lower and "user" not in col_name_lower:
            return f"{random.choice(self.first_names)} {random.choice(self.last_names)}"
        elif "address" in col_name_lower and "email" not in col_name_lower:
            return self.generate_address()
        elif "city" in col_name_lower:
            return self.generate_city()
        elif "state" in col_name_lower:
            return self.generate_state()
        elif "zip" in col_name_lower or "postal" in col_name_lower:
            return self.generate_zip()
        elif "country" in col_name_lower:
            return random.choice(["USA", "Canada", "UK", "Germany", "France", "Australia"])
        elif "description" in col_name_lower or "comment" in col_name_lower or "notes" in col_name_lower:
            return f"Sample {col.name} text for testing purposes."
        
        # Type-based generation
        if "INT" in col_type or "SERIAL" in col_type or "BIGINT" in col_type or "SMALLINT" in col_type:
            # Check for UNSIGNED or constraint info
            min_val = 1
            max_val = 100000
            
            if "price" in col_name_lower or "cost" in col_name_lower or "amount" in col_name_lower:
                min_val = 1
                max_val = 10000
            elif "quantity" in col_name_lower or "count" in col_name_lower:
                min_val = 1
                max_val = 1000
            elif "year" in col_name_lower:
                min_val = 1900
                max_val = 2030
            
            return random.randint(min_val, max_val)
        
        elif "DECIMAL" in col_type or "NUMERIC" in col_type or "FLOAT" in col_type or "DOUBLE" in col_type or "REAL" in col_type:
            precision = col.numeric_precision or 10
            scale = col.numeric_scale or 2
            
            max_val = 10 ** (precision - scale) - 1
            val = random.uniform(0, max_val)
            
            if scale:
                return round(val, scale)
            return val
        
        elif "CHAR" in col_type or "TEXT" in col_type or "VARCHAR" in col_type or "CLOB" in col_type:
            # Check for ENUM or SET
            if "ENUM" in col_type or "SET" in col_type:
                m = CompiledPatterns.ENUM_PATTERN.findall(col.column_type or "")
                if m:
                    return random.choice(m).replace("''", "'")
            
            # Check for SET specifically
            if "SET" in col_type:
                m = CompiledPatterns.ENUM_PATTERN.findall(col.column_type or "")
                if m:
                    # For SET, can pick multiple values
                    choices = [v.replace("''", "'") for v in m]
                    num_picks = random.randint(1, min(3, len(choices)))
                    return ",".join(random.sample(choices, num_picks))
            
            max_len = col.character_maximum_length or 50
            max_len = min(max_len, 255)  # Cap for practicality
            
            # Generate random text
            length = random.randint(5, max_len) if max_len > 5 else max_len
            chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 "
            return "".join(random.choice(chars) for _ in range(length)).strip()
        
        elif "BOOL" in col_type or "BIT(1)" in col_type:
            return random.choice([True, False])
        
        elif "DATE" in col_type:
            year = random.randint(2000, 2025)
            month = random.randint(1, 12)
            day = random.randint(1, 28)  # Safe for all months
            return f"{year:04d}-{month:02d}-{day:02d}"
        
        elif "TIME" in col_type and "DATETIME" not in col_type and "TIMESTAMP" not in col_type:
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            return f"{hour:02d}:{minute:02d}:{second:02d}"
        
        elif "DATETIME" in col_type or "TIMESTAMP" in col_type:
            year = random.randint(2000, 2025)
            month = random.randint(1, 12)
            day = random.randint(1, 28)
            hour = random.randint(0, 23)
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"
        
        elif "BLOB" in col_type or "BINARY" in col_type or "BYTEA" in col_type:
            # Generate random bytes
            length = random.randint(10, 100)
            return bytes(random.randint(0, 255) for _ in range(length))
        
        elif "JSON" in col_type or "JSONB" in col_type:
            return json.dumps({"key": "value", "number": random.randint(1, 100)})
        
        elif "UUID" in col_type:
            import uuid
            return str(uuid.uuid4())
        
        # Fallback
        return f"value_{random.randint(1000, 9999)}"

    def generate_fk_value(self, fk: ForeignKeyConstraint, table_data: Dict[str, List[Dict]]) -> Any:
        """
        Generate a foreign key value by selecting from available referenced table data.
        """
        if fk.ref_table not in table_data or not table_data[fk.ref_table]:
            return None  # No data to reference
        
        # Pick a random row from the referenced table
        ref_row = random.choice(table_data[fk.ref_table])
        return ref_row.get(fk.ref_column)

def parse_fk_condition(condition_str: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse a foreign key condition string like "column_name = 'ref_table.ref_column'"
    Returns (column_name, ref_spec) where ref_spec is "ref_table.ref_column"
    """
    match = CompiledPatterns.FK_CONDITION_PATTERN.match(condition_str)
    if match:
        return match.group(1), match.group(2)
    return None, None

def format_value_for_sql(value: Any, col: ColumnDefinition) -> str:
    """
    Format a value for SQL INSERT statement based on column type.
    """
    if value is None:
        return "NULL"
    
    col_type = col.column_type.upper()
    
    # String types
    if any(t in col_type for t in ["CHAR", "TEXT", "VARCHAR", "CLOB", "ENUM", "SET", "DATE", "TIME", "UUID"]):
        # Escape single quotes
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
    
    # Binary types
    elif any(t in col_type for t in ["BLOB", "BINARY", "BYTEA"]):
        if isinstance(value, bytes):
            # Convert to hex string
            return f"X'{value.hex()}'"
        return f"'{value}'"
    
    # JSON types
    elif "JSON" in col_type:
        escaped = json.dumps(value).replace("'", "''")
        return f"'{escaped}'"
    
    # Boolean types
    elif "BOOL" in col_type or "BIT(1)" in col_type:
        return "TRUE" if value else "FALSE"
    
    # Numeric types - return as-is
    else:
        return str(value)

def generate_insert_statements(table_def: TableDefinition, num_rows: int, 
                               table_data: Dict[str, List[Dict]] = None,
                               generator: DataGenerator = None) -> List[str]:
    """
    Generate INSERT statements for a table.
    
    Args:
        table_def: Table definition
        num_rows: Number of rows to generate
        table_data: Existing table data for FK resolution (dict: table_name -> list of row dicts)
        generator: DataGenerator instance to use
    
    Returns:
        List of SQL INSERT statements
    """
    if generator is None:
        generator = DataGenerator()
    
    if table_data is None:
        table_data = {}
    
    statements = []
    rows_data = []  # Store generated rows for this table
    
    for _ in range(num_rows):
        row = {}
        values = []
        
        # Generate values for each column
        for col in table_def.columns:
            # Skip auto-increment columns
            if col.is_auto_increment:
                continue
            
            # Check if this column is a foreign key
            fk = None
            for fk_constraint in table_def.foreign_keys:
                if fk_constraint.column_name == col.name:
                    fk = fk_constraint
                    break
            
            if fk:
                value = generator.generate_fk_value(fk, table_data)
            else:
                value = generator.generate_value_for_column(col, table_data)
            
            row[col.name] = value
            values.append(format_value_for_sql(value, col))
        
        # Build INSERT statement
        col_names = [col.name for col in table_def.columns if not col.is_auto_increment]
        cols_str = ", ".join(col_names)
        vals_str = ", ".join(values)
        
        statement = f"INSERT INTO {table_def.schema}.{table_def.name} ({cols_str}) VALUES ({vals_str});"
        statements.append(statement)
        rows_data.append(row)
    
    # Store generated data for this table
    if table_def.name not in table_data:
        table_data[table_def.name] = []
    table_data[table_def.name].extend(rows_data)
    
    return statements

def escape_sql_string(value: str) -> str:
    """Escape a string for use in SQL."""
    return value.replace("'", "''").replace("\\", "\\\\")

def parse_default_value(default_str: Optional[str]) -> Optional[str]:
    """
    Parse a default value expression from schema.
    Returns None if it's NULL, CURRENT_TIMESTAMP, or other special values.
    """
    if not default_str:
        return None
    
    default_upper = default_str.upper().strip()
    
    if default_upper in ("NULL", "CURRENT_TIMESTAMP", "NOW()", "GETDATE()"):
        return None
    
    # Remove surrounding quotes if present
    if (default_str.startswith("'") and default_str.endswith("'")) or \
       (default_str.startswith('"') and default_str.endswith('"')):
        return default_str[1:-1]
    
    return default_str

def validate_generated_data(row_data: Dict[str, Any], table_def: TableDefinition) -> Tuple[bool, Optional[str]]:
    """
    Validate that generated row data meets basic constraints.
    
    Returns:
        (is_valid, error_message)
    """
    for col in table_def.columns:
        value = row_data.get(col.name)
        
        # Check NOT NULL constraint
        if not col.is_nullable and value is None and not col.is_auto_increment:
            return False, f"Column {col.name} cannot be NULL"
        
        # Check string length
        if col.character_maximum_length and isinstance(value, str):
            if len(value) > col.character_maximum_length:
                return False, f"Column {col.name} exceeds max length {col.character_maximum_length}"
        
        # Check numeric precision (basic)
        if col.numeric_precision and isinstance(value, (int, float, Decimal)):
            try:
                if isinstance(value, float):
                    value = Decimal(str(value))
                # This is a simplified check
                str_val = str(value).replace(".", "").replace("-", "")
                if len(str_val) > col.numeric_precision:
                    return False, f"Column {col.name} exceeds numeric precision {col.numeric_precision}"
            except (InvalidOperation, ValueError):
                pass
    
    return True, None

def parse_enum_or_set_values(type_definition: str) -> List[str]:
    """
    Parse ENUM or SET type definition to extract allowed values.
    Example: "ENUM('value1','value2','value3')" -> ["value1", "value2", "value3"]
    """
    m = CompiledPatterns.ENUM_PATTERN.findall(set_definition or "")
    return [v.replace("''", "'") for v in m]

def calculate_data_size(value: Any) -> int:
    """
    Estimate the size in bytes of a data value.
    """
    if value is None:
        return 0
    elif isinstance(value, bool):
        return 1
    elif isinstance(value, int):
        return 8
    elif isinstance(value, float):
        return 8
    elif isinstance(value, str):
        return len(value.encode('utf-8'))
    elif isinstance(value, bytes):
        return len(value)
    elif isinstance(value, (list, dict)):
        return len(json.dumps(value).encode('utf-8'))
    else:
        return len(str(value).encode('utf-8'))

def is_user_variable(value: str) -> bool:
    """Check if a value is a user variable (starts with @)."""
    if value.startswith("@") and CompiledPatterns.USER_VAR_PATTERN.match(value):
        return True
    return False

def generate_batch_inserts(table_def: TableDefinition, num_rows: int,
                           batch_size: int = 1000,
                           table_data: Dict[str, List[Dict]] = None,
                           generator: DataGenerator = None) -> List[str]:
    """
    Generate batch INSERT statements (multi-row inserts) for better performance.
    
    Args:
        table_def: Table definition
        num_rows: Total number of rows to generate
        batch_size: Number of rows per INSERT statement
        table_data: Existing table data for FK resolution
        generator: DataGenerator instance to use
    
    Returns:
        List of batch SQL INSERT statements
    """
    if generator is None:
        generator = DataGenerator()
    
    if table_data is None:
        table_data = {}
    
    statements = []
    rows_data = []
    
    col_names = [col.name for col in table_def.columns if not col.is_auto_increment]
    cols_str = ", ".join(col_names)
    
    batch_values = []
    
    for i in range(num_rows):
        row = {}
        values = []
        
        for col in table_def.columns:
            if col.is_auto_increment:
                continue
            
            # Check for FK
            fk = None
            for fk_constraint in table_def.foreign_keys:
                if fk_constraint.column_name == col.name:
                    fk = fk_constraint
                    break
            
            if fk:
                value = generator.generate_fk_value(fk, table_data)
            else:
                value = generator.generate_value_for_column(col, table_data)
            
            row[col.name] = value
            values.append(format_value_for_sql(value, col))
        
        batch_values.append(f"({', '.join(values)})")
        rows_data.append(row)
        
        # Create batch insert when batch is full or at the end
        if len(batch_values) >= batch_size or i == num_rows - 1:
            values_str = ",\n  ".join(batch_values)
            statement = f"INSERT INTO {table_def.schema}.{table_def.name} ({cols_str})\nVALUES\n  {values_str};"
            statements.append(statement)
            batch_values = []
    
    # Store generated data
    if table_def.name not in table_data:
        table_data[table_def.name] = []
    table_data[table_def.name].extend(rows_data)
    
    return statements
