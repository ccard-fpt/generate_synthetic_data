import random
import string
from datetime import datetime, timedelta
from generate_synthetic_data_patterns import CompiledPatterns

def rand_name():
    """Generate a random name using pre-compiled patterns."""
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", 
                   "James", "Mary", "William", "Patricia", "Richard", "Jennifer", "Thomas"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", 
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson"]
    
    name = f"{random.choice(first_names)} {random.choice(last_names)}"
    
    # Validate using pre-compiled pattern
    if CompiledPatterns.NAME_PATTERN.match(name):
        return name
    return "John Doe"  # Fallback

def rand_email():
    """Generate a random email using pre-compiled patterns."""
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "company.com"]
    username = ''.join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(5, 10)))
    email = f"{username}@{random.choice(domains)}"
    
    # Validate using pre-compiled pattern
    if CompiledPatterns.EMAIL_PATTERN.match(email):
        return email
    return "user@example.com"  # Fallback

def rand_phone():
    """Generate a random phone number using pre-compiled patterns."""
    area_code = random.randint(200, 999)
    exchange = random.randint(200, 999)
    number = random.randint(1000, 9999)
    phone = f"{area_code}-{exchange}-{number}"
    
    # Validate using pre-compiled pattern
    if CompiledPatterns.PHONE_PATTERN.match(phone):
        return phone
    return "555-555-5555"  # Fallback

def rand_address():
    """Generate a random address."""
    street_numbers = random.randint(1, 9999)
    street_names = ["Main St", "Oak Ave", "Pine Rd", "Maple Dr", "Cedar Ln", 
                    "Elm St", "Washington Ave", "Park Pl", "Broadway", "Market St"]
    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", 
              "Philadelphia", "San Antonio", "San Diego", "Dallas", "San Jose"]
    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "FL", "OH", "MI", "GA"]
    
    address = f"{street_numbers} {random.choice(street_names)}, {random.choice(cities)}, {random.choice(states)} {random.randint(10000, 99999)}"
    return address

def rand_date(start_year=2020, end_year=2026):
    """Generate a random date between start_year and end_year."""
    start_date = datetime(start_year, 1, 1)
    end_date = datetime(end_year, 12, 31)
    time_between = end_date - start_date
    random_days = random.randrange(time_between.days)
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime("%Y-%m-%d")

def rand_ssn():
    """Generate a random SSN using pre-compiled patterns."""
    area = random.randint(100, 999)
    group = random.randint(10, 99)
    serial = random.randint(1000, 9999)
    ssn = f"{area}-{group}-{serial}"
    
    # Validate using pre-compiled pattern
    if CompiledPatterns.SSN_PATTERN.match(ssn):
        return ssn
    return "123-45-6789"  # Fallback

def rand_credit_card():
    """Generate a random credit card number using pre-compiled patterns."""
    # Generate a simple 16-digit number (not a valid card via Luhn algorithm)
    card_number = ''.join([str(random.randint(0, 9)) for _ in range(16)])
    formatted = f"{card_number[:4]}-{card_number[4:8]}-{card_number[8:12]}-{card_number[12:]}"
    
    # Validate using pre-compiled pattern
    if CompiledPatterns.CREDIT_CARD_PATTERN.match(formatted):
        return formatted
    return "1234-5678-9012-3456"  # Fallback

def validate_data_format(data_type, value):
    """Validate data format using pre-compiled patterns."""
    pattern_map = {
        'email': CompiledPatterns.EMAIL_PATTERN,
        'phone': CompiledPatterns.PHONE_PATTERN,
        'name': CompiledPatterns.NAME_PATTERN,
        'ssn': CompiledPatterns.SSN_PATTERN,
        'credit_card': CompiledPatterns.CREDIT_CARD_PATTERN
    }
    
    pattern = pattern_map.get(data_type)
    if pattern:
        return bool(pattern.match(value))
    return False

def sanitize_string(text):
    """Sanitize string using pre-compiled patterns."""
    # Remove special characters except spaces and basic punctuation
    return CompiledPatterns.SPECIAL_CHARS_PATTERN.sub('', text)

def extract_numbers(text):
    """Extract all numbers from text using pre-compiled patterns."""
    return CompiledPatterns.NUMBER_PATTERN.findall(text)

def rand_company_name():
    """Generate a random company name."""
    prefixes = ["Tech", "Data", "Cloud", "Smart", "Digital", "Global", "Innovative"]
    suffixes = ["Solutions", "Systems", "Technologies", "Corp", "Inc", "Group", "Labs"]
    return f"{random.choice(prefixes)} {random.choice(suffixes)}"

def rand_product_name():
    """Generate a random product name."""
    adjectives = ["Premium", "Pro", "Elite", "Ultimate", "Advanced", "Standard", "Basic"]
    products = ["Widget", "Gadget", "Tool", "Device", "System", "Platform", "Service"]
    return f"{random.choice(adjectives)} {random.choice(products)}"
