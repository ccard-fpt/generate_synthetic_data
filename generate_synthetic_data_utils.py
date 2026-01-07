#!/usr/bin/env python3
"""Utility functions and data structures for synthetic data generation"""
import hashlib, hmac, re, random, sys
from datetime import datetime, timedelta
from collections import namedtuple
from generate_synthetic_data_patterns import CompiledPatterns

# Data structures
Transaction = namedtuple('Transaction', ['timestamp', 'card_number', 'amount', 'merchant', 'category', 'location'])

# Constants
CARD_TYPES = {
    '4': 'Visa',
    '5': 'MasterCard',
    '3': 'American Express',
    '6': 'Discover'
}

MERCHANTS = [
    'Amazon', 'Walmart', 'Target', 'Best Buy', 'Home Depot',
    'Starbucks', 'McDonalds', 'Shell Gas', 'Chevron', 'Whole Foods',
    'CVS Pharmacy', 'Walgreens', 'Costco', 'Apple Store', 'Netflix'
]

CATEGORIES = [
    'Groceries', 'Gas', 'Dining', 'Shopping', 'Entertainment',
    'Travel', 'Healthcare', 'Utilities', 'Insurance', 'Other'
]

CITIES = [
    'New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix',
    'Philadelphia', 'San Antonio', 'San Diego', 'Dallas', 'San Jose'
]

def luhn_checksum(card_number):
    """Calculate the Luhn checksum for a card number"""
    def digits_of(n):
        return [int(d) for d in str(n)]
    
    digits = digits_of(card_number)
    odd_digits = digits[-1::-2]
    even_digits = digits[-2::-2]
    checksum = sum(odd_digits)
    for d in even_digits:
        checksum += sum(digits_of(d * 2))
    return checksum % 10

def generate_card_number(prefix='4'):
    """Generate a valid credit card number using Luhn algorithm"""
    card_number = prefix + ''.join([str(random.randint(0, 9)) for _ in range(14)])
    check_digit = (10 - luhn_checksum(int(card_number))) % 10
    return card_number + str(check_digit)

def generate_transaction(card_number, base_date):
    """Generate a single transaction"""
    timestamp = base_date + timedelta(
        days=random.randint(0, 365),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    amount = round(random.uniform(5.0, 500.0), 2)
    merchant = random.choice(MERCHANTS)
    category = random.choice(CATEGORIES)
    location = random.choice(CITIES)
    
    return Transaction(timestamp, card_number, amount, merchant, category, location)

def hash_card_number(card_number, secret_key):
    """Hash a card number using HMAC-SHA256"""
    return hmac.new(
        secret_key.encode(),
        card_number.encode(),
        hashlib.sha256
    ).hexdigest()

def validate_transaction(transaction):
    """Validate a transaction object"""
    if not isinstance(transaction, Transaction):
        return False
    if not transaction.card_number or len(transaction.card_number) != 16:
        return False
    if transaction.amount <= 0:
        return False
    if transaction.merchant not in MERCHANTS:
        return False
    if transaction.category not in CATEGORIES:
        return False
    if transaction.location not in CITIES:
        return False
    return True

def format_transaction(transaction):
    """Format a transaction for output"""
    return f"{transaction.timestamp.strftime('%Y-%m-%d %H:%M:%S')},{transaction.card_number},{transaction.amount:.2f},{transaction.merchant},{transaction.category},{transaction.location}"

def parse_transaction(line):
    """Parse a transaction from a CSV line"""
    parts = line.strip().split(',')
    if len(parts) != 6:
        return None
    
    try:
        timestamp = datetime.strptime(parts[0], '%Y-%m-%d %H:%M:%S')
        card_number = parts[1]
        amount = float(parts[2])
        merchant = parts[3]
        category = parts[4]
        location = parts[5]
        
        return Transaction(timestamp, card_number, amount, merchant, category, location)
    except (ValueError, IndexError):
        return None
