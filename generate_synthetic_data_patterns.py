#!/usr/bin/env python3
"""Pre-compiled regex patterns for performance optimization"""
import re

class CompiledPatterns:
    """
    Pre-compiled regex patterns to avoid repeated compilation.
    
    Performance benefit: Patterns are compiled once at module import time
    rather than on every use.
    """
    
    # Column name detection patterns
    AGE_PATTERN = re.compile(r"age|years?", re.I)
    
    # SQL parsing patterns for ENUM/SET extraction
    ENUM_PATTERN = re.compile(r"'((?:[^']|(?:''))*)'")
