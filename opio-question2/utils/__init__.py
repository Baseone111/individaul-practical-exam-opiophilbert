# This file makes the utils directory a Python package
# Import key functions to make them easily accessible
from .math import factorial, gcd, fibonacci
from .string import count_vowels, reverse_string

__all__ = ['factorial', 'gcd', 'fibonacci', 'count_vowels', 'reverse_string']