def factorial(n):
    """Calculate factorial of a number."""
    if n < 0:
        raise ValueError("Factorial is not defined for negative numbers")
    result = 1
    for i in range(1, n + 1):
        result *= i
    return result

def gcd(a, b):
    """Calculate Greatest Common Divisor using Euclidean algorithm."""
    while b:
        a, b = b, a % b
    return a

def fibonacci(n):
    """Generate Fibonacci sequence up to n terms."""
    sequence = []
    a, b = 0, 1
    for _ in range(n):
        sequence.append(a)
        a, b = b, a + b
    return sequence