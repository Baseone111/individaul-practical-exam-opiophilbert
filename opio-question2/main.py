from utils import factorial, gcd, fibonacci, count_vowels, reverse_string

def demonstrate_math_utils():
    print("=== MATH UTILITIES ")
    print(f"Factorial of 5: {factorial(5)}")
    print(f"GCD of 48 and 18: {gcd(48, 18)}")
    print(f"Fibonacci sequence (8 terms): {fibonacci(8)}")
    print()

def demonstrate_string_utils():
    print("=== STRING UTILITIES ===")
    test_string = "Hello World"
    print(f"Original string: '{test_string}'")
    print(f"Vowel count: {count_vowels(test_string)}")
    print(f"Reversed string: '{reverse_string(test_string)}'")

def main():
    demonstrate_math_utils()
    demonstrate_string_utils()

if __name__ == "__main__":
    main()