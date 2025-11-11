def count_vowels(text):
    #Count the number of vowels in the given text.
    vowels = "aeiouAEIOU"
    return sum(1 for char in text if char in vowels)

def reverse_string(text):
    #Reverse the given string.
    return text[::-1]