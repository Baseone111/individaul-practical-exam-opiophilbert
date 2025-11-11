# Question 4: Functional Programming on Real Data
import csv
import urllib.request
from functools import reduce
import time


# 1. Read the CSV using the csv module

url = "https://raw.githubusercontent.com/datasets/population/master/data/population.csv"
response = urllib.request.urlopen(url)
lines = [l.decode('utf-8') for l in response.readlines()]
reader = csv.DictReader(lines)

data = list(reader)  # convert iterator to list for reuse
print(f"Total records loaded: {len(data)}")

# Show immutability: print a small preview of original data
print("\nOriginal Data Sample (before transformations):")
print(data[:3])

# 2. Filter rows for Year == "2020"

data_2020 = list(filter(lambda row: row["Year"] == "2020", data))


# 3. Map to tuples (Country Name, Population)

country_population = list(
    map(lambda row: (row["Country Name"], int(float(row["Value"]))), data_2020)
)


# 4. Sort and print top 5 most populated countries

top5 = sorted(country_population, key=lambda x: x[1], reverse=True)[:5]

print("\nTop 5 most populated countries in 2020:")
for country, pop in top5:
    print(f"{country}: {pop:,}")


# 5. Compute total world population using reduce()

total_population = reduce(lambda acc, x: acc + x[1], country_population, 0)
print(f"\nTotal World Population (2020): {total_population:,}")


# 6. Average population for African countries using filter + reduce

# For simplicity, we'll use a known list of African countries
african_countries = {
    'Nigeria', 'Ethiopia', 'Egypt', 'DR Congo', 'Tanzania', 'South Africa',
    'Kenya', 'Uganda', 'Algeria', 'Sudan', 'Morocco', 'Angola', 'Ghana',
    'Mozambique', 'Madagascar', 'Cameroon', 'Côte d\'Ivoire', 'Niger',
    'Burkina Faso', 'Mali', 'Malawi', 'Zambia', 'Senegal', 'Chad', 'Somalia',
    'Zimbabwe', 'Guinea', 'Rwanda', 'Benin', 'Burundi'
}

africa_data = list(filter(lambda x: x[0] in african_countries, country_population))
africa_total = reduce(lambda acc, x: acc + x[1], africa_data, 0)
africa_avg = africa_total / len(africa_data) if africa_data else 0
print(f"\nAverage Population for African countries (2020): {africa_avg:,.0f}")


# 7. Show immutability of data

print("\nData after transformations (still original structure unchanged):")
print(data[:3])  # unchanged since filter/map/reduce don't mutate


# 8. Implement a higher-order function apply_and_log(func, iterable)

def apply_and_log(func, iterable):
    print(f"\nApplying {func.__name__} to data...")
    result = list(map(func, iterable))
    print("First 5 results:", result[:5])
    return result

# Example usage
apply_and_log(lambda x: (x[0], x[1] * 2), top5)


# 9. Create a composed functional pipeline for top 5 populated countries
#
def compose(*funcs):
    """Function composition utility"""
    return lambda x: reduce(lambda acc, f: f(acc), reversed(funcs), x)

def filter_2020(data):
    return list(filter(lambda r: r["Year"] == "2020", data))

def to_country_pop(data):
    return list(map(lambda r: (r["Country Name"], int(float(r["Value"]))), data))

def sort_top5(data):
    return sorted(data, key=lambda x: x[1], reverse=True)[:5]

pipeline = compose(sort_top5, to_country_pop, filter_2020)
top5_pipeline = pipeline(data)

print("\nFunctional Pipeline Result (Top 5 countries):")
for c, p in top5_pipeline:
    print(f"{c}: {p:,}")


# 10. (Bonus) Compare performance: Functional vs List Comprehension

start_func = time.time()
_ = list(map(lambda x: (x[0], x[1] * 2), country_population))
end_func = time.time()

start_list = time.time()
_ = [(x[0], x[1] * 2) for x in country_population]
end_list = time.time()

print("\nPerformance comparison:")
print(f"Functional map() time: {end_func - start_func:.6f}s")
print(f"List comprehension time: {end_list - start_list:.6f}s")