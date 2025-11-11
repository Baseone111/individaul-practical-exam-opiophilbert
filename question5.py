"""
Question 5: Concurrency on Real Data - Professional Solution
Author: [Your Name]
Date: November 11, 2025

This script demonstrates threading and multiprocessing concepts using real-world datasets.
"""

import threading
import time
import requests
import pandas as pd
import io
from multiprocessing import Process, Queue
import sys



def print_numbers(thread_name):
    """
    Function for each thread to print its name and numbers 1-5.
    
    Args:
        thread_name (str): Name of the thread
    """
    for i in range(1, 6):
        print(f"{thread_name}: {i}")
        time.sleep(1)

def question_5a():
    """
    Solution for Question 5a: Creates three threads that print numbers 1-5.
    """
    print("=" * 60)
    print("QUESTION 5A: Basic Threading")
    print("=" * 60)
    
    # Create three threads
    threads = []
    for i in range(1, 4):
        thread = threading.Thread(target=print_numbers, args=(f"Thread-{i}",))
        threads.append(thread)
    
    # Start all threads
    for thread in threads:
        thread.start()
    
    # Wait for all threads to complete
    for thread in threads:
        thread.join()
    
    print("\nAll threads complete.")
    print()

# ============================================================================
# QUESTION 5B: Real Dataset Processing with Threading
# ============================================================================

# Dataset URLs
DATASETS = {
    'population': 'https://raw.githubusercontent.com/datasets/population/master/data/population.csv',
    'covid': 'https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv',
    'temperature': 'https://datahub.io/core/global-temp/r/annual.csv'
}

# Thread-safe storage for downloaded data
data_lock = threading.Lock()
downloaded_data = {}

def download_dataset(dataset_name, url):
    """
    Downloads a dataset from the given URL.
    
    Args:
        dataset_name (str): Name identifier for the dataset
        url (str): URL to download from
    """
    print(f"[{dataset_name}] Starting download from {url}")
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Store data in thread-safe manner
        with data_lock:
            downloaded_data[dataset_name] = response.text
        
        print(f"[{dataset_name}] Download completed successfully")
    except Exception as e:
        print(f"[{dataset_name}] Error downloading: {str(e)}")
        with data_lock:
            downloaded_data[dataset_name] = None

def process_population_data():
    """
    Thread 1: Compute total world population for 2020.
    """
    print("\n[Population Analysis] Starting processing...")
    
    try:
        with data_lock:
            csv_data = downloaded_data.get('population')
        
        if csv_data is None:
            print("[Population Analysis] No data available")
            return
        
        df = pd.read_csv(io.StringIO(csv_data))
        
        # Filter for year 2020 and sum population
        if 'Year' in df.columns and 'Value' in df.columns:
            pop_2020 = df[df['Year'] == 2020]['Value'].sum()
            print(f"[Population Analysis] Total World Population (2020): {pop_2020:,.0f}")
        else:
            print("[Population Analysis] Expected columns not found")
            
    except Exception as e:
        print(f"[Population Analysis] Error: {str(e)}")

def process_covid_data():
    """
    Thread 2: Compute total new COVID cases.
    """
    print("\n[COVID Analysis] Starting processing...")
    
    try:
        with data_lock:
            csv_data = downloaded_data.get('covid')
        
        if csv_data is None:
            print("[COVID Analysis] No data available")
            return
        
        df = pd.read_csv(io.StringIO(csv_data))
        
        # Sum total new cases
        if 'new_cases' in df.columns:
            total_cases = df['new_cases'].sum()
            print(f"[COVID Analysis] Total New COVID Cases: {total_cases:,.0f}")
        else:
            print("[COVID Analysis] 'new_cases' column not found")
            
    except Exception as e:
        print(f"[COVID Analysis] Error: {str(e)}")

def process_temperature_data():
    """
    Thread 3: Compute average global temperature.
    """
    print("\n[Temperature Analysis] Starting processing...")
    
    try:
        with data_lock:
            csv_data = downloaded_data.get('temperature')
        
        if csv_data is None:
            print("[Temperature Analysis] No data available")
            return
        
        df = pd.read_csv(io.StringIO(csv_data))
        
        # Calculate average temperature
        # Look for common temperature column names
        temp_columns = ['Mean', 'MEAN', 'mean', 'temperature', 'Temperature']
        temp_col = None
        
        for col in temp_columns:
            if col in df.columns:
                temp_col = col
                break
        
        if temp_col:
            avg_temp = df[temp_col].mean()
            print(f"[Temperature Analysis] Average Global Temperature: {avg_temp:.2f}°C")
        else:
            print(f"[Temperature Analysis] Temperature column not found. Available columns: {df.columns.tolist()}")
            
    except Exception as e:
        print(f"[Temperature Analysis] Error: {str(e)}")

def question_5b_part_a():
    """
    Part A: Multithreaded Download of datasets.
    """
    print("=" * 60)
    print("QUESTION 5B - PART A: Multithreaded Download")
    print("=" * 60)
    
    # Create download threads
    download_threads = []
    for name, url in DATASETS.items():
        thread = threading.Thread(target=download_dataset, args=(name, url))
        download_threads.append(thread)
    
    # Start all download threads
    for thread in download_threads:
        thread.start()
    
    # Wait for all downloads to complete
    for thread in download_threads:
        thread.join()
    
    print("\n[Main Thread] All downloads completed")

def question_5b_part_b():
    """
    Part B: Concurrent Data Processing.
    """
    print("\n" + "=" * 60)
    print("QUESTION 5B - PART B: Concurrent Data Processing")
    print("=" * 60)
    
    # Create processing threads
    processing_threads = [
        threading.Thread(target=process_population_data, name="PopulationThread"),
        threading.Thread(target=process_covid_data, name="COVIDThread"),
        threading.Thread(target=process_temperature_data, name="TemperatureThread")
    ]
    
    # Start all processing threads
    for thread in processing_threads:
        thread.start()
    
    # Wait for all processing to complete
    for thread in processing_threads:
        thread.join()
    
    print("\n[Main Thread] All processing completed")

# ============================================================================
# QUESTION 5B - PART C (BONUS): Multiprocessing Comparison
# ============================================================================

def download_and_process_multiprocess(dataset_name, url, result_queue):
    """
    Function for multiprocessing: downloads and processes data.
    
    Args:
        dataset_name (str): Name of the dataset
        url (str): URL to download from
        result_queue (Queue): Queue to store results
    """
    try:
        # Download
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Simple processing based on dataset
        df = pd.read_csv(io.StringIO(response.text))
        
        if dataset_name == 'population':
            result = df[df['Year'] == 2020]['Value'].sum() if 'Year' in df.columns else 0
        elif dataset_name == 'covid':
            result = df['new_cases'].sum() if 'new_cases' in df.columns else 0
        elif dataset_name == 'temperature':
            temp_cols = ['Mean', 'MEAN', 'mean']
            temp_col = next((col for col in temp_cols if col in df.columns), None)
            result = df[temp_col].mean() if temp_col else 0
        else:
            result = 0
        
        result_queue.put((dataset_name, result))
    except Exception as e:
        result_queue.put((dataset_name, f"Error: {str(e)}"))

def question_5b_part_c():
    """
    Part C (Bonus): Compare threading vs multiprocessing performance.
    """
    print("\n" + "=" * 60)
    print("QUESTION 5B - PART C (BONUS): Performance Comparison")
    print("=" * 60)
    
    # Test 1: Threading approach
    print("\n[Performance Test] Running with Threading...")
    start_threading = time.perf_counter()
    
    # Clear previous data
    downloaded_data.clear()
    
    # Download with threads
    download_threads = []
    for name, url in DATASETS.items():
        thread = threading.Thread(target=download_dataset, args=(name, url))
        download_threads.append(thread)
        thread.start()
    
    for thread in download_threads:
        thread.join()
    
    # Process with threads
    processing_threads = [
        threading.Thread(target=process_population_data),
        threading.Thread(target=process_covid_data),
        threading.Thread(target=process_temperature_data)
    ]
    
    for thread in processing_threads:
        thread.start()
    
    for thread in processing_threads:
        thread.join()
    
    end_threading = time.perf_counter()
    threading_time = end_threading - start_threading
    
    # Test 2: Multiprocessing approach
    print("\n[Performance Test] Running with Multiprocessing...")
    start_multiprocess = time.perf_counter()
    
    result_queue = Queue()
    processes = []
    
    for name, url in DATASETS.items():
        process = Process(target=download_and_process_multiprocess, 
                         args=(name, url, result_queue))
        processes.append(process)
        process.start()
    
    for process in processes:
        process.join()
    
    # Retrieve results
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    end_multiprocess = time.perf_counter()
    multiprocess_time = end_multiprocess - start_multiprocess
    
    # Display comparison
    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON RESULTS")
    print("=" * 60)
    print(f"Threading Runtime:       {threading_time:.4f} seconds")
    print(f"Multiprocessing Runtime: {multiprocess_time:.4f} seconds")
    print(f"Difference:              {abs(threading_time - multiprocess_time):.4f} seconds")
    
    if threading_time < multiprocess_time:
        speedup = multiprocess_time / threading_time
        print(f"Threading was {speedup:.2f}x faster")
        print("\nNote: For I/O-bound tasks (like network requests), threading")
        print("is often more efficient due to lower overhead.")
    else:
        speedup = threading_time / multiprocess_time
        print(f"Multiprocessing was {speedup:.2f}x faster")
        print("\nNote: For CPU-bound tasks, multiprocessing typically")
        print("performs better by utilizing multiple CPU cores.")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """
    Main execution function that runs all parts of Question 5.
    """
    print("\n" + "=" * 60)
    print("CONCURRENCY ON REAL DATA - COMPLETE SOLUTION")
    print("=" * 60)
    print()
    
    try:
        # Question 5a: Basic Threading
        question_5a()
        time.sleep(1)  # Brief pause for readability
        
        # Question 5b Part A: Download datasets
        question_5b_part_a()
        time.sleep(1)
        
        # Question 5b Part B: Process datasets
        question_5b_part_b()
        time.sleep(1)
        
        # Question 5b Part C: Performance comparison (Bonus)
        question_5b_part_c()
        
        print("\n" + "=" * 60)
        print("ALL TASKS COMPLETED SUCCESSFULLY")
        print("=" * 60)
        
    except KeyboardInterrupt:
        print("\n\nExecution interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\nUnexpected error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()