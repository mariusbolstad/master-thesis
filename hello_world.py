import concurrent.futures

def hello_world(process_number):
    return f"Hello from process {process_number}"

def main():
    with concurrent.futures.ProcessPoolExecutor() as executor:
        # Map `hello_world` function across values
        results = executor.map(hello_world, range(4))
        
        for result in results:
            print(result)

if __name__ == "__main__":
    main()
