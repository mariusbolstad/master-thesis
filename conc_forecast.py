from concurrent.futures import ProcessPoolExecutor

def hello(name):
    print("hello")
    print(name)
    
    
def main():
    names = ["erik", "ole", "stina"]


    results_list = []
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(hello, name) for name in names]
        for future in futures:
            results_list.append(future.result())
            
            
if __name__ == "__main__":
    main()