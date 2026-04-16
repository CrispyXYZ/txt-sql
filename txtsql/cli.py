import sys
from .engine import execute_sql

def print_table(rows):
    """Prints a list of dictionaries as a formatted ASCII table."""
    if not rows:
        print("Empty set.")
        return

    # Extract headers
    headers = list(rows[0].keys())
    
    # Calculate column widths
    widths = {h: len(h) for h in headers}
    for row in rows:
        for h in headers:
            val = str(row.get(h, ''))
            widths[h] = max(widths[h], len(val))
    
    # Header
    header_str = " | ".join(h.ljust(widths[h]) for h in headers)
    print(header_str)
    print("-+-".join("-" * widths[h] for h in headers))
    
    # Rows
    for row in rows:
        print(" | ".join(str(row.get(h, '')).ljust(widths[h]) for h in headers))

def run_cli():
    """
    Starts an interactive CLI loop.
    Reads SQL input, executes it, and prints the result in a table format.
    """
    print("Welcome to TxTSQL CLI. Type your SQL statement and press Enter.")
    print("Type 'exit' or 'quit' to exit.")
    
    while True:
        try:
            line = input("txtsql> ")
            if line.strip().lower() in ('exit', 'quit'):
                break
            if not line.strip():
                continue
                
            result = execute_sql(line.strip())
            
            if isinstance(result, list):
                print_table(result)
            elif result is not None:
                print(result)
            else:
                print("Query OK.")
                
        except EOFError:
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    run_cli()
