from pathlib import Path
import pandas as pd

def process_interactions():
    # Get project root directory (assuming the script is in src/data)
    project_root = Path(__file__).parent.parent.parent
    
    # Set input and output paths
    input_file = project_root / 'data' / 'raw' / 'interaction_NPInterv5.txt'
    output_dir = project_root / 'data' / 'processed'
    output_file = output_dir / 'NPInter5_processed_data.csv'

    # Define columns to keep in output
    columns_to_keep = ['ncName', 'ncID', 'tarName', 'tarID', 'tag', 'class']

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Processing file: {input_file}")
    print(f"Output directory: {output_dir}")

    # List to store filtered data
    filtered_data = []

    # Read large file line by line
    with input_file.open('r', encoding='utf-8') as f:
        # Read header
        header = f.readline().strip().split('\t')
        
        # Find required column indices
        try:
            tag_class_idx = header.index('class')
            level_idx = header.index('level')
            # Get indices for columns to keep
            keep_indices = [header.index(col) for col in columns_to_keep]
        except ValueError as e:
            print(f"Error: Required column names not found. Please check the file format.")
            print(f"Available columns: {header}")
            return

        # Process data line by line
        for i, line in enumerate(f, 1):
            if i % 100000 == 0:  # Print progress every 100,000 lines
                print(f"Processed {i} lines...")
                
            fields = line.strip().split('\t')
            if len(fields) != len(header):
                continue  # Skip lines with incorrect format
                
            # Check conditions: class is binding and level is RNA-Protein
            if (fields[tag_class_idx].lower() == 'binding' and 
                fields[level_idx].lower() == 'rna-protein'):
                # Only keep specified columns
                filtered_row = {columns_to_keep[i]: fields[keep_indices[i]] 
                              for i in range(len(columns_to_keep))}
                filtered_data.append(filtered_row)

    # Convert results to DataFrame and save as CSV
    if filtered_data:
        df = pd.DataFrame(filtered_data)
        # Ensure columns are in the specified order
        df = df[columns_to_keep]
        df.to_csv(output_file, index=False)
        print(f"Processing complete! Found {len(filtered_data)} matching records.")
        print(f"Results saved to: {output_file}")
        print(f"Columns in output: {', '.join(columns_to_keep)}")
    else:
        print("No matching data found.")

if __name__ == '__main__':
    process_interactions()