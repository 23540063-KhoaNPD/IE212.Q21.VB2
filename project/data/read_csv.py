import pandas as pd

chunk_size = 10  # adjust based on memory
chunks = pd.read_csv("final_dataset.csv", chunksize=chunk_size)

for chunk in chunks:
    # process each chunk
    print(chunk.head())