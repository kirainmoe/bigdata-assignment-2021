import sys
import pandas as pd

args_total = len(sys.argv)

frames = []

for index in range(1, args_total):
    filename = sys.argv[index]
    dataframe = pd.read_csv(filename)
    frames.append(dataframe)

merged_dataframe = pd.concat(frames, ignore_index=True)
result = merged_dataframe.drop(merged_dataframe.columns[0], axis=1)
result = result.sample(frac=1).reset_index(drop=True)
result.to_csv("dataset_merged.csv")
