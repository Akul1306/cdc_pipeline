# import kagglehub
# import os
# import pandas as pd

# path = kagglehub.dataset_download("neelesh0602/bankcsv")

# print("Dataset location:", path)
# print("Files:", os.listdir(path))

# # df = pd.read_csv(os.path.join(path, "banking.csv"))

# # print(df.columns.tolist())
# # print(df.head())
# # print(df.dtypes)

# import oracledb

# conn = oracledb.connect(
#     user="system",
#     password="oracle",
#     dsn="localhost:1521/XEPDB1"
# )

# print("Connected to Oracle!")


import kagglehub
import os
import shutil

# -----------------------------------
# 1. Download Kaggle dataset
# -----------------------------------

path = kagglehub.dataset_download(
    "ksabishek/massive-bank-dataset-1-million-rows"
)

print("Kaggle dataset location:", path)
print("Files:", os.listdir(path))


# -----------------------------------
# 2. Locate the Excel file
# -----------------------------------

excel_path = os.path.join(path, "bankdataset.xlsx")

if not os.path.exists(excel_path):
    raise FileNotFoundError(
        f"Could not find bankdataset.xlsx in {path}"
    )

print("Excel file found:", excel_path)


# -----------------------------------
# 3. Save a copy in your CDC project
# -----------------------------------

project_folder = r"C:\Users\akul6\Documents\Nehish\cdc"

saved_path = os.path.join(
    project_folder,
    "bankdataset.xlsx"
)

shutil.copy2(excel_path, saved_path)

print("Saved Excel file to:")
print(saved_path)
