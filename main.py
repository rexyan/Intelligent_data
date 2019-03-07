import dask.dataframe as dd
from Utils import DataFrameColumnsName

df = dd.read_csv("D:/test_data.csv")
name = ["id_card", "username", "age", "gender", "job", "phone_number", "email", "registration_time", "region", "annual_consumption"]
df = DataFrameColumnsName.add_new_columns_name(df, *name)


# print(df.columns.values)
# print(df.head()) # 默认取前4
print(df.__len__())


