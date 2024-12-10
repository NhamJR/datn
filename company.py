from vnstock import *
import pandas as pd
import numpy as np
'''
list_company = listing_companies()
if list_company.empty :
    print("No data")
else :
    list_company.to_csv("company.csv", index = False, header = True)
    schema = list_company.dtypes
    print("Schema của file company:")
    print(schema)
    print("Done!")
    '''
stock_symbols = pd.read_csv("company.csv")["ticker"].to_list()
print(stock_symbols)
print(pd.__version__)
