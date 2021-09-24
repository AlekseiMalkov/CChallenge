import pandas as pd
import numpy as np


# this file is to generate data for testing.
# ideally border cases need to be simulated in separate folders.


input_path = "../input/"
       
df_input = pd.read_json("../sample_input\events.txt")


def random_dates(start, end, n=10):
    start_u = start.value//10**9
    end_u = end.value//10**9

    return pd.to_datetime(np.random.randint(start_u, end_u, n), unit='s')

def random_customers(n = 100 ):
    customers_array_len = 20
    df = pd.DataFrame({
        "customer" : [rf"customer_{chr(i)}" for i  in range(ord('a'), ord('a')+customers_array_len)],
        "customer_keys" : [rf"key_{i}" for i in range(0, customers_array_len)]}
    )
    i = np.random.randint(0, customers_array_len, n)
    return df.iloc[i]

# generate nfiles, size of file is up to 2 in power nMaxPower
def GenFiles(nFile = 5, nMaxPower = 5):
    for i in range (1, nFile):
        df = df_input
        for j in range(1, np.random.randint(0, nMaxPower)):
            df = pd.concat([df, df])

        df = df.reset_index()

        # replace values
        customers = random_customers(len(df)).reset_index()
        df.last_name = customers.customer
        df.customer_id = customers.customer_keys

        df.total_amount = np.random.randint(0, 1000, len(df))/100
        df.total_amount = df.total_amount.astype(str) + ' USD'
        df.event_time = random_dates(pd.to_datetime('2017-01-01'), pd.to_datetime('2018-01-01'), len(df))
        df.to_json(rf"{input_path}input{np.random.randint(0, 200000)}.txt", orient="records")

    

GenFiles(20, 6)