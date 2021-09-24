#https://github.com/sflydwh/code-challenge

from numpy.core.numeric import argwhere
import pandas as pd
import numpy as np
import glob
from concurrent.futures import ThreadPoolExecutor

class EventParser:

    def __init__(self):
        self.output_path = "../output/"
        self.input_pattern = "../input/*.txt"
        self.LTV_range = 52*10
    # for purpos of this implementation, and to control speed we'll 
    # aggregate data to only important information for sub-file.
    # Aggregate Per Customer / Year / Week pair:
    # - Number of visit
    # - Spent amount

    #ASSUMPTION - if there was any type of visit, that is the visit.
    # for that we may drop data with empty "valuable" fields 
    #  - (Date for visit calculation)
    #  - (total amount for)

    # read single file, reduce to needed set only and aggregate per customer / year / week for single batch only
    def ReadSingleFile(self, single_file):
            df = pd.read_json(single_file,
                dtype = {
                    'event_time':'datetime',
                    'customer_id':'string',
                    'total_amount':'string'
                }
            )

            df["Year"] = df['event_time'].dt.isocalendar().year
            df["Week"] = df['event_time'].dt.isocalendar().week

            df[["Amount_Value", "Currency"]] = df.total_amount.str.split(expand = True)

            # here to add currency conversion if needed to get to single currency
            # also question if thousands are separated with comma and we need to switch to regex here.
            df["Amount_Value"] = df["Amount_Value"].astype("double")


            # "last_name" is optional, depends on quality of data and can bring issu becuase of this
            # using only key is much stable solution, but that'll require having customers separately.

            # situation with missing customer_id - this approach will weed that out, and without customer, joining by any other 
            # condition - will bring invalid results
            file_agg_data = df.groupby(["customer_id", "Year", "Week"]).agg({"key":"count", "Amount_Value":"sum"})
            file_agg_data = file_agg_data.rename(columns={"key": "Visits"})

            #identify customer names (but ideally they should come separate)
            customers = df.groupby('customer_id').last_name.max()

            return (file_agg_data, customers)


    # return data fully aggregated per customer / year / week across the whole range
    def ReadFiles(self, input_files):
      
        # if we're to go multi-threaded parallelize reading is the biggest boost.
        files_data = []
        customers = []


        # SINGLE PROCESS IMPLEMENTATION
        for single_file in input_files:
            # I think that reasonable fail protection goes per single data batch (file). Otherwise it should go per record level, which is heavy.
            try:
                data, cust = self.ReadSingleFile(single_file)
                files_data.append(data)
                customers.append(cust)
            except Exception as e:
                print(e)
        

        #MULTITHREADED IMPLEMENTATION 
        # with ThreadPoolExecutor(max_workers=4) as pool:
        #     results = pool.map(self.ReadSingleFile, input_files)

        # for batch_result in results:
        #     data, cust = batch_result
        #     files_data.append(data)
        #     customers.append(cust)


        aggregated_data = pd.concat(files_data)
        aggregated_customers = pd.concat(customers)
        # one week can be split across multiple files
        aggregated_data = aggregated_data.groupby(["customer_id", "Year", "Week"]).agg({"Visits":"sum", "Amount_Value":"sum"})

        # and find single name for the customer
        aggregated_customers = aggregated_customers.groupby("customer_id").max()

        return (aggregated_data, aggregated_customers)


    # calculate top N Simple LTV customers
    def TopXSimpleLTVCustomers(self, TopSize = 10):
        input_files = glob.glob(self.input_pattern)

        aggregated_data, customers = self.ReadFiles(input_files)
        aggregated_data["WeekAverage"] = aggregated_data.Amount_Value / aggregated_data.Visits

        # calculate LTV
        total_range_weeks = aggregated_data.groupby(["Year", "Week"]).ngroups

        # to account "empty" week, i.e. weeks without user activity, have to calculate mean manually
        # week with activity per customer (to account for weeks without any activity)
        total_stats_per_customer = aggregated_data.groupby(["customer_id"]).agg({"WeekAverage":"sum"})
        total_stats_per_customer = total_stats_per_customer.rename(columns={"WeekAverage":"SumOfWeeksAverage"})

        # if I undestand correctly requirement LTV is average across whole period for weekly $/Visit KPI multiplied by 10 years
        total_stats_per_customer["LTV"] = (total_stats_per_customer.SumOfWeeksAverage / total_range_weeks) * self.LTV_range
        LTV_Data = total_stats_per_customer.sort_values(by='LTV', ascending=False).head(TopSize)

        #add customer name back
        LTV_Data = LTV_Data.join(customers, on='customer_id')

        #prepare for output
        LTV_Data = LTV_Data.drop("SumOfWeeksAverage", axis = 1).reset_index()
        LTV_Data.to_json(rf"{self.output_path}Top{TopSize}_LTV_Customers.txt", orient="records")

        print(LTV_Data)


ep = EventParser()
ep.TopXSimpleLTVCustomers(10)

#final notes:
# 1. week concept - is a bit faulty, as there are "short weeks" at the year end, and these weeks will have higher weight.
# reasonable would go to somethign like  week# from 1/1/1970
# 2. requirement say nothing about multi-year data, and when same week number can exist in many years.
# 3. had to pass last_name in parallel with main dataset. Didn't know if last_name was needed in the output.

