import argparse
import pandas as pd
import numpy as np
import os
from datetime import datetime
import json
import logging

logging.basicConfig(filename="../logs/logsdata.txt")
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())



def main():
    params = get_params()
    print(params)

    Customer_df = pd.read_csv("../input_data/starter/customers.csv")
    Product_df = pd.read_csv("../input_data/starter/products.csv")
    rootdir = '../input_data/starter/transactions'

    final_df = []
    for file in os.listdir(rootdir):

        # reading the File from the Location
        temp = pd.read_json(f"{rootdir}/{file}/transactions.json", lines=True)

        # the below two Lines are used for exploding and Flattening the Json
        # each record will have single Purchase
        temp = temp.explode('basket')
        A = pd.json_normalize(temp['basket'])

        # Combining the JSON to the Data Frame
        temp = temp.join(A)

        # Merging the Customer and Product Data Frame
        temp = pd.merge(Customer_df, temp, on='customer_id')
        temp = pd.merge(Product_df, temp, on='product_id')

        # Adding the week no so that we can Group by using it
        temp['week'] = datetime.strptime(file.split('=')[1], "%Y-%M-%d").isocalendar()[1]

        # Droping Unwanted Column
        temp.drop(['basket', 'price', 'date_of_purchase', 'product_description'], axis=1, inplace=True)

        # Appending all the Data Frame to a Single Data Frame
        if len(final_df) == 0:
            final_df = temp
        else:
            final_df = final_df.append(temp, ignore_index=True)

    # getting the Customers Weekly Purchase Count
    final_df['purchase_count'] = final_df.groupby(['week', 'customer_id'])['customer_id'].transform(len)

    # Saving the Files Week wise
    grouped = final_df.groupby(['week'])
    for week, group in grouped:
        group.drop(['week'], axis=1, inplace=True)
        group.to_json(f"../output/Week-{week}.json", orient='records', lines=True)
    

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.error(f"Oops! {e.__class__} occurred.")
    else:
        logger.info("Code Executed Successfully")
