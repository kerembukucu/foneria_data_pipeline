# bu dosya kullanicinin BIT sonucunu alarak yeni bir dataframe olusturur

import pandas as pd

def bit(inputdf):
    #sadece musteri id ve meslek sutunlarini al
    bit_df = inputdf[inputdf['result_key'] == 'BIT'][['customer_id', 'result_value']]

    #sutun isimlerini degistir
    bit_df.columns = ['CUST_ID', 'BIT_SCORE']

    return bit_df