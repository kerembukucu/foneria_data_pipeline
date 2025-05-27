import pandas as pd

def account_ready_date(inputdf):
    #sadece musteri id ve meslek sutunlarini al
    account_ready_date = inputdf[['customer_id', 'account_ready_time']]

    #sutun isimlerini degistir
    account_ready_date.columns = ['CUST_ID', 'ACCOUNT_READY_DATE']

    return account_ready_date