import pandas as pd

def registration_date(inputdf):
    #sadece musteri id ve meslek sutunlarini al
    registration_date = inputdf[['customer_id', 'registration_time']]

    #sutun isimlerini degistir
    registration_date.columns = ['CUST_ID', 'REGISTRATION_DATE']

    return registration_date