# bu dosya kullanicinin meslegini input olarak alarak yeni daha sade bir dataframe olusturan fonksiyonu tanimlar

import pandas as pd

def meslek(inputdf):
    #sadece musteri id ve meslek sutunlarini al
    meslek_df = inputdf[['customer_id', 'occupation']]

    #sutun isimlerini degistir
    meslek_df.columns = ['CUST_ID', 'OCCUPATION']

    return meslek_df