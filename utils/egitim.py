# bu dosya kullanicinin egitim bilgisini input olarak alarak yeni daha sade bir dataframe olusturan fonksiyonu tanimlar

import pandas as pd

def egitim(inputdf):
    #sadece musteri id ve meslek sutunlarini al
    egitim_df = inputdf[['customer_id', 'education']]

    #sutun isimlerini degistir
    egitim_df.columns = ['CUST_ID', 'EDUCATION']

    return egitim_df