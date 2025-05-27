# bu dosya kullanicinin anket risk sonucunu alarak yeni bir dataframe olusturur

import pandas as pd

def anket_risk(inputdf):
    #sadece musteri id ve meslek sutunlarini al
    anket_risk_df = inputdf[inputdf['result_key'] == 'Risk-Score'][['customer_id', 'result_value']]

    #sutun isimlerini degistir
    anket_risk_df.columns = ['CUST_ID', 'SURVEY_RISK']

    return anket_risk_df