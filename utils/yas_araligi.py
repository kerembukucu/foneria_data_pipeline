# bu dosya kullanicinin dogum tarihini input olarak alarak onu bir yas araligina yerlestiren fonksiyonu tanimlar

import pandas as pd
from datetime import date

def yas_araligi(inputdf):
    # sadece dogum yili ve musteri id sutunlarini al
    dogum_yili_df = inputdf[['customer_id', 'birth_year']]

    # guncel seneyi al
    guncel_tarih = date.today().year

    # her bir musteri icin guncel tarihten dogum senesini cikararak yasini hesaplayarak musteri id ile birlikte yeni bir dataframe olustur
    dogum_yili_df.loc[:, "Yaş Aralığı"] = dogum_yili_df['birth_year'].apply(lambda x: guncel_tarih - x)

    # yas araligini belirle
    dogum_yili_df.loc[:, 'Yaş Aralığı'] = dogum_yili_df['Yaş Aralığı'].apply(lambda x: '0-25' if x <= 25 else ('26-40' if x <= 40 else '40+'))

    # dogum yili sutununu sil
    dogum_yili_df = dogum_yili_df.drop(columns=['birth_year'])

    # sutun isimlerini degistir
    dogum_yili_df.columns = ['CUST_ID', 'AGE_RANGE']

    return dogum_yili_df
