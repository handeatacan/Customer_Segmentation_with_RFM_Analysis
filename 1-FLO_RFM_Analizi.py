 #                 ###############################################################                                #
################# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)  #################################
#                  ###############################################################                                #


###############################################################
# 1. İş Problemi (Business Problem)
###############################################################


# Recency: son alış veriş tarihinden itibaren bugüne kaç gün geçti bilgisi
# Frequency: Alış veriş sıklığı, toplam satın alma adedi
# Monetary: Toplam yaptığı harcama


# Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama
# stratejileri belirlemek istiyor. Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu
# davranışlardaki öbeklenmelere göre gruplar oluşturulacak.

# Değişkenler
#
# master_id: Eşsiz müşteri numarası
# order_channel: Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)
# last_order_channel: En son alışverişin yapıldığı kanal
# first_order_date: Müşterinin yaptığı ilk alışveriş tarihi
# last_order_date: Müşterinin yaptığı son alışveriş tarihi
# last_order_date_online: Müşterinin online platformda yaptığı son alışveriş tarihi
# last_order_date_offline: Müşterinin offline platformda yaptığı son alışveriş tarihi
# order_num_total_ever_online: Müşterinin online platformda yaptığı toplam alışveriş sayısı
# order_num_total_ever_offline: Müşterinin offline'da yaptığı toplam alışveriş sayısı
# customer_value_total_ever_offline: Müşterinin offline alışverişlerinde ödediği toplam ücret
# customer_value_total_ever_online: Müşterinin online alışverişlerinde ödediği toplam ücret
# interested_in_categories_12: Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi

# recency: last_order_date
# frequency: order_num_total
# monetary: customer_value_total

import pandas as pd
import datetime as dt
pd.set_option('display.max_columns', None) # bütün sütunları gösterir.
# pd.set_option('display.max_rows', None) # Bütün satırları gösterir
pd.set_option('display.float_format', lambda x: '%.3f' % x) # % ifadesi virgülden sonra kaç satır görünsün

###############################################################
# Görev 1: Veriyi Anlama ve Hazırlama
###############################################################

# Adım1: flo_data_20K.csv verisini okuyunuz.Dataframe’in kopyasını oluşturunuz.

df_ = pd.read_csv("datasets/flo_data_20k.csv")
df = df_.copy()

# Adım2: Veri setinde
# a. İlk 10 gözlem,

df.head()
# b. Değişken isimleri,
df.columns

# c. Betimsel istatistik,
df.describe().T

# d. Boş değer,
df.isnull().sum()

# e. Değişken tipleri, incelemesi yapınız.
df.dtypes # date ler object olmuş


# Adım3: Omnichannel müşterilerin hem online'dan hemde offline platformlardan alışveriş yaptığını ifade etmektedir. Her bir müşterinin toplam
# alışveriş sayısı ve harcaması için yeni değişkenler oluşturunuz.

df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
df["customer_value_total"] = df["order_num_total_ever_offline"] + df["customer_value_total_ever_online"]


# Adım4: Değişken tiplerini inceleyiniz. Tarih ifade eden değişkenlerin tipini date'e çeviriniz.

for col in df.columns:
    if "date" in col:
        df[col] = df[col].apply(pd.to_datetime)
        # df[col] = df[col].astpye("datetime64[ns]")

df.dtypes

# Adım5: Alışveriş kanallarındaki müşteri sayısının,
# toplam alınan ürün sayısının ve toplam harcamaların dağılımına bakınız.

df.groupby("order_channel").agg({"order_num_total": "sum",
                                 "customer_value_total": "sum"})

#df.groupby("order_channel").agg({"order_num_total": "count",
#                                "customer_value_total": "sum"})

# Adım6: En fazla kazancı getiren ilk 10 müşteriyi sıralayınız.

df.sort_values(by="customer_value_total", ascending=False).head(10)

# Adım7: En fazla siparişi veren ilk 10 müşteriyi sıralayınız.

df.sort_values(by="order_num_total", ascending=False).head(10)

# Adım8: Veri ön hazırlık sürecini fonksiyonlaştırınız.

def function(df, head=10):
    print(df.head(head))
    print("#####################################")
    print(df.columns)
    print("#####################################")
    print(df.describe().T)
    print("#####################################")
    print(df.isnull().sum())
    print("#####################################")
    df["order_num_total"] = df["order_num_total_ever_online"] + df["order_num_total_ever_offline"]
    df["customer_value_total"] = df["order_num_total_ever_offline"] + df["customer_value_total_ever_online"]
    print(df.dtypes)
    for col in df.columns:
        if "date" in col:
            df[col] = df[col].apply(pd.to_datetime)  # df[col] = df[col].astpye("datetime64[ns]")
    df.groupby("order_channel").agg({"order_num_total": "count",
                                     "customer_value_total": "count"})
    df.sort_values(by="customer_value_total", ascending=False).head(head)
    df.sort_values(by="order_num_total", ascending=False).head(head)


function(df)

###############################################################
# Görev 2: RFM Metriklerinin Hesaplanması
###############################################################

# Adım 1: Recency, Frequency ve Monetary tanımlarını yapınız.

# Recency : Yenilik, son tarihten itibaren geçen zaman
# Frequency : Alışveriş sayısı -
# Monetary : müşterinin toplam bıraktığı parasal değer

# Adım 2: Müşteri özelinde Recency, Frequency ve Monetary metriklerini hesaplayınız.

# recency: last_order_date -
# frequency: order_num_total
# monetary: customer_value_total

df["last_order_date"].max()
today_date = dt.datetime(2021, 6, 1)
df.groupby("master_id").agg({"last_order_date": lambda date: (today_date - date.max()).days,
                             "order_num_total": lambda x: x.sum(),
                             "customer_value_total": lambda x: x.sum()})

# yanlış gibi soralım !!!
# df.groupby("master_id").agg({"last_order_date": lambda date: (today_date - date.max()).days,
#                             "order_num_total": lambda x: x.nunique(),
#                             "customer_value_total": lambda x: x.sum()})

# Adım 3: Hesapladığınız metrikleri rfm isimli bir değişkene atayınız.

rfm = df.groupby("master_id").agg({"last_order_date": lambda date: (today_date - date.max()).days,
                             "order_num_total": lambda x: x.sum(),
                             "customer_value_total": lambda x: x.sum()})

# Adım 4: Oluşturduğunuz metriklerin isimlerini recency, frequency ve monetary olarak değiştiriniz.

rfm.columns = ["recency", "frequency", "monetary"]

###############################################################
# Görev 3: RF Skorunun Hesaplanması
###############################################################

# Adım 1: Recency, Frequency ve Monetary metriklerini qcut yardımı ile 1-5 arasında skorlara çeviriniz.
# pd.qcut(rfm.recency, )
pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

# Adım 2: Bu skorları recency_score, frequency_score ve monetary_score olarak kaydediniz.

rfm["recency_score"] = pd.qcut(rfm["recency"], 5, labels=[5, 4, 3, 2, 1])
rfm["frequency_score"] = pd.qcut(rfm["frequency"].rank(method="first"), 5, labels=[1, 2, 3, 4, 5])
rfm["monetary_score"] = pd.qcut(rfm["monetary"], 5, labels=[1, 2, 3, 4, 5])

# Adım 3: recency_score ve frequency_score’u tek bir değişken olarak ifade ediniz ve RF_SCORE olarak kaydediniz.

rfm["RF_SCORE"] = rfm.recency_score.astype(str) + rfm.frequency_score.astype(str)

###############################################################
# Görev 4: RF Skorunun Segment Olarak Tanımlanması
###############################################################

# Adım 1: Oluşturulan RF skorları için segment tanımlamaları yapınız.

seg_map = {
    r'[1-2][1-2]': 'hibernating',
    r'[1-2][3-4]': 'at_risk',
    r'[1-2]5': 'cant_loose',
    r'3[1-2]': 'about_to_sleep',
    r'33': 'need_attention',
    r'[3-4][4-5]': 'loyal_customers',
    r'41': 'promising',
    r'51': 'new_customers',
    r'[4-5][2-3]': 'potential_loyalists',
    r'5[4-5]': 'champions'
}

rfm.RF_SCORE.replace(seg_map, regex=True)


# Adım 2: Aşağıdaki seg_map yardımı ile skorları segmentlere çeviriniz.

rfm["segment"] = rfm.RF_SCORE.replace(seg_map, regex=True)

###############################################################
# Görev 5: Aksiyon Zamanı !
###############################################################


# Adım1: Segmentlerin recency, frequnecy ve monetary ortalamalarını inceleyiniz.

rfm.describe().T

# Adım2: RFM analizi yardımıyla aşağıda verilen 2 case için ilgili profildeki müşterileri bulun ve
# müşteri id'lerini csv olarak kaydediniz.

# a. FLO bünyesine yeni bir kadın ayakkabı markası dahil ediyor. Dahil ettiği markanın ürün fiyatları genel müşteri
# tercihlerinin üstünde. Bu nedenle markanın tanıtımı ve ürün satışları için ilgilenecek profildeki müşterilerle özel olarak
# iletişime geçmek isteniliyor. Sadık müşterilerinden (champions, loyal_customers) ve kadın kategorisinden alışveriş
# yapan kişiler özel olarak iletişim kurulacak müşteriler. Bu müşterilerin id numaralarını csv dosyasına kaydediniz.

df.head()
rfm["Category"] = df["interested_in_categories_12"].values

rfm_ = rfm[(rfm.segment == "champions") | (rfm.segment == "loyal_customers")]
# rfm_target = rfm_.iloc[[index for index, i in enumerate(rfm_.Category.values) if "KADIN" in i], :]
rfm_target = rfm_[rfm_["Category"].str.contains("KADIN", na=False)]
rfm_target.reset_index(inplace=True)
rfm_target["master_id"].to_csv("rfm_target_customer.csv")


# b. Erkek ve Çocuk ürünlerinde %40'a yakın indirim planlanmaktadır. Bu indirimle ilgili kategorilerle ilgilenen geçmişte
# iyi müşteri olan ama uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler, uykuda olanlar ve yeni
# gelen müşteriler özel olarak hedef alınmak isteniyor. Uygun profildeki müşterilerin id'lerini csv dosyasına kaydediniz.

# uzun süredir alışveriş yapmayan kaybedilmemesi gereken müşteriler : cant_loose
# uykuda olanlar : hibernating
# yeni gelen müşteriler : new_customers
rfm_2 = rfm[(rfm.segment == "cant_loose") | (rfm.segment == "hibernating") | (rfm.segment == "new_customers")]
rfm_2.reset_index(inplace=True)
rfm_target_2 = rfm_2[rfm_2["Category"].str.contains("COCUK", na=False)]
rfm_target_3 = rfm_2[rfm_2["Category"].str.contains("ERKEK", na=False)]
rfm_cocuk_erkek = pd.merge(rfm_target_2, rfm_target_3)
rfm_cocuk_erkek.master_id.to_csv("rfm_cocuk_erkek.csv")









