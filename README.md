# RFM ile Müşteri Segmentasyonu (Customer Segmentation with RFM)

İş Problemi (Business Problem):

Online ayakkabı mağazası olan FLO müşterilerini segmentlere ayırıp bu segmentlere göre pazarlama
stratejileri belirlemek istiyor. Buna yönelik olarak müşterilerin davranışları tanımlanacak ve bu
davranışlardaki öbeklenmelere göre gruplar oluşturulacak.


Recency: son alış veriş tarihinden itibaren bugüne kaç gün geçti bilgisi

Frequency: Alış veriş sıklığı, toplam satın alma adedi

Monetary: Toplam yaptığı harcama


Değişkenler:

master_id: Eşsiz müşteri numarası

order_channel: Alışveriş yapılan platforma ait hangi kanalın kullanıldığı (Android, ios, Desktop, Mobile)

last_order_channel: En son alışverişin yapıldığı kanal

first_order_date: Müşterinin yaptığı ilk alışveriş tarihi

last_order_date: Müşterinin yaptığı son alışveriş tarihi

last_order_date_online: Müşterinin online platformda yaptığı son alışveriş tarihi

last_order_date_offline: Müşterinin offline platformda yaptığı son alışveriş tarihi

order_num_total_ever_online: Müşterinin online platformda yaptığı toplam alışveriş sayısı

order_num_total_ever_offline: Müşterinin offline'da yaptığı toplam alışveriş sayısı

customer_value_total_ever_offline: Müşterinin offline alışverişlerinde ödediği toplam ücret

customer_value_total_ever_online: Müşterinin online alışverişlerinde ödediği toplam ücret

interested_in_categories_12: Müşterinin son 12 ayda alışveriş yaptığı kategorilerin listesi
