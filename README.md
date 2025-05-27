# 📊 Foneria Data Pipeline Dokümantasyonu

## 📁 A. Tablolar

### 1. Ana Tablolar

- **Tanım:**  
  Foneria'nın kendi sunucusunda aktif olarak kullandığı temel veri tablolarıdır.

---

### 2. Filtrelenmiş Tablolar (Raw Data)

- **Tanım:**  
  Analiz için işlenecek, ana tablolardan türetilen veri kümesidir.

- **Teknik Bilgi:**  
  - Ana tablolardan türetilir.
  - Günlük çalışan SQL scriptleri kullanılır.
  - Çalışma ortamı: [Airflow](https://airflow.foneria.com.tr/home)

---

### 3. Özet Tablolar

- **Tanım:**  
  Analiz edilmiş, karar destek sistemlerine değerli veri sunan tablolardır.

- **Teknik Bilgi:**  
  - Raw data üzerinde çalışan günlük Python scriptleri ile üretilir.
  - Çalışma ortamı: [Airflow](https://airflow.foneria.com.tr/home)

#### Özet Tablolar Listesi

| Tablo Adı | Açıklama | Grouping | Örnek SQL |
|-----------|----------|----------|-----------|
| `dws.static` | Müşterilerin zaman içinde değişmeyen sabit verileri (demografi vb.) | `customer_id` | `SELECT * FROM dws.static;` |
| `dws.year_month_based` | Zamana göre değişen müşteri davranış verileri | `customer_id, year, month` | `SELECT * FROM dws.year_month_based;` |
| `dws.year_month_fund_based` | Fon bazlı, zamana göre değişen veriler | `customer_id, year, month, fund_code` | `SELECT * FROM dws.year_month_fund_based;` |

---

## ⚙️ B. Teknik Mimari

### Airflow DAG'ları

- **Çalışma Ortamı:** [Airflow UI]([http://airflow.com](https://airflow.foneria.com.tr/home))
- **DAG klasörü path'i:** `path_to_dags`
- **Script tipi:** Python scriptleri

#### DAG 1: `transfer_job`

- **Amacı:**  
  Ana tablolardan filtrelenmiş raw veriyi türetir.
- **Teknoloji:**  
  SQL scriptleri

#### DAG 2: `ack_script`

- **Amacı:**  
  Raw veriden özet tabloları üretir.
- **Ek Özellik:**  
  `digital` scripti bu DAG içinde bir task olarak tanımlanmıştır.

---

## 🔄 Veri Akış Diyagramı

```text
Ana Tablolar
    ↓ (SQL - transfer_job)
Filtrelenmiş Tablolar (Raw Data)
    ↓ (Python - ack_script)
Özet Tablolar
```

## ⚙️ C. Dokumentatif tablolar
- **documentation.xlsx:**
  utils klasoru altinda calisan python scriptleri hakkinda bilgiler icerir. ozet tablolarin kolonlari hakkindaki bilgiler.
- **Digital Data for arketypes_reviewed version_2025_02_10-CANC.xlsx:**
  "G" isimli kolonda, ilgili satirdaki verinin hangi kaynaktan nasil turetildigi bilgisi yaziyor
