# **Analisis Penargetan Subsidi Kendaraan Listrik (EV) untuk Mendorong Adopsi yang Merata di Seluruh County Berdasarkan Faktor Sosio-Ekonomi.**
---

## **Latar Belakang**

Pemerintah memiliki tujuan untuk mempercepat adopsi kendaraan listrik (EV) sebagai bagian dari komitmen terhadap Tujuan Pembangunan Berkelanjutan (SDGs) PBB, khususnya Goal 7 (Energi Bersih), Goal 11 (Kota Berkelanjutan), dan Goal 13 (Aksi Iklim). Namun, data saat ini menunjukkan bahwa penyebaran EV tidak merata, dengan beberapa wilayah (county) yang tertinggal jauh. Diduga, faktor sosio-ekonomi seperti Upah Minimum Regional (Gaji rata-rata) dan karakteristik wilayah seperti kepadatan penduduk menjadi penghalang utama.

Analisis ini bertujuan untuk mengidentifikasi wilayah-wilayah dengan tingkat penetrasi EV yang rendah, memahami hubungannya dengan faktor ekonomi, dan merekomendasikan model-model EV yang paling cocok untuk disubsidi di wilayah tersebut. Dengan demikian, program subsidi pemerintah dapat menjadi lebih tepat sasaran, efektif, dan mendorong adopsi yang lebih merata.

---

## **Problem Statement**

Penyebaran kendaraan listrik (EV) di Washington masih belum merata, dengan beberapa county menunjukkan tingkat penetrasi EV yang rendah. Kondisi ini diduga dipengaruhi oleh faktor sosio-ekonomi seperti rendahnya Upah Minimum Regional (Gaji rata-rata) serta karakteristik wilayah seperti kepadatan penduduk. Tanpa analisis yang tepat, kebijakan subsidi pemerintah berisiko tidak efektif dan tidak tepat sasaran. Oleh karena itu, perlu dilakukan identifikasi wilayah dengan penetrasi EV rendah, analisis hubungan dengan faktor ekonomi, serta rekomendasi model EV yang paling sesuai untuk disubsidi agar program pemerintah dapat lebih adil, efisien, dan mendorong adopsi EV secara merata.

---

## **Defining the Problem Statement (Kerangka SMART)**

* **Specific**: Mengidentifikasi county, lalu menganalisis karakteristik UMR dan kepadatan penduduk untuk menetapkan strategi subsidi berbasis diferensiasi wilayah.

* **Measurable**: Mengukur tingkat penetrasi EV, rata-rata Gaji rata-rata, kepadatan penduduk, serta pangsa pasar dan harga dari model-model EV yang relevan.

* **Achievable**: Analisis dapat dicapai menggunakan empat dataset yang disediakan (EV Population, Gaji rata-rata, Harga Mobil, Populasi Total) dan library Python dalam waktu yang ditentukan.

* **Relevant**: Hasil analisis akan memberikan rekomendasi langsung yang dapat digunakan untuk merancang kebijakan subsidi yang lebih efektif dan adil, mendukung SDG 10 (Mengurangi Kesenjangan) selain tujuan lingkungan.

* **Time-Bound**: Analisis diselesaikan sesuai timeline proyek untuk memberikan masukan kebijakan yang relevan.

---

## **Key Questions**

1. ⁠Bagaimana distribusi, tendensi sentral, dispersi (sebaran), dan outlier pada data numerik kunci: Harga Mobil, Jangkauan Listrik (Electric Range), Gaji rata-rata, dan Kepadatan Penduduk?

2. Faktor apa yang paling mempengaruhi penetrasi EV di tiap county?

3. ⁠Bagaimana tingkat penetrasi EV di tiap county jika dibandingkan dengan total populasi, dan sejauh mana tingkat Gaji rata-rata di wilayah tersebut memengaruhi keterjangkauan EV?

4. ⁠Model EV apa yang paling banyak diminati, berapa harganya, dan sejauh mana harga tersebut sebanding dengan pendapatan tahunan masyarakat?

5. Apakah terdapat perbedaan signifikan secara statistik dalam rata-rata tingkat penetrasi EV antara kelompok county dengan upah tinggi (kuartil atas) dan kelompok county dengan upah rendah (kuartil bawah)?

---

## **Datasets**

https://catalog.data.gov/dataset/electric-vehicle-population-data

https://data.wa.gov/demographics/WAOFM-Census-Population-Density-by-County-by-Decad/e6ip-wkqq/about_data

https://data.wa.gov/demographics/WAOFM-April-1-Population-by-State-County-and-City-/2hia-rqet/about_data

https://hdpulse.nimhd.nih.gov/data-portal/social/table?age=001&age_options=ageall_1&demo=00011&demo_options=income_3&race=00&race_options=race_7&sex=0&sex_options=sexboth_1&socialtopic=030&socialtopic_options=social_6&statefips=53&statefips_options=area_states

https://afdc.energy.gov/vehicles/search/download?utm_source=chatgpt.com

* Feature datasets:

- Dataset shows the Battery Electric Vehicles (BEVs) and Plug-in Hybrid Electric Vehicles (PHEVs) that are currently registered through Washington State Department of Licensing (DOL).
- Washington state population density by county by decade 1900 to 2020.
- Intercensal and postcensal population estimates for the state, counties and cities, 1990 to present.
- Income (Median household income) for Washington by County
All Races (includes Hispanic/Latino), Both Sexes, All Ages, 2019-2023
Sorted by Value (Dollars).
- Dataset includes various vehicle types such as all-electric vehicles (EVs), plug-in hybrid electric vehicles (PHEVs), hybrid electric vehicles (HEVs), flexible fuel vehicles (FFVs), biodiesel vehicles, and compressed natural gas (CNG) vehicles.

---

## **Data Pipeline**

Proses dimulai dengan perancangan data modeling, di mana ditentukan dimension table dan fact table yang sesuai dengan kebutuhan analisis. Selanjutnya dilakukan extract data dari sumber dataset, dilanjutkan dengan data cleaning dan data transformation untuk memastikan kualitas dan konsistensi data.

Setelah itu, data akan divalidasi secara menyeluruh sebelum dilakukan load ke Supabase (PostgreSQL). Seluruh proses ETL diotomasi menggunakan Airflow untuk menjamin efisiensi, konsistensi, dan keterulangan proses.

Tahap terakhir adalah pembuatan data mart, yang dirancang khusus untuk mendukung kebutuhan analisis dan pengambilan keputusan berbasis data.

---

## **Tools**

- Supabase
- Apache Airflow
- Docker
- Python
- Pyspark
- Looker Studio
- Google Api

---

## **Contributors**

- Abraham Jordy Ollen
- Ardi Kurniawan Kusuma
- Marco Tuwanakotta
- Rifqi Nadhir Aziz
- Tazqia Ranyanisha

---

## **Datamart**

https://docs.google.com/spreadsheets/d/1bcMdN7tpZzxAFVduj5v-pYVy0ovezBrzcRdx14BZaVs/edit?gid=1216836236#gid=1216836236

---

## **Dashboard**

https://lookerstudio.google.com/reporting/ed363425-9fa0-435e-a832-b236bec5f93e

---

## **Presentation**

https://docs.google.com/presentation/d/11T9XdyOLYDkYZdKXZwNFe7baQ3cE7pV5/edit?usp=sharing&ouid=115670577210267877252&rtpof=true&sd=true

---