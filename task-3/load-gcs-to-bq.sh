bq load --autodetect <your_dataset>.inventory gs://<your_bucket>/task3/2021-10-10/2021-10-10-inventory.csv
bq load --autodetect <your_dataset>.products gs://<your_bucket>/task3/2021-10-10/2021-10-10-products.csv
bq load --autodetect <your_dataset>.sales gs://<your_bucket>/task3/2021-10-10/2021-10-10-sales.csv
bq load --autodetect <your_dataset>.stores gs://<your_bucket>/task3/2021-10-10/2021-10-10-stores.csv

bq load --autodetect <your_dataset>.new_inventory gs://<your_bucket>/task3/2021-10-11/2021-10-11-inventory.csv
bq load --autodetect <your_dataset>.new_products gs://<your_bucket>/task3/2021-10-11/2021-10-11-products.csv
bq load --autodetect <your_dataset>.new_sales gs://<your_bucket>/task3/2021-10-11/2021-10-11-sales.csv
bq load --autodetect <your_dataset>.new_stores gs://<your_bucket>/task3/2021-10-11/2021-10-11-stores.csv