bq load --autodetect fasttech_dataset.inventory gs://fasttech-test/task3/2021-10-10/2021-10-10-inventory.csv
bq load --autodetect fasttech_dataset.products gs://fasttech-test/task3/2021-10-10/2021-10-10-products.csv
bq load --autodetect fasttech_dataset.sales gs://fasttech-test/task3/2021-10-10/2021-10-10-sales.csv
bq load --autodetect fasttech_dataset.stores gs://fasttech-test/task3/2021-10-10/2021-10-10-stores.csv

bq load --autodetect fasttech_dataset.new_inventory gs://fasttech-test/task3/2021-10-11/2021-10-11-inventory.csv
bq load --autodetect fasttech_dataset.new_products gs://fasttech-test/task3/2021-10-11/2021-10-11-products.csv
bq load --autodetect fasttech_dataset.new_sales gs://fasttech-test/task3/2021-10-11/2021-10-11-sales.csv
bq load --autodetect fasttech_dataset.new_stores gs://fasttech-test/task3/2021-10-11/2021-10-11-stores.csv