# Covid-19 report latest update

Simple dashboard on [Google Data Studio](https://datastudio.google.com/s/n1sxlBJVXcU) that visualize the situation of Covid-19 outbreak right now on the world. 

Data is taken from the the github repo [2019 Novel Coronavirus COVID-19 (2019-nCoV) Data Repository by Johns Hopkins CSSE.](https://github.com/CSSEGISandData/COVID-19). Data in CSV format are then transform and loaded to Bigquery and finally visuallized on Google Data Studio.

Data is updated daily at 1:00 in UTC timezone (8:00 in Asia/Ho_chi_minh timezone) using cronjob from [Prefect](https://www.prefect.io/). An instance on GCP is in charge of handle this cronjob everyday
