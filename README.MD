# Covid-19 report latest update

Simple dashboard on [Google Data Studio](https://datastudio.google.com/s/n1sxlBJVXcU) that visualize the situation of Covid-19 outbreak right now on the world. 

Data taken from the the github repo [2019 Novel Coronavirus COVID-19 (2019-nCoV) Data Repository by Johns Hopkins CSSE.](https://github.com/CSSEGISandData/COVID-19). Data in CSV format are then transform and loaded to Bigquery and finally visuallized on Google Data Studio.

Right now data being pushed daily using python crontab locally. To improve: using prefect and deploy on GCP VM.
