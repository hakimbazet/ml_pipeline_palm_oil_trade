# ML Pipeline for Palm Oil Trade
End-to-End Simple Implementation for palm oil trading

Overall for 3 months prediction we will use SARIMAX a univariate model and for daily data we will try to predict the price trend of up and down for the next following days. All of the analysis are in the daily-data-forecast-analysis.ipynb and monthly-data-forecast-analysis.ipynb.

The error for 3 months prediction on average is at 8% and the accuracy for trend up/down trend is at 60%.

To run the file simply 
```
chmod +x start_script.sh
./start_script.sh
```

Wait a little while after all of the container is run there should be 2 container that will be running postgres and python services, Then run the following command:
```
### To check the status of container
docker -ps

chmod +x /src/load_py_to_scheduler.sh
./src/load_py_to_scheduler.sh
```

All of the workflow will be automated via Apache Airflow.

