from datetime import datetime, timedelta


def interval_date (date_init, days):
    arr_date = []
    for i in range (days):
        min_date = date_init - timedelta(days=i)
        min_date = min_date.strftime("%Y-%m-%d %H:%M:%S")
        arr_date.append(min_date)
    
    return arr_date

