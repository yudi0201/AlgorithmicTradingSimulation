import csv
from numpy import linspace
import random

if __name__ == '__main__':
    #companies = ['AAPL','NKE','GS','CSCO','KO','GOOGL','PFE','DIS','INTC','MSFT','VZ','MRK','MCD','GE','AABA','JPM','HD','TRV','WMT',\
    #    'AMZN','AXP','XOM','UTX','CVX','CAT','PG','JNJ','UNH','BA','MMM','IBM']

    low = linspace(10,25,1500)

    #num_companies = len(companies)
    
    header = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Name']
    data = []
    date = 0
    volume = 100000
    for i in range(1000000):
        data.append([date,low[random.randint(0,1499)], low[random.randint(0,1499)],low[random.randint(0,1499)],low[random.randint(0,1499)],volume,"ABCD"])     
        date += 1
    
    with open('1_million_stock_UNIX_single_firm.csv', 'w', newline = '') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(data)
    


    