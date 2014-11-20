
# coding: utf-8

# In[131]:

import requests
import json
from datetime import date, timedelta
import csv
import os


# In[132]:

alltime_start = "2014-07-28"
last_3_start = date.today() - timedelta(days=21)
yesterday = date.today() - timedelta(days=1)


# In[133]:

ios_url = "https://api.appannie.com/v1.2/accounts/159384/products"
headers = {'Authorization' : 'bearer 41ebcfccb46084937b7caef86e982b5ff1df1d3d'}
g_url = "https://api.appannie.com/v1.2/accounts/182490/products"


# In[134]:

# google = requests.get(g_url, headers=headers).json()


# In[135]:

ios_url = "https://api.appannie.com/v1.2/accounts/159384/products/874498884/sales"
google_url = "https://api.appannie.com/v1.2/accounts/182490/products/20600003283643/sales"
headers = {'Authorization' : 'bearer 41ebcfccb46084937b7caef86e982b5ff1df1d3d'}
payload_alltime = {'start_date':alltime_start, 'end_date':yesterday}
payload_last3 = {'start_date':last_3_start, 'end_date':yesterday}


# In[136]:

alltime_response_ios = requests.get(ios_url,headers=headers,params=payload_alltime).json()
last_3_response_ios = requests.get(ios_url,headers=headers,params=payload_last3).json()
alltime_downloads_ios = alltime_response_ios['sales_list'][0]['units']['product']['downloads']
last_3weeks_downloads_ios = last_3_response_ios['sales_list'][0]['units']['product']['downloads']


# In[137]:

alltime_response_gplay = requests.get(google_url,headers=headers,params=payload_alltime).json()
last_3_response_gplay = requests.get(google_url,headers=headers,params=payload_last3).json()


# In[138]:

alltime_downloads_gplay = alltime_response_gplay['sales_list'][0]['units']['product']['downloads']
last_3weeks_downloads_gplay = last_3_response_gplay['sales_list'][0]['units']['product']['downloads']


# In[139]:

summary = {'ios_alltime': alltime_downloads_ios,
               'ios_last3': last_3weeks_downloads_ios,
               'android_alltime': alltime_downloads_gplay,
               'android_last3': last_3weeks_downloads_gplay}


# In[141]:
try:
	os.chdir('/home/developer/retention_funnels')
except:
	print 'directory not found'

writer = csv.writer(open('app_annie_downloads_current.csv', 'wb'))
for key, value in summary.items():
		writer.writerow([key, value])


# In[ ]:



