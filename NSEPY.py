import requests

import pandas as pd
from time import sleep
from datetime import date
from datetime import datetime
import os
import json
import winsound
from sqlalchemy import create_engine
import mysql.connector

from sqlalchemy.orm import sessionmaker

cnx = mysql.connector.connect(user='root', password='15w60ps',
                              host='localhost',
                              database='test')  
cursor = cnx.cursor()
def Get_time():
    #function to get the current system time
    time = str(datetime.now().strftime('%H:%M'))
    return time




def beep():
    #A function which serves as an "audio alarm" if there is some error 
    frequency = 2500  # Set Frequency To 2500 Hertz
    duration = 5000  # Set Duration To 1000 ms == 1 second
    winsound.Beep(frequency,duration)

print('['+Get_time()+']>>>librairies and basic functions loaded')


#Setting up the pandas library
pd.set_option('display.width', 1500)
pd.set_option('display.max_columns', 75)
pd.set_option('display.max_rows', 1500)


#Reading the expiry TXT file and storing the dates in a list
expiry_list =[]
file1 = open('expiry.txt','r')
for i in file1.readlines():
    if i[0] == '#' :        
        pass
    else:
        expiry_list.append(i[:-1])

expiry_list = expiry_list[:-1]

#printing the dates so as to check if the code is working properly
print(expiry_list)




#the symbol of the stock we will be getting the data for
symbol = 'NIFTY'



print('['+Get_time()+']>>>constants loaded')

def fetch_oi(expiry):
#Here 'expiry' is the expiry-date for which we will be getting the data for 




#attempting to connect to the NSE website
#"max_retries denotes the number of times code will attempt to connect to the website before giving up"
    max_retries = 9
    tries = 1

    end = 0
    while tries <= max_retries or end != 1:
#defining some important variables for successful connection with the website
        headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
                  "Accept-Language": "en-US,en;q=0.5","Accept-Encoding": "gzip, deflate"
                  }
        cookie_dict = {'bm_sv':'D4B12FF7ED731A7DA9A7FF5F2E82F219~Fkgs+aQbSMjZbEVeVewalaSResOVRIT/Qv060V57sp78r3VeIqsmcl/Cnbtzg9RVWnJLK2yFjFYY2ND5pPgXZm14O7QyReHh25W2KoL9iLxQPsZiT8C+J18mrsIwPOP9xnW0QlKS4GyDNQdWpPrNImIvavivzU/kB3QYa2XNLCI='
                                      
                       }
        session = requests.session()

#setting up the cookies
        for cookie in cookie_dict:
            session.cookies.set(cookie, cookie_dict[cookie])

        url = "https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY"
        try:
            r = session.get(str(url), headers = headers).json()
            timestamp = r['records']['timestamp']
            f = open("timestamp.txt", "w")
            f.write(timestamp)
            f.close()
            
        except:
            print('['+Get_time()+']>>>connection error')
            return 10
            break
        '''
        Now r contains the json file which has the data for the stock and ALL THE EXPIRY DATES
        however now we will generate two panda dataframes pe_values and ce_values
        to store the required data OF A PARTICULAR DATE in a neat fashion

        NOTE: I am using try here in order to make sure the code doesnt shut down due to some error
        '''

        try:


    
            if expiry:

                #Note all the comments given below are present for troubleshooting any errors
                #uncomment the following comments to check what values go into the variables
                ce_values = []
                pe_values = []

                for o in r['records']['data']:
                    strike = 0
                    ide = ''

                    pe_written = False
                    ce_written = False
                    if 'CE' in o:
                        
                        data = o['CE']
                        #print(str(data['expiryDate']).lower() +'    '+ str(expiry).lower())
                        if str(data['expiryDate']).lower() == str(expiry).lower():
                            #print(1)
                            ce_written = True
                            strike =data['strikePrice']
                            ide = data['identifier']
                            ce_values.append(data)
                           # print(strike)
                    if 'PE' in o :

                        
                        data = o['PE']
                        if str(data['expiryDate']).lower() == str(expiry).lower():
                           #print('2')
                            pe_written = True
                            strike =data['strikePrice']
                            ide = data['identifier']
                            pe_values.append(data)
                            #print(strike)
                            
                    pe_value_strike = []
                    ce_value_strike = []
                    for ib in pe_values:
                        pe_value_strike.append(ib['strikePrice'])

                    for ib in ce_values:
                        ce_value_strike.append(ib['strikePrice'])

                    if not pe_written and strike not in pe_value_strike:
                        data1 = {'strikePrice': strike, 'expiryDate': expiry, 'underlying': symbol, 'identifier':ide, 'openInterest': 0, 'changeinOpenInterest': 0, 'pchangeinOpenInterest': 0, 'totalTradedVolume': 0, 'impliedVolatility': 0, 'lastPrice': 0, 'change': 0, 'pChange': 0, 'totalBuyQuantity': 10, 'totalSellQuantity': 0, 'bidQty': 0, 'bidprice': 0, 'askQty': 0, 'askPrice': 0, 'underlyingValue': 0}
                        pe_values.append(data1)

                    if not ce_written and strike not in ce_value_strike:
                        data1 = {'strikePrice': strike, 'expiryDate': expiry, 'underlying': symbol, 'identifier':ide, 'openInterest': 0, 'changeinOpenInterest': 0, 'pchangeinOpenInterest': 0, 'totalTradedVolume': 0, 'impliedVolatility': 0, 'lastPrice': 0, 'change': 0, 'pChange': 0, 'totalBuyQuantity': 10, 'totalSellQuantity': 0, 'bidQty': 0, 'bidprice': 0, 'askQty': 0, 'askPrice': 0, 'underlyingValue': 0}
                        ce_values.append(data1)





            else:
                
                ce_values = []
                pe_values = []

                for o in r['filtered']['data']:

                    data = o['CE']
                    if 'CE' in data:
                        ce_values.append(data['CE'])
                

                for o in r['filtered']['data']:
                    data = o['PE']
                    if 'PE' in data :
                        pe_values.append(data['PE'])
        except:
            
            print('['+Get_time()+']>>>error while tidying data')#Announcing an error ocurred
            beep()#sound the alarm
            break
        """
        The following code will be adding more data to the already "tidied up" dataframes by performing
        some logical and arithematic operations

        """
        for ce in ce_values:
            
            strike = ce['strikePrice']
            if strike == '':
                pass
            for pe in pe_values:
                if pe['strikePrice'] == strike:
                    if ce['openInterest'] == 0:
                        pcr = 'Null'
                    else:
                        pcr = float(pe['openInterest'])/float(ce['openInterest'])
                    if ce['changeinOpenInterest'] == 0:
                        changepcr = 'Null'
                    else:
                        changepcr = float(pe['changeinOpenInterest'])/float(ce['changeinOpenInterest'])

                    call_analysis = ''
                    pcall_analysis = ''
                    if ce['change'] >= 0 and ce['changeinOpenInterest'] >=0:
                        call_analysis =  'Fresh Long'
                    if ce['change'] >=0 and ce['changeinOpenInterest'] <=0:
                        call_analysis =  'Short Covering'
                    if ce['change'] <= 0 and ce['changeinOpenInterest'] >=0:
                        call_analysis =  'Fresh Short'
                    if ce['change'] <= 0 and ce['changeinOpenInterest'] <=0:
                        call_analysis =  'Long Unwinding'

                        
                    if pe['change'] >= 0 and pe['changeinOpenInterest'] >=0:
                        pcall_analysis =  'Fresh Long'
                    if pe['change'] >= 0 and pe['changeinOpenInterest'] <=0:
                        pcall_analysis =  'Short Covering'
                    if pe['change'] <= 0 and pe['changeinOpenInterest'] >=0:
                        pcall_analysis =  'Fresh Short'
                    if pe['change'] <= 0 and pe['changeinOpenInterest'] <=0:
                        pcall_analysis =  'Long Unwinding'


                    if ce['change'] == 0 and ce['changeinOpenInterest'] ==0:
                        call_analysis =  'NULL'
                    if pe['change'] == 0 and pe['changeinOpenInterest'] ==0:
                        pcall_analysis =  'NULL'


                    

                    pe['PCR'] = pcr
                    pe['Change in PCR'] = changepcr
                    pe['Sum of oi'] = pe['openInterest']+ce['openInterest']
                    pe['Call Analysis'] = call_analysis
                    pe['Put Analysis'] = pcall_analysis
                    pe['Call Premium turnover']= ce['lastPrice']*ce['changeinOpenInterest']
                    pe['Put Premium turnover']= pe['lastPrice']*pe['changeinOpenInterest']
                    
                   # print(ce['underlyingValue'])
                   #print(ce['strikePrice'])
                    pe['Strike Price - underlying'] = abs(float(ce['underlyingValue'])-float(ce['strikePrice']))

                    pe['Differ IV'] = abs(float(ce['impliedVolatility'])-float(pe['impliedVolatility']))

        ce_data = pd.DataFrame(ce_values)
        pe_data = pd.DataFrame(pe_values)
        ce_data = ce_data.sort_values(['strikePrice'])
        pe_data = pe_data.sort_values(['strikePrice'])
        #sheet.clear_contents()


#PASTING THE DATA IN MYSQL
        data1 = ce_data.drop(
            ['askPrice','askQty','bidQty','bidprice','expiryDate','identifier','totalBuyQuantity','totalSellQuantity','underlying'], axis = 1)[
             ['openInterest','changeinOpenInterest','pchangeinOpenInterest','impliedVolatility','lastPrice','change','pChange','totalTradedVolume','underlyingValue','strikePrice']]

        data2 = pe_data.drop(
            ['askPrice','askQty','bidQty','bidprice','expiryDate','identifier','totalBuyQuantity','totalSellQuantity'], axis = 1)[
             ['strikePrice','openInterest','changeinOpenInterest','pchangeinOpenInterest','impliedVolatility','totalTradedVolume','lastPrice','change','pChange','underlying','Sum of oi','PCR','Change in PCR','Call Analysis','Put Analysis','underlyingValue','Strike Price - underlying','Differ IV','Put Premium turnover','Call Premium turnover']]
        Session = sessionmaker()



        
        engine = create_engine('mysql+mysqlconnector://root:15w60ps@localhost:3306/test', echo=False)
        Session.configure(bind=engine)
        session = Session()

        
        today = date.today()
        day = (today.strftime("%b_%d")).lower()
        
        data1['Time'] = Get_time()
        data2['Time'] = Get_time()
        
        data1.to_sql(name='call'+day+'expir_'+expiry.lower(), con=engine, if_exists = 'append', index=False)
        data2.to_sql(name='put'+day+'expir_'+expiry.lower(), con=engine, if_exists = 'append', index=False)
        session.commit()
        del data1
        del data2
        print('['+Get_time()+']>>>Data Pasted')
        return 180
        break
        






def main():
#MAIN FUNCTION
    print('['+Get_time()+']>>>running MAIN FUNCTION-----------')
    print()
    print()
    print()

    for ia in expiry_list:
        #This loop will cycle through each expiry date
        print('['+Get_time()+']>>>Aquiring data for date:'+str(ia))

        print('['+Get_time()+']>>>Fetching data')
        x = fetch_oi(ia)
        print()
        print()
        print()
    return x

while True:
    #MAIN LOOP


    start_time = Get_time()
    print('-----------------------------------------------------------------------------------------------------------------------')
    print('['+Get_time()+']>>>Initiating loop')
    print('-----------------------------------------------------------------------------------------------------------------------')
    print()
    print()
    
    x = main()#CALLING MAIN FUNCTION
    
    end_time =Get_time()
    
    minutes = str(int(end_time[3:])-int(start_time[3:]))
    
    hours = str(int(end_time[:-3])-int(start_time[:-3]))
    
    print('-----------------------------------------------------------------------------------------------------------------------')
    print('['+Get_time()+']>>>Ending loop')
    print('>>>Time Taken :'+hours+' hours and '+minutes+' minutes')
    print()
    print()
    print('-----------------------------------------------------------------------------------------------------------------------')

    sleep(x)
