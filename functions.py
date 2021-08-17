import pandas as pd
from sqlalchemy import create_engine
import pymysql
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
import matplotlib.pyplot as plt




sqlEngine = create_engine('mysql+mysqlconnector://root:15w60ps@localhost:3306/test', echo=False)
SIZE = (36,18)

def Moving_Average(numbers,window_size):
    

    numbers_series = pd.Series(numbers)
    windows = numbers_series.rolling(window_size)
    moving_averages = windows.mean()

    moving_averages_list = moving_averages.tolist()

    return moving_averages_list

def Sql_To_Frame(command):
    
    dbConnection    = sqlEngine.connect()
    frame           = pd.read_sql(command, dbConnection);
    dbConnection.close()
    return frame

def Plot_Frame(sub_axis1,sub_axis2,x_axis,frame1,frame2,color1 = ['blue','yellow','green'],color2 = ['black']):
    fig = plt.figure(figsize = SIZE)
    ax1 = fig.add_subplot(111)
    for i in range(0,len(sub_axis1)):
        ax1.plot( x_axis, sub_axis1[i], data=frame1,color=color1[i], linewidth=2)


    
    ax2 = ax1.twinx()
    for i in range(0,len(sub_axis2)):
        ax2.plot( x_axis, sub_axis2[i], data=frame2,color=color2[i], linewidth=2)
    
    ax2.tick_params(axis='both', which='major',colors = 'black', labelsize=20)
    ax1.tick_params(axis='y', which='major',colors = 'blue', labelsize=20)

    
    fig.legend(prop={'size': 36})
    return fig
