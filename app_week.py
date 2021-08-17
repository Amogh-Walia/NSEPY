from flask import Flask
from flask import render_template

import io
import base64
from datetime import date, datetime



from flask import Response
import pandas as pd
from functions import *
app = Flask(__name__)
app.secret_key = 'your secret key'

dates = [17,18,19,20]

def Get_time():
    #function to get the current system time
    time = str(datetime.now().strftime('%H:%M:%S'))
    return time
def Day(i):
    i = str(i)
    today = date.today()
    day = (today.strftime("%b_")).lower()
    put_table = 'put'+day+i
    call_table = 'call'+day+i
    return(put_table,call_table)

    
@app.route('/')
def View():
    

    return render_template('view.html')

@app.route('/dynamic')

def dynamic():
    msg = Get_time()
    f = open("timestamp.txt", "r")
    timestamp = f.read()
    f.close()
    
    return render_template('dynamic.html',msg=msg,timestamp = timestamp)    
@app.route('/plot_Graph1.png')

def plot_png():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

def create_figure():
    frame = pd.DataFrame()
    underlyingValue = pd.DataFrame()
    for i in dates:
        put_table,call_table = Day(i)
        Frame           = Sql_To_Frame("SELECT   sum(p.changeinOpenInterest)-sum(c.changeinOpenInterest) as sigmaChangeInOI  FROM  "+call_table+" c INNER JOIN  "+put_table+" p  ON c.strikePrice = p.strikePrice and c.Time = p.Time group by c.Time");
        Uframe = Sql_To_Frame("select underlyingValue from "+put_table+" where strikePrice = '12800'");
        Time = Sql_To_Frame("select Time from "+put_table+" where strikePrice = '12800'");
        Time['Time'] = str(i)+'_'+Time['Time']
        Frame = pd.concat([Frame, Time], axis=1)
        Uframe = pd.concat([Uframe, Time], axis=1)
        frame = pd.concat([frame, Frame], axis=0)
        underlyingValue = pd.concat([underlyingValue, Uframe], axis=0)
    
    frame['sigmaChangeInOI'] = abs(frame['sigmaChangeInOI'])
    frame['Moving_Average3'] = Moving_Average(frame['sigmaChangeInOI'],3)
    frame['Moving_Average5'] = Moving_Average(frame['sigmaChangeInOI'],5)
    frame['Moving_Average8'] = Moving_Average(frame['sigmaChangeInOI'],8)
    

    fig = Plot_Frame(['sigmaChangeInOI','Moving_Average3','Moving_Average5','Moving_Average8'],['underlyingValue'],'Time',frame,underlyingValue,['blue','yellow','green','orange'],['black'])

    return fig


@app.route('/plot_Graph2.png')

def plot_png2():
    fig = create_figure2()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

def create_figure2():
    frame = pd.DataFrame()
    underlyingValue = pd.DataFrame()
    for i in dates:
        put_table,call_table = Day(i)
        Frame           = Sql_To_Frame("SELECT   sum(p.openInterest) as put_oi,sum(c.openInterest) as call_oi  FROM  "+call_table+" c INNER JOIN "+put_table+" p  ON c.strikePrice = p.strikePrice and c.Time = p.Time  group by c.Time");
        Uframe = Sql_To_Frame("select underlyingValue from "+put_table+" where strikePrice = '12800'");
        Time = Sql_To_Frame("select Time from "+put_table+" where strikePrice = '12800'");
        Time['Time'] = str(i)+'_'+Time['Time']
        Frame = pd.concat([Frame, Time], axis=1)
        Uframe = pd.concat([Uframe, Time], axis=1)
        frame = pd.concat([frame, Frame], axis=0)
        underlyingValue = pd.concat([underlyingValue, Uframe], axis=0)
    print(frame)
    fig = Plot_Frame(['put_oi','call_oi'],['underlyingValue'],'Time',frame,underlyingValue,['green','red'],['black'])
    return fig


@app.route('/plot_Graph3.png')

def plot_png3():
    fig = create_figure3()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

def create_figure3():
    frame = pd.DataFrame()
    underlyingValue = pd.DataFrame()
    for i in dates:
        put_table,call_table = Day(i)
        Frame           = Sql_To_Frame("select (Sum(`Call Premium turnover`)-SUM(`Put Premium turnover` )) as delta_Premium from "+put_table+" group by Time");

        Uframe = Sql_To_Frame("select underlyingValue from "+put_table+" where strikePrice = '12800'");
        Time = Sql_To_Frame("select Time from "+put_table+" where strikePrice = '12800'");
        Time['Time'] = str(i)+'_'+Time['Time']
        Frame = pd.concat([Frame, Time], axis=1)
        Uframe = pd.concat([Uframe, Time], axis=1)
        frame = pd.concat([frame, Frame], axis=0)
        underlyingValue = pd.concat([underlyingValue, Uframe], axis=0)

    fig = Plot_Frame(['delta_Premium'],['underlyingValue'],'Time',frame,underlyingValue,['blue'],['black'])
    return fig

if __name__ == '__main__':
    app.debug = True
    app.run(host = '192.168.1.7',debug = True)
