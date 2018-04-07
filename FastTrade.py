# -*- coding: utf-8 -*-
"""
Created on Thu Jan 11 11:28:53 2018
模拟盘服务器端
0账号，1时间，2方向，3代码，4价格，5可用余额，6总余额
0账号，1委买，-1委卖，2委买成交，-2委卖成交，3转入持仓，-3转出持仓，4买入撤单，-4卖出撤单
@author: LiuWeipeng
"""
import pandas as pd
from time import sleep,time,localtime,strftime
import numpy as np
import tushare as ts
#import socket
import threading
#import sys
#from datetime import datetime
import h5py
#from re import split
#from math import ceil
import logging


from vnrpc import RpcClient,RpcServer
class TestClient(RpcClient):
    """行情服务器"""

    def __init__(self, reqAddress, subAddress):
        """Constructor"""
        super(TestClient, self).__init__(reqAddress, subAddress)

    def callback(self, topic, data):
        global hq,times,speed
        """回调函数实现"""
        #print ('回调函数测试', topic, ', data:', data)
        if topic==b'sinahq':        
            hq=data
        if topic==b'time':
            times=data[0]
            speed=data[1]
# ----------------------------------------------------------------------

class FastServer(RpcServer):
    """模拟服务器"""

    def __init__(self, repAddress, pubAddress):
        """Constructor"""
        super(FastServer, self).__init__(repAddress, pubAddress)

        self.register(self.login)
        self.register(self.prebuy)
        self.register(self.presell)
        self.register(self.prebuy)        
        self.register(self.buy)
        self.register(self.sell)        
        self.register(self.cashvalue)
        self.register(self.holding)        
        self.register(self.waitbuy)
        self.register(self.waitsell) 
        self.register(self.sellall)        
        self.register(self.cancelbuy)
        self.register(self.cancelsell)         
        
    def login(self, account, password,cash):
        return login(account,password,cash)
    def prebuy(self,ids,code,price,num):
        print(ids,':buy',code,price,num)
        return prebuy(ids,code,price,num)
    def presell(self,ids,code,price,num):
        print(ids,':sell',code,price,num)
        return presell(ids,code,price,num)
    def buy(self,ids,code,num,cash):
        print(ids,':buyin',code,num)
        return buy(ids,code,num,cash)
    def sell(self,ids,code,num):
        print(ids,':sellout',code,num)
        return sell(ids,code,num)
    def cashvalue(self,ids):    
        return cashvalue(ids)
    def holding(self,ids):
        return holdings(ids)
    def waitbuy(self,ids):
        return waitbuy(ids)
    def waitsell(self,ids):
        return waitsell(ids)
    def sellall(self,ids):
        print(ids,':clear')
        return sellall(ids)
    def cancelbuy(self,ids,code):
        print(ids,':cancelbuy',code)
        return cancelbuy(ids,code)
    def cancelsell(self,ids,code):
        print(ids,':cancelsell',code)
        return cancelsell(ids,code)  
    def del_account(self,ids):
        return del_account(ids)
def login(account,password,cash=1000000):
    '''
    登陆账号和密码，设置初始资金，传入参数分别为账号名称，密码，模拟盘资金量，如果不填默认1000000，如果账号不存在，则创建账号。
    '''
    global df,td
    if df[(df.account==account)].empty:#创建账号
        
        ids=len(df)+1
        dfn=pd.DataFrame([[account,password,ids]],columns=['account','password','ids'])
        df=pd.concat([df,dfn], ignore_index=True)
        #账号0，时间1，方向2，代码3，价格4，可用5，总量6
        tdn=np.array([ids,times,0,0,0,cash,cash]).reshape(1,7)
        td=np.vstack((td,tdn))
        ret=ids
        print(account,cashvalue(ret))
    else:
        if (df[(df.account==account)]['password']==password).bool():
            ret=int(df[(df.account==account)]['ids'])
            print(account,cashvalue(ret))
        else:
            ret=-1  #登陆失败
            
    
    return ret 
    
def del_account(ids):
    '''
    注销账户
    '''
    global td
    td=td[td[:,0]!=ids]
    return 1
def cancelallbuy():
    '''
    取消所有账户的买入单
    '''    
    wb=td[td[:,2]==1]
    for i in range(len(wb)):
        cancelbuy(wb[i,0],wb[i,3])  
def cancelallsell():
    '''
    取消所有账户的卖出单
    '''    
    wb=td[td[:,2]==-1]
    for i in range(len(wb)):
        cancelsell(wb[i,0],wb[i,3])         
        
        
def account_clear():
    '''
    账户清算，模拟收盘后的资金清算过程，所有未成交委托单将被撤销，所有不可用股票余额变为可用
    '''    
    global td,stock
    cancelallbuy()
    cancelallsell()
    lock.acquire()
    try:
        td[td[:,2]==3,5]=td[td[:,2]==3,6]
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))  
    lock.release()
    zh=td[td[:,2]==0]
    for i in range(len(zh)):
        cashvalue(zh[i,0])
    folder=strftime('%Y-%m-%d',structtime)
    #folder=strftime('%Y-%m-%d')
    #f.pop('detail/'+folder)
    f['detail/'+folder]=td #写入数据集
    df.to_hdf('record.h5','accounts',append=False, complib='zlib', complevel=9)
    zhanghu=td[td[:,2]==0][:,[0,5,6]]
    value=pd.DataFrame(zhanghu,index=[folder]*len(zhanghu),columns=['ids','cash','value'])
    value.to_hdf('record.h5','value',append=True, complib='zlib', complevel=9)    
#新表储存
    td=td[np.any([td[:,2]==0,td[:,2]==3],axis=0)]
    f.pop('last')  
    f['last']=td
    
    print(value)

    stock=ts.get_stock_basics()        
        
        
def monitoring():  #禁止超多档价位委托
    '''
    盘中实时监控委托单是否成交，当价格达到委托价格，即按成交处理
    '''
    #global df
    wb=td[td[:,2]==1]
    codes=[dic[x] for x in wb[:,3]]
    price=hq.loc[codes]['sp1']
    cj=wb[(wb[:,4]>=price) & (price>0)]
    for i in range(len(cj)):
        dealbuy(cj[i,0],cj[i,3],cj[i,4],cj[i,6])
        
    ws=td[td[:,2]==-1]    
    codes=[dic[x] for x in ws[:,3]]
    price=hq.loc[codes]['bp1']
    cj=ws[(ws[:,4]<=price) & (price>0)]     
    for i in range(len(cj)):
        dealsell(cj[i,0],cj[i,3],cj[i,4],cj[i,6])





def cashvalue(ids):
    '''
    计算账户的资金及总市值
    '''    
    global td
    holding=holdings(ids)  
    lock.acquire()
    try:    
    #cash=td[np.all([td[:,0]==ids,td[:,2]==0],axis=0),5]
        cash=td[(td[:,0]==ids)*(td[:,2]==0),5]
        value=sum(holding[:,8])
        #wt=td[np.all([td[:,0]==ids,td[:,2]==1],axis=0)]
        wt=td[(td[:,0]==ids)*(td[:,2]==1)]
        zhan=sum(wt[:,4]*wt[:,6])
        totalvalue=cash+value+zhan
        td[(td[:,0]==ids)*(td[:,2]==0),6]=totalvalue
        #td[np.all([td[:,0]==ids,td[:,2]==0],axis=0),6]=totalvalue
        zijin=[float(cash),float(totalvalue)]
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))          
    lock.release()
    return zijin
def holdings(ids):
    '''
    计算账户持仓状况，返回列项分别为账户id，时间，方向，股票代码，成本价，股票可用数量，股票总数，市价，市值，盈亏百分比
    '''    
    #hold=td[np.all([td[:,0]==ids,td[:,2]==3],axis=0)]
    lock.acquire() 
    try:
        hold=td[(td[:,0]==ids)*(td[:,2]==3)]
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))      
    lock.release()
    
    marketprice=[]
    marketvalue=[]
    profit=[]
    for i in range(len(hold)):
        if float(hq.loc[dic[hold[i,3]],['sp1']])==0 and float(hq.loc[dic[hold[i,3]],['bp1']])==0:
            mp=float(hq.loc[dic[hold[i,3]],['refclose']])
        else:
            mp=float(hq.loc[dic[hold[i,3]],['now']])
        if np.isnan(mp):
            mp=hold[i,4]
        marketprice.append(mp)
        marketvalue.append(mp*hold[i,6])
        #profit.append(int((mp/hold[i,4]-1)*10000)/100)
        profit.append((mp/hold[i,4]-1)*100)
    holding=np.hstack((hold,np.array(marketprice).reshape(-1,1),np.array(marketvalue).reshape(-1,1),np.array(profit).reshape(-1,1)))
    return holding
def waitbuy(ids):
    '''
    查询委托买入，返回列项分别为0时间，1股票代码，2成本价，3股票总数
    '''    
    #cash=td[np.where(np.all([td[:,0]==ids,td[:,2]==0],axis=0)),5]
    #wait=td[np.all([td[:,0]==ids,td[:,2]==1],axis=0)][:,[1,3,4,6]]
    #waits=wait
    lock.acquire()
    try:
        wait=td[(td[:,0]==ids)*(td[:,2]==1)]
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))      
    lock.release()
    waits=wait[:,[1,3,4,6]]
    #waits=df[(df.account==account) & (df.password==password) & (df.direct==1)]
    return waits
def waitsell(ids):
    '''
    查询委托卖出，返回列项分别为0时间，1股票代码，2成本价，3股票总数
    '''    
    lock.acquire()
    try:
        wait=td[(td[:,0]==ids)*(td[:,2]==-1)]
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))      
    lock.release()
    #wait=td[np.all([td[:,0]==ids,td[:,2]==-1],axis=0)]
    waits=wait[:,[1,3,4,6]]
    #waits=df[(df.account==account) & (df.password==password) & (df.direct==-1)]
    return waits
def buy(ids,code,num,cash=False):
    '''
    现价买入，传入参数分别为账号id，股票代码，股票数量,cash=Ture,固定资金买入
    '''    
    code=float(code)
  
    #price=hq.loc[dic[code]][20]
    price=float(hq.loc[dic[code],['sp1']])
    if price>0:
        if cash==True:
            num=int(num/price/100)*100
        else:
            num=float(num)      
        ret=prebuy(ids,code,price,num)
    else:
        ret=0
    return ret



def sell(ids,code,num='all'):
    '''
    现价卖出，传入参数分别为账号id，股票代码，股票数量
    '''    
    code=float(code)
    if num!='all':
        num=float(num)    
    #price=hq.loc[dic[code]][10]
    price=float(hq.loc[dic[code],['bp1']])
    ret=presell(ids,code,price,num)
    return ret
def prebuy(ids,code,price,num):#委买，direct==1,全部改为数值型
    '''
    指定价买入，传入参数分别为账号id，股票代码，股票价格，股票数量
    '''
    global td
    code=float(code)
    price=float(price)
    num=float(num)
    lock.acquire()
    try:
    #cash=td[np.all([td[:,0]==ids,td[:,2]==0],axis=0),5]
        cash=td[(td[:,0]==ids)*(td[:,2]==0),5]
        if cash>=price*num  and price>0 and num>0:
            td[np.all([td[:,0]==ids,td[:,2]==0],axis=0),5]=cash-price*num
            tdn=np.array([ids,times,1,code,price,0,num]).reshape(1,7)
            td=np.vstack((td,tdn))
    
            ret=1
        else:
            ret=0
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))  
        ret=0
    lock.release()
    return ret
def presell(ids,code,price,num='all'):#委卖，direct==-1
    '''
    指定价卖出，传入参数分别为账号id，股票代码，股票价格，股票数量
    '''    
    global td
    code=float(code)
    price=float(price)
  
    
    #cansells=td[np.all([td[:,0]==ids,td[:,2]==3,td[:,3]==code],axis=0),5]
    lock.acquire()
    try:
        cansells=td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),5]
        if num=='all':
            num=cansells
        else:
            num=float(num) 
        if cansells>=num and price>0 and num>0:
            td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),5]=cansells-num
            tdn=np.array([ids,times,-1,code,price,num,num]).reshape(1,7)
            td=np.vstack((td,tdn))   
            ret=1
        else:        
            ret=0
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))  
        ret=0            
    lock.release()    
    return ret
        
def dealbuy(ids,code,price,num):#买入成交，derect==2
    '''
    处理委托买入的成交单，传入参数分别为账号id，股票代码，股票价格，股票数量
    '''
    global td
  
    lock.acquire()
    try:
        tds=len(td[(td[:,0]==ids)*(td[:,2]==1)*(td[:,3]==code)*(td[:,4]==price)*(td[:,6]==num)])
        #tds=len(td[np.all([td[:,0]==ids,td[:,2]==1,td[:,3]==code,td[:,4]==price,td[:,6]==num],axis=0)])
        #td[np.all([td[:,0]==ids,td[:,2]==1,td[:,3]==code,td[:,4]==price,td[:,6]==num],axis=0),2]=2
        td[(td[:,0]==ids)*(td[:,2]==1)*(td[:,3]==code)*(td[:,4]==price)*(td[:,6]==num),2]=2
    
        #ncash=td[np.all([td[:,0]==ids,td[:,2]==0],axis=0),5]-price*num*fee*tds
        ncash=td[(td[:,0]==ids)*(td[:,2]==0),5]-price*num*fee*tds
        td[(td[:,0]==ids)*(td[:,2]==0),5]=ncash
        if len(td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code)])==0:
            tdn=np.array([ids,times,3,code,price,0,num*tds]).reshape(1,7)
            td=np.vstack((td,tdn))        
        else:
            tdy=td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code)]
            total=tdy[0,6]+num*tds
            cost=(tdy[0,4]*tdy[0,6]+price*num*tds)/total
            td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),6]=total
            #td[np.all([td[:,0]==ids,td[:,2]==3,td[:,3]==code],axis=0),4]=cost
            td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),4]=cost
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))  
    lock.release()
def dealsell(ids,code,price,num):#卖出成交，derect==-2
    '''
    处理委托卖出的成交单，传入参数分别为账号id，股票代码，股票价格，股票数量
    '''
    global td
    lock.acquire()
    try:
        tds=len(td[(td[:,0]==ids)*(td[:,2]==-1)*(td[:,3]==code)*(td[:,4]==price)*(td[:,6]==num)])    
        #tds=len(td[np.all([td[:,0]==ids,td[:,2]==-1,td[:,3]==code,td[:,4]==price,td[:,6]==num],axis=0)])
        td[(td[:,0]==ids)*(td[:,2]==-1)*(td[:,3]==code)*(td[:,4]==price)*(td[:,6]==num),2]=-2
        ncash=td[(td[:,0]==ids)*(td[:,2]==0),5]+price*num*(1-tax-fee)*tds
        td[(td[:,0]==ids)*(td[:,2]==0),5]=ncash
        dfy=td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code)]
        if num*tds==dfy[0,6]:
            td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),2]=-3
        else:
            total=dfy[0,6]-num*tds
            td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),6]=total
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))  
    lock.release()
def sellall(ids):
    '''
    全部清仓，传入参数为账户id
    '''    
    h=holdings(ids)
    for i in range(len(h)):
        presell(ids,h[i,3],float(hq.loc[dic[h[i,3]],['sp1']]),h[i,5])
    ret=1
    #ret=account+':'+'全部清仓' 
    return ret
def cancelbuy(ids,code):
    '''
    撤销买入，传入参数为账户id，股票代码    
    '''    
    global td
    code=float(code)
    lock.acquire()
    try:
        dfy=td[(td[:,0]==ids)*(td[:,2]==1)*(td[:,3]==code)]
        if len(dfy)==0:
            ret=0
        else:
            ncash=td[(td[:,0]==ids)*(td[:,2]==0),5]+sum(dfy[:,4]*dfy[:,6])
            td[(td[:,0]==ids)*(td[:,2]==0),5]=ncash
            td[(td[:,0]==ids)*(td[:,2]==1)*(td[:,3]==code),2]=4
            ret=1
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))  
        ret=0        
    lock.release()
    return ret
def cancelsell(ids,code):
    '''
    撤销卖出，传入参数为账户id，股票代码    
    '''    
    global td
    code=float(code)
    lock.acquire()
    try:    
        dfy=td[(td[:,0]==ids)*(td[:,2]==-1)*(td[:,3]==code)]
        if len(dfy)==0:
            ret=0
        else:
            ncansell=td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),5]+sum(dfy[:,6])
            td[(td[:,0]==ids)*(td[:,2]==3)*(td[:,3]==code),5]=ncansell
            td[(td[:,0]==ids)*(td[:,2]==-1)*(td[:,3]==code),2]=-4
            ret=1
    except Exception as e:
        logger.exception('ERROR:%s' % str(e))  
        ret=0            
    lock.release()
    return ret    
    
 


        
if __name__ == '__main__':

    logger = logging.getLogger('sim')  
    logger.setLevel(logging.DEBUG)  
      
    # 创建一个handler，用于写入日志文件  
    fh = logging.FileHandler('Trade.log')  
    fh.setLevel(logging.DEBUG)  
      
    # 再创建一个handler，用于输出到控制台  
    ch = logging.StreamHandler()  
    ch.setLevel(logging.DEBUG)  
      
    # 定义handler的输出格式  
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')  
    fh.setFormatter(formatter)  
    ch.setFormatter(formatter)  
      
    # 给logger添加handler  
    logger.addHandler(fh)  
    logger.addHandler(ch)    
#title=['账号','密码','买卖方向','股票代码','股票名称','成本价','卖出价','股票总数','可用数量','可用资金','总市值']
#title=['account','password','direct','code','name','cost','sellprice','total','cansell','cash','value']
    df=pd.read_hdf('record.h5',key='accounts')
    #values=pd.read_hdf('record.h5',key='value')
    f=h5py.File('record.h5','a')
    #f=h5py.File('record.h5','r')
    dset=f['last']
    td=dset[...]

    stock=ts.get_stock_basics()
    dic={}
    for val in stock.index:
        dic[float(val)]=val
    #stock.loc[['300033','000625']]
    #fee=0.0003
    fee=0.0003
    tax=0.001
    lock = threading.Lock()    
    times=time()
    #reqAddress = 'tcp://60.205.205.213:8888'
    #subAddress = 'tcp://60.205.205.213:8889'     
    reqAddress = 'tcp://localhost:2014'
    subAddress = 'tcp://localhost:0602'

    tc = TestClient(reqAddress, subAddress)
    tc.subscribeTopic(b'sinahq')
    tc.subscribeTopic(b'time')
    tc.start()    
    sleep(3)
   
    repAddress2 = 'tcp://*:2018'
    pubAddress2 = 'tcp://*:9888'
    tsever = FastServer(repAddress2, pubAddress2)
    tsever.start()

    print('Ready for conection——FastTrade')

    #speed=10 #速度，正常速度为1，模拟可设置为3
    sleep(1)
    while 1:
        try:
            '''
            hour= datetime.now().hour
            mini=datetime.now().minute
            timenum=hour*60+mini
            week=datetime.now().weekday()
            
            if (( timenum>=570 and timenum<690) or ( timenum>=780 and timenum<897) ) and week<5: 
            '''
            structtime=localtime(int(times))
            timenum=structtime[3]*60+structtime[4]
            #print(strftime('%Y-%m-%d %H:%M:%S',structtime))
            if ( timenum>=570 and timenum<690) or ( timenum>=780 and timenum<897):
                t1=time()
                try:
                    #hq=getdata()
                    monitoring()

                except Exception as e:
                    logger.exception('ERROR:%s' % str(e))  
                    #hq=gettx()
                    #monitoring()
                t2=time()
                if 1/speed-t2+t1>0:
                    sleep(1/speed-t2+t1)
                else:            
                    sleep(0.001)
                    print('超时',1/speed-t2+t1)
                
            else:
                sleep(3/speed)
            if timenum==899:
                account_clear()
                sleep(65/speed)
        except Exception as e:
            logger.exception('ERROR:%s' % str(e))  
        
        
        
        
        
        
        
        
        
        
        