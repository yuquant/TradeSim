# encoding: UTF-8

from time import sleep, time,strptime,mktime
from vnrpc import RpcServer
import pandas as pd
import numpy as np
import h5py

class TestServer(RpcServer):
    """测试服务器"""

    # ----------------------------------------------------------------------
    def __init__(self, repAddress, pubAddress):
        """Constructor"""
        super(TestServer, self).__init__(repAddress, pubAddress)
'''
        self.register(self.add)
    def add(self, a, b):
        """测试函数"""
        print( 'receiving: %s, %s' % (a, b))
        return a + b
        

'''

if __name__ == '__main__':
    repAddress = 'tcp://*:2014'
    pubAddress = 'tcp://*:0602'
    #update_stock()
    ts = TestServer(repAddress, pubAddress)
    ts.start()
    speed=25
    f=h5py.File(r'D:\StockData.hdf5','r')
    dst=f['stock']
    code=f['stockcode'][...]
    date=f['date'][...]
    it=f['item'][...]
    tick=f['tick'][...]
    times=[str(t) for t in tick]
    items=[str(i) for i in it]
    codes=[str(c)[2:] for c in code][:3559]
    dates=[str(d) for d in date]
    start=dates.index('20170623')
    end=dates.index('20171229')
    hqy=dst[:3559,start,0,[0,2,4,9]]
    last=pd.DataFrame(hqy,index=codes,columns=['now','amount','bp1','sp1'])
    ref=last.now
    for i in range(start,end):
        shou=ref.copy()
        sleep(50/speed)
        timeArray=1
        ts.publish(b'time',[timeArray,speed])
        data=dst[:3559,i,:,[0,2,4,9]]
        a=0
        for j in range(1,4802):
            t1=time()
            timeArray =mktime(strptime(date[i]+tick[j], "%Y%m%d%H:%M:%S"))
            a+=1
            if a==20:
                print(date[i]+'  '+tick[j])
                a=0
            hqy=data[:,j,:]
            hq=pd.DataFrame(hqy,index=codes,columns=['now','amount','bp1','sp1'])
            #last=hq.combine_first(last.iloc[:,[0,2,3]])
            last=hq.combine_first(last.loc[:,['now','bp1','sp1']]) 
            quzero=last.now
            quzero[quzero==0]=np.nan            
            ref=quzero.combine_first(ref)
            refc=pd.DataFrame(np.array(shou),index=codes,columns=['refclose'])
            hqn=pd.concat([last,refc],axis=1)        
            ts.publish(b'time',[timeArray,speed])
            ts.publish(b'sinahq', hqn)
            t2=time()
            if 3/speed-t2+t1>0:
                sleep(3/speed-t2+t1)
            else:            
                sleep(0.0001)
                print('超时',3/speed-t2+t1)
