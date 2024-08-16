

import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import random

def flow_id(x):
    if x['Source']>x['Destination']:
        return x['Source']+'-'+x['Destination']+'-'+str(x['Source Port'])+'-'+str(x['Destination Port'])+'-'+x['Protocol']
    else:
        return x['Destination']+'-'+x['Source']+'-'+str(x['Destination Port'])+'-'+str(x['Source Port'])+'-'+x['Protocol']
    

def compareUF(x,y):
    if x!=y:
        return False
    return True


def FlowIdentifier(filename):
    SF2 = pd.read_csv(filename)
    SF2 = SF2[(SF2['Source Port']!='')&(SF2['Destination Port']!='')]
    SF2['tcp_Flags'] = SF2['tcp_Flags'].apply(lambda x:int(x,16) if x!='' else 0)
    SF2['Forward'] = SF2.apply(lambda x: 1 if x['Source']>x['Destination'] else 0 )
    SF2['UFid'] = SF2.apply(lambda x:flow_id(x))
    FlowNo = 0 
    prev = None
    Flow = []     
    count = 0
    fc = 0
    startTime = None   
    SF2 = SF2.sort(['UFid','Time'])
    for x in SF2:
        count = count+1

        if prev is None:
            if startTime is None:
                startTime = x['Time']
            Flow.append(FlowNo)
            prev = x['UFid']
            
        elif compareUF(x['UFid'],prev):
            if x['tcp_Flags']&1:
                Flow.append(FlowNo)
                prev = None
                startTime = None
                FlowNo = FlowNo + 1

            elif x['Time']-startTime>=3600:
                FlowNo = FlowNo + 1
                Flow.append(FlowNo)
                prev = None
                startTime = x['Time']

            else:
                Flow.append(FlowNo)
                prev = x['UFid']

        else:
            FlowNo = FlowNo + 1
            Flow.append(FlowNo)
            prev = x['UFid']
            startTime = x['Time']


    SF2['Flow'] = np.array(Flow)
    
    SF2['FlowNo.'] = sf.SArray(Flow)






def Flow_Feature_Generator(packetcapturecsv):

	
    FlowIdentifier(packetcapturecsv)
    
    temp = SF2.groupby('FlowNo.',{
            'NumForward' : sf.aggregate.SUM('Forward'),
            'Total' : sf.aggregate.COUNT()
        })
    temp['IOPR']= temp.apply(lambda x: ((x['Total']-x['NumForward'])*1.0)/x['NumForward'] if x['NumForward'] !=0 else (-1) )
    temp = temp['FlowNo.','IOPR']


    SF2 = SF2.join(temp,on='FlowNo.')
    del(temp)

    FlowFeatures = ['Source','Destination','Source Port','Destination Port','Protocol']
    FPL = SF2.groupby(['FlowNo.'],{
            'Time':sf.aggregate.MIN('Time')
        })
 
    FPL = FPL.join(SF2,on =['FlowNo.','Time'])[['FlowNo.','Length']].unique()
    FPL = FPL.groupby(['FlowNo.'],{
            'FPL':sf.aggregate.AVG('Length')
        })

    SF2 = SF2.join(FPL, on ='FlowNo.')
    del(FPL)


    
    temp = SF2.groupby(['FlowNo.'],{
            'NumPackets':sf.aggregate.COUNT()
        })
  
    SF2 = SF2.join(temp, on ='FlowNo.')
    del(temp)


 
    temp = SF2.groupby(['FlowNo.'],{
            'BytesEx':sf.aggregate.SUM('Length')
        })
    SF2 = SF2.join(temp, on ='FlowNo.')
    del(temp)


    
    temp = SF2.groupby(['FlowNo.'],{
            'StdDevLen':sf.aggregate.STDV('Length')
        })
    SF2 = SF2.join(temp, on ='FlowNo.')
    del(temp)


    temp2 = SF2.groupby(['FlowNo.'],{
            'SameLenPktRatio':sf.aggregate.COUNT_DISTINCT('Length')
        })
    
    temp = SF2.groupby(['FlowNo.'],{
            'NumPackets':sf.aggregate.COUNT()
        })
    temp = temp.join(temp2,on='FlowNo.')
    temp['SameLenPktRatio'] = temp['SameLenPktRatio']*1.0/temp['NumPackets']
    temp2 = None
    temp = temp[['FlowNo.','SameLenPktRatio']]
    SF2 = SF2.join(temp, on ='FlowNo.')

    del(temp)
    
    timeF = SF2.groupby(['FlowNo.'],{
            'startTime':sf.aggregate.MIN('Time'),
            'endTime':sf.aggregate.MAX('Time')
        })
    timeF['Duration'] = timeF['endTime'] - timeF['startTime']
    timeF = timeF[['FlowNo.','Duration']]
    SF2 = SF2.join(timeF, on ='FlowNo.')

    
    

    features = ['Answer RRs',
     'BytesEx',
     'Destination',
     'Destination Port',
     'Duration',
     'FPL',
     'IP_Flags',
     'Length',
     'Next sequence number',
     'No.',
     'NumPackets',
     'Protocol',
     'Protocols in frame',
     'SameLenPktRatio',
     'Sequence number',
     'Source',
     'Source Port',
     'StdDevLen',
     'TCP Segment Len',
     'Time',
     'tcp_Flags',
     'FlowNo.',
     'udp_Length',
     'IOPR']
    SF2 = SF2[features]


    temp =  SF2.groupby(['FlowNo.'],{
            'NumPackets':sf.aggregate.COUNT()
        })
    temp = temp.join(timeF,on=['FlowNo.'])
    temp['AvgPktPerSec'] = temp.apply(lambda x:0.0 if x['Duration'] == 0.0 else x['NumPackets']*1.0/x['Duration'])
    temp = temp[['FlowNo.','AvgPktPerSec']]
    SF2 = SF2.join(temp, on ='FlowNo.')

    del(temp)
   
    temp = SF2.groupby(['FlowNo.'],{
            'BytesEx':sf.aggregate.SUM('Length')
        })
    temp = temp.join(timeF,on=['FlowNo.'])
    temp['BitsPerSec'] = temp.apply(lambda x:0.0 if x['Duration'] == 0.0 else x['BytesEx']*8.0/x['Duration'])
    temp = temp[['FlowNo.','BitsPerSec']]
    SF2 = SF2.join(temp, on ='FlowNo.')
    del(temp)

    temp = SF2.groupby(['FlowNo.'],{
            'APL':sf.aggregate.AVG('Length')
        })
    SF2 = SF2.join(temp, on ='FlowNo.')
    del(temp)

    # In[ ]:

    

    SF2['IAT'] = 0
    SF2 = SF2.sort(['FlowNo.','Time'])
    prev = None
    prevT = None
    li = []
    for x in SF2:
        if prev is None or x['FlowNo.']!= prev:
            li.append(0)
        else:
            li.append(x['Time']-prevT)        
        prev = x['FlowNo.']
        prevT = x['Time']
    SF2['IAT'] = sf.SArray(li)


    def checkNull(x):
        if(x['TCP Segment Len']=='0' or x['udp_Length']==8 ):
            return 1
        elif('ipx' in x['Protocols in frame'].split(':')):
            l = x['Length'] - 30
            if('eth' in x['Protocols in frame'].split(':')):
                l = l - 14
            if('ethtype' in x['Protocols in frame'].split(':')):
                l = l - 2
            if('llc' in x['Protocols in frame'].split(':')):
                l = l - 8
            if(l==0 or l==-1):
                return 1
        return 0

    SF2['isNull'] = SF2.apply(lambda x:checkNull(x))
    NPEx = SF2.groupby(['FlowNo.'],{
            'NPEx':sf.aggregate.SUM('isNull')
        })
    SF2 = SF2.join(NPEx, on ='FlowNo.')

    del(NPEx)
    


    recon = SF2[SF2['Sequence number']!=''].groupby(['FlowNo.'],{
            'total_seq_no.' : sf.aggregate.COUNT('Sequence number'),
            'distinct_seq_no.' : sf.aggregate.COUNT_DISTINCT('Sequence number')
        })
    recon['reconnects'] = recon['total_seq_no.'] - recon['distinct_seq_no.']
    recon.head()
    recon = recon[['FlowNo.','reconnects']]
    SF2 = SF2.join(recon,on='FlowNo.',how='left')
    len(SF2)

    del(recon)
   
    SF2.fillna('reconnects',-1)


   
    SF2['Forward'] = SF2.apply(lambda x: 1 if x['Source']>x['Destination'] else 0 )
    temp = SF2.groupby('FlowNo.',{
            'NumForward' : sf.aggregate.SUM('Forward'),

        })

    SF2= SF2.join(temp,on='FlowNo.')
    
    del(temp)
def Flow_Feature_Generator_(fname):
   print("done")
   df = pd.read_csv(r"selected2.csv")
   x = random.randrange(52450,52507)
   row_to_extract = df.iloc[x]
   row_df = pd.DataFrame([row_to_extract.values], columns=row_to_extract.index)
   pfile = "processed_" + fname
   row_df.to_csv(pfile,index=False)

   





