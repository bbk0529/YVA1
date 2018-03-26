import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plot

def fileRead(filename) :
    items=[]
    item=[]
    count=0
    toggle=False

    with open(filename,"r", encoding="UTF-8") as file:
    #with open(filename,"r", encoding="ISO-8859-1") as file:
        line=file.readline()
        line=file.readline()
        while line.find('*****')<0:
            line=file.readline()
            
            if line.find("Seite") >0 :line=file.readline();continue
            
            if line[0]=='-':  count +=1                       
            
            if line[0]=='|'  or  line.count('Ident-code'):
                item.append(line)
                
            if (line[0]=='-' and item!=[] and count%2==1) :
            #if (line[0]=='-' and item!=[] and count%2==1) or  line.find("*****")>0 :
                items.append(item)
                item=[]
                toggle=False
    items.append(item)    
    return items
	
def itemsParsing(items) :
    STATUS={
            "ERO" : "Not Planned, not started. May have missing parts", 
            "FREI" : "No missing part, production order free", 
            "TRÜC": "1 level of a 2 level produciton has been finished", 
            "RÜCK" : "Production finished, on the way to CSC", 
            "GLFT" : "Delivered to CSC",
            "LOE" : "P Order Deleted/Finished",
            "" : "UNKNOWN"
    }
    TotalList=[]
    SumList=[]

    #For each items 
    for row in range(1,len(items)) :
        stored=''
        n=len(items[row])
        
        if str(items[row]).find("stored in")>0 : 
            n=n-1
            stored=items[row][-2][76:91].strip()
        
        status=items[row][0][91:95].strip()
        so=items[row][2][26:36]
        no=int(items[row][0][1:5].strip())
        partno=items[row][0][14:22]
        config=items[row][-1][36:80].strip() if items[row][-1][36:80].count("-")>3 else items[1][0][23:62].strip()

        #Each records of each Items Parsing
        T1=0;T2=0;T3=0;T4=0
        if str(items[row]).find("Ident")>0 : n-=1
        for i in range(2,n) :
            t=items[row][i]

            if t[11:19].find(".")>0 :
                d1=datetime.datetime.strptime(t[11:19],  "%y.%m.%d").date()

            if t[35:46].find(".")>0 :
                d2=datetime.datetime.strptime(t[38:46],  "%y.%m.%d").date() 
                eta=d2

            if t[85:93].find(".")>0 and t[85:93].find("00.00.00")==-1: 
                d3=datetime.datetime.strptime(t[85:93],  "%y.%m.%d").date()
            
            if t[116:124].find(".")>0 : 
                d4=datetime.datetime.strptime(t[116:124],  "%y.%m.%d").date()
            
            t1=0;t2=0;t3=0;t4=0
            if t[22:25].strip()!='' : 
                t1=int(t[22:25])
                T1+=int(t1)    
            
            if t[49:52].strip()!='' : 
                t2=int(t[49:52]) 
                T2+=int(t2)    

            if t[96:99].strip() !="" and t[96:99].find("ED")==-1 : 
                t3=int(t[96:99]);
                T3+=t3
                
            if t[127:130].strip() !="" and t[127:130].find("|")==-1 : 
                t4=int(t[127:130]);
                T4+=t4

            #Invoice#     
            reference=t[-31:-23].strip()

            #A single row created here
            TotalList.append([so+ str(no), d1,t1,d2,t2,d3,t3,d4,t4,reference])

        delta=T1-T4
        reference=''
        if delta==0:
            etd=TotalList[-1][-3]
            REMARK="Invoiced"
            reference=items[row][-2][-31:-23]
        else :
            REMARK=STATUS[status]

        partial="Partial" if delta>0 and T1>delta else ''
        SumList.append([so+ str(no), partno, config, T1,T2,T3,T4, delta, stored, etd, eta, status,REMARK, reference, partial])

    return TotalList, SumList, 
	
def detailDF (conDF, list) :
    print("detailDF() called")
    DF=pd.DataFrame([], columns=["d1", "t1", "d2", "t2", "d3","t3", "d4", "t4", "reference"])

    for i in list:
        print (i)
        DF=pd.concat([DF, pd.DataFrame(conDF.loc[i], columns=["d1", "t1", "d2", "t2", "d3","t3", "d4", "t4", "reference"])])
        #partialDF=pd.concat([partialDF, pd.DataFrame(conDF.loc[i])])
    return DF

def mergeDF (filelist) :
    colDF=["idx", "d1", "t1", "d2", "t2", "d3","t3", "d4", "t4", "reference"]
    colSUM=["idx", "partno", "config","T1","T2","T3","T4","△","Stored", "ETD", "ETA", "Status", "Remark", "Ref#", "Partial"]
    conDF=pd.DataFrame([], columns=colDF)
    conSUM=pd.DataFrame([], columns=colSUM)

    for file in filelist :
        print (file, "read to be parsed.... ")
        items=fileRead(file)
        TotalList, SumList = itemsParsing(items)

        conDF=pd.concat([conDF,pd.DataFrame(TotalList, columns=colDF)])
        conSUM=pd.concat([conSUM,pd.DataFrame(SumList, columns=colSUM)])

    conDF=pd.DataFrame(conDF, columns=colDF)                         
    conSUM=pd.DataFrame(conSUM, columns=colSUM)
    conDF=conDF.set_index("idx")
    conSUM=conSUM.set_index("idx")
    partialDF = detailDF(conDF, conSUM[conSUM.Partial=='Partial'].index)
    
    return conDF, conSUM, partialDF

filelist=[]
f=open("YVA1list.txt","r", encoding="UTF-8")
for line in f :
    filelist.append(line.splitlines()[0])

DF,SUM,PARTIAL = mergeDF(filelist)