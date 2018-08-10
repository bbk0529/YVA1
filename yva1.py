import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plot
import mysql.connector
import time

STATUS={
            "PLAF" : "", 
			"ERO" : "", 
            "FREI" : "To be produced soon", 
            "TRÜC": "In Production", 
            "RÜCK" : "Production finished", 
            "GLFT" : "Production finished",
            "BEST" : "Oursourcing",
            "LOE" : "Deleted or Finished",
            "TGLI" : "Partial delivery",
            "PACK" : "Production finished",
            "" : ""
    }

# STATUS={
            # "PLAF" : "Plan order, created automatically after the order has been created", 
			# "ERO" : "Not Planned, not started. May have missing parts", 
            # "FREI" : "No missing part, production order free", 
            # "TRÜC": "1 level of a 2 level produciton has been finished", 
            # "RÜCK" : "Production finished, on the way to CSC", 
            # "GLFT" : "Delivered to CSC",
            # "BEST" : "Parts are purchased at our supplier",
            # "LOE" : "P Order Deleted/Finished",
            # "TGLI" : "Partical delivery, the production order has been splitted and partical delivered",
            # "PACK" : "Packed",
            # "" : "UNKNOWN"
    # }


#Extract only data within confined boxes    
def fileRead(filename) :
    items=[]
    item=[]
    count=0
    toggle=False

    #with open(filename,"r", encoding="UTF-8") as file:
    with open(filename,"r", encoding="ISO-8859-1") as file:
        for i in range(6) :
            line=file.readline()
        line=file.readline()
        so=line.split()[5]
        po=line.split()[8]
        
        podate=file.readline().split()[6]                
        shipcondition=file.readline()[64:84].strip()
        #print(podate, shipcondition)
        
        
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
    result = [items,so,po,podate,shipcondition]
    return items, so, po, podate, shipcondition
	
def itemsParsing(result) :
    
    items = result[0] 
    so = result[1] 
    po = result[2] 
    podate = result[3]
    shipcondition =result[4]
    
    TotalList=[]
    SumList=[]
    #For each items 
    start=1    

    for row in range(0,len(items)) :
        if str(items[row]).find("|Item / M.|RR|MATERIAL|Description                             |Bi.bl|Rea|Cat In.no.+Status    |PS|            |Di|  |  |S |L |T   |")>0 :
            start=row+1 
            break      
    #print("start from this number :", start)    
    for row in range(start,len(items)) :          
    
        #print(row)
        stored=None
        cancelled=False
        blocked=False
                
        n=len(items[row])
        
        if str(items[row]).find("stored in")>0 : 
            n=n-1
            stored=items[row][-2][76:91].strip()
                
        status=items[row][0][91:95].strip()         
        no=int(items[row][0][1:5].strip())
        partno=items[row][0][14:22].strip()
        
        config=items[row][-1][36:80].strip() if items[row][-1][36:80].count("-")>=3 else items[row][0][23:62].strip()
        #print(partno, config)   
               
        #Each records of each Items Parsing
        T1=0;T2=0;T3=0;T4=0
        
        if str(items[row]).find("Ident")>0 : n-=1
        
        soidx=items[row][0].split("|")[1]
        if len(items[row][0].split("|")[2].strip())>1 : 
            cancelled = True 
       
        
        
        poidx=items[row][1].split("|")[1]
        
        d1=None
        d2=None
        d3=None
        d4=None
        eta=None
        reference=''
        
        
        for i in range(2,n) :
            t=items[row][i] 
            #t is a line data as below:
            #|         |18/06/30     6|                            ZR    |                                      |                               |
            #|         |              |            unconfir     6        |                                      |                               |
                        
            if t.find("ZR")>0 : 
                blocked=True                
            if t[11:19].find("/")>0 :d1=t[11:19]              
            if t[35:    46].find("/")>0 : d2=t[38:46]; eta=d2.replace("18/","2018/")
            if t[85:93].find("/")>0 and t[85:93].find("00/00/00")==-1: d3=t[85:93]
            if t[85:93].find("00/00/00")>=0 : status="PACK"                            
            if t[116:124].find("/")>0 : d4=t[116:124]
                
				
            t1=0;t2=0;t3=0;t4=0
            if t[21:25].strip()!='' : 
                t1=int(t[21:25])
                T1+=int(t1)    
            
            if t[48:52].strip()!='' : 
                t2=int(t[48:52]) 
                T2+=int(t2)    

            if t[96:99].strip() !="" and t[96:99].find("ED")==-1 : 
                t3=int(t[96:99]);
                T3+=t3
                
            if t[127:130].strip() !="" and t[127:130].find("|")==-1 : 
                t4=int(t[127:130]);
                T4+=t4

            #Invoice#
            if t[-31:-23].strip() !='' :
                reference=t[-31:-23].strip()
                                   

            #A single row created here
            singleRow=[so, soidx.strip(), po, poidx.strip(), (so+ str(no)).strip(), d1,t1,d2,t2,d3,t3,d4,t4,reference]
            TotalList.append(singleRow)
            #print(singleRow)

        delta=T1-T4
        
        if delta==0:
            etd=TotalList[-1][-3]
            REMARK="Invoiced"            
            
        else :
            REMARK=STATUS[status]
            etd=None

        partial="Partial" if delta>0 and T1>delta else '' #Partial Delivery Case, to be saved in the tab "Partial"
        if cancelled==True : status="CANCELLED"
        if blocked==True : status="BLOCKED"
        
        
        SumList.append([so, soidx.strip(), str(po)+str(poidx.strip()), po, poidx.strip(), podate, shipcondition, (so+ str(no)).strip(), partno, config, T1,T2,T3,T4, delta, stored, etd, eta, status,REMARK + " " + reference, reference, partial])

    return TotalList, SumList 
	
def detailDF (conDF, list) :
    
    DF=pd.DataFrame([], columns=["so", "soidx", "po", "poidx" "d1", "t1", "d2", "t2", "d3","t3", "d4", "t4", "reference"])

    for i in list:
        #print ("DETAIL DF", i)
        DF=pd.concat([DF, pd.DataFrame(conDF.loc[i], columns=["d1", "t1", "d2", "t2", "d3","t3", "d4", "t4", "reference"])])    
    return DF

def mergeDF (filelist) :
    colDF=["so", "soidx", "po", "poidx", "idx", "d1", "t1", "d2", "t2", "d3","t3", "d4", "t4", "reference"]
    colSUM=["so", "soidx", "popk", "po","poidx",  "podate", "shipcondition", "idx", "partno", "config","T1","T2","T3","T4","△","Stored", "ETD", "ETA", "Status", "Remark", "Ref#", "Partial"]
    conDF=pd.DataFrame([], columns=colDF)
    conSUM=pd.DataFrame([], columns=colSUM)

    for file in filelist :
        print (file, "read to be parsed.... ")
        result =fileRead(file)
        TotalList, SumList = itemsParsing(result)        
        conDF=pd.concat([conDF,pd.DataFrame(TotalList, columns=colDF)])
        conSUM=pd.concat([conSUM,pd.DataFrame(SumList, columns=colSUM)])
    
    #Converted to DataFrame
    conDF=pd.DataFrame(conDF, columns=colDF)                         
    conSUM=pd.DataFrame(conSUM, columns=colSUM)
    conDF=conDF.set_index("idx")
    conSUM=conSUM.set_index("idx")        
    
    partialDF = detailDF(conDF, conSUM[conSUM.Partial=='Partial'].index)    
    return conDF, conSUM, partialDF

def ticketRead(filename) :
    tickets=[]
    ticket=[]    
    
    with open(filename,"r", encoding="ISO-8859-1") as file:
        for i in range(6) :
            line=file.readline()
        line=file.readline()
        so=line.split()[5]
        po=line.split()[8]
        while line.find('*****')<0:
            #print(line.find('*****'))
            line=file.readline()
            if (line.count("|")==15):                
                posNum=line.split("|")[1].strip()                                                            
            if (line.count("Terminreklamation") or line.count("Top Priority")) :                
                line=line.replace("Top Priority", "TopPriority")
                line=line.replace("fr.", "")
                
                ticket=[so] + [posNum] +  line.split()
                
                if len(ticket)<=7 :  #if not answered, the length of column is 7 or less. 
                    for i in range(len(ticketHeader) - len(ticket)) :
                        ticket.append("")
                tickets.append(ticket)
        return tickets

def ticketsToDataFrame(filelist) :
    conTIckets=[]
    for file in filelist :
        tickets=ticketRead(file)
        conTIckets=conTIckets+tickets
    
    conTIckets=pd.DataFrame(conTIckets, columns=ticketHeader)
    return conTIckets

    
ticketHeader=["so","pos","no","type","issuedDate","IssuedTime","Initiator","responseDate","responseTime"]   


import sys

if __name__ == "__main__":
    mypath = sys.argv[1]
    
    #print(mypath)


##########################
#
# RUN FROM HERE
#
##########################    
#filelist=[]
#f=open("YVA1list.txt","r", encoding="UTF-8")
#for line in f :
#    filelist.append(line.splitlines()[0])

##################################################
# Filelist read from the current directory
##################################################
from os import listdir
from os.path import isfile, join
#mypath="."
filelist = [mypath + "\\" + f for f in listdir(mypath) if len(f)==10]
print(filelist)

DF,SUM,PARTIAL = mergeDF(filelist)
conTickets=ticketsToDataFrame(filelist)

##################################################
#MySQL Database 
##################################################
# f=open('mysql.key').readline().split()

# try :     
    # cnx = mysql.connector.connect(user=f[0], password=f[1],
                                  # host='127.0.0.1',
                                  # database='lo')
    # cursor = cnx.cursor()    
    # query = "delete from yva1"
    # cursor.execute(query)
    # cnx.commit()

# except Exception as e:
    # print(e)

# for k,v in SUM.iterrows():
    # value= (str(list(v)).replace("[","").replace("]","").replace("None","Null"))
    # query = ("INSERT INTO yva1 VALUES(" + value + ") ")
    # print(query)
    
    # try :
        # cursor.execute(query)
    # except Exception as e:
        # print(e)
        
# cnx.commit()
# cnx.close()
##################################################
#Excel File output
##################################################

#To save excel file 
writer=pd.ExcelWriter(mypath + "\\" + time.strftime("%Y%m%d%H%M%S")+'YVA1.xlsx')
SUM.to_excel(writer,'SUM')
DF.to_excel(writer,'DF')
PARTIAL.to_excel(writer,'PARTIAL')
conTickets.to_excel(writer,'tickets')
writer.save()