from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import Column, String, Integer, Date, and_, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import expression
from sqlalchemy.sql.schema import UniqueConstraint  

import json

import zeep
import datetime, time
import psycopg2
import re







def connectionDB():
    
    db_string = ''
    engine = create_engine(db_string) 

    
    base = declarative_base()

    class CrewSchChang(base):

        __tablename__ = 'CrewSchChang'

        id = Column(Integer, primary_key=True, autoincrement=True)
        LogForDay = Column(Date, nullable=True)
        LogMessage = Column(String, nullable=True)
        LogByUser = Column(String, nullable=True)
        LogOnDate = Column(Date, nullable=True)
        LogOnTime = Column(Time, nullable=True)
        LogOnDayTime = Column(String, nullable=True)
        LogCrewID = Column(String, nullable=True)


        __table_args__ = (UniqueConstraint('LogOnDate', 'LogCrewID', 'LogMessage', 'LogForDay', 'LogOnTime', name='LogOnDate_LogCrewID_LogMessage_LogForDay_LogOnTime'), )



        
    
    
    if not engine.dialect.has_table(engine, 'CrewSchChang', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine, autoflush=False)  
    session = Session()

    return [CrewSchChang, session]


def cleanMessages(message):
    if message != None:
        line = re.sub('[>*]', '', message)
        return line.strip()
    else:
        return message
def main():
    # add below Column to table 
    ### add member name 
    ### add memeber base
    ## add wave
    start = time.time()
    print("timer start")



    CrewSchChang, session = connectionDB()

    startDate = datetime.datetime.now() + datetime.timedelta()
    endDate = datetime.datetime.now() + datetime.timedelta()
    print(startDate)
    print(endDate)
    # try:
    req = zeep.Client(
            wsdl="")
    data_se = {
        'UN': '1',
        'PSW': '1111',
        'FromDD': startDate.day,
        'FromMM': startDate.month,
        'FromYYYY': startDate.year,
        'ToDD': endDate.day,
        'TOMM': endDate.month,
        'TOYYYY': endDate.year,
    }
    res = req.service.CrewScheduleChangesForPeriod(**data_se)
    #Showing resp len
    print(len(res.CrewLogList))

    try:
        ### dismiss the Performed messages 
        word = 'Performed'
        a =  []
        for member in res.CrewLogList:
            if re.search(word, member.LogMessage) == None:
                listt = zeep.helpers.serialize_object(member)
                listt = json.loads(json.dumps(listt))
                a.append(listt)
        listt = [dict(t) for t in {tuple(d.items()) for d in a}]
        
        

        for member in listt:
            try:
                memberLogForDay = datetime.datetime.strptime(member['LogForDay'], '%d/%m/%Y')
                memberLogOnDate = datetime.datetime.strptime(member['LogOnDate'], '%d/%m/%Y')
                memberLogOnTime = datetime.datetime.strptime(member['LogOnTime'], '%H:%M').time()


                memberList = session.query(CrewSchChang).filter(and_(
                        CrewSchChang.LogOnDate==memberLogOnDate.date(),
                        CrewSchChang.LogForDay==memberLogForDay.date(),
                        CrewSchChang.LogOnTime==memberLogOnTime,
                        CrewSchChang.LogCrewID==member['LogCrewID'],
                        CrewSchChang.LogMessage==cleanMessages(member['LogMessage']))).first()
                
                if memberList == None:
                    print(member)
                    crewLogList = CrewSchChang(
                        LogForDay = memberLogForDay.date(),
                        LogMessage = cleanMessages(member['LogMessage']),
                        LogByUser = member['LogByUser'],
                        LogOnDate = memberLogOnDate.date(),
                        LogOnTime = memberLogOnTime,
                        LogOnDayTime = member['LogOnDayTime'],
                        LogCrewID = member['LogCrewID'],
                    )
                    session.add(crewLogList)
                else:
                    print('update')
                    memberList.LogForDay = memberLogForDay.date(),
                    memberList.LogMessage = cleanMessages(member['LogMessage']),
                    memberList.LogByUser = member['LogByUser'],
                    memberList.LogOnDate = memberLogOnDate.date(),
                    memberList.LogOnTime = memberLogOnTime,
                    memberList.LogOnDayTime =  member['LogOnDayTime'],
                    memberList.LogCrewID =  member['LogCrewID'],
            except Exception as er:
                print(er)
        session.commit()
        session.close()
        print('commit and close')
    except Exception as er:
        print(er)

    

    end = time.time()
    print(end - start)
    print("timer ended")




main()