from sqlalchemy.sql import base, expression
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, engine
from sqlalchemy import Column, String, Integer, Date, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import UniqueConstraint  
import zeep
import datetime, time
import json
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

 





def connectionDB():
    db_string = ''
    engine = create_engine(db_string) 

    
    base = declarative_base()

    class CrewPendingNotif(base):
        __tablename__ = 'CrewPendingNotif'

        id = Column(Integer, primary_key=True, autoincrement=True)
        LogCrewID = Column(Integer, nullable=True)
        PendNotifCount = Column(Integer, nullable=True)
        LogOnDayTime = Column(String, nullable=True)
        LogOnTime = Column(String, nullable=True)
        LogOnDate = Column(String, nullable=True)
        LogByUser = Column(String, nullable=True)
        LogMessage = Column(String, nullable=True)
        LogForDay = Column(String, nullable=True)

        __table_args__ = (UniqueConstraint('LogCrewID', 'LogMessage','LogOnDayTime','LogForDay', name='LogCrewID_LogMessage_LogOnDayTime_LogForDay'), )


        
    if not engine.dialect.has_table(engine, 'CrewPendingNotif', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine, autoflush=False)  
    session = Session()

    return [CrewPendingNotif, session, engine]




















############
## Run every 3 min


def main():
    start = time.time()
    print("timer start")
    startDate = datetime.datetime.now() + datetime.timedelta(days=-1)
    endDate = datetime.datetime.now() + datetime.timedelta(days=2)
    CrewPendingNotif, session, engine = connectionDB()
    
    def getIDs():
        legmemberID = []
        query = """
                        SELECT DISTINCT(member_id)
                        FROM "legMember"
                        ORDER by member_id
                        """
        with engine.connect() as connection:
            memberIDs = connection.execute(query)
            for id in memberIDs:
                legmemberID.append(id[0])
        schmemberID = []
        query = """
                        SELECT DISTINCT("LogCrewID")
                        FROM "CrewSchChang"
                        ORDER by "LogCrewID"
                        """
        with engine.connect() as connection:
            memberIDs = connection.execute(query)
            for id in memberIDs:
                schmemberID.append(int(id[0]))


        listIds = set(legmemberID + schmemberID)
        
        return listIds
    
    ids = getIDs()
    
    try:
        
        req = zeep.Client(
            wsdl="")

        for memberIDs in list(ids):
            print(memberIDs)
            print(type(memberIDs))
            try:
                data_se = {
                    'UN': '1',
                    'PSW': '1111',
                    'ID': str(memberIDs), 
                    'FmDD': startDate.day,
                    'FmMM': startDate.month,
                    'FmYY': startDate.year,
                    'ToDD': endDate.day,
                    'ToMM': endDate.month,
                    'ToYY': endDate.year, 
                }
                res = req.service.CrewPendingNotifForPeriod(**data_se)
                print(res)
                print(res.PendNotifCount)

                    
                try:
                    listt = zeep.helpers.serialize_object(res.PendNotifList)
                    listt = json.loads(json.dumps(listt))
                    listt = [dict(t) for t in {tuple(d.items()) for d in listt}]
                    for member in res.PendNotifList:
                            
                        members = session.query(CrewPendingNotif).filter(and_(
                            CrewPendingNotif.LogCrewID == int(member['LogCrewID']),
                            CrewPendingNotif.LogMessage ==str(member['LogMessage']),
                            CrewPendingNotif.LogOnDayTime == str(member['LogOnDayTime']),
                            CrewPendingNotif.LogForDay == str(member['LogForDay']),
                            )).first()

                        if members == None:
                            pendNotifList = CrewPendingNotif(
                                LogCrewID = int(member['LogCrewID']),
                                LogOnDayTime = str(member['LogOnDayTime']),
                                LogOnTime =member['LogOnTime'],
                                LogOnDate =member['LogOnDate'],
                                LogByUser =member['LogByUser'],
                                LogMessage =str(member['LogMessage']),
                                LogForDay = str(member['LogForDay']),
                                PendNotifCount= res['PendNotifCount']
                            )
    
                            session.add(pendNotifList)
                            print('add')
                except Exception as er:
                    print(er)
            except Exception as er:
                print('kkd')
    except Exception as er:
        print(er)

    session.commit()
    
    
    session.close()
    print('commit and closed')
    end = time.time()
    print(end - start)
    print("timer ended")

main()