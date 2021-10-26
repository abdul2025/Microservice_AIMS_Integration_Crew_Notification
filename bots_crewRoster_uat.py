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

def connectionDB():
    
    db_string = ''

    engine = create_engine(db_string) 

    
    base = declarative_base()

    class CrewRostList(base):
        __tablename__ = 'CrewRostList'
        id = Column(Integer, primary_key=True, autoincrement=True)

        member_Id= Column(String, nullable=True)
        Day =  Column(Date, nullable=True)
        Carrier = Column(String, nullable=True)
        Flt= Column(String, nullable=True)
        Legcd= Column(String, nullable=True)
        Dep= Column(String, nullable=True)
        Arr= Column(String, nullable=True)
        CrewBase= Column(String, nullable=True)
        STD= Column(String, nullable=True)
        STDLocal= Column(String, nullable=True)
        STDBase= Column(String, nullable=True)
        STA= Column(String, nullable=True)
        STALocal= Column(String, nullable=True)
        STABase= Column(String, nullable=True)
        ATD= Column(String, nullable=True)
        ATDLocal= Column(String, nullable=True)
        ATDBase= Column(String, nullable=True)
        ATA= Column(String, nullable=True)
        ATALocal= Column(String, nullable=True)
        ATABase= Column(String, nullable=True)
        GDBEG= Column(String, nullable=True)
        GDBEGLocal= Column(String, nullable=True)
        GDBEGBase= Column(String, nullable=True)
        GDEND= Column(String, nullable=True)
        GDENDLocal= Column(String, nullable=True)
        GDENDBase= Column(String, nullable=True)
        PAX= Column(String, nullable=True)
        CROUTE= Column(String, nullable=True)
        CDATE= Column(String, nullable=True)

        __table_args__ = (UniqueConstraint('member_Id', 'Flt', 'ATD', 'Day', 'STD', name='member_Id_Day_FLT_ATD_STD'), )


        
    if not engine.dialect.has_table(engine, 'CrewRostList', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine, autoflush=False)  
    session = Session()

    return [CrewRostList, session, engine]









def main():
    start = time.time()
    print("timer start")
    ##### set the request to be one day
    startDate = datetime.datetime.now() + datetime.timedelta(days=-35)
    endDate = datetime.datetime.now() + datetime.timedelta()
    CrewRostLis, session, engine = connectionDB()

    

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
    print(len(list(ids)))
    try:
        req = zeep.Client(
            wsdl="")
        for memberIDs in list(ids):
            try:
                
                data_se = {
                    'UN': '1',
                    'PSW': '1111',
                    'ID':  str(memberIDs), #----- per crew member id
                    'FmDD': str(startDate.day),
                    'FmMM': str(startDate.month),
                    'FmYY': str(startDate.year),
                    'ToDD': str(endDate.day),
                    'ToMM': str(endDate.month),
                    'ToYY': str(endDate.year),
                }
        
                res = req.service.CrewMemberRosterDetailsForPeriod(**data_se)
                for member in res.CrewRostList:
                    print(member.Day)
                    day = datetime.datetime.strptime(member.Day, '%d/%m/%Y')
                    try:
                        members = session.query(CrewRostLis).filter(and_(
                                CrewRostLis.ATD==str(member.ATD),
                                CrewRostLis.STD==str(member.STD),
                                CrewRostLis.Flt==str(member.Flt),
                                CrewRostLis.member_Id==str(memberIDs),
                                CrewRostLis.Day==day.date()
                                )).first()
                        print(members)
                        if members == None:

                            print('adding')
                            crewRostList = CrewRostLis(
                                member_Id = str(memberIDs),
                                Day = day.date(),
                                Carrier = member.Carrier,
                                Flt = str(member.Flt),
                                Legcd = member.Legcd,
                                Dep = member.Dep,
                                Arr = member.Arr,
                                CrewBase = member.CrewBase,
                                STD = str(member.STD),
                                STDLocal = member.STDLocal,
                                STDBase = member.STDBase,
                                STA = member.STA,
                                STALocal = member.STALocal,
                                STABase = member.STABase,
                                ATD = str(member.ATD),
                                ATDLocal = member.ATDLocal,
                                ATDBase = member.ATDBase,
                                ATA = member.ATA,
                                ATALocal = member.ATALocal,
                                ATABase = member.ATABase,
                                GDBEG = member.GDBEG,
                                GDBEGLocal = member.GDBEGLocal,
                                GDBEGBase = member.GDBEGBase,
                                GDEND = member.GDEND,
                                GDENDLocal = member.GDENDLocal,
                                GDENDBase = member.GDENDBase,
                                PAX = member.PAX,
                                CROUTE = member.CROUTE,
                                CDATE = member.CDATE,
                                )
                            session.add(crewRostList)
                            session.commit()
                            print('commit')

                            
                        else:
                            
                            members.Carrier = member.Carrier,
                            members.Legcd = member.Legcd,
                            members.Dep = member.Dep,
                            members.Arr = member.Arr,
                            members.CrewBase = member.CrewBase,
                            members.STDLocal = member.STDLocal,
                            members.STDBase = member.STDBase,
                            members.STA = member.STA,
                            members.STALocal = member.STALocal,
                            members.STABase = member.STABase,
                            members.ATDLocal = member.ATDLocal,
                            members.ATDBase = member.ATDBase,
                            members.ATA = member.ATA,
                            members.ATALocal = member.ATALocal,
                            members.ATABase = member.ATABase,
                            members.GDBEG = member.GDBEG,
                            members.GDBEGLocal = member.GDBEGLocal,
                            members.GDBEGBase = member.GDBEGBase,
                            members.GDEND = member.GDEND,
                            members.GDENDLocal = member.GDENDLocal,
                            members.GDENDBase = member.GDENDBase,
                            members.PAX = member.PAX,
                            members.CROUTE = member.CROUTE,
                            members.CDATE = member.CDATE,
                            session.commit()
                            print('commit')
                    except Exception as er:
                        print(er)
            except:
                print('Aims connection')
                pass
    except:
        print('Aims connection')
        pass
            

    session.close()
    print('commit and closed')
    end = time.time()
    print(end - start)
    print("timer ended")  
            
  

main()





