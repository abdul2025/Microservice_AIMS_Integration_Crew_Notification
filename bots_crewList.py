from time import process_time
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
    db_stringProd = ''
    db_string = ""

    engine = create_engine(db_string, pool_pre_ping=True) 

    
    base = declarative_base()

    class CrewList(base):
        __tablename__ = 'CrewList'
        id = Column(Integer, primary_key=True, autoincrement=True)

        member_Id= Column(String, nullable=True)
        ShortName= Column(String, nullable=True)
        CrewName= Column(String, nullable=True)
        Location= Column(String, nullable=True)
        Quals= Column(String, nullable=True)
        Sex= Column(String, nullable=True)
        Marital= Column(String, nullable=True)
        Address= Column(String, nullable=True)
        City= Column(String, nullable=True)
        State=Column(String, nullable=True)
        Zip=Column(String, nullable=True)
        NationalityCode=Column(String, nullable=True)
        Nationality=Column(String, nullable=True)
        ContactCell=Column(String, nullable=True)
        ContactTel= Column(String, nullable=True)
        Email=Column(String, nullable=True)
        NextKinName=Column(String, nullable=True)
        NextKinRelsn=Column(String, nullable=True)
        NextKinContact=Column(String, nullable=True)
        EmploymentDate=Column(String, nullable=True)
        Language1=Column(String, nullable=True)
        Language2=Column(String, nullable=True)
        LicenceNum=Column(String, nullable=True)

        __table_args__ = (UniqueConstraint('member_Id', name='member_Id'), )


        
    if not engine.dialect.has_table(engine, 'CrewList', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine)  
    session = Session()

    return [CrewList, session, engine]







def GetCrewList():
    start = time.time()
    print("timer start")

    startDate = datetime.datetime.now() + datetime.timedelta(days=-1)
    endDate = datetime.datetime.now() + datetime.timedelta()
    CrewList, session, engine = connectionDB()
    
    req = zeep.Client(
            wsdl="")
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


    listOfCrew = getIDs()
    print(len(listOfCrew))

    for i in listOfCrew:
        #### the coming pos from the legmember query above 
        #### would not match the pos coming form the below request 
        #### would escape ----> keep PosStr as ''

        print(i)
        data_se = {
            'UN': '1',
            'PSW': '1111',
            'ID': str(i), # 0 ---> will return a list of crew member 
            'PrimaryQualify': True,
            'FmDD': str(startDate.day),
            'FmMM': str(startDate.month),
            'FmYY': str(startDate.year),
            'ToDD': str(endDate.day),
            'ToMM': str(endDate.month),
            'ToYY': str(endDate.year),
            'BaseStr': '',
            'ACStr': '',
            'PosStr': ''
        }
        try:
            res = req.service.GetCrewList(**data_se)
            print(res)

            for member in res.GetCrewList:
                print(member.Id)
                try:
                    crewList = session.query(CrewList).filter(CrewList.member_Id==member.Id).first()
                    if crewList == None:
                        print('adding')
                        crewList = CrewList(
                                member_Id = member.Id,
                                ShortName = member.ShortName,
                                CrewName = member.CrewName,
                                Location = member.Location,
                                Quals = member.Quals,
                                Sex = member.Sex,
                                Marital = member.Marital,
                                Address = member.Address, 
                                City = member.City,
                                State = member.State,
                                Zip = member.Zip,
                                NationalityCode = member.NationalityCode,
                                Nationality = member.Nationality,
                                ContactCell = member.ContactCell,
                                ContactTel = member.ContactTel,
                                Email = member.Email,
                                NextKinName = member.NextKinName,
                                NextKinRelsn = member.NextKinRelsn,
                                NextKinContact = member.NextKinContact,
                                EmploymentDate = member.EmploymentDate,
                                Language1 = member.Language1,
                                Language2 = member.Language2,
                                LicenceNum = member.LicenceNum,
                            )
                        session.add(crewList)
                        print('end saving')
                    else:
                        print('update')
                        crewList.ShortName = member.ShortName,
                        crewList.CrewName = member.CrewName,
                        crewList.Location = member.Location,
                        crewList.Quals = member.Quals,
                        crewList.Sex = member.Sex,
                        crewList.Marital = member.Marital,
                        crewList.Address = member.Address, 
                        crewList.City = member.City,
                        crewList.State = member.State,
                        crewList.Zip = member.Zip,
                        crewList.NationalityCode = member.NationalityCode,
                        crewList.Nationality = member.Nationality,
                        crewList.ContactCell = member.ContactCell,
                        crewList.ContactTel = member.ContactTel,
                        crewList.Email = member.Email,
                        crewList.NextKinName = member.NextKinName,
                        crewList.NextKinRelsn = member.NextKinRelsn,
                        crewList.NextKinContact = member.NextKinContact,
                        crewList.EmploymentDate = member.EmploymentDate,
                        crewList.Language1 = member.Language1,
                        crewList.Language2 = member.Language2,
                        crewList.LicenceNum = member.LicenceNum, 
                except Exception as er: 
                    print(er)
        except Exception as er:
            print(er)

    session.commit()
    session.close()
    print('saved and commited')

    end = time.time()
    print(end - start)
    print("timer ended")


GetCrewList()