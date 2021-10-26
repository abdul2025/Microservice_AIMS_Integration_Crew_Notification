from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, engine
from sqlalchemy import Column, String, Integer, Date, and_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import UniqueConstraint  


import zeep
import datetime, time
import psycopg2







def connectionDB():

    db_string = ''
    
    engine = create_engine(db_string) 

    
    base = declarative_base()

    class crewQuals(base):

        __tablename__ = 'crewQuals'

        id = Column(Integer, primary_key=True, autoincrement=True)
        CrewID = Column(String, nullable=True)
        CrewName = Column(String, nullable=True)
        FirstName = Column(String, nullable=True)
        LastName = Column(String, nullable=True)
        Crew3LC = Column(String, nullable=True)
        Gender = Column(String, nullable=True)
        Address1 = Column(String, nullable=True)
        Address2 = Column(String, nullable=True)
        Address3 = Column(String, nullable=True)
        City = Column(String, nullable=True)
        State = Column(String, nullable=True)
        ZipCode = Column(String, nullable=True)
        Residence = Column(String, nullable=True)
        Telephone = Column(String, nullable=True)
        Fax = Column(String, nullable=True)
        CellPhone = Column(String, nullable=True)
        Email= Column(String, nullable=True)
        Email2 = Column(String, nullable=True)
        KinName = Column(String, nullable=True)
        Relation = Column(String, nullable=True)
        Contact = Column(String, nullable=True)
        Marital = Column(String, nullable=True)
        EmployBeg = Column(String, nullable=True)
        EmployEnd = Column(String, nullable=True)
        EndReason = Column(String, nullable=True)
        Mailbox = Column(String, nullable=True)
        PayrollNm = Column(String, nullable=True)
        SecNumber = Column(String, nullable=True)
        QualBase = Column(String, nullable=True)
        QualAc = Column(String, nullable=True)
        QualPos = Column(String, nullable=True)
        RosterGroup = Column(String, nullable=True)
        QualBDay = Column(String, nullable=True)
        QualEDay = Column(String, nullable=True)
        PrimaryQual = Column(String, nullable=True)
        NotToBeRoster = Column(String, nullable=True)


        __table_args__ = (UniqueConstraint('CrewID','QualBase', 'QualPos', 'QualBDay', name='CrewID'), )



        
    
    
    if not engine.dialect.has_table(engine, 'crewQuals', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine, autoflush=False)  
    session = Session()

    return [crewQuals, session, engine]


def FetchCrewQuals():
    start = time.time()
    print("timer start")
    crewQuals, session, engine = connectionDB()
    startDate = datetime.datetime.now() + datetime.timedelta(days=-1)
    endDate = datetime.datetime.now() + datetime.timedelta()
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
    members = getIDs()
    for id in members:
        print(id)


        
        data_se = {
            'UN': '1',
            'PSW': '1111',
            'FmDD': str(startDate.day),
            'FmMM': str(startDate.month),
            'FmYYYY': str(startDate.year),
            'ToDD': str(endDate.day),
            'ToMM': str(endDate.month),
            'ToYYYY': str(endDate.year),
            'CrewID': id,  # 0 return all the list, given 20000476 per one 
            'PrimaryQualify': True,
            'GetAllQualsInPeriod': True,
        }
        try:
            res = req.service.FetchCrewQuals(**data_se)

            

            for crew in res.CrewQualList:
                try:
                    for i in crew.CrewQuals:
                        crewList = session.query(crewQuals).filter(
                            crewQuals.CrewID==crew.CrewID,
                            crewQuals.QualBase==i.QualBase,
                            crewQuals.QualPos==i.QualPos,
                            crewQuals.QualBDay==i.QualBDay,
                        
                            ).first()
                        if crewList == None:
                            print('add new')
                            crewQualifications = crewQuals(
                                CrewID = crew.CrewID,
                                CrewName = crew.CrewName,
                                FirstName = crew.FirstName, 
                                LastName = crew.LastName,
                                Crew3LC =crew.Crew3LC,
                                Gender =crew.Gender,
                                Address1 = crew.Address1,
                                Address2 = crew.Address2,
                                Address3 = crew.Address3,
                                City =crew.City,
                                State =crew.State,
                                ZipCode = crew.ZipCode,
                                Residence = crew.Residence,
                                Telephone = crew.Telephone,
                                Fax = crew.Fax,
                                CellPhone =crew.CellPhone,
                                Email= crew.Email,
                                Email2 = crew.Email2,
                                KinName = crew.KinName,
                                Relation = crew.Relation,
                                Contact = crew.Contact,
                                Marital = crew.Marital,
                                EmployBeg = crew.EmployBeg,
                                EmployEnd = crew.EmployEnd,
                                EndReason = crew.EndReason,
                                Mailbox = crew.Mailbox,
                                PayrollNm = crew.PayrollNm,
                                SecNumber = crew.SecNumber,
                                QualBase = i.QualBase,
                                QualAc = i.QualAc,
                                QualPos = i.QualPos,
                                RosterGroup = i.RosterGroup,
                                QualBDay = i.QualBDay,
                                QualEDay = i.QualEDay,
                                PrimaryQual = i.PrimaryQual,
                                NotToBeRoster = i.NotToBeRoster
                                )
                            session.add(crewQualifications)
                        else:
                            print('update')
                            print(crew.CrewID)
                            crewList.Address1 = crew.Address1,
                            crewList.Address2 = crew.Address2,
                            crewList.Address3 = crew.Address3,
                            crewList.City =crew.City,
                            crewList.State =crew.State,
                            crewList.ZipCode = crew.ZipCode,
                            crewList.Residence = crew.Residence,
                            crewList.Telephone = crew.Telephone,
                            crewList.Fax = crew.Fax,
                            crewList.CellPhone =crew.CellPhone,
                            crewList.Email= crew.Email,
                            crewList.Email2 = crew.Email2,
                            crewList.KinName = crew.KinName,
                            crewList.Relation = crew.Relation,
                            crewList.Contact = crew.Contact,
                            crewList.Marital = crew.Marital,
                            crewList.EmployBeg = crew.EmployBeg,
                            crewList.EmployEnd = crew.EmployEnd,
                            crewList.EndReason = crew.EndReason,
                            crewList.Mailbox = crew.Mailbox,
                            crewList.PayrollNm = crew.PayrollNm,
                            crewList.SecNumber = crew.SecNumber,
                            crewList.QualBase = i.QualBase,
                            crewList.QualAc = i.QualAc,
                            crewList.QualPos = i.QualPos,
                            crewList.RosterGroup = i.RosterGroup,
                            crewList.QualBDay = i.QualBDay,
                            crewList.QualEDay = i.QualEDay,
                            crewList.PrimaryQual = i.PrimaryQual,
                            crewList.NotToBeRoster = i.NotToBeRoster            
                except Exception as er:
                    session.rollback()
                    print('er----- rolling back')
        except Exception as er:
            session.rollback()
            print('er----- rolling back')
    session.commit()
    session.close()
    print('commited and closed')
    end = time.time()
    print(end - start)
    print("timer ended")
FetchCrewQuals()