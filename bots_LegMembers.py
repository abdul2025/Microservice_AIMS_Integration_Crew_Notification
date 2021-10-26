from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, and_, Column, String, Integer, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import UniqueConstraint  

import zeep
import datetime,time
import psycopg2
import json


def emptyfieldToNone(field):
    if field == '' or field == ' ':
        return None
    else:
        return field


def convertStrToTime(string):
    if string != '' and string != ' ':
        return datetime.datetime.strptime(string, '%d/%m/%Y')
    else:
        return None

def connectionDB():
    
    
    db_string = ''
    engine = create_engine(db_string) 

    
    base = declarative_base()

    class LegMembers(base):

        __tablename__ = 'legMember'

        id = Column(Integer, primary_key=True, autoincrement=True)
        member_id = Column(Integer, nullable=True)
        flight_date = Column(Date, nullable=True)
        flight_carrier = Column(String, nullable=True)
        flight_no = Column(Integer, nullable=True)
        flight_desc = Column(String, nullable=True)
        flight_dep = Column(String, nullable=True)
        name = Column(String, nullable=True)
        last_name = Column(String, nullable=True)
        first_name = Column(String, nullable=True)
        shot_name = Column(String, nullable=True)
        base = Column(String, nullable=True)
        ac = Column(String, nullable=True)
        pos = Column(String, nullable=True)
        qualcat = Column(Integer, nullable=True)
        trnduty = Column(String, nullable=True)
        request = Column(String, nullable=True)
        notif = Column(String, nullable=True)
        pax = Column(String, nullable=True)
        crte = Column(String, nullable=True)
        crsday = Column(Integer, nullable=True)
        crsdaydd = Column(String, nullable=True)
        crsdaymm = Column(String, nullable=True)
        crsdayyy = Column(String, nullable=True) 
    
        __table_args__ = (UniqueConstraint('flight_date', 'member_id', 'flight_no', name='flightData_memberid_flight_no'), )
        
    
    
    if not engine.dialect.has_table(engine, 'legMember', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine)  
    session = Session()

    return [LegMembers, session]

def getFlights():
    startDate = datetime.datetime.now() + datetime.timedelta()
    endDate = datetime.datetime.now() + datetime.timedelta()

    g = datetime.datetime.strptime(str(endDate), '%d/%m/%Y %H:%M:%S')
    print(g[:4])
    print(endDate)
    
    req = zeep.Client(
            wsdl="")
        
    data_se = {
            'UN': '1',
            'PSW': '1111',
            'FromDD': str(startDate.day),
            'FromMMonth': str(startDate.month),
            'FromYYYY': str(startDate.year),
            'FromHH': '00',
            'FromMMin': '01',
            'ToDD': str(endDate.day),
            'ToMMonth': str(endDate.month),
            'ToYYYY': str(endDate.year),
            'ToHH': '23',
            'ToMMin': '59',
        }
    res = req.service.FlightDetailsForPeriod(**data_se)
    listFlights = []
    for flight in res.FlightList:  
        listFlights.append({'flightNum': flight.FlightNo, 'flightDate': convertStrToTime(flight.FlightDate).date(), 'flightDep': flight.FlightDep })
    
    return listFlights



def FetchLegMembers():
    print('fun start')
    start = time.time()
    print("timer start")
    
    #### Create a session
    LegMembers, session = connectionDB()
    print('session is made')



    try:
        req = zeep.Client(wsdl="https://fad-aws.aims.aero/wtouch/AIMSWebService.exe/wsdl/IAIMSWebService/")
    
        listOfFlight = getFlights()
        print(len(listOfFlight))
        for flight in listOfFlight:   
            data_se = {
                'UN': '1',
                'PSW': '1111',
                'DD': str(flight['flightDate'].day),
                'MM': str(flight['flightDate'].month),
                'YY': str(flight['flightDate'].year),
                'Flight': str(flight['flightNum']),
                'Dep': str(flight['flightDep']) 
            }
        
            try:
                res = req.service.FetchLegMembers(**data_se)
                for items in res.FMember:
                    flightDate = res.FlightDD +'-'+res.FlightMM+'-'+res.FlightYY
                    #### filter the table by the uniqe keys
                    members = session.query(LegMembers).filter(and_(
                            LegMembers.flight_date==datetime.datetime.strptime(flightDate, '%d-%m-%Y'),
                            LegMembers.flight_no==str(res.FlightNo),
                            LegMembers.member_id==items.id
                            )).first()
                    
                    # #### check if row does not exsited add new one else update it  
                    if members == None:
                        print('add')
                        try:
                            leg = LegMembers(
                                member_id=items.id,
                                flight_date=datetime.datetime.strptime(flightDate, '%d-%m-%Y'),
                                flight_carrier=res.FlightCarrier,
                                flight_no=res.FlightNo,
                                flight_desc=res.FlightDesc,
                                flight_dep=res.FlightDep,
                                name=items.name,
                                last_name=items.lastname,
                                first_name=items.firstname,
                                shot_name=items.shortname,
                                base=items.base,
                                ac=items.ac,
                                pos=items.pos, 
                                qualcat=items.qualcat,
                                trnduty=emptyfieldToNone(items.trnduty),
                                request=items.request,
                                notif=emptyfieldToNone(items.notif),
                                pax=emptyfieldToNone(items.pax),
                                crte=items.crte,
                                crsday=items.crsday,
                                crsdaydd=items.crsdaydd,
                                crsdaymm=items.crsdaymm, 
                                crsdayyy=items.crsdayyy
                            )
                            session.add(leg)
                            print(items['id'])
                        except Exception as er:
                            print(er)
                            print('ERORR Saving Crew Members')
                    else:
                        print('update')
                        try:
                            members.member_id=items.id,
                            members.flight_date=datetime.datetime.strptime(flightDate, '%d-%m-%Y'),
                            members.flight_carrier=res.FlightCarrier,
                            members.flight_no=res.FlightNo,
                            members.flight_desc=res.FlightDesc,
                            members.flight_dep=res.FlightDep,
                            members.name=items.name,
                            members.last_name=items.lastname,
                            members.first_name=items.firstname,
                            members.shot_name=items.shortname,
                            members.base=items.base,
                            members.ac=items.ac,
                            members.pos=items.pos, 
                            members.qualcat=items.qualcat,
                            members.trnduty=emptyfieldToNone(items.trnduty),
                            members.request=items.request,
                            members.notif=emptyfieldToNone(items.notif),
                            members.pax=emptyfieldToNone(items.pax),
                            members.crte=items.crte,
                            members.crsday=items.crsday,
                            members.crsdaydd=items.crsdaydd,
                            members.crsdaymm=items.crsdaymm, 
                            members.crsdayyy=items.crsdayyy
                        except Exception as er:
                            print(er)
            except Exception as er:
                print(er)
        session.commit()
        session.close()
        print('session commited and closed')
    except Exception as er:
        print(er)
        print("ERROR Fetch Leg Members")
    end = time.time()
    print(end - start)
    print("timer ended")




FetchLegMembers()

