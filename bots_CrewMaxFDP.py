
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, and_, Column, String, Integer, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import UniqueConstraint  

import zeep
import datetime
import psycopg2
import json
import time



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
    db_stringProd = ''

    engine = create_engine(db_string) 
    base = declarative_base()

    class CrewMaxFDP(base):

        __tablename__ = 'CrewMaxFDP'
        id = Column(Integer, primary_key=True, autoincrement=True)

        flight_date = Column(Date, nullable=True)
        flight_no = Column(Integer, nullable=True)
        flight_dep =  Column(String, nullable=True)
        FlightCarrier = Column(String, nullable=True)
        FlightDesc = Column(String, nullable=True)
        Rank = Column(String, nullable=True)
        StaffNum = Column(String, nullable=True)
        CrewName = Column(String, nullable=True)
        FDPStart = Column(String, nullable=True)
        MaxFDP = Column(String, nullable=True)
        FDPEnd = Column(String, nullable=True)
        CrewRoute = Column(String, nullable=True)
        CRouteDate = Column(String, nullable=True)
        Count = Column(Integer, nullable=True, default=0)
    
        __table_args__ = (UniqueConstraint('flight_date', 'StaffNum', 'flight_no', name='flightData_StaffNum_flight_no'),)
        
    
    
    if not engine.dialect.has_table(engine, 'CrewMaxFDP', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine)  
    session = Session()

    return [CrewMaxFDP, session]

def getFlights():
    startDate = datetime.datetime.now() + datetime.timedelta(days=-47)
    endDate = datetime.datetime.now() + datetime.timedelta(days=-45)
    print(startDate)
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









def main():
    print('ll')


    CrewMaxFDP, session = connectionDB()
    print('session is made')


    start = time.time()
    print("timer start")
    
    req = zeep.Client(
            wsdl="")
    try:
        listOfFlight = getFlights()
        print(len(listOfFlight))
        try:
            for flight in listOfFlight[:10]:
                print(flight['flightDate'].day)     
                data_se = {
                    'UN': '1',
                    'PSW': '1111',
                    'DD': str(flight['flightDate'].day),
                    'MM': str(flight['flightDate'].month),
                    'YYYY': str(flight['flightDate'].year),
                    'FlightNo': str(flight['flightNum']),
                    'Dep': str(flight['flightDep']) 
                }
                res = req.service.FetchCrewMaxFDP(**data_se)
                print(res)
                for items in res.CrewMaxFDP:
                    flightDate = res['FlightDD']+'-'+res['FlightMM']+'-'+res['FlightYY']
                    #### filter the table by the uniqe keys
                    members = session.query(CrewMaxFDP).filter(and_(
                            CrewMaxFDP.flight_date==datetime.datetime.strptime(flightDate, '%d-%m-%Y'),
                            CrewMaxFDP.flight_no==str(res['FlightNo']),
                            CrewMaxFDP.StaffNum==items['StaffNum']
                            )).first()
                    if members == None:
                        try:
                            crewMaxFDP = CrewMaxFDP(
                                flight_date=datetime.datetime.strptime(flightDate, '%d-%m-%Y'),
                                flight_no=res['FlightNo'],
                                flight_dep=res['FlightDep'],
                                FlightCarrier = res['FlightCarrier'],
                                FlightDesc = res['FlightDesc'],
                                Rank = items['Rank'],
                                StaffNum = items['StaffNum'], 
                                CrewName = items['CrewName'],
                                FDPStart = items['FDPStart'],
                                MaxFDP = items['MaxFDP'],
                                FDPEnd = items['FDPEnd'],
                                CrewRoute = items['CrewRoute'],
                                CRouteDate = items['CRouteDate'],
                                Count = res['Count']
                            )
                            session.add(crewMaxFDP)
                            print(items['StaffNum'])
                        except Exception as er:
                            print(er)
                    else:
                        print('update')
                        try:
                            members.flight_no=res['FlightNo'],
                            members.flight_dep=res['FlightDep'],
                            members.FlightCarrier = res['FlightCarrier'],
                            members.FlightDesc = res['FlightDesc'],
                            members.Rank = items['Rank'],
                            members.StaffNum = items['StaffNum'], 
                            members.CrewName = items['CrewName'],
                            members.FDPStart = items['FDPStart'],
                            members.MaxFDP = items['MaxFDP'],
                            members.FDPEnd = items['FDPEnd'],
                            members.CrewRoute = items['CrewRoute'],
                            members.CRouteDate = items['CRouteDate'],
                            members.Count = res['Count']
                        except Exception as er:
                            print(er)
        except Exception as er:
            print(er)
    except Exception as er:
        print(er)
    session.commit()
    session.close()
    print('session commited and closed')

    end = time.time()
    print(end - start)
    print("timer ended")





main()
