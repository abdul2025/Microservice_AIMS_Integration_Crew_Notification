from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, and_
from sqlalchemy import Column, String, Integer, Date, DateTime, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import ForeignKey, UniqueConstraint
from sqlalchemy.sql.sqltypes import PickleType
import zeep
import datetime
import json
import time


def convertStrToTime(string):
    if string != '' and string != ' ' and string != None:
        return datetime.datetime.strptime(
            string, "%H:%M").time()
    else:
        return None


def checkArraysOfDelays(arr):
    valueList = []
    strVal = ' '
    listt = zeep.helpers.serialize_object(arr)
    listt = json.loads(json.dumps(listt))
    for item in listt:
        if len(item) > 0:
            for value in item.values():
                valueList.append(value)
    strVal = ' '.join(valueList)
    if len(valueList) > 0:
        return strVal
    else:
        return None

def currentDataTimetoStr(element):
    return str(element)

def connectionDB():
    

    db_string_automation = ''

    engine = create_engine(db_string_automation) 
    print('creating engin')
    
    base = declarative_base()
    print('base')
    class Flight(base):
        
        __tablename__ = 'Flight'  
        
        id = Column(Integer, primary_key=True, autoincrement=True)
        cdt = Column(DateTime, default=datetime.datetime.utcnow())
    
        flightDate = Column(DateTime, nullable=False)
        flightDateTime = Column(DateTime, nullable=True)
        flightNo = Column(String(4), nullable=False)
        
        flightCarrier = Column(String(2), nullable=True)  
        reg = Column(String(6), nullable=True)  
        dep = Column(String(3), nullable=True)  
        arr = Column(String(3), nullable=True)  
        std = Column(Time, nullable=True)  
        sta = Column(Time, nullable=True)  
    
        landed = Column(Integer, default=0)
        pax = Column(Integer, default=0)
        ddt = Column(DateTime, nullable=True)
    
    
        ddly = Column(String, nullable=True)  
        adly = Column(String, nullable=True)  
        etd = Column(Time, nullable=True)  
        eta = Column(Time, nullable=True)  
        atd = Column(Time, nullable=True)  
        ata = Column(Time, nullable=True)  
        tkof = Column(Time, nullable=True)  
        tdown = Column(Time, nullable=True)  
        acType = Column(String, nullable=True)  
        fType = Column(String, nullable=True)  
        BlkTime = Column(String, nullable=True)  
        LegCD = Column(String, nullable=True)  
        Desc =Column(String, nullable=True)  
        Status = Column(String, nullable=True)  
        NoOfPax = Column(String, nullable=True)  
    
        __table_args__ = (UniqueConstraint('flightDate', 'flightNo', name='flightData_flightNo'), )
        
    
    
    if not engine.dialect.has_table(engine, 'Flight', schema=None):
        base.metadata.create_all(engine)


    Session = sessionmaker(engine)  
    session = Session()

    return [Flight, session]


def flightDetails():
    
    try:
        print('before url')
        startDate = datetime.datetime.now() + datetime.timedelta(days=-20)
        endDate = datetime.datetime.now() + datetime.timedelta(days=5)
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
                'FromHH': '',
                'FromMMin': '',
                'ToDD': str(endDate.day),
                'ToMMonth': str(endDate.month),
                'ToYYYY': str(endDate.year),
                'ToHH': '',
                'ToMMin': '',

            }
        res = req.service.FlightDetailsForPeriod(**data_se)
        print("RES AIMS API")
        return res
    except Exception as er:
        print("ERROR AIMS API")
        print(er)


# flightDetails()


def main():
    start = time.time()
    print("timer start")
    try:
        print('before')
        res = flightDetails()
        print('after')
        try:
            Flight, session = connectionDB()
            for soapFlight in res.FlightList:
                # print(soapFlight.FlightDep)
                
                try:
                    flights = session.query(Flight).filter(and_(
                        Flight.flightDate==datetime.datetime.strptime(
                            soapFlight.FlightDate, "%d/%m/%Y").date(),
                        Flight.flightNo==str(soapFlight.FlightNo))).first()

                    if flights.flightNo:
                        print('update flight')
                        flights.flightDateTime=datetime.datetime.combine(datetime.datetime.strptime(soapFlight.FlightDate, "%d/%m/%Y").date(),
                                                                    datetime.datetime.strptime(soapFlight.FlightStd, "%H:%M").time()),
                        flights.flightCarrier=soapFlight.FlightCarrier,
                        flights.reg=soapFlight.FlightReg,
                        flights.dep=soapFlight.FlightDep,
                        flights.arr=soapFlight.FlightArr,
                        flights.std=convertStrToTime(soapFlight.FlightStd),
                        flights.sta=convertStrToTime(soapFlight.FLightSta),
                        flights.ddly=checkArraysOfDelays(soapFlight.FlightDepDelays),
                        flights.adly=checkArraysOfDelays(soapFlight.FlightArrDelays),
                        flights.etd=convertStrToTime(soapFlight.FlightEtd),
                        flights.eta=convertStrToTime(soapFlight.FlightEta),
                        flights.atd=convertStrToTime(soapFlight.FlightAtd),
                        flights.ata=convertStrToTime(soapFlight.FlightAta),
                        flights.tkof=convertStrToTime(soapFlight.FlightTKOFF),
                        flights.tdown=convertStrToTime(soapFlight.FlightTDOWN),
                        flights.acType=soapFlight.FlightAcType,
                        flights.fType=soapFlight.FlightType,
                        flights.BlkTime=soapFlight.FlightBlkTime,
                        flights.LegCD=soapFlight.FlightLegCD,
                        flights.Desc=soapFlight.FlightDesc,
                        flights.Status=soapFlight.FlightStatus,
                        flights.NoOfPax=soapFlight.FlightNoOfPax  
                except Exception as er:
                    print('adding new flights')

                    fly = Flight(
                        flightDate=datetime.datetime.strptime(
                            soapFlight.FlightDate, "%d/%m/%Y").date(),

                        flightDateTime=datetime.datetime.combine(datetime.datetime.strptime(soapFlight.FlightDate, "%d/%m/%Y").date(),
                                                                    datetime.datetime.strptime(soapFlight.FlightStd, "%H:%M").time()),
                                                                    
                        flightNo=soapFlight.FlightNo,
                        flightCarrier=soapFlight.FlightCarrier,
                        reg=soapFlight.FlightReg,
                        dep=soapFlight.FlightDep,
                        arr=soapFlight.FlightArr,
                        std=convertStrToTime(soapFlight.FlightStd),
                        sta=convertStrToTime(soapFlight.FLightSta),
                        ddly=checkArraysOfDelays(soapFlight.FlightDepDelays),
                        adly=checkArraysOfDelays(soapFlight.FlightArrDelays),
                        etd=convertStrToTime(soapFlight.FlightEtd),
                        eta=convertStrToTime(soapFlight.FlightEta),
                        atd=convertStrToTime(soapFlight.FlightAtd),
                        ata=convertStrToTime(soapFlight.FlightAta),
                        tkof=convertStrToTime(soapFlight.FlightTKOFF),
                        tdown=convertStrToTime(soapFlight.FlightTDOWN),
                        acType=soapFlight.FlightAcType,
                        fType=soapFlight.FlightType,
                        BlkTime=soapFlight.FlightBlkTime,
                        LegCD=soapFlight.FlightLegCD,
                        Desc=soapFlight.FlightDesc,
                        Status=soapFlight.FlightStatus,
                        NoOfPax=soapFlight.FlightNoOfPax  
                    )
                    session.add(fly)
                    print(soapFlight.FlightNo)
            session.commit()
            session.close()
            print('saved and closed')
            
        except Exception as er:
            print(er)
            print('model and seesion issues')


    except Exception as er:
        print('no aims data')
    end = time.time()
    print(end - start)
    print("timer ended")

main()
