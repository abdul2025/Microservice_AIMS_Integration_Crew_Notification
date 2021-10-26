from sqlalchemy.sql import base, expression
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy import create_engine, engine
from sqlalchemy import Column, String, Integer, Date, and_, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import UniqueConstraint  
import datetime, time
import json
import psycopg2
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import urllib
import urllib.request
from bs4 import BeautifulSoup 
 




def connectionDB():
    
    db_string = ''

    engine = create_engine(db_string) 

    
    base = declarative_base()

    class SendCrewPendingNotif(base):
        __tablename__ = 'SendCrewPendingNotif'

        id = Column(Integer, primary_key=True, autoincrement=True)
        CrewID = Column(String, nullable=True)
        Message = Column(String, nullable=True)
        DutyDate = Column(Date, nullable=True)
        Phone = Column(String, nullable=True)
        Email = Column(String, nullable=True)
        Fullname = Column(String, nullable=True)
        EmailStatus = Column(Integer, nullable=True)
        SMSStatus = Column(Integer, nullable=True)
        __table_args__ = (UniqueConstraint('CrewID', 'Message','DutyDate', name='CrewID_Message_DutyDate',), )


        
    if not engine.dialect.has_table(engine, 'SendCrewPendingNotif', schema=None):
        base.metadata.create_all(engine)

    Session = sessionmaker(engine, autoflush=False)  
    session = Session()

    return [SendCrewPendingNotif, session, engine]

def getMemberNotification(engine):
    past = datetime.datetime.now() + datetime.timedelta(days=-2)
    current = datetime.datetime.now() + datetime.timedelta(days=2)
    print(past)
    print(current)
    past.replace(second=0, microsecond=0)
    current.replace(second=0, microsecond=0)
    query = """
                    
                

                    """
    try:
        newData = []
        with engine.connect() as connection:
            res = connection.execute(query.replace('%s1', str(current)).replace('%s2', str(past)))
            data = res.fetchall()
            for rec in data:
                newData.append(list(rec))
    except Exception as er:
        print(er)
    return newData



def checkstatus(records,session,SendCrewPendingNotif):
    members = session.query(SendCrewPendingNotif).filter(and_(
                        SendCrewPendingNotif.CrewID == str(records[0]),
                        SendCrewPendingNotif.Message ==str(records[-2]),
                        SendCrewPendingNotif.DutyDate ==str(records[-1])
                        )).first()
    
    return True if members != None else False

def insertRecoards(records,session,SendCrewPendingNotif, emailStatus, smsStatus):

    pendNotifList = SendCrewPendingNotif(
                        CrewID = str(records[0]),
                        DutyDate =str(records[-1]),
                        Phone =str(records[2]),
                        Email =str(records[3]),
                        Fullname =str(records[1]),
                        Message = str(records[-2]),
                        EmailStatus = emailStatus,
                        SMSStatus = smsStatus
                    )

    session.add(pendNotifList)

def sendSMS(obj):
    try:
        if (obj[-2][0]).lower() == 'a':
            messages = list(reversed(str(obj[-2]).split(',')))
        else:
            messages  = str(obj[-2]).split(',')
        messages = ' // '.join(messages)
    
        #EN
        CrewPhone = str(obj[2])
        phone = CrewPhone
        body = 'Crew No. ' + str(obj[0]) + ' (' + messages + '). Date: (' + str(obj[-1])+').'
        url = ''
        arrData = {
            'AppSid': '',
            'SenderID': '',
            'Body': body,
            'Recipient': phone.replace(' ','')
        }

        data = urllib.parse.urlencode(arrData).encode('utf-8')
        response = urllib.request.urlopen(url=url, data=data)

        return 1
    except Exception as er:
        return 0
 

def sendEmail(obj):
    #EN
    
    try:
        
        FromEmail = ''
        CrewEmail = str(obj[3])
        ToEmail = CrewEmail
        

        if (obj[-2][0]).lower() == 'a':
            messages = list(reversed(str(obj[-2]).split(',')))
        else:
            messages  = str(obj[-2]).split(',')
        

        elem = []
        for ele in messages:
            # print(ele)
            elem.append(f'''
            <tr>
                <td style="border:1px solid #939393;cursor:hand;color:#000000;font-family: Arial,'Helvetica Neue',Helvetica,sans-serif; font-size: 12px; line-height: 15px; text-align: center;">{ele}</td>
            </tr>''')

            with open("Notification_EN.html") as html:
                file = html.read()
                soup = BeautifulSoup(file, 'html.parser')
                soup.div.append(BeautifulSoup((' '.join(map(str, elem))), 'html.parser'))

            html = soup.prettify()

        
        html = html.replace("{{Fullname}}", str(obj[1])).replace("{{dateTime}}", str(obj[-1]))

        print('email start sent')
        msg = MIMEMultipart('alterbative')
        msg['Subject'] = "Duty Change Notification"+" "+str(obj[1])+" "+str(obj[0])+" "+ str(obj[-1]) 
        msg['From'] = FromEmail
        msg['To'] = ToEmail
        msg['X-SES-CONFIGURATION-SET'] = 'ses-delivery-set'
        msg['X-SES-MESSAGE-TAGS'] = 'campaign=batch1'

    


        msg.attach(MIMEText(html, 'html', "utf-8"))
        print('email will sent')
        
        s = smtplib.SMTP('email-smtp.eu-west-1.amazonaws.com',587)
        s.starttls()
        s.login('', '')
        s.sendmail(FromEmail, [ToEmail], msg.as_string())
        print(s)
        s.quit()
        
        
        return 1
    except:
        return 0

 


def main():
    start = time.time()
    print("timer start")
    
    #establish connection to DB
    SendCrewPendingNotif, session, engine = connectionDB()
    #GET notified members
    members = getMemberNotification(engine)   

    print(len(members))
    currentDate = datetime.datetime.now()
    count = 0
    for rec in members:
        ## check the duty date is equal or greater to current date
        if rec[-1] >= currentDate.date():
            # Dismiss any massages start with Route 
            if rec[-2].startswith('Route') != True:
                count += 1
                if checkstatus(rec,session,SendCrewPendingNotif) != True:
                    # emailStatus = sendEmail(rec)
                    # smsStatus = sendSMS(rec)
                    insertRecoards(rec,session,SendCrewPendingNotif, 0, 0)

    print(count)
    session.commit()
    session.close()
    print('commit and closed')
    end = time.time()
    print(end - start)
    print("timer ended")

main()