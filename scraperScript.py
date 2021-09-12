import mysql.connector
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from datetime import date, datetime, timedelta
from mysql.connector import Error
import time
import math

def store(date, open, high, low, close, volume, barCount, average):
    x = lambda a: datetime.strptime(str(a), '%Y%m%d %H:%M:%S')
    cur.execute("INSERT INTO table(bartime, open, high, low, close, vol, bar, average) "\
                "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)", (x(date), open, high, low, close, volume, barCount, average))
    cnx.commit()

def storeba(date, open, high, low, close):
    x = lambda a: datetime.strptime(str(a), '%Y%m%d %H:%M:%S')
    cur.execute("INSERT INTO table(bartime, open, high, low, close) "\
                "VALUES(%s, %s, %s, %s, %s)", (x(date), open, high, low, close))
    cnx.commit()

class MyWrapper(EWrapper):

    def nextValidId(self, orderId:int):

        print("setting nextValidOrderId: %d", orderId)
        self.nextValidOrderId = orderId

        self.start()

    def historicalData(self, reqId:int, bar):

        #print("HistoricalData. ReqId:", reqId, "BarData.", bar)
        #print(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average)
        #store(bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.barCount, bar.average)
        #print(bar.date, bar.open, bar.high, bar.low, bar.close)
        storeba(bar.date, bar.open, bar.high, bar.low, bar.close)
    def historicalDataEnd(self, reqId: int, start: str, end: str):

        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)
        app.disconnect()
        print("finished")

    def headTimestamp(self, reqId: int, headTimestamp: str):
        print("HeadTimestamp. ReqId:", reqId, "HeadTimeStamp:", headTimestamp)

    def error(self, reqId, errorCode, errorString):

        print("Error. Id: " , reqId, " Code: " , errorCode , " Msg: " , errorString)

    def start(self):
        contract = Contract()
        #contract.symbol = "DUK";
        #contract.secType = "STK";
        contract.symbol = "CL";
        contract.secType = "FUT";
        contract.exchange = "NYMEX";
        #contract.exchange = "CME";
        #contract.exchange = "SMART"
        #contract.primaryExchange = "NASDAQ"
        contract.currency = "USD";
        contract.lastTradeDateOrContractMonth = "202105";
        contract.includeExpired = True;
        app.reqHistoricalData(4001, contract, queryTime, "1800 S", "1 secs", "BID_ASK", 0, 1, False, [])
        #app.reqHeadTimeStamp(4001, contract, "TRADES", 0, 1)

cnx = mysql.connector.connect(host='127.0.0.1', unix_socket='/var/run/mysqld/mysqld.sock',
                              user='USERNAME', passwd='PASSWORD', db='mysql', charset='utf8', auth_plugin='mysql_native_password')
cur = cnx.cursor()
cur.execute('USE DB_NAME')
jter = 672*2 #number of half hours

try:
    for i in range(jter):
        try:
            y = datetime.now()
            queryTime = (datetime(2020, 1, 28, 0, 30, 0) + timedelta(hours=(.5*i))).strftime("%Y%m%d %H:%M:%S")
            app = EClient(MyWrapper())
            app.connect("127.0.0.1", 4001, clientId=123)
            app.run()
            print("Currently on", i,"/%d."%(jter), i / jter,"% completed. Approximately ", (jter-i)*20/60/60, "hours remain.")
            td = datetime.now()-y
            if td.seconds > 20 or td.seconds < 0:
                time.sleep(20)
            else:
                time.sleep(math.ceil(abs(20-(td.seconds))))
        except Error as e:
            print('Error:', e)

except Error as e:
    print('Error:', e)

finally:
    cur.close()
    cnx.close()
