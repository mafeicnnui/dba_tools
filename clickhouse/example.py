from clickhouse_driver import Client
import random

class ck_demo:

    def __init__(self,host,port,user,password,database,send_receive_timeout):
        self.client = Client(host=host, port=port, user=user, password=password,database=database, send_receive_timeout=send_receive_timeout)

    def insert_data(self):
        st="insert into sbtest2(EventDate,CounterID,UserID) values(now(),{},{})".format(random.randint(10,100),random.randint(100,1000))
        print(st)
        self.client.execute(st)

    def insert_data_local(self):
        st = "insert into sbtest_local(EventDate,CounterID,UserID) values(now(),{},{})".format(random.randint(10, 100),
                                                                                         random.randint(100, 1000))
        print(st)
        self.client.execute(st)

    def query_data(self):
        st = 'select EventDate,CounterID,UserID from sbtest2 '
        rs = self.client.execute(st)
        for i in rs:
            print(i)

    def close(self):
        self.client.disconnect()

if __name__ == '__main__':
    ck1 = ck_demo('10.2.39.18',9000,'default','6lYaUiFi','testDB',5)
    # ck2 = ck_demo('10.2.39.20', 9000, 'default', '6lYaUiFi', 'testDB', 5)
    # ck3 = ck_demo('10.2.39.21', 9000, 'default', '6lYaUiFi', 'testDB', 5)
    # ck4 = ck_demo('10.2.39.41', 9002, 'default', '6lYaUiFi', 'testDB', 5)
    #ck5 = ck_demo('10.2.39.42', 9002, 'default', '6lYaUiFi', 'testDB', 5)
    #ck6 = ck_demo('10.2.39.43', 9002, 'default', '6lYaUiFi', 'testDB', 5)

    # 查询数据
    # ck1.query_data()

    # 插入数据
    ck1.insert_data()
    # ck1.insert_data_local()
    # ck2.insert_data_local()
    # ck3.insert_data_local()
    # ck4.insert_data_local()
    # ck5.insert_data_local()
    # ck6.insert_data_local()

    # 查询数据
    ck1.query_data()


    # 关闭连接
    ck1.close()
    # ck2.close()
    # ck3.close()