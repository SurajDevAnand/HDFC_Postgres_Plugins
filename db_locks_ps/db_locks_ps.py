#!/usr/bin/python3
import json
import time
PLUGIN_VERSION=1
HEARTBEAT=True
METRICS_UNITS={'execution_time':'ms'}
class psql:

    def __init__(self,args):
        
        self.maindata={}
        self.maindata['plugin_version'] = PLUGIN_VERSION
        self.maindata['heartbeat_required']=HEARTBEAT
        self.maindata['units']=METRICS_UNITS

        self.db_name=args.db_name
        self.username=args.username
        self.password=args.password
        self.hostname=args.hostname
        self.port=args.port
        self.query= "select pid, pg_blocking_pids(pid) as blocked_by from pg_stat_activity where cardinality(pg_blocking_pids(pid))>0;"

        
    def metriccollector(self):
        
        try:
            start_time=time.time()
            import psycopg2

        except Exception as e:
            self.maindata['msg']=str(e)
            self.maindata['status']=0
            return self.maindata

        try:
            connection = psycopg2.connect(user=self.username,
                                  password=self.password,
                                  host=self.hostname,
                                  port=self.port,
                                  database=self.db_name)

            cursor=connection.cursor()

            try:
                cursor.execute(self.query)                
                data=cursor.fetchall()
                self.maindata['Tot Blocked Queries']=len(data)
                if data:
                    i=1
                    for row in data:
                        if row:
                            self.maindata['Blocked PID_'+str(i)]=str(row[0])+"-->"+str(row[1])
                        i+=1
                        if i>10:
                            break
                    if i<10:
                        k=i
                        for j in range(11-i):
                            self.maindata['Blocked PID_'+str(k)]="0"
                            k+=1
                else:
                    for j in range(10):
                            self.maindata['Blocked PID_'+str(j+1)]="0"
                            j+=1



                cursor.close()
                connection.close()
                end_time=time.time()
                total_time=(end_time-start_time) * 1000
                self.maindata['execution_time']="%.3f" % total_time 
                    

            except Exception as e:
                self.maindata['msg']=str(e)
                self.maindata['status']=0
                return self.maindata
        
        except Exception as e:
            self.maindata['msg']=str(e)
            self.maindata['status']=0
            return self.maindata

        return self.maindata

if __name__=="__main__":

    
    DB = 'postgres'                 
    USERNAME = None
    PASSWORD = None
    HOSTNAME = 'localhost'            
    PORT = 5432 

    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--db_name', help='name of db',default=DB)
    parser.add_argument('--username', help='name of username',default=USERNAME)
    parser.add_argument('--password', help='password of db',default=PASSWORD)
    parser.add_argument('--hostname', help='name of hostname',default=HOSTNAME)
    parser.add_argument('--port', help='port of postgres',default=PORT)
    args=parser.parse_args()
    obj=psql(args)

    result=obj.metriccollector()
    print(json.dumps(result,indent=True))
