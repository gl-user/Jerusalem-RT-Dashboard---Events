from datetime import datetime,timedelta
import random
from random import randrange
#import run_import_gtfs_to_sql_2 as sql
import sys
import psycopg2
import time
import mpu
import SiriManLink
import pandas as pd
import numpy as np
import shutil
#argv = sys.argv
from sqlalchemy import create_engine
from os import listdir
from os.path import isfile, join
import os
from shutil import copyfile, move
import zipfile
import subprocess
import csv
import ftplib
import paramiko
import subprocess


#import run_import_gtfs_to_sql_2 as import_gtfs
day_periods=[[1,'04:00:00','06:29:59','04:00-6:29'],[2,'06:30:00','08:29:59','06:30-8:29'],[3,'08:30:00','11:59:59','08:30-11:59'],[4,'12:00:00','14:59:59','12:00-14:59'],[5,'15:00:00','18:59:59','15:00-18:59'],[6,'19:00:00','23:59:59','19:00-23:59'],[7,'00:00:00','03:59:59','0:00-03:59'],]

days_of_week = {1: 'Sunday', 2: 'Monday', 3: 'Tuesday', 4: 'Wednesday', 5: 'Thursday', 6: 'Friday',7: 'Saturday'}


class SiriManLink:
    def __init__(self,LinkID,RouteID,tripUniqueKey,firstTimeStamp,FromLongLat,ToLongLat,FromTime,ToTime,siriDistance):
        try:
            self.LinkID = int(LinkID)
        except:
            print("LinkID has to be an INT ")
            return 0
        try:
            self.RouteID = int(RouteID)
        except:
            print("RouteID has to be an INT ")
            return 0
        try:
            self.tripUniqueKey = tripUniqueKey
        except:
            print("Error in tripUniqueKey")
            return 0
        try:
            self.firstTimeStamp = firstTimeStamp
        except:
            print("Error in firstTimeStamp")
            return 0
        try:
            self.FromLong=FromLongLat[0]
        except:
            print("FromLongLat is not in list form")
            return 0
        try:
            self.FromLat = FromLongLat[1]
        except:
            print("FromLongLat is not in list form")
            return 0
        try:
            self.ToLong = ToLongLat[0]
        except:
            print("ToLongLat is not in list form")
            return 0
        try:
            self.ToLat = ToLongLat[1]
        except:
            print("ToLongLat is not in list form")
            return 0
        self.FromTime=FromTime
        self.ToTime=ToTime
        self.siriDistance=siriDistance
        self.siriSpeed = round(self.__GetSpeedFromTimeAndDistance__(self.FromTime, self.ToTime, self.siriDistance),3)
    def __GetSpeedFromTimeAndDistance__(self, FromTime, ToTime, siriDistance):
            FromTime= datetime.strptime(str(FromTime), '%Y-%m-%d %H:%M:%S')
            ToTime=datetime.strptime(str(ToTime), '%Y-%m-%d %H:%M:%S')
            return (siriDistance / (abs((ToTime - FromTime).total_seconds())) * 3.6)
    def __str__(self):
        print('LinkID:{:>20}\nRouteID:{:>19}\ntripUniqueKey:{:>39}\nfirst Time Stamp:      {} \nFromLongLat:{:>20},{}\nToLongLat:{:>21},{}\nFromTime:              {}\nToTime:                {}\nDistance:              {}\nSpeed:{:>22}\n'.\
              format(self.LinkID,self.RouteID,self.tripUniqueKey,self.firstTimeStamp,self.FromLong,self.FromLat,self.ToLong,self.ToLat,self.FromTime,self.ToTime,self.siriDistance,self.siriSpeed))
    def convertObjectToList(self):
        self.siriObjectList=[]
        self.siriObjectList.append(self.LinkID)
        self.siriObjectList.append(self.RouteID)
        self.siriObjectList.append(self.tripUniqueKey)
        self.siriObjectList.append(self.firstTimeStamp)
        self.siriObjectList.append(self.FromLong)
        self.siriObjectList.append(self.FromLat)
        self.siriObjectList.append(self.ToLong)
        self.siriObjectList.append(self.ToLat)
        self.siriObjectList.append(self.FromTime)
        self.siriObjectList.append(self.ToTime)
        self.siriObjectList.append(self.siriDistance)
        self.siriObjectList.append(self.siriSpeed)
        return self.siriObjectList

def setWorkMem(conn,mem_size,unit):

    if unit == 'MB' and mem_size >= 1064:
        unit = 'GB'
        mem_size = mem_size / 1064.0
    else:
        unit = 'MB'

    query = "SET work_mem TO '" + str(mem_size) + " " + unit + "'"
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print (query)
        print ("I can't SET work_mem")
        return (False)
    return (True)


def getClosestPointToLink(conn,corlist,link_id,street_layer):
    query = "SELECT ST_AsText(ST_ClosestPoint(line,pt)) As cp_line_pt FROM (SELECT ST_GeomFromText('POINT("+str(corlist[0])+" "+str(corlist[1])+")',4326)::geometry As pt, " + street_layer + ".geom As line from " + street_layer + " where " + street_layer + ".id = "+ str(link_id) +") As foo;"
    cur = conn.cursor()
    try:
        cur.execute(query)
    except:
        print (query)
        print ("I can't SELECT from " + street_layer + "")
    rows = cur.fetchall()
    lat = float(rows[0][0][rows[0][0].find(' '):-1])
    lon = float(rows[0][0][6:rows[0][0].find(' ')])
    return [lon,lat]

def getClosestPointToShape(conn,corlist,shape_id):
    query = "SELECT ST_AsText(ST_ClosestPoint(line,pt)) As cp_line_pt FROM (SELECT ST_GeomFromText('POINT("+str(corlist[0])+" "+str(corlist[1])+")',4326)::geometry As pt, gtfs_shape_geoms.the_geom As line from gtfs_shape_geoms where gtfs_shape_geoms.shape_id = '"+ shape_id +"') As foo;"
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print (query)
        print ("I can't SELECT from gtfs_shape_geoms")
    rows = cur.fetchall()
    lat = float(rows[0][0][rows[0][0].find(' '):-1])
    lon = float(rows[0][0][6:rows[0][0].find(' ')])
    return [lon,lat]

def getManLinkMainEndPoints(conn,link_id,street_layer):
    query = "SELECT from_main_id, to_main_id from " + street_layer + " where id="+str(link_id)
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "I can't SELECT from " + street_layer + ""
    rows = cur.fetchall()
    from_main_id =rows[0][0]
    to_main_id = rows[0][1]
    return [int(from_main_id),to_main_id]

def getLinkLengthFromPointToPointFromIsMan(conn,pta_coord,ptb_coord,link_id,street_layer):
    query = "SELECT ST_Length(ST_LineSubstring(line, least(ST_LineLocatePoint(line, pta),\
    ST_LineLocatePoint(line, ptb)), greatest(ST_LineLocatePoint(line, pta), ST_LineLocatePoint(line, ptb)))::geography)\
    FROM (SELECT " + street_layer + ".geom_line line, 'SRID=4326;POINT("+str(pta_coord[0])+" "+str(pta_coord[1])+")'::geometry pta, 'SRID=4326;POINT("+str(ptb_coord[0])+" "+str(ptb_coord[1])+")'::geometry ptb\
    from " + street_layer + " where " + street_layer + ".id = "+str(link_id)+") data;"
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "I can't SELECT from " + street_layer + ""
    rows = cur.fetchall()
    return rows[0][0]

def getLinkLengthFromPointToPointFromGtfsGeom(conn,pta_coord,ptb_coord,shape_id):
    query="SELECT ST_Length(ST_LineSubstring(line, least(ST_LineLocatePoint(line, pta),\
    ST_LineLocatePoint(line, ptb)), greatest(ST_LineLocatePoint(line, pta), ST_LineLocatePoint(line, ptb)))::geography)\
    FROM (SELECT gtfs_shape_geoms.the_geom line, 'SRID=4326;POINT("+str(pta_coord[0])+" "+str(pta_coord[1])+")'::geometry pta, 'SRID=4326;POINT("+str(ptb_coord[0])+" "+str(ptb_coord[1])+")'::geometry ptb\
    from gtfs_shape_geoms where gtfs_shape_geoms.shape_id = '"+str(shape_id)+"') data;"
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "I can't SELECT from gtfs_shape_geom"
    rows = cur.fetchall()
    return rows[0][0]

def getEndPointCoordsByID(conn,id):
    query="select long_lat from endpoints_man where id="+str(id)
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "I can't SELECT from endpoints_man"
    rows = cur.fetchall()
    lon_coor,lat_coor=rows[0][0].split('_')
    lon_coor=float(str(lon_coor[:2]+"."+ lon_coor[2:]))
    lat_coor=float(str(lat_coor[:2]+"."+ lat_coor[2:]))
    return [lon_coor,lat_coor]

def GetTimeDifferenceFromSiriTimeStamps(conn,FirstTimeStamp,SecondTimeStamp,DateTimeFormat = '%Y-%m-%d %H:%M:%S'):
    try:
        if(isinstance(FirstTimeStamp,str)):
            FirstTimeStamp = datetime.strptime(FirstTimeStamp, DateTimeFormat)
    except ValueError:
        print "Bad DateTime Format! "+FirstTimeStamp+" does not fit with DateTime Format '"+DateTimeFormat+"'"
        return None
    try:
        if (isinstance(SecondTimeStamp, str)):
            SecondTimeStamp = datetime.strptime(SecondTimeStamp, DateTimeFormat)
    except ValueError:
        print "Bad DateTime Format! "+SecondTimeStamp+" does not fit with DateTime Format '"+DateTimeFormat+"'"
        return None
    return abs(SecondTimeStamp-FirstTimeStamp)

def getLinkClosestPositionToPoint(conn,pt_coor,link_id,street_layer):
    query="SELECT ST_LineLocatePoint(line, pt) FROM(SELECT " + street_layer + ".geom_line line, 'SRID=4326;POINT("+str(pt_coor[0])+" "+str(pt_coor[1])+")'::geometry pt from " + street_layer + " where id = '"+str(link_id)+"') data;"
    #query="SELECT ST_LineLocatePoint(line, pt) FROM(SELECT " + street_layer + ".geom line, 'SRID=4326;POINT(" + str(pt_coor[0]) + " " + str(pt_coor[1]) + ")'::geometry pt from " + street_layer + " where id = '" + str(link_id) + "') data;"
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "I can't SELECT from " + street_layer + ""
    rows = cur.fetchall()
    return rows[0][0]

def getRouteClosestPositionToPoint(conn,pt_coor,shape_id):
    query="SELECT ST_LineLocatePoint(line, pt) FROM(SELECT gtfs_shape_geoms.the_geom line, 'SRID=4326;POINT("+str(pt_coor[0])+" "+str(pt_coor[1])+")'::geometry pt from gtfs_shape_geoms where shape_id = '"+str(shape_id)+"') data;"
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "I can't SELECT from gtfs_shape_geoms"
    rows = cur.fetchall()
    #return rows[0][0]
    print rows

def checkDistanceBetweenPointsOnLinkAndShape(shape_lat_lon,link_lat_lon,distance=3):
    dist = mpu.haversine_distance((shape_lat_lon[0],shape_lat_lon[1]), (link_lat_lon[0],link_lat_lon[1]))
    return False if(dist*1000>distance) else True

def checkIfSiriPointsOnLine(conn,link_id,shape_id,siriPoints):
    newSiriPoints=[]
    for point in siriPoints:
        if (checkDistanceBetweenPointsOnLinkAndShape(getClosestPointToLink(conn,point,link_id),getClosestPointToShape(conn,point,shape_id))):
            point.append(getLinkClosestPositionToPoint(conn,point,link_id))
        else:
            point.append(False)
        newSiriPoints.append(point)
    return newSiriPoints

def createLinkTripRepository(conn,table_name='link_trip_repository',schema_name="raw_ltrs"):
    drop_queries="drop table if exists raw_ltrs."+table_name+"; DROP INDEX IF EXISTS link_id_index;DROP INDEX IF EXISTS route_id_index;DROP INDEX IF EXISTS from_time_index;"
    query1='CREATE TABLE '+schema_name+'.'+table_name+' (Link_ID int ,Route_ID int,trip_Unique_Key text,firstTimeStamp TIMESTAMP,from_Lon float,from_Lat float,to_Lon float,to_Lat float,from_Time TIMESTAMP,to_Time TIMESTAMP,siri_Distance float, siri_Speed float, day_period text, from_time_hour int, Dir int)'
    query2='create index link_id_index_'+table_name+' on '+schema_name+'.'+table_name+' (link_id)'
    query3='create index route_id_index_'+table_name+' on '+schema_name+'.'+table_name+'  (route_id)'
    query4 = 'create index from_time_index_'+table_name+' on '+schema_name+'.'+table_name+' (from_time)'
    query5='create index unique_trip_id_index_'+table_name+' on '+schema_name+'.'+table_name+' (from_time)'
    try:
        cur = conn.cursor()
        cur.execute(drop_queries)
    except:
        print drop_queries
        print "Can't Create Table '"+table_name+"'"
    try:
        cur = conn.cursor()
        cur.execute(query1)
    except:
        print query1
        print "Can't Create Table '"+table_name+"'"
    try:
        cur = conn.cursor()
        cur.execute(query2)
    except:
        print query2
        print "Can't Create Index 'link_id_index' on table '"+table_name+"'"
    try:
        cur = conn.cursor()
        cur.execute(query3)
    except:
        print query3
        print "Can't Create Index 'route_id_index' on table '"+table_name+"'"
    try:
        cur = conn.cursor()
        cur.execute(query4)
    except:
        print query4
        print "Can't Create Index 'from_time_index' on table '"+table_name+"'"
    try:
        cur = conn.cursor()
        cur.execute(query5)
    except:
        print query4
        print "Can't Create Index 'unique_trip_id_index_' on table '"+table_name+"'"
    conn.commit()

def createAndFillHourToPeriodTable(conn,day_periods):
    query1='CREATE TABLE Hour_Day_Period_Conversion (period_num int,from_time time, to_time time, period_name text)'
    counter=0
    try:
        cur = conn.cursor()
        cur.execute(query1)
    except:
        print query1
        print ("Can't Create Table 'Hour_Day_Period_Conversion'")
    for period in day_periods:
        query2 = "insert into Hour_Day_Period_Conversion (period_num,from_time,to_time,period_name) values ("+str(period[0])+",'"+str(period[1])+"','"+str(period[2])+"','"+str(period[3])+"')"
        counter+=1
        try:
            cur = conn.cursor()
            cur.execute(query2)
        except:
             print query2
             print ("Can't insert period "+str(counter+1))
    conn.commit()

def createAndFillDayOfWeekTable(conn,days_of_week):
    query1='CREATE TABLE days_of_week (dow_num int,dow_name text)'
    "drop table IF EXISTS days_of_week;\
    CREATE TABLE days_of_week (dow_num int,dow_name text);"
    counter=0
    try:
        cur = conn.cursor()
        cur.execute(query1)
    except:
        print query1
        print ("Can't Create Table 'days_of_week'")
    for dow_num,dow_name in days_of_week.items():
        query2 = "insert into days_of_week (dow_num, dow_name) values ("+str(dow_num)+",'"+ str(dow_name)+"')"
        counter+=1
        try:
            cur = conn.cursor()
            cur.execute(query2)
        except:
             print query2
             print ("Can't insert dow "+str(counter+1))
    conn.commit()


def truncateLinkTripRepository(conn):
    query='truncate table Link_Trip_Repository'
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except:
        print query
        print "Can't TRUNCATE Table 'Link_Trip_Repository'"

def insertSiriManLinkToDb(conn,siriRecord,table_name="link_trip_repository"):
    query='INSERT INTO '+table_name+' (Link_ID , Route_ID ,trip_Unique_Key, firstTimeStamp ,from_Lon ,from_Lat ,to_Lon ,to_Lat ,from_Time ,to_Time ,siri_Distance , siri_Speed, day_period, from_time_hour, Dir)\
    VALUES ('+str(siriRecord.LinkID)+','+str(siriRecord.RouteID)+",'"+siriRecord.tripUniqueKey+"', '"+str(siriRecord.firstTimeStamp)+"',"+str(siriRecord.FromLong)+','\
    +str(siriRecord.FromLat)+','+str(siriRecord.ToLong)+','+str(siriRecord.ToLat)+", '"+str(siriRecord.FromTime)+"','"+str(siriRecord.ToTime)+"',"+str(siriRecord.siriDistance)+','+str(siriRecord.siriSpeed)+", \
    case \
    when("+str(siriRecord.FromTime.hour)+" >= 4 and "+str(siriRecord.FromTime.hour)+" < 6) or ("+str(siriRecord.FromTime.hour)+"=6 and "+str(siriRecord.FromTime.minute)+" <= 29) then '4:00-6:29'\
    when("+str(siriRecord.FromTime.hour)+" = 6 and "+str(siriRecord.FromTime.minute)+" >= 30) or "+str(siriRecord.FromTime.hour)+" = 7 or ("+str(siriRecord.FromTime.hour)+"=8 and "+str(siriRecord.FromTime.minute)+" <= 29) then '6:30-8:29' \
    when("+str(siriRecord.FromTime.hour)+" = 8 and "+str(siriRecord.FromTime.minute)+" >= 30) or ("+str(siriRecord.FromTime.hour)+" >= 9 and "+str(siriRecord.FromTime.hour)+" <= 11) then '8:30-11:59'\
    when "+str(siriRecord.FromTime.hour)+" >= 12 and "+str(siriRecord.FromTime.hour)+" <= 14 then '12:00-14:59'\
    when "+str(siriRecord.FromTime.hour)+" >= 15 and "+str(siriRecord.FromTime.hour)+" <= 18 then '15:00-18:59'\
    when "+str(siriRecord.FromTime.hour)+" >= 19 and "+str(siriRecord.FromTime.hour)+" <= 23 then '19:00-23:59'\
    else '0:00-03:59' end, "+str(siriRecord.FromTime.hour)+","+str(isDirectionLikeTopology(conn, siriRecord.tripUniqueKey, [siriRecord.FromLong,siriRecord.FromLat],[siriRecord.ToLong,siriRecord.ToLat],siriRecord.LinkID))+")"
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print "Can't Insert siri record to Table 'LinkTripRepository'"

def insertSiriManLinkToDbVer2(conn, siriRecord,table_name="link_trip_repository"):
    q1 = "INSERT INTO "+table_name+" (Link_ID ,Route_ID ,trip_Unique_Key ,firstTimeStamp ,from_Lon ,from_Lat ,to_Lon ,to_Lat ,from_Time ,to_Time ,siri_Distance , siri_Speed, day_period, from_time_hour, dir) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    try:
        cur = conn.cursor()
        cur.execute(q1, (
        siriRecord.LinkID, siriRecord.RouteID, siriRecord.tripUniqueKey, siriRecord.firstTimeStamp, siriRecord.FromLong,
        siriRecord.FromLat, siriRecord.ToLong, siriRecord.ToLat, siriRecord.FromTime, siriRecord.ToTime,
        siriRecord.siriDistance, siriRecord.siriSpeed,getDayPeriod(siriRecord.FromTime.time),siriRecord.FromTime.hour), siriRecord.dir)
    except:
        print q1
        print "Can't Insert to Table '"+table_name+"'"
    conn.commit()

def insertSiriRowsManLinkToDbVer2(conn, siriRecordList,table_name="link_trip_repository"):
    siri_list=[]
    for siri_rec in siriRecordList:
        siri_list.append(siri_rec.convertObjectToList())
    q1 = "INSERT INTO raw_ltrs."+table_name+" (Link_ID ,Route_ID ,trip_Unique_Key ,firstTimeStamp ,from_Lon ,from_Lat ,to_Lon ,to_Lat ,from_Time ,to_Time ,siri_Distance , siri_Speed, day_period, from_time_hour , dir) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #try:
    cur = conn.cursor()
    cur.executemany(q1,siri_list)

    #except:
    #    print q1
    #    print "Can't Insert to Table 'Link_Trip_Repository'"
    conn.commit()

def insertSiriRowsManLinkToDbVer3(conn, siriRecordList,table_name="link_trip_repository"):
    siri_list=[]
    for siri_rec in siriRecordList:
        siri_list.append(siri_rec.convertObjectToList())
    q1 = "INSERT INTO "+table_name+" (Link_ID ,Route_ID ,trip_Unique_Key ,firstTimeStamp ,from_Lon ,from_Lat ,to_Lon ,to_Lat ,from_Time ,to_Time ,siri_Distance , siri_Speed, day_period, from_time_hour , dir) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    #try:
    cur = conn.cursor()
    cur.executemany(q1,siri_list)

    #except:
    #    print q1
    #    print "Can't Insert to Table 'Link_Trip_Repository'"
    conn.commit()


def insertLinkMeanSpeedByPeriodToDb(engine,link_avg_speed_by_period):
    #for row in link_avg_speed_by_period.iterrows():
        #query='INSERT INTO link_avg_speed_by_period (Link_ID ,day_period ,siri_speed_mean_speed, from_time_min ,from_time_max ,from_Lon_min,from_Lon_max,from_lat_min,from_lat_max ,num_of_samples) \
        #VALUES ('+str(row[0])+','+str(row[1])+",'"+str(row[2])+"', '"+str(row[3])+"',"+str(row[4])+','\
        #+str(row[5])+','+str(row[6])+','+str(row[7])+str(row[8])+"','"+str(row[9])
        #try:
        #    cur = conn.cursor()
        #    cur.execute(query)
        #    print row
        #except:
        #    print "Can't Insert siri record to Table 'LinkTripRepository'"
    #    print (row)
    link_avg_speed_by_period.to_sql(name='link_avg_speed_by_period', con=engine, if_exists='replace', index=False)

def getDayPeriod(TimeStamp):
    if('04:00:00'<=TimeStamp<='06:29:59'): return '4:00-6:29'
    elif('06:30:00'<=TimeStamp<='08:29:59'): return '6:30-8:29'
    elif('08:30:00'<=TimeStamp<='11:59:59'): return '8:30-11:59'
    elif('12:00:00'<=TimeStamp<='14:59:59'): return '12:00-14:59'
    elif('15:00:00'<=TimeStamp<='18:59:59'): return '15:00-18:59'
    elif('19:00:00'<=TimeStamp<='23:59:59'): return '19:00-23:59'
    else: '0:00-03:59'


def gen_datetime(start, end):
    delta = end - start
    int_delta = (delta.days * 24 * 60 * 60) + delta.seconds
    random_second = randrange(int_delta)
    return start + timedelta(seconds=random_second)

def fillSiriManLink():
    LinkID=random.randint(1000,7000)
    RouteID=random.randint(1000,9999)
    FromLong=round(random.uniform(34.3,35.5), 6)
    FromLat=round(random.uniform(29.49,33.8), 6)
    ToLong=round(random.uniform(34.3,35.5), 6)
    ToLat=round(random.uniform(29.49,33.8), 6)
    firstTimeStamp = gen_datetime(datetime.strptime('01/01/2018 00:00:00', '%d/%m/%Y %H:%M:%S'),datetime.strptime('01/02/2019 23:59:59', '%d/%m/%Y %H:%M:%S'))
    FromTime = firstTimeStamp + timedelta(minutes=randrange(30))
    ToTime = FromTime + timedelta(seconds=randrange(1,600))
    tripUniqueKey=str(RouteID)+"-"+str(random.randint(1,100000))+"-"+str(firstTimeStamp)[-8:]
    siriDistance=round(random.uniform(0,1000), 3)
    siriInstance=SiriManLink(LinkID, RouteID, tripUniqueKey, firstTimeStamp, [FromLong,FromLat], [ToLong,ToLat], FromTime, ToTime,siriDistance)
    return siriInstance

def fillManySiriManLinkObjects(numOfInstances):
    siriObjectsList=[]
    counter=0
    while(counter<numOfInstances):
        siriObjectsList.append(fillSiriManLink())
        counter+=1
    return siriObjectsList

def fillLinkTripRepositoryWithRandomRecords(conn,num=1000000):
    count=0
    #createLinkTripRepository(conn)
    while(count<num):
        siriInstance=fillSiriManLink()
        insertSiriManLinkToDb(conn, siriInstance)
        count+=1
    conn.commit()

def printLinkTripRepository(conn,table_name="link_trip_repository"):
    query='select * from '+table_name
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "Can't Print Table '"+table_name+"'"
    rows= cur.fetchall()
    for row in rows:
        print row

def selectByDateAndDayPeriod(conn,start_date,end_date,day_period=0,table_name="link_trip_repository"):
    query="select * from "+table_name+" where From_Time between '"+start_date+"' and '"+end_date+"' and To_Time between '"+start_date+"' and '"+end_date+"'"
    if(day_period!=0):
        if(day_period==1):
            query=query+" and day_period='4:00-6:29'"
        elif(day_period==2):
            query = query + " and day_period='6:30-8:29'"
        elif (day_period == 3):
            query = query + " and day_period='8:30-11:59'"
        elif (day_period == 4):
            query = query + " and day_period='12:00-14:59'"
        elif (day_period == 5):
            query = query + " and day_period='15:00-18:59'"
        elif (day_period == 6):
            query = query + " and day_period='19:00-23:59'"
        elif (day_period == 7):
            query = query + " and day_period='00:00-03:59'"
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows= cur.fetchall()
        for row in rows:
            print row
    except:
        print query
        print "Can't select by date and day period from Table '"+table_name+"'"

def selectByDateAndHour(conn,start_date,end_date,from_time_hour,table_name="link_trip_repository"):
    query="select * from "+table_name+" where From_Time between '"+start_date+"' and '"+end_date+"' and To_Time between '"+start_date+"' and '"+end_date+"' and From_Time_hour="+str(from_time_hour)
    counter=0
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows= cur.fetchall()
        for row in rows:
            print row
            counter+=1
        print("Total rows retrieved: " + str(counter))
    except:
        print query
        print "Can't select by date and hour from Table '"+table_name+"'"

def selectByCoords(conn,FromLongLat,ToLongLat,table_name="link_trip_repository"):
    query="select * from "+table_name+" where from_lon between '"+str(FromLongLat[0])+"' and '"+str(ToLongLat[0])\
    +"' and from_lat between '"+str(FromLongLat[1])+"' and '"+str(ToLongLat[1])+"'and to_lon between '"+str(FromLongLat[0])\
    +"' and '"+str(ToLongLat[0])+"' and to_lat between '"+str(FromLongLat[1])+"' and '"+str(ToLongLat[1])+"'"
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows= cur.fetchall()
        for row in rows:
            print row
    except:
        print query
        print "Can't select by coordinates from Table '"+table_name+"'"

    try:
        cur = conn.cursor()
        cur.execute(query)
        rows= cur.fetchall()
        for row in rows:
            print row
    except:
        print query
        print "Can't select by coordinates, date and day period from Table '"+table_name+"'"

def retrieveRowsFromTable(conn,table_name,numOfRows=100):
    query="select * from "+table_name+" limit "+str(numOfRows)
    counter=0
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "Can't retrieve rows from Table '"+table_name+"'"
    rows = cur.fetchall()
    for row in rows:
        print row
        counter += 1
    print("Total rows retrieved:"+str(counter))

def calculateLinkPeriodAvg(conn,table_name):
    counter = 0
    query="select * from "+table_name
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "Can't retrieve rows from Table '"+table_name+"'"
    rows = cur.fetchall()
    for row in rows:
        print row
        counter += 1
    print("Total rows retrieved:"+str(counter))

def convertSiriObjectsToList(siriObjects):
    siriInstancesList=[]
    for sObject in siriObjects:
        siriInstancesList.append(sObject.convertObjectToList())
    return siriInstancesList

def createAndFillLinkAvgSpeedByPeriod(conn,printTable=0,table_name="link_trip_repository"):
    query = "drop table IF EXISTS link_avg_speed_by_period;\
    CREATE table link_avg_speed_by_period as (select link_id,dir,day_period, avg(siri_speed) as avg_speed, min(from_lon) as min_from_lon,\
    max(from_lon) as max_from_lon,min(from_lat) as min_from_lat, max(from_lat) as max_from_lat, min(from_time) as min_from_time, \
    max(from_time) as max_from_time, count(*) as mun_of_samples from "+table_name+" group by link_id,dir,day_period order by link_id,day_period);\
    select * from link_avg_speed_by_period"
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except:
        print query
        print "Can't create and fill Table 'link_avg_speed_by_period'"
    if(printTable==1):
        rows = cur.fetchall()
        for row in rows:
            print row

def createAndFillLinkAvgSpeedByHour(conn,printTable=0,table_name="link_trip_repository"):
    query = "drop table IF EXISTS link_avg_speed_by_hour;\
    CREATE table link_avg_speed_by_hour as (select link_id,dir,from_time_hour, avg(siri_speed) as avg_speed, min(from_lon) as min_from_lon, \
    max(from_lon) as max_from_lon,min(from_lat) as min_from_lat, max(from_lat) as max_from_lat, min(from_time) as min_from_time, \
    max(from_time) as max_from_time, count(*) as mun_of_samples from "+table_name+" group by link_id,dir, from_time_hour order by link_id,from_time_hour);\
    select * from link_avg_speed_by_hour"
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except Exception, e:
        print query
        print "Exception " + str(e)
        print "Can't create and fill Table 'link_avg_speed_by_hour'"
    if(printTable==1):
        rows = cur.fetchall()
        for row in rows:
            print row

def createAndFillLinkAvgSpeedByDowAndDayPeriod(conn,printTable=0,table_name="link_trip_repository"):
    query = "drop table IF EXISTS link_avg_speed_by_dow_and_day_period;\
    CREATE table link_avg_speed_by_dow_and_day_period as (select link_id,dir, cast(extract(dow from from_time)+1 as int) as day_of_week, day_period, avg(siri_speed) as avg_speed, min(from_lon) as min_from_lon, \
    max(from_lon) as max_from_lon,min(from_lat) as min_from_lat, max(from_lat) as max_from_lat, min(from_time) as min_from_time, \
    max(from_time) as max_from_time, count(*) as mun_of_samples from  "+table_name+" group by link_id,dir, day_of_week, day_period order by link_id, day_of_week, day_period);\
    select * from link_avg_speed_by_dow_and_day_period"
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except:
        print query
        print "Can't create and fill Table 'link_avg_speed_by_dow_and_day_period'"
    if(printTable==1):
        rows = cur.fetchall()
        for row in rows:
            print row

def createAndFillLinkAvgSpeedByDowAndHour(conn,printTable=0,table_name="link_trip_repository"):
    query = "drop table IF EXISTS link_avg_speed_by_dow_and_hour;\
    CREATE table link_avg_speed_by_dow_and_hour as (select link_id, dir, cast(extract(dow from from_time)+1 as int) as day_of_week ,from_time_hour, avg(siri_speed) as avg_speed, min(from_lon) as min_from_lon, \
    max(from_lon) as max_from_lon,min(from_lat) as min_from_lat, max(from_lat) as max_from_lat, min(from_time) as min_from_time, \
    max(from_time) as max_from_time, count(*) as mun_of_samples from  "+table_name+" group by link_id,dir, day_of_week, from_time_hour order by link_id, day_of_week, from_time_hour);\
    select * from link_avg_speed_by_dow_and_hour"
    try:
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
    except:
        print query
        print "Can't create and fill Table 'link_avg_speed_by_dow_and_hour'"
    if(printTable==1):
        rows = cur.fetchall()
        for row in rows:
            print row

def isDirectionLikeTopology(conn,uniqe_trip_id,from_pt_siri,to_pt_siri,link_id,street_layer):
    snaped_from_pt_siri=getClosestPointToLink(conn,from_pt_siri,link_id,street_layer)
    snaped_to_pt_siri=getClosestPointToLink(conn,to_pt_siri,link_id,street_layer)
    from_street_layer_end_point=getEndPointCoordsByID(conn,getManLinkMainEndPoints(conn, link_id,street_layer)[0])
    to_street_layer_end_point=getEndPointCoordsByID(conn,getManLinkMainEndPoints(conn, link_id,street_layer)[1])
    #print("end point A:{}\n from pt:{}\n to pt:{}\nend point B:{}\n".format(from_" + street_layer + "_end_point,snaped_from_pt_siri,snaped_to_pt_siri,to_" + street_layer + "_end_point))
    if((getLinkLengthFromPointToPointFromIsMan(conn, from_street_layer_end_point, snaped_from_pt_siri, link_id,street_layer)< getLinkLengthFromPointToPointFromIsMan(conn, from_street_layer_end_point, snaped_to_pt_siri, link_id,street_layer))):
        return 1
    else:
        return -1

def addDirToLinkTripRepositoryBK(street_layer,repository_name='link_trip_repository',alsoFailed=False):
    engine = create_engine('postgresql://postgres:123qwe@localhost:5432/postgis_siri')
    repository=pd.read_sql_query("select distinct on (concat(link_id,'_',trip_unique_key)) * from "+repository_name,engine)
    repository['comb_index']=repository[['link_id','trip_unique_key']].astype(str).apply('_'.join,1)
    repository['match_dir']=0
    repository.set_index('comb_index',inplace =True)
    isman=pd.read_sql_query("select id,dir_transit from " + street_layer + "",engine,index_col='id')
    counter=0
    start_time=datetime.now()
    for i in repository.index:
        if(repository.get_value(i,'dir')==1):
            if(isman.get_value(repository.get_value(i,'link_id'),'dir_transit')!=-1):
                repository.set_value(i,'match_dir',1)
        elif(repository.get_value(i,'dir')==-1):
            if(isman.get_value(repository.get_value(i,'link_id'),'dir_transit')!=1):
                repository.set_value(i,'match_dir',1)
        else:
            raise ValueError
        counter += 1
    output_repository=repository.loc[repository['match_dir']==1]
    output_repository.to_sql(name=repository_name+'_success',con=engine, if_exists='replace',method='multi')
    if(alsoFailed):
        failed_output_repository=repository.loc[repository['match_dir']==0]
        failed_output_repository.to_sql(name=repository_name+'_failed', con=engine, if_exists='replace', method='multi')
        print("Total input records: {}\nTotal successful records: {}\nTotal failed records:{}".format(repository.shape[0],\
        output_repository.shape[0],failed_output_repository.shape[0]))
    else:
        print("Total input records: {}\nTotal successful records: {}".format(repository.shape[0], \
            output_repository.shape[0]))
    print("Total operation Time: {}".format(datetime.now()-start_time))


def addDirToLinkTripRepositorybk2(street_layer,repository_name='link_trip_repository',alsoFailed=False):
    engine = create_engine('postgresql://postgres:123qwe@localhost:5432/postgis_siri')
    repository=pd.read_sql_query("select distinct on (concat(link_id,'_',trip_unique_key)) * from "+repository_name,engine)
    #print repository
    repository['comb_index']=repository[['link_id','trip_unique_key']].astype(str).apply('_'.join,1)
    repository['match_dir']=0
    #repository.set_index('comb_index',inplace =True)
    isman=pd.read_sql_query("select id,dir_transit from " + street_layer + "",engine, index_col='id')
    #print isman
    counter=0
    start_time=datetime.now()
    for i in repository.index:
        linkid=repository.iloc[i]['link_id']
        #print ("link_id ->{}".format(linkid))
        if(repository.iloc[i]['dir']==1):
            if(isman.at[linkid,'dir_transit']!=-1):
                repository.at[i,'match_dir']=1
        elif (repository.iloc[i]['dir'] == -1):
            if(isman.at[linkid,'dir_transit']!=1):
                repository.at[i,'match_dir']=1
        else:
            raise ValueError
        counter += 1
    output_repository=repository.loc[repository['match_dir']==1]
    output_repository.to_sql(name=repository_name+'_success',con=engine, if_exists='replace',method='multi')
    if(alsoFailed):
        failed_output_repository=repository.loc[repository['match_dir']==0]
        failed_output_repository.to_sql(name=repository_name+'_failed', con=engine, if_exists='replace', method='multi')
        print("Total input records: {}\nTotal successful records: {}\nTotal failed records:{}".format(repository.shape[0],\
        output_repository.shape[0],failed_output_repository.shape[0]))
    else:
        print("Total input records: {}\nTotal successful records: {}".format(repository.shape[0], \
            output_repository.shape[0]))


def getMatchingDumpsByDatesRange(from_date,to_date,mypath='\\\\mars//gl//Users//asafeh//siri//Dumps//',string_to_search='siri_sm_res_monitor'):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and f[:len(string_to_search)].lower()==string_to_search]
    relevantDateFiles=[]
    for x in onlyfiles:
        tableFrom=x[20:24]+'-'+x[24:26]+'-'+x[26:28]
        tableTo ="{}-{}-{}".format(x[32:36],x[36:38],x[38:40])
        #hourFrom=int(x[29:31])
        hourTo = int(x[41:43])
        if((tableFrom>=from_date and (tableFrom<to_date or (tableFrom==to_date and tableTo>to_date))) or (tableFrom<=from_date and (tableTo>from_date or tableTo==from_date and hourTo>4))):
            relevantDateFiles.append(x[:-5].lower())
    if(len(relevantDateFiles)>0):
        print ("{} matching dumps found. ".format(len(relevantDateFiles)))
        for dump in relevantDateFiles:
            print (dump)
    else:
        print ("No matching dumps found.")
    return sorted(relevantDateFiles)

def getMatchingDumpsByDatesList(dates_list,mypath='\\\\mars//gl//Users//asafeh//siri//Dumps//',string_to_search='siri_sm_res_monitor'):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and f[:len(string_to_search)].lower()==string_to_search.lower()]
    relevantDateFiles=[]
    for x in onlyfiles:
        print x
        tableFrom=datetime.strptime(x[20:24]+'-'+x[24:26]+'-'+x[26:28],'%Y-%m-%d').date()
        tableTo=datetime.strptime("{}-{}-{}".format(x[32:36],x[36:38],x[38:40]),'%Y-%m-%d').date()
        #hourFrom=int(x[29:31])
        hourTo = int(x[41:43])
        for date in dates_list:
            if(tableFrom<=date<tableTo or (tableFrom<=date and date==tableTo and hourTo>4)):
                relevantDateFiles.append(x[:-5].lower())
    if(len(relevantDateFiles)>0):
        print ("{} matching dumps found: ".format(len(relevantDateFiles)))
        #for dump in relevantDateFiles:
            #print (dump)
    else:
        print ("No matching dumps found.")
    return list(set(relevantDateFiles))

def getMatchingLtrsDumpsByDatesList(dates_list,mypath='D://exported_ltrs//',string_to_search='ltr_everything_'):
    onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f)) and f[:len(string_to_search)].lower()==string_to_search.lower()]
    relevantDateFiles=[]
    for x in onlyfiles:
        ltrDumpDate=datetime.strptime(x[len(string_to_search):len(string_to_search)+4]+'-'+x[len(string_to_search)+5:len(string_to_search)+7]+'-'+x[len(string_to_search)+8:len(string_to_search)+10],'%Y-%m-%d').date()
        #print ltrDumpDate
        if(ltrDumpDate in dates_list):
                relevantDateFiles.append(x[:-5])
    if(len(relevantDateFiles)>0):
        print ("{} matching dumps found: ".format(len(relevantDateFiles)))
        #for dump in relevantDateFiles:
            #print (dump)
    else:
        print ("No matching dumps found.")
    return relevantDateFiles

def DoesLtrExist(export_table_name,schema_name='raw_ltrs'):
    query = "SELECT EXISTS (select c.relname from pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace where n.nspname='{}' and c.relname='{}')".format(schema_name,export_table_name.lower())
    #print query
    cur = conn.cursor()
    cur.execute(query)
    query_feedback = cur.fetchall()
    return True if query_feedback[0][0] == True else False

def isTableBlank(export_table_name,schema_name="raw_ltrs"):
    if(DoesLtrExist(export_table_name,schema_name)==True):
        query="select count(*) from {}.{}".format(schema_name,export_table_name)
        cur = conn.cursor()
        cur.execute(query)
        query_feedback = cur.fetchall()
        if(query_feedback[0][0]>0):
            print("ltr {}.{} exists and has {:,} records. Skipping.".format(schema_name,export_table_name,query_feedback[0][0]))
            return False
        else:
            print("ltr {}.{} exists but is blank. Deleting.".format(schema_name,export_table_name))
            return True

    else:
        print("ltr {} does not exist. Processing.".format(export_table_name))
        return True

def createIndexesForSiri(siri_name,schema_name='processed_siris',fields_to_index=['lineref','vehicleref','vehiclelocationx','vehiclelocationy','originaimeddeparturetime']):
    #create index lineref_20200108_04_20200109_04_processed_idx on processed_siris.siri_sm_res_monitor_20200108_04_20200109_04_processed(lineref);
    print ("Creating indexes for table {}.{}:".format(schema_name,siri_name))
    for field in fields_to_index:
        index_name="{}_{}_idx".format(field,siri_name[20:])
        index_query="create index IF NOT EXISTS {} on {}.{} ({});".format(index_name,schema_name,siri_name,field)
        #print (index_query)
        cur = conn.cursor()
        cur.execute(index_query)
        conn.commit()
        print ("Index {} created.".format(index_name))



def getMatchingTablesByDatesRange(from_date,to_date,schema_name='processed_ltrs',string_to_search='ltr_everything_'):
    matching_tables_query = "select c.relname from pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace where n.nspname='{}' and left(c.relname,{})='{}'".format(schema_name,len(string_to_search),string_to_search)
    cur = conn.cursor()
    cur.execute(matching_tables_query)
    t=cur.fetchall()
    tables = [r[0] for r in t]
    relevantDateFiles=[]
    print tables
    for table in tables:
        tableDate=table[len(string_to_search):len(string_to_search)+4]+'-'+table[len(string_to_search)+5:len(string_to_search)+7]+'-'+table[len(string_to_search)+8:len(string_to_search)+10]
        if((from_date<=tableDate<=to_date)):
            relevantDateFiles.append(table)
    if(len(relevantDateFiles)>0):
        print ("{} matching tables found: ".format(len(relevantDateFiles)))
        for table in relevantDateFiles:
            print (table)
    else:
        print ("No matching tables found between {} and {}.".format(from_date,to_date))
    return relevantDateFiles

def restoreDumpsToDB(dumps_list,mypath='\\\\mars//gl//Users//asafeh//siri//Dumps//',schema_to_restore='raw_siris'):
    dumps_counter=0
    for dump in dumps_list:
        dumps_counter += 1
        in_processed_query = "SELECT EXISTS (SELECT relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname='processed_siris' and relname = '" + dump.lower() + "_processed');"
        cur = conn.cursor()
        cur.execute(in_processed_query)
        in_processed_query_feedback = cur.fetchall()
        if (in_processed_query_feedback[0][0] == False):
            in_raw_query="SELECT EXISTS (SELECT relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname='raw_siris' and relname = '"+dump.lower()+"');"
            #print query
            cur = conn.cursor()
            cur.execute(in_raw_query)
            in_raw_query_feedback = cur.fetchall()
            #print(query_feedback[0][0])
            if(in_raw_query_feedback[0][0]==False):
                print ("restoring dump {} of {}: {}".format(dumps_counter,len(dumps_list),dump))
                start_time=datetime.now()
                command="pg_restore -h localhost -p 5432 -U postgres -d postgis_siri -Fc -j 8  {}.dump".format(mypath+dump)
                #print command
                try:
                    os.system(command)
                    print ('Restoring successfull. Time: {}'.format(datetime.now()-start_time))
                except:
                    print(command)
                    print("Unable to restore dump {}".format(dump))
                moveSchema(dump,'public',schema_to_restore)
            else:
                print('Dump {} already in raw DB.'.format(dump))
        else:
            print('Dump {} already in processed DB.'.format(dump))



def restoreSingleDumpToDB(dump,mypath='\\\\mars//gl//Users//asafeh//siri//Dumps//',schema_to_restore='public',schema_to_move='raw_siris',processed_schema='processed_siris'):
    if(isinstance(dump,str)):
        in_processed_query = "SELECT EXISTS (SELECT relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname='"+processed_schema+"' and relname = '" + dump.lower() + "_processed');"
        cur = conn.cursor()
        cur.execute(in_processed_query)
        in_processed_query_feedback = cur.fetchall()
        if (in_processed_query_feedback[0][0] == False):
            in_raw_query="SELECT EXISTS (SELECT relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname='"+schema_to_restore+"' and relname = '"+dump.lower()+"');"
            #print (query)
            cur = conn.cursor()
            cur.execute(in_raw_query)
            in_raw_query_feedback = cur.fetchall()
            #print(query_feedback[0][0])
            if(in_raw_query_feedback[0][0]==False):
                print ("restoring dump : {}".format(dump))
                start_time=datetime.now()
                command="pg_restore -h localhost -p 5432 -U postgres -d postgis_siri -Fc -j 8  {}.dump".format(mypath+dump)
                #print command
                try:
                    os.system(command)
                    print ('Restoring successfull. Time: {}'.format(datetime.now()-start_time))
                except:
                    print(command)
                    print("Unable to restore dump {}".format(dump))
                moveSchema(dump,schema_to_restore,schema_to_move)
            else:
                print('Dump {} already in raw DB.'.format(dump))
        else:
            print('Dump {} already in processed DB.'.format(dump))


def import_ltrs_from_dump(last_date=(datetime.now() - timedelta(days=2)), schema_to_restore='processed_ltrs',
                          path='\\\\mars//gl//GL_Transit//ltrs_backups//', string_to_search='ltr_everything_',num_of_days_to_check=30):
    first_date = last_date - timedelta(days=num_of_days_to_check)
    dates_list = getRelevantDatesString(first_date.strftime("%Y-%m-%d"), last_date.strftime("%Y-%m-%d"), 4, 1)
    siris_in_schema = getAllSirisInDb(schema_to_restore, string_to_search)
    print (last_date)
    for date in dates_list:
        date = date.strftime("%Y_%m_%d")
        if (string_to_search + date not in siris_in_schema):
            print ("Ltr {} not in Schema {}. Continue.".format(string_to_search + date, schema_to_restore))
            all_ltrs_in_bk = [f for f in listdir(path) if
                              isfile(join(path, f)) and f[:len(string_to_search)].lower() == string_to_search]
            if (string_to_search + date + '.dump' in all_ltrs_in_bk):
                print("Ltr {} found in Backup {}. Restoring.".format(string_to_search + date, path))
                restoreSingleDumpToDB(string_to_search + date, path, schema_to_restore, schema_to_restore)
            else:
                print ("Ltr {} not found in Backup {}. Skipping.".format(string_to_search + date, path))

        else:
            print ("Ltr {} found in Schema {}. Continue.".format(string_to_search + date, schema_to_restore))

def dropBlankTables(schema_name='processed_siris',string_to_search='siri_sm_res_monitor',tolerance=50000):
    query="SELECT relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname='{}' and left(relname,{})='{}' and relkind = 'r' and c.reltuples<{}".format(schema_name,len(string_to_search),string_to_search,tolerance)
    print query
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    result = [r[0] for r in rows]
    print ("{} Blank tables found:".format(len(result)))
    for r in result:
        print r
    for s in result:
        print ("Table {} is blank. Dropping.".format(s))
        drop_query="drop table {}.{} cascade".format(schema_name,s)
        cur = conn.cursor()
        cur.execute(drop_query)
        conn.commit()



def cleanSiriFromDuplicatesAndBadRecords(from_schema='raw_siris',to_schema='processed_siris'):
    relations_in_raw_siris=getAllSirisInDb(from_schema,'siri_sm_res_monitor')
    relations_in_processed_siris=getAllSirisInDb(to_schema,'siri_sm_res_monitor')
    relations_to_process = [f for f in relations_in_raw_siris if f+"_processed" not in relations_in_processed_siris]
    count_relations=0
    for siri_name in relations_to_process:
        print("Cleaning {} of {} : {}".format(count_relations+1,len(relations_to_process),siri_name))
        cur = conn.cursor()
        cur.execute("select count(*) from {}.{}".format(from_schema,siri_name))
        count_before_query=cur.fetchall()[0][0]
        start_time=datetime.now()
        query="create table if not exists {}.{}_processed as select distinct on(lineref, originaimeddeparturetime," \
              "vehicleref, recordedattime) * from {}.{} where vehiclelocationx != 0 and vehiclelocationy != 0 and originaimeddeparturetime!='0001-01-01 00:00:00'"\
            .format(to_schema,siri_name,from_schema,siri_name)
        cur.execute(query)
        conn.commit()
        end_time=datetime.now()
        cur.execute("select count(*) from {}.{}_processed".format(to_schema,siri_name))
        count_after_query=cur.fetchall()[0][0]
        if(count_after_query>0):
            print("Relation {} of {} : {} was cleaned successfully. Records before: {:,} | Records after: {:,}. dropped {:.2%}. Duration: {}"
            .format(count_relations+1,len(relations_to_process),siri_name,count_before_query,count_after_query,float(count_before_query-count_after_query)/count_before_query,end_time-start_time))
        count_relations+=1

def findMatchingGTFSbyDate(gtfs_name,path='\\\\mars//gl//Users//asafeh//GTFS//Download//'):
    print gtfs_name
    #print path+gtfs_name
    #print listdir(path)
    all_file_names = [f for f in listdir(path) if isfile(path+f)]
    for x in all_file_names:
        if x==gtfs_name:
            print("Matching GTFS file found: {}".format(x))
            return True
    print('No GTFS files with matching date {} found'.format(gtfs_name))
    return False

def moveSchema(table_name, from_schema, to_schema):
    if (from_schema == to_schema):
        print ('From schema ({}) is same as to schema ({})'.format(from_schema, to_schema))
        return False
    query = "SELECT table_schema FROM information_schema.tables WHERE table_name='{}' ".format(table_name.lower())
    cur = conn.cursor()
    cur.execute(query)
    true_from_schema = cur.fetchall()[0][0]
    if (true_from_schema == to_schema):
        print ('From schema ({}) is same as to schema ({})'.format(true_from_schema, to_schema))
        return False
    query="ALTER TABLE {}.{} SET SCHEMA {};".format(from_schema,table_name,to_schema)
    cur = conn.cursor()
    try:
        cur.execute(query)
        conn.commit()
        print("Table {} moved from schema '{}' to schema '{}'".format(table_name, from_schema, to_schema))
    except:
        print ("Failed To Move Table {}".format(table_name))
        sendEmailWhenCodeFails("Failed to move Table {} from schema '{}' to schema '{}'".format(table_name, from_schema, to_schema))

def restoreGTFSinfoByDate(gtfs_date,path_from='\\\\mars//gl//Users//asafeh//GTFS//Download//',path_to='D://GTFS//gtfs_to_import//'):
    if (os.path.isdir(path_to) == False):
        os.makedirs(path_to)
    gtfs_name="israel-public-transportation-"+gtfs_date[:4]+"-"+str(int(gtfs_date[5:7]))+"-"+str(int(gtfs_date[8:10]))+".zip"
    is_file_already_there=False
    if(findMatchingGTFSbyDate(gtfs_name)):
        all_file_names = [f for f in listdir(path_to) if isfile(join(path_to, f))]
        for x in all_file_names:
            if x == gtfs_name:
                print("Matching GTFS file already in directory: {}".format(x))
                is_file_already_there=True
        if(is_file_already_there==False):
            print("Copying File '{}' From '{}' To '{}'".format(gtfs_name, path_from,path_to))
            copyfile(path_from+gtfs_name, path_to+gtfs_name)
            print("File {} Succesfully copied to path: '{}'".format(gtfs_name, path_to))
            #port, dirname, host, db, user, password, db_type
            #import_gtfs.importGtfsToSQL()
            #subprocess.Popen("run_import_gtfs_to_sql_2.py D:\GTFS\gtfs_to_import localhost postgres postgis_siri 123qwe 5432 gis", shell=True)
            #os.system("run_import_gtfs_to_sql.py D:\GTFS\gtfs_to_import localhost postgres postgis_siri 123qwe 5432 gis")
            with zipfile.ZipFile(path_to+gtfs_name, 'r') as zip_ref:
                zip_ref.extractall(path_to)
                print("File {} unziped in {}".format(gtfs_name,path_to))


def clearGTFSFolder(folder_path="D://GTFS//gtfs_to_import//"):
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))


def sendEmailWhenCodeFails(content_to_send="",success_or_fail=1,):
    import smtplib, ssl
    import datetime
    import socket
    hostname = socket.gethostname()
    IPAddr = socket.gethostbyname(hostname)
    port = 587  # For starttls
    smtp_server = "smtp.gmail.com"
    sender_email = "***"
    receiver_email = "***"
    password = "***"
    if(success_or_fail==1):
        message = "Subject: {}: Process Failed.\n\n Code failed at {}.\n{}".format(IPAddr,datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),content_to_send)
    else:
        message = "Subject: {}: Process completed successfully!.\n\n Code finished at {}.\n{}".format(IPAddr,
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), content_to_send)
    context = ssl.create_default_context()
    server=smtplib.SMTP(smtp_server, port)
    server.ehlo()  # Can be omitted
    server.starttls()
    server.ehlo()  # Can be omitted
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)


def removeNullRowsFromGtfs(table_name,column_name,schema_name='public'):
    query="delete from {}.{} where {} is null;".format(schema_name,table_name,column_name)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

def copyMissingSirisToLocal(path_from="\\\\mars//gl//users//asafeh//siri//Dumps//",path_to="D://Siris//Dumps//"):
    string_to_search = 'siri_sm_res_monitor'
    siris_already_in_output_folder=[f for f in listdir(path_to) if isfile(join(path_to, f)) and f[:len(string_to_search)].lower()==string_to_search]
    siris_not_in_input_folder=[f for f in listdir(path_from) if isfile(join(path_from, f)) and f[:len(string_to_search)].lower()==string_to_search and f not in siris_already_in_output_folder]
    print("siris already in local: {}".format(len(siris_already_in_output_folder)))
    print("siris not yet in local: {}".format(len(siris_not_in_input_folder)))
    for siri in siris_not_in_input_folder:
        print("Copying File '{}' From '{}' To '{}'".format(siri, path_from, path_to))
        copyfile(path_from + siri, path_to + siri)
        print("File {} successfully copied to path: '{}'".format(siri, path_to))

def turnRepository2linkAvgSpeed(street_layer,RepositoryName='Link_Trip_Repository',add_to_name=""):
    min_date='2040-12-31'
    max_date='2000-01-01'
    if isinstance(RepositoryName,list):
        repository_to_proccess='('
        for repository in RepositoryName:
            repository_to_proccess+='(select * from processed_ltrs.{}) union'.format(repository)
            repository_date='{}_{}_{}'.format(repository[15:19],repository[20:22],repository[23:25])
            if repository_date<min_date:
                min_date = repository_date
            if repository_date>max_date:
                max_date = repository_date
        output_isman_name="shp_{}_{}_{}".format(street_layer,min_date,max_date)
        repository_to_proccess = repository_to_proccess[:-5]+')'
    else:
        output_isman_name = "shp_" + street_layer + "_" + str(RepositoryName)
        repository_to_proccess=RepositoryName
    df= pd.read_sql_query("select l.link_id,l.dir,l.from_time_hour, avg(l.siri_speed) as avg_speed, min(l.from_lon) as min_from_lon, \
    max(l.from_lon) as max_from_lon,min(l.from_lat) as min_from_lat, max(l.from_lat) as max_from_lat, min(l.from_time) as min_from_time, \
    max(l.from_time) as max_from_time, stddev(l.siri_speed) as std, count(*) as num_of_samples from {} l, {} m\
    where m.id=l.link_id and ((l.dir = m.dir_transit) or (m.dir_transit=0)) group by l.link_id,l.dir, l.from_time_hour order by l.link_id,l.from_time_hour;".format(repository_to_proccess,street_layer),con=conn)
    pivoted=pd.pivot_table(df, columns=['dir','from_time_hour'], values=['avg_speed','num_of_samples','std'], index='link_id',  fill_value=None, margins=False, dropna=True, margins_name='All')#.to_csv('d:\outputs.csv', sep='\t', encoding='utf-8')
    flattened = pd.DataFrame(pivoted.to_records())
    flattened.columns = [hdr.replace("(","").replace(")", "").replace("'","").replace(", ","_").replace("_-1L_","_BA_").replace("_1L_","_AB_").replace("L","").replace("num_of_samples","nos").replace("avg_speed","spe") for hdr in flattened.columns]
    #flattened.to_csv('d:\gtfs\outputs.csv', sep=',', encoding='utf-8')
    flattened.to_sql(name='link_id_avg_speed_dir_hour',con=engine,if_exists='replace')
    join_query="drop table IF EXISTS "+output_isman_name+add_to_name+"; create table "+output_isman_name+add_to_name+" as select * from " + street_layer + " m left join link_id_avg_speed_dir_hour l on m.id=l.link_id"
    cur = conn.cursor()
    print join_query
    cur.execute(join_query)
    conn.commit()
    print("{} created successfully".format(output_isman_name+add_to_name))

def addDirToLinkTripRepository(street_layer,RepositoryName='link_trip_repository'):#,alsoFailed=False):
    if isinstance(RepositoryName,list):
        for repo in RepositoryName:

            count_before_query='select count(*) from raw_ltrs.{}'.format(repo)
            cur = conn.cursor()
            cur.execute(count_before_query)
            recs_before=cur.fetchall()[0][0]
            #does_exist_query = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE  table_schema = 'processed_ltrs' AND table_name = '{}');".format(repo.lower())
            # print query
            #cur = conn.cursor()
            #cur.execute(does_exist_query)
            #query_feedback = cur.fetchall()
            # print(query_feedback[0][0])
            #if (query_feedback[0][0] == True):
            if (isTableBlank(repo,'processed_ltrs')==True):
                filter_by_dir_query = 'drop table if exists processed_ltrs.{}; create table processed_ltrs.{} as (select l.* from {} m, raw_ltrs.{} l where m.id=l.link_id and ((l.dir = m.dir_transit) or (m.dir_transit=0)))' \
                    .format(repo, repo, street_layer, repo)
                # print filter_by_dir_query
                cur = conn.cursor()
                cur.execute(filter_by_dir_query)
                conn.commit()
                count_after_query = 'select count(*) from processed_ltrs.{}'.format(repo)
                cur = conn.cursor()
                cur.execute(count_after_query)
                recs_after = cur.fetchall()[0][0]
                print(
                    "LTR {} processed. Total records before: {:,}\nTotal records after:{:,}\nTotal records removed:{:,} -> {:.2%}".format(
                        repo, recs_before, recs_after, recs_before - recs_after,
                        ((float(recs_before - recs_after)) / recs_before)))
            #else:
            #    filter_by_dir_query='drop table if exists processed_ltrs.{}; create table processed_ltrs.{} as (select l.* from {} m, raw_ltrs.{} l where m.id=l.link_id and ((l.dir = m.dir_transit) or (m.dir_transit=0)))'\
            #        .format(repo,repo,street_layer,repo)
            #    #print filter_by_dir_query
            #    cur = conn.cursor()
            #    cur.execute(filter_by_dir_query)
            #    conn.commit()
            #    count_after_query='select count(*) from processed_ltrs.{}'.format(repo)
            #    cur = conn.cursor()
            #    cur.execute(count_after_query)
            #    recs_after = cur.fetchall()[0][0]
            #    print("LTR {} processed. Total records before: {:,}\nTotal records after:{:,}\nTotal records removed:{:,} -> {:.2%}".format(repo,recs_before,recs_after,recs_before-recs_after,((float(recs_before-recs_after))/recs_before)))
    else:
        count_before_query='select count(*) from raw_ltrs.{}'.format(RepositoryName)
        cur = conn.cursor()
        cur.execute(count_before_query)
        recs_before=cur.fetchall()[0][0]
        filter_by_dir_query='drop table if exists processed_ltrs.{}; create table processed_ltrs.{} as (select l.* from {} m, raw_ltrs.{} l where m.id=l.link_id and ((l.dir = m.dir_transit) or (m.dir_transit=0)))'\
            .format(RepositoryName,RepositoryName,street_layer,RepositoryName)
        print filter_by_dir_query
        cur = conn.cursor()
        cur.execute(filter_by_dir_query)
        conn.commit()
        count_after_query='select count(*) from processed_ltrs.{}'.format(RepositoryName)
        cur = conn.cursor()
        cur.execute(count_after_query)
        recs_after = cur.fetchall()[0][0]
        if(recs_before==0):
            print ("No records in DB.")
        else:
            if(recs_after==0):
                print("No records were removed while cleaning. Total records: {:,}".format(recs_before))
            else:
                print("Total records before: {:,}\nTotal records after:{:,}\nTotal records removed:{:,} -> {:.2%}".format(recs_before,recs_after,recs_before-recs_after,((float(recs_before-recs_after))/recs_before)))
#    if(alsoFailed):
#        query='drop table if exists {}_failed; create table {}_failed as (select l.* from {} m, {} l where m.id=l.link_id and ((l.dir != m.dir_transit) and (m.dir_transit!=0)))'.format(
#            repository_name, repository_name, street_layer, repository_name)
#        cur = conn.cursor()
#       cur.execute(query)
#        conn.commit()

def getAllDbsWithStringInName(string_to_search,schema_to_search_in='public',from_date='2000-01-01',to_date='2040-12-31',date_filter_type=4):
    relevant_dates=getRelevantDatesString(from_date,to_date,date_filter_type,1)
    query="SELECT table_name FROM information_schema.tables WHERE table_schema='{}' and left(table_name,{})='{}'".format(schema_to_search_in,len(string_to_search),string_to_search)
    #print query
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "Can't retrieve DBs with string in name"
    rows=cur.fetchall()
    results = [r[0] for r in rows]
    results_in_dates=[]
    for result in results:
        result_date=datetime.strptime("{}-{}-{}".format(result[-10:-6],result[-5:-3],result[-2:]),'%Y-%m-%d').date()
        if(result_date in relevant_dates):
            results_in_dates.append(result)
    #print sorted(results_in_dates)
    return sorted(results_in_dates)

def dropSirisThatHaveBeenCleaned(string_to_search='siri_sm_res_monitor',raw_siris_schema_name='raw_siris',processed_siris_schema_name='processed_siris'):
    query_for_raw_siris = "SELECT table_name FROM information_schema.tables WHERE table_schema='{}' and left(table_name,{})='{}'".format(raw_siris_schema_name, len(string_to_search), string_to_search)
    query_for_processed_siris = "SELECT table_name FROM information_schema.tables WHERE table_schema='{}' and left(table_name,{})='{}'".format(processed_siris_schema_name, len(string_to_search), string_to_search)
    try:
        cur = conn.cursor()
        cur.execute(query_for_raw_siris)
        rows=cur.fetchall()
        raw_siris = [r[0] for r in rows]
    except:
        raw_siris=[]
        print query_for_raw_siris
        print "Can't retrieve raw DBs"
    try:
        cur = conn.cursor()
        cur.execute(query_for_processed_siris)
        rows=cur.fetchall()
        processed_siris = [r[0] for r in rows]
    except:
        print query_for_raw_siris
        print "Can't retrieve raw DBs that were cleaned"
    for siri in raw_siris:
        query="SELECT EXISTS (SELECT relname FROM pg_class WHERE relname = '"+siri.lower()+"_processed');"
        #print query
        cur = conn.cursor()
        cur.execute(query)
        query_feedback = cur.fetchall()
        #print(query_feedback[0][0])
        if(query_feedback[0][0]==True):
            try:
                cur = conn.cursor()
                cur.execute("drop table {}.{} cascade;".format(raw_siris_schema_name,siri))
                conn.commit()
                #print("drop table {}.{} cascade;".format(raw_siris_schema_name,siri))
                print ("Table {} was dropped successfully.".format(siri))
            except:
                print("Can't drop table {}".format(siri))


def dropLtrsThatHaveBeenCleaned(string_to_search='ltr_everything',raw_siris_schema_name='raw_ltrs',processed_siris_schema_name='processed_ltrs'):
    query_for_raw_siris = "SELECT table_name FROM information_schema.tables WHERE table_schema='{}' and left(table_name,{})='{}'".format(raw_siris_schema_name, len(string_to_search), string_to_search)
    query_for_processed_siris = "SELECT table_name FROM information_schema.tables WHERE table_schema='{}' and left(table_name,{})='{}'".format(processed_siris_schema_name, len(string_to_search), string_to_search)
    try:
        cur = conn.cursor()
        cur.execute(query_for_raw_siris)
        rows=cur.fetchall()
        raw_ltrs = [r[0] for r in rows]
    except:
        print query_for_raw_siris
        print "Can't retrieve raw DBs"
    try:
        cur = conn.cursor()
        cur.execute(query_for_processed_siris)
        rows=cur.fetchall()
        processed_siris = [r[0] for r in rows]
    except:
        print query_for_raw_siris
        print "Can't retrieve raw DBs that were cleaned"
    for siri in raw_ltrs:
        query="SELECT EXISTS (SELECT relname FROM pg_class WHERE relname = '"+siri.lower()+"');"
        #print query
        cur = conn.cursor()
        cur.execute(query)
        query_feedback = cur.fetchall()
        #print(query_feedback[0][0])
        if(query_feedback[0][0]==True):
            try:
                cur = conn.cursor()
                print (1)
                pg_dump(siri,'D:\export\ltrs//raw_ltrs//','raw_ltrs')
                print (2)
                cur.execute("drop table {}.{} cascade;".format(raw_siris_schema_name,siri))
                print (3)
                conn.commit()
                print (4)
                #print("drop table {}.{} cascade;".format(raw_siris_schema_name,siri))
                print ("Table {} was dropped successfully.".format(siri))
            except:
                print("Can't drop table {}".format(siri))

def turnRepository2linkAvgSpeed(street_layer,RepositoryName='Link_Trip_Repository',filter_type=2,output_layer_name_begining='shp',to_shp=False,to_csv=False):
    min_date=datetime.strptime('2040-12-31', '%Y-%m-%d').date()
    max_date=datetime.strptime('2000-01-01', '%Y-%m-%d').date()
    if isinstance(RepositoryName,list):
        repository_to_proccess='('
        for repository in RepositoryName:
            time_of_repository=datetime.strptime(repository[-10:], '%Y_%m_%d').date()
            min_date=min(min_date,time_of_repository)
            max_date=max(max_date,time_of_repository)
        dates_by_filter_type = getRelevantDatesString(datetime.strftime(min_date,'%Y-%m-%d'), datetime.strftime(max_date,'%Y-%m-%d'), filter_type, 1)
        for repository_again in RepositoryName:
            time_of_repository=datetime.strptime(repository_again[-10:], '%Y_%m_%d').date()
            if(time_of_repository in dates_by_filter_type):
                repository_to_proccess+='(select * from processed_ltrs.{}) union'.format(repository_again)
        repository_to_proccess = repository_to_proccess[:-5]+')'
        print repository_to_proccess
        if(repository_to_proccess==')'):
            print("No Siris found for dates {} and {}".format(min_date,max_date))
            raise ValueError
        output_isman_name = "sn_{}to{}".format(min_date,max_date).replace('-','_')
        df= pd.read_sql_query("select l.link_id,l.dir,l.from_time_hour, avg(l.siri_speed) as avg_speed, min(l.from_lon) as min_from_lon, \
        max(l.from_lon) as max_from_lon,min(l.from_lat) as min_from_lat, max(l.from_lat) as max_from_lat, min(l.from_time) as min_from_time, \
        max(l.from_time) as max_from_time, stddev(l.siri_speed) as std, count(l.siri_speed) as num_of_samples from {} l, {} m\
        where m.id=l.link_id and ((l.dir = m.dir_transit) or (m.dir_transit=0)) group by l.link_id,l.dir, l.from_time_hour order by l.link_id,l.from_time_hour;"
                              .format(repository_to_proccess,street_layer),con=conn)
    else:
        repository_to_proccess=RepositoryName
        output_isman_name = "{}_{}_{}".format(output_layer_name_begining,street_layer,RepositoryName)
        df= pd.read_sql_query("select l.link_id,l.dir,l.from_time_hour, avg(l.siri_speed) as avg_speed, min(l.from_lon) as min_from_lon, \
        max(l.from_lon) as max_from_lon,min(l.from_lat) as min_from_lat, max(l.from_lat) as max_from_lat, min(l.from_time) as min_from_time, \
        max(l.from_time) as max_from_time, stddev(l.siri_speed) as std, count(l.siri_speed) as num_of_samples from processed_ltrs.{} l, {} m\
        where m.id=l.link_id and ((l.dir = m.dir_transit) or (m.dir_transit=0)) group by l.link_id,l.dir, l.from_time_hour order by l.link_id,l.from_time_hour;"
                              .format(repository_to_proccess,street_layer),con=conn)

    pivoted=pd.pivot_table(df, columns=['dir','from_time_hour'], values=['avg_speed','num_of_samples','std'], index='link_id',  fill_value=None, margins=False, dropna=True, margins_name='All')#.to_csv('d:\outputs.csv', sep='\t', encoding='utf-8')
    flattened = pd.DataFrame(pivoted.to_records())
    flattened.columns = [hdr.replace("(","").replace(")", "").replace("'","").replace(", ","_").replace("_-1L_","_BA_").replace("_1L_","_AB_").replace("L","").replace("num_of_samples","nos").replace("avg_speed","spe") for hdr in flattened.columns]
    #flattened.to_csv('d:\gtfs\outputs.csv', sep=',', encoding='utf-8')
    flattened.to_sql(name='link_id_avg_speed_dir_hour',con=engine,if_exists='replace')
    daily_nos_AB_columns_query="SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'link_id_avg_speed_dir_hour'  and left(column_name,6)='nos_AB'"
    cur = conn.cursor()
    cur.execute(daily_nos_AB_columns_query)
    result = cur.fetchall()
    daily_nos_AB_columns_query_feedback = [r[0] for r in result]
    daily_nos_AB_columns_string=stringQueryResultsAsList(daily_nos_AB_columns_query_feedback,'coalesce(l."','",0)',"+")
    daily_nos_BA_columns_query="SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = 'link_id_avg_speed_dir_hour'  and left(column_name,6)='nos_BA'"
    cur = conn.cursor()
    cur.execute(daily_nos_BA_columns_query)
    result = cur.fetchall()
    daily_nos_BA_columns_query_feedback = [r[0] for r in result]
    daily_nos_BA_columns_string=stringQueryResultsAsList(daily_nos_BA_columns_query_feedback,'coalesce(l."','",0)',"+")
    print (daily_nos_BA_columns_string)
    join_query="drop table IF EXISTS processed_daily_layers.{}; create table processed_daily_layers.{} as " \
        "select *, {} as d_nos_AB, {} as d_nos_BA from {} m left join link_id_avg_speed_dir_hour l on m.id=l.link_id"\
        .format(output_isman_name,output_isman_name,daily_nos_AB_columns_string,daily_nos_BA_columns_string,street_layer)
    #print join_query
    cur = conn.cursor()
    cur.execute(join_query)
    conn.commit()
    print("{} created successfully.".format(output_isman_name))
    if(to_csv):
        exportDb2Csv(output_isman_name)
    if(to_shp):
        exportDb2Shp(output_isman_name)
    return output_isman_name

def turnRepository2linkAvgSpeedWithModifiedIsMan(street_layer_name,ltr_name='Link_Trip_Repository',filter_type=4,join_with_street_layer=False,schema_name="processed_ltrs"):
    min_date=datetime.strptime('2040-12-31', '%Y-%m-%d').date()
    max_date=datetime.strptime('2000-01-01', '%Y-%m-%d').date()
    if isinstance(ltr_name,list):

        repository_to_proccess='('
        for repository in ltr_name:
            time_of_repository=datetime.strptime(repository[-10:], '%Y_%m_%d').date()
            min_date=min(min_date,time_of_repository)
            max_date=max(max_date,time_of_repository)
        dates_by_filter_type = getRelevantDatesString(datetime.strftime(min_date,'%Y-%m-%d'), datetime.strftime(max_date,'%Y-%m-%d'), filter_type, 1)
        for repository_again in ltr_name:
            time_of_repository=datetime.strptime(repository_again[-10:], '%Y_%m_%d').date()
            if(time_of_repository in dates_by_filter_type):
                repository_to_proccess+='(select * from processed_ltrs.{}) union'.format(repository_again)
        repository_to_proccess = repository_to_proccess[:-5]+')'
        if join_with_street_layer:
            output_table_name = "shp_{}_{}_{}".format(street_layer_name,min_date,max_date).replace('-','_')
        else:
            output_table_name='dsn_{}_{}'.format(min_date,max_date).replace('-','_')
        df = pd.read_sql_query("select concat(l.link_id,'_',case when l.dir='1' then 'AB' when l.dir='-1' then 'BA' end) as new_id, l.from_time_hour, avg(l.siri_speed) as avg_speed, min(l.from_lon) as min_from_lon, max(l.from_lon)\
        as max_from_lon,min(l.from_lat) as min_from_lat, max(l.from_lat) as max_from_lat, min(l.from_time) as min_from_time,\
        max(l.from_time) as max_from_time, count(*) as num_of_samples from {} l, {} m \
        where concat(l.link_id,'_',case when l.dir='1' then 'AB' when l.dir='-1' then 'BA' end)=m.id_with_topology group by concat(l.link_id,'_',case when l.dir='1' then 'AB' when l.dir='-1' then 'BA' end) , l.from_time_hour "
                               "order by new_id,l.from_time_hour;"
                               .format(repository_to_proccess,street_layer_name), con=conn)
    else:
        repository_to_proccess=ltr_name
        time_of_repository = datetime.strptime(repository_to_proccess[-10:], '%Y_%m_%d').date()
        if join_with_street_layer:
            output_table_name = "shp_" + street_layer_name + "_" + str(ltr_name)
        else:
            output_table_name= 'dsn_{}'.format(time_of_repository).replace('-','_')
        df = pd.read_sql_query("select concat(l.link_id,'_',case when l.dir='1' then 'AB' when l.dir='-1' then 'BA' end) as new_id, l.from_time_hour, avg(l.siri_speed) as avg_speed, min(l.from_lon) as min_from_lon, max(l.from_lon)\
        as max_from_lon,min(l.from_lat) as min_from_lat, max(l.from_lat) as max_from_lat, min(l.from_time) as min_from_time,\
        max(l.from_time) as max_from_time, count(*) as num_of_samples from {}.{} l, {} m \
        where concat(l.link_id,'_',case when l.dir='1' then 'AB' when l.dir='-1' then 'BA' end)=m.id_with_topology group by concat(l.link_id,'_',case when l.dir='1' then 'AB' when l.dir='-1' then 'BA' end) , l.from_time_hour \
        order by new_id,l.from_time_hour;".format(schema_name,repository_to_proccess,street_layer_name), con=conn)
    pivoted = pd.pivot_table(df, columns=['from_time_hour'], values=['avg_speed', 'num_of_samples'],
                             index='new_id', fill_value=None, margins=False, dropna=True,
                             margins_name='All')  # .to_csv('d:\outputs.csv', sep='\t', encoding='utf-8')
    flattened = pd.DataFrame(pivoted.to_records())
    print (flattened.columns)
    # flattened.columns = [hdr.replace("(", "").replace(")", "").replace("'", "").replace(", ", "_").replace("_-1L_", "_BA_").replace("_1L_","_AB_").replace(        "L", "").replace("num_of_samples", "nos").replace("avg_speed", "spe") for hdr in flattened.columns]
    flattened.columns = [
        hdr.replace("(", "").replace(")", "").replace("'", "").replace(", ", "_").replace("num_of_samples","nos")
            .replace("avg_speed", "spe").replace("L", "") for hdr in flattened.columns]
    # flattened.to_csv('d:\gtfs\outputs.csv', sep=',', encoding='utf-8')
    flattened.to_sql(name='link_id_avg_speed_dir_hour', con=engine, if_exists='replace')
    for i in range(0,24):
        buildAllColumnsQuery="alter table public.link_id_avg_speed_dir_hour add column if not exists spe_{} int;" \
                             "alter table public.link_id_avg_speed_dir_hour add column if not exists nos_{} int;".format(i,i)
        cur = conn.cursor()
        cur.execute(buildAllColumnsQuery)
        conn.commit()

    if join_with_street_layer:
        join_query = "drop table IF EXISTS processed_daily_layers.{}; create table processed_daily_layers.{} as select m.*," \
                     "l.new_id,l.spe_0,l.spe_1,l.spe_2,l.spe_3,l.spe_4,l.spe_5,l.spe_6,l.spe_7,l.spe_8,l.spe_9,l.spe_10,l.spe_11,l.spe_12,l.spe_13,l.spe_14,l.spe_15,l.spe_16,l.spe_17,l.spe_18,l.spe_19,l.spe_20,l.spe_21,l.spe_22,l.spe_23,l.nos_0,l.nos_1,l.nos_2,l.nos_3,l.nos_4,l.nos_5,l.nos_6,l.nos_7,l.nos_8,l.nos_9,l.nos_10,l.nos_11,l.nos_12,l.nos_13,l.nos_14,l.nos_15,l.nos_16,l.nos_17,l.nos_18,l.nos_19,l.nos_20,l.nos_21,l.nos_22,l.nos_23" \
                     " from {}  m left join link_id_avg_speed_dir_hour l on l.new_id=m.id_with_topology;  create index {}_idx on processed_daily_layers.{} (id_with_topology)".format(output_table_name, output_table_name, street_layer_name,output_table_name,output_table_name)
        #print join_query
        cur = conn.cursor()
        cur.execute(join_query)
        conn.commit()
        print("{} created successfully".format(output_table_name))
        return output_table_name
    else:
        export_query="drop table IF EXISTS processed_daily_layers.{}; create table processed_daily_layers.{} as select m.id_with_topology,l.spe_0,l.spe_1,l.spe_2,l.spe_3,l.spe_4,l.spe_5,l.spe_6,l.spe_7,l.spe_8,l.spe_9,l.spe_10,l.spe_11,l.spe_12,l.spe_13,l.spe_14,l.spe_15,l.spe_16,l.spe_17,l.spe_18,l.spe_19,l.spe_20,l.spe_21,l.spe_22,l.spe_23,l.nos_0,l.nos_1,l.nos_2,l.nos_3,l.nos_4,l.nos_5,l.nos_6,l.nos_7,l.nos_8,l.nos_9,l.nos_10,l.nos_11,l.nos_12,l.nos_13,l.nos_14,l.nos_15,l.nos_16,l.nos_17,l.nos_18,l.nos_19,l.nos_20,l.nos_21,l.nos_22,l.nos_23" \
                     " from {} m left join link_id_avg_speed_dir_hour l on l.new_id=m.id_with_topology; create index {}_idx on processed_daily_layers.{} (id_with_topology)".format(output_table_name, output_table_name,street_layer_name,output_table_name,output_table_name)
        cur = conn.cursor()
        cur.execute(export_query)
        conn.commit()
        print("{} created successfully".format(output_table_name))
        return output_table_name

def pg_dump(siriArchiveName, dir_name, schema_name='processed_daily_layers',db='postgis_siri',port=5432, host='localhost', user='postgres', password='123qwe'):
    start_time=datetime.now()
    os.chdir(dir_name)
    setenv_command = "set PGPASSWORD={}".format(password)
    pg_dump_cmd = "pg_dump -Fc -h {} -p {} -U {} -d {} -t {}.{} > {}.dump".format(host,port,user,db,schema_name,siriArchiveName,siriArchiveName)
    print "Dumping db {} to {}".format(siriArchiveName,dir_name)
    #print pg_dump_cmd
    os.system(setenv_command + "&" + pg_dump_cmd)
    print "Dumping successfull. Dump {} created at {}. Time:{}".format(siriArchiveName,dir_name,datetime.now()-start_time)


def turn_twoways_to_oneway(street_layer_name,offset='0.0001'):
    start_time=datetime.now()
    query="drop table if exists {}_modified; create table {}_modified as (select id, concat(id,'_','AB') as " \
          "id_with_topology, geom, geom_line, ST_AsGeoJSON(geom) as geom_json,ST_AsGeoJSON(geom_line) as geom_line_json," \
          "length,dir,street,fromleft,toleft,fromright,toright,streetcode,objid,userid," \
          "fjunction,tjunction,length1,roadtype,oneway,f_zlev,t_zlev,flanes,tlanes,intended,cityname,citycode,seconds," \
          "fromspeedl,tospeedl,aprxspeedl,autonomy,regulation,roadnamemz,directionm,status,roadfuncti,flag,identifier," \
          "roaddirect,clearstree,reverstree,clearcityn,revercityn,from_main_id,to_main_id,from_id,to_id,dir_transit," \
          "dir_transit as new_dir_transit from {} where dir_transit='1') union (select id, concat(id,'_','BA') as" \
          " id_with_topology, geom, geom_line, ST_AsGeoJSON(geom) as geom_json, ST_AsGeoJSON(geom_line) as geom_line_json," \
          "length,dir,street,fromleft,toleft,fromright,toright,streetcode,objid,userid," \
          "fjunction,tjunction,length1,roadtype,oneway,f_zlev,t_zlev,flanes,tlanes,intended,cityname,citycode,seconds," \
          "fromspeedl,tospeedl,aprxspeedl,autonomy,regulation,roadnamemz,directionm,status,roadfuncti,flag,identifier," \
          "roaddirect,clearstree,reverstree,clearcityn,revercityn,from_main_id,to_main_id,from_id,to_id,dir_transit," \
          "dir_transit as new_dir_transit from {} where dir_transit='-1') union (select id, concat(id,'_','AB') as" \
          " id_with_topology, ST_Reverse(ST_OffsetCurve(geom, -{})) as geom, ST_Reverse(ST_OffsetCurve(geom_line, -{}))" \
          " as geom_line, " \
          "ST_AsGeoJSON(ST_Reverse(ST_OffsetCurve(geom, -{}))) as geom_json," \
          "ST_AsGeoJSON(ST_Reverse(ST_OffsetCurve(geom_line, -{}))) as geom_line_json," \
          "length,dir,street,fromleft,toleft,fromright,toright,streetcode,objid,userid,fjunction," \
          "tjunction,length1,roadtype,oneway,f_zlev,t_zlev,flanes,tlanes,intended,cityname,citycode,seconds,fromspeedl," \
          "tospeedl,aprxspeedl,autonomy,regulation,roadnamemz,directionm,status,roadfuncti,flag,identifier,roaddirect," \
          "clearstree,reverstree,clearcityn,revercityn,from_main_id,to_main_id,from_id,to_id,dir_transit," \
          "1 as new_dir_transit from {} where dir_transit='0') union (select id, concat(id,'_','BA') as id_with_topology," \
          " ST_Reverse(ST_OffsetCurve(geom, {})) as geom, ST_Reverse(ST_OffsetCurve(geom_line, {})) as geom_line, " \
          "ST_AsGeoJSON(ST_Reverse(ST_OffsetCurve(geom, {}))) as geom_json," \
          "ST_AsGeoJSON(ST_Reverse(ST_OffsetCurve(geom_line, {}))) as geom_line_json," \
          "length,dir,street,toright as fromleft," \
          "fromright as toleft,toleft as fromright,fromleft as toright,streetcode,objid,userid,tjunction as fjunction," \
          " fjunction as tjunction,length1,roadtype,oneway,t_zlev as f_zlev,f_zlev as t_zlev,tlanes as flanes,flanes as" \
          " tlanes,intended,cityname,citycode,seconds,tospeedl as fromspeedl,fromspeedl as tospeedl,aprxspeedl,autonomy," \
          "regulation,roadnamemz,directionm,status,roadfuncti,flag,identifier,roaddirect,clearstree,reverstree,clearcityn," \
          "revercityn,to_main_id as from_main_id,from_main_id as to_main_id,to_id as from_id,from_id as to_id,dir_transit," \
          "1 as new_dir_transit from {} where dir_transit='0')"\
        .format(street_layer_name,street_layer_name,street_layer_name,street_layer_name,offset,offset,offset,offset,street_layer_name,offset,offset,offset,offset,street_layer_name)
    print query
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    print ("Layer {}_modified created successfully. Time taken:{}".format(street_layer_name,datetime.now()-start_time))


def dump_ltrs_by_date(from_date, to_date, schema_name='processed_ltrs',filter_type=4, output_path='d://export//ltr_dumps//'):
    # filter 1: mon-wed, filter 2: sun-thu, filter 3: thu, filter 4:sun-sat, 5:fri-sat, 6:tue-tue
    ltrsInDB=getAllSirisInDb(schema_name,'ltr_everything')
    #print ltrsInDB
    dates_range=getRelevantDatesString(from_date,to_date,filter_type,1)
    #print dates_range
    ltrs_to_export=[]
    for date in dates_range:
        date=datetime.strftime(date, '%Y-%m-%d')
        found_for_date = False
        table_name='ltr_everything_{}'.format(date.replace("-","_"))
        for ltr in ltrsInDB:
            if(ltr==table_name):
                found_for_date = True
                ltrs_to_export.append(ltr)
                print ("Date {} : LTR found: {}".format(date, ltr))
                break
        if(found_for_date==False):
            print ("Date {} : LTR not found!".format(date))
    if(os.path.isdir(output_path)==False):
        os.mkdir(output_path)
    for ltr in ltrs_to_export:
        pg_dump(ltr,output_path,schema_name)

def restore_ltrs_by_date(from_date,to_date,schema_name='processed_ltrs',filter_type=4,input_path='D://import//ltrs_dumps//',string_to_search='ltr_everything'):
    if(os.path.isdir(input_path)==False):
        os.mkdir(input_path)
    dates_range=getRelevantDatesString(from_date,to_date,filter_type,1)
    ltrs_to_import=[]
    dumps_list = [f for f in listdir(input_path) if isfile(join(input_path, f)) and f[:len(string_to_search)].lower()==string_to_search]
    for date in dates_range:
        is_ltr_found=False
        date=datetime.strftime(date, '%Y-%m-%d')
        table_name='ltr_everything_{}'.format(date.replace("-","_"))
        #print table_name
        for dump in dumps_list:
            #print dump
            if(dump[:len(table_name)]==table_name):
                is_ltr_found=True
                ltrs_to_import.append(dump)
                print ("Date {} : LTR found: {}".format(date, dump))
                break
        if(is_ltr_found==False):
            print("Date {} : LTR not found!".format(date))
    print("{} dumps found between dates {} and {}.".format(len(ltrs_to_import),from_date,to_date))
    if (len(ltrs_to_import) == 0):
        return 0
    dumps_count=0
    for dump in ltrs_to_import:
        dumps_count+=1
        in_processed_query = "SELECT EXISTS (SELECT relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname='{}' and relname = '{}');".format(schema_name,dump[:-5].lower())
        cur = conn.cursor()
        cur.execute(in_processed_query)
        in_processed_query_feedback = cur.fetchall()
        if (in_processed_query_feedback[0][0] == False):
            in_raw_query="SELECT EXISTS (SELECT relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE nspname='raw_ltrs' and relname = '"+dump[:-5].lower()+"');"
            #print query
            cur = conn.cursor()
            cur.execute(in_raw_query)
            in_raw_query_feedback = cur.fetchall()
            #print(query_feedback[0][0])
            if(in_raw_query_feedback[0][0]==False):
                print ("restoring ltr {} of {}: {}".format(dumps_count,len(ltrs_to_import),dump))
                start_time=datetime.now()
                command="pg_restore -h localhost -p 5432 -U postgres -d postgis_siri -Fc -j 8  {}".format(input_path+dump)
                #print command
                try:
                    os.system(command)
                    print ('Restoring successfull. Time: {}'.format(datetime.now()-start_time))
                except:
                    print(command)
                    print("Unable to restore dump {}".format(dump))
                #moveSchema(dump[:-5],'public',schema_name)
            else:
                print('Dump {} already in raw DB.'.format(dump))
        else:
            print('Dump {} already in processed DB.'.format(dump))

def getAllSirisInDb(schema_name='public',string_to_search='siri_sm_res_monitor'):
    query="SELECT table_name FROM information_schema.tables WHERE table_schema='{}' and left(table_name,{})='{}'".format(schema_name,len(string_to_search),string_to_search)
    try:
        cur = conn.cursor()
        cur.execute(query)
    except:
        print query
        print "Can't retrieve siris from db"
    rows = cur.fetchall()
    result = [r[0] for r in rows]
    return sorted(result)

def dropGtfsTables(schema_to_clear="public"):
    print("starting")
    gtfs_files_list=getAllSirisInDb(schema_to_clear,'gtfs_')
    print("finishing")
    if(len(gtfs_files_list)>0):
        for gtfs in gtfs_files_list:
            query = "drop table {}.{} cascade".format(schema_to_clear,gtfs)
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            print("Table {}.{} dropped.".format(schema_to_clear,gtfs))

    else:
        print("No GTFS files found in schema {}".format(schema_to_clear))



def dropTable(table_name,schema_name):
    query="drop table {}.{} cascade;".format(schema_name,table_name)
    print (query)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    print ('Table {}.{} dropped.'.format(schema_name,table_name))

def dropSirisByDate(from_date,to_date,schema_name='raw_siris',string_to_search='siri_sm_res_monitor'):
    siris_list=getAllSirisInDb(schema_name,string_to_search)
    siris_between_dates_list=[]
    if(len(siris_list)>0):
        for siri in siris_list:
            print (siri)
            #siri=siri[0]
            tableFrom="{}-{}-{}".format(siri[20:24],siri[24:26],siri[26:28])
            tableTo ="{}-{}-{}".format(siri[32:36],siri[36:38],siri[38:40])
            print (tableFrom, tableTo)
            if(from_date<=tableFrom<=to_date and from_date<=tableTo<=to_date):
                siris_between_dates_list.append(siri)
        if(len(siris_between_dates_list)>0):
            print ("dropping siris between {} and {} from Db".format(from_date, to_date))
            for siri in siris_between_dates_list:
                print("dropping siri {}".format(siri))
                query = "drop table {}.{} cascade".format(schema_name,siri)
                try:
                    cur = conn.cursor()
                    cur.execute(query)
                    conn.commit()
                    print "table {}  dropped from db.".format(siri)
                except:
                    print query
                    print "Can't drop table {} from db.".format(siri)
        else:
            print("No siris to drop between {} and {}".format(from_date,to_date))
    else:
        print("No siris to drop.")

def exportDb2Csv(dbName,mypath='D://export//csv//',schema_name='processed_daily_layers'):
    if(os.path.isdir(mypath+dbName)==False):
        os.mkdir(mypath+dbName)
    to_csv_query = "COPY (SELECT * from {}.{}) TO '{}//{}.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8'".format(schema_name,
        dbName,mypath+dbName,dbName)
    cur = conn.cursor()
    cur.execute(to_csv_query)
    print('CSV {} created successfully. Location: {}'.format(dbName,mypath))

def exportDb2Shp(dbName,mypath='D://export//shp//',schema_name='processed_daily_layers'):
    if(os.path.isdir(mypath+dbName)==False):
        os.mkdir(mypath+dbName)
    setenv_command = "set pgclientencoding=WIN1255"
    #print pg_dump_cmd
    command='pgsql2shp -f {}//{}.shp -h localhost -u postgres -P 123qwe postgis_siri "select * from {}.{}'.format(mypath+dbName,dbName,schema_name,dbName)
    print command
    os.system(setenv_command + "&" + command)
    print('SHP {} created successfully. Location: {}'.format(dbName,mypath))

def exportDb2ShpV2(dbName,mypath='D://export//shp//',schema_name='processed_daily_layers',export_name=None):
    if(export_name==None):
        export_name=dbName
    if(os.path.isdir(mypath+export_name)==False):
        os.mkdir(mypath+export_name)
    setenv_command = "set pgclientencoding=WIN1255"
    #print pg_dump_cmd
    command='pgsql2shp -f {}//{}.shp -h localhost -u postgres -P 123qwe postgis_siri "select * from {}.{}'.format(mypath+export_name,export_name,schema_name,dbName)
    print command
    os.system(setenv_command + "&" + command)
    print('SHP {} created successfully. Location: {}'.format(export_name,mypath))


def createComperativeIsManLayer(before_db,after_db,newDbName, to_shp=False, to_csv=False, schema_name='processed_ltrs'):
    if(before_db[:2]=='sn' and after_db[:2]=='sn'):
            first_from_date=before_db[3:13]
            first_to_date=before_db[-10:]
            second_from_date=after_db[3:13]
            second_to_date = after_db[-10:]
            print(first_from_date, first_to_date, second_from_date, second_to_date)
            newDbName='csn_{}to{}vs{}to{}'.format(first_from_date,first_to_date,second_from_date,second_to_date)
    query = 'drop table if exists {}.{};\
    create table {}.{} as select b.id,b.geom,b.length,b.dir,b.street,b.fromleft,b.toleft,b.fromright,b.toright,' \
    'b.streetcode,b.objid,b.userid,b.fjunction,b.tjunction,b.length1,b.roadtype,b.oneway,b.f_zlev,b.t_zlev,' \
    'b.flanes,b.tlanes,b.intended,b.cityname,b.citycode,b.seconds,b.fromspeedl,b.tospeedl,b.aprxspeedl' \
    ',b.autonomy,b.regulation,b.roadnamemz,b.directionm,b.status,b.roadfuncti,b.flag,b.identifier,b.roaddirect,' \
    'b.clearstree,b.reverstree,b.clearcityn,b.revercityn,b.from_main_id,b.to_main_id,b.from_id,b.to_id,' \
    ' b."d_nos_ab" as bef_dns_AB,b."d_nos_ba" as bef_dns_BA,a."d_nos_ab" as aft_dns_AB, a."d_nos_ba" as aft_dns_BA,' \
    ' b."spe_AB_5" as AB_5_bef, a."spe_AB_5" as AB_5_aft, a."spe_AB_5"-b."spe_AB_5" as AB_5_dif, a."spe_AB_5"/b."spe_AB_5" as AB_5_rel,' \
    ' b."spe_BA_5" as BA_5_bef, a."spe_BA_5" as BA_5_aft, a."spe_BA_5"-b."spe_BA_5" as BA_5_dif, a."spe_BA_5"/b."spe_BA_5" as BA_5_rel,' \
    ' b."spe_AB_6" as AB_6_bef, a."spe_AB_6" as AB_6_aft, a."spe_AB_6"-b."spe_AB_6" as AB_6_dif, a."spe_AB_6"/b."spe_AB_6" as AB_6_rel,' \
    ' b."spe_BA_6" as BA_6_bef, a."spe_BA_6" as BA_6_aft, a."spe_BA_6"-b."spe_BA_6" as BA_6_dif, a."spe_BA_6"/b."spe_BA_6" as BA_6_rel,' \
    ' b."spe_AB_7" as AB_7_bef, a."spe_AB_7" as AB_7_aft, a."spe_AB_7"-b."spe_AB_7" as AB_7_dif, a."spe_AB_7"/b."spe_AB_7" as AB_7_rel,' \
    ' b."spe_BA_7" as BA_7_bef, a."spe_BA_7" as BA_7_aft, a."spe_BA_7"-b."spe_BA_7" as BA_7_dif, a."spe_BA_7"/b."spe_BA_7" as BA_7_rel,' \
    ' b."spe_AB_8" as AB_8_bef, a."spe_AB_8" as AB_8_aft, a."spe_AB_8"-b."spe_AB_8" as AB_8_dif, a."spe_AB_8"/b."spe_AB_8" as AB_8_rel,' \
    ' b."spe_BA_8" as BA_8_bef, a."spe_BA_8" as BA_8_aft, a."spe_BA_8"-b."spe_BA_8" as BA_8_dif, a."spe_BA_8"/b."spe_BA_8" as BA_8_rel,' \
    ' b."spe_AB_9" as AB_9_bef, a."spe_AB_9" as AB_9_aft, a."spe_AB_9"-b."spe_AB_9" as AB_9_dif, a."spe_AB_9"/b."spe_AB_9" as AB_9_rel,' \
    ' b."spe_BA_9" as BA_9_bef, a."spe_BA_9" as BA_9_aft, a."spe_BA_9"-b."spe_BA_9" as BA_9_dif, a."spe_BA_9"/b."spe_BA_9" as BA_9_rel,' \
    ' b."spe_AB_10" as AB_10_bef, a."spe_AB_10" as AB_10_aft, a."spe_AB_10"-b."spe_AB_10" as AB_10_dif, a."spe_AB_10"/b."spe_AB_10" as AB_10_rel,' \
    ' b."spe_BA_10" as BA_10_bef, a."spe_BA_10" as BA_10_aft, a."spe_BA_10"-b."spe_BA_10" as BA_10_dif, a."spe_BA_10"/b."spe_BA_10" as BA_10_rel,' \
    ' b."spe_AB_11" as AB_11_bef, a."spe_AB_11" as AB_11_aft, a."spe_AB_11"-b."spe_AB_11" as AB_11_dif, a."spe_AB_11"/b."spe_AB_11" as AB_11_rel,' \
    ' b."spe_BA_11" as BA_11_bef, a."spe_BA_11" as BA_11_aft, a."spe_BA_11"-b."spe_BA_11" as BA_11_dif, a."spe_BA_11"/b."spe_BA_11" as BA_11_rel,' \
    ' b."spe_AB_12" as AB_12_bef, a."spe_AB_12" as AB_12_aft, a."spe_AB_12"-b."spe_AB_12" as AB_12_dif, a."spe_AB_12"/b."spe_AB_12" as AB_12_rel,' \
    ' b."spe_BA_12" as BA_12_bef, a."spe_BA_12" as BA_12_aft, a."spe_BA_12"-b."spe_BA_12" as BA_12_dif, a."spe_BA_12"/b."spe_BA_12" as BA_12_rel,' \
    ' b."spe_AB_13" as AB_13_bef, a."spe_AB_13" as AB_13_aft, a."spe_AB_13"-b."spe_AB_13" as AB_13_dif, a."spe_AB_13"/b."spe_AB_13" as AB_13_rel,' \
    ' b."spe_BA_13" as BA_13_bef, a."spe_BA_13" as BA_13_aft, a."spe_BA_13"-b."spe_BA_13" as BA_13_dif, a."spe_BA_13"/b."spe_BA_13" as BA_13_rel,' \
    ' b."spe_AB_14" as AB_14_bef, a."spe_AB_14" as AB_14_aft, a."spe_AB_14"-b."spe_AB_14" as AB_14_dif, a."spe_AB_14"/b."spe_AB_14" as AB_14_rel,' \
    ' b."spe_BA_14" as BA_14_bef, a."spe_BA_14" as BA_14_aft, a."spe_BA_14"-b."spe_BA_14" as BA_14_dif, a."spe_BA_14"/b."spe_BA_14" as BA_14_rel,' \
    ' b."spe_AB_15" as AB_15_bef, a."spe_AB_15" as AB_15_aft, a."spe_AB_15"-b."spe_AB_15" as AB_15_dif, a."spe_AB_15"/b."spe_AB_15" as AB_15_rel,' \
    ' b."spe_BA_15" as BA_15_bef, a."spe_BA_15" as BA_15_aft, a."spe_BA_15"-b."spe_BA_15" as BA_15_dif, a."spe_BA_15"/b."spe_BA_15" as BA_15_rel,' \
    ' b."spe_AB_16" as AB_16_bef, a."spe_AB_16" as AB_16_aft, a."spe_AB_16"-b."spe_AB_16" as AB_16_dif, a."spe_AB_16"/b."spe_AB_16" as AB_16_rel,' \
    ' b."spe_BA_16" as BA_16_bef, a."spe_BA_16" as BA_16_aft, a."spe_BA_16"-b."spe_BA_16" as BA_16_dif, a."spe_BA_16"/b."spe_BA_16" as BA_16_rel,' \
    ' b."spe_AB_17" as AB_17_bef, a."spe_AB_17" as AB_17_aft, a."spe_AB_17"-b."spe_AB_17" as AB_17_dif, a."spe_AB_17"/b."spe_AB_17" as AB_17_rel,' \
    ' b."spe_BA_17" as BA_17_bef, a."spe_BA_17" as BA_17_aft, a."spe_BA_17"-b."spe_BA_17" as BA_17_dif, a."spe_BA_17"/b."spe_BA_17" as BA_17_rel,' \
    ' b."spe_AB_18" as AB_18_bef, a."spe_AB_18" as AB_18_aft, a."spe_AB_18"-b."spe_AB_18" as AB_18_dif, a."spe_AB_18"/b."spe_AB_18" as AB_18_rel,' \
    ' b."spe_BA_18" as BA_18_bef, a."spe_BA_18" as BA_18_aft, a."spe_BA_18"-b."spe_BA_18" as BA_18_dif, a."spe_BA_18"/b."spe_BA_18" as BA_18_rel,' \
    ' b."spe_AB_19" as AB_19_bef, a."spe_AB_19" as AB_19_aft, a."spe_AB_19"-b."spe_AB_19" as AB_19_dif, a."spe_AB_19"/b."spe_AB_19" as AB_19_rel,' \
    ' b."spe_BA_19" as BA_19_bef, a."spe_BA_19" as BA_19_aft, a."spe_BA_19"-b."spe_BA_19" as BA_19_dif, a."spe_BA_19"/b."spe_BA_19" as BA_19_rel,' \
    ' b."spe_AB_20" as AB_20_bef, a."spe_AB_20" as AB_20_aft, a."spe_AB_20"-b."spe_AB_20" as AB_20_dif, a."spe_AB_20"/b."spe_AB_20" as AB_20_rel,' \
    ' b."spe_BA_20" as BA_20_bef, a."spe_BA_20" as BA_20_aft, a."spe_BA_20"-b."spe_BA_20" as BA_20_dif, a."spe_BA_20"/b."spe_BA_20" as BA_20_rel,' \
    ' b."spe_AB_21" as AB_21_bef, a."spe_AB_21" as AB_21_aft, a."spe_AB_21"-b."spe_AB_21" as AB_21_dif, a."spe_AB_21"/b."spe_AB_21" as AB_21_rel,' \
    ' b."spe_BA_21" as BA_21_bef, a."spe_BA_21" as BA_21_aft, a."spe_BA_21"-b."spe_BA_21" as BA_21_dif, a."spe_BA_21"/b."spe_BA_21" as BA_21_rel,' \
    ' b."spe_AB_22" as AB_22_bef, a."spe_AB_22" as AB_22_aft, a."spe_AB_22"-b."spe_AB_22" as AB_22_dif, a."spe_AB_22"/b."spe_AB_22" as AB_22_rel,' \
    ' b."spe_BA_22" as BA_22_bef, a."spe_BA_22" as BA_22_aft, a."spe_BA_22"-b."spe_BA_22" as BA_22_dif, a."spe_BA_22"/b."spe_BA_22" as BA_22_rel,' \
    ' b."spe_AB_23" as AB_23_bef, a."spe_AB_23" as AB_23_aft, a."spe_AB_23"-b."spe_AB_23" as AB_23_dif, a."spe_AB_23"/b."spe_AB_23" as AB_23_rel,' \
    ' b."spe_BA_23" as BA_23_bef, a."spe_BA_23" as BA_23_aft, a."spe_BA_23"-b."spe_BA_23" as BA_23_dif, a."spe_BA_23"/b."spe_BA_23" as BA_23_rel,' \
    ' b."spe_AB_0" as AB_24_bef, a."spe_AB_0" as AB_24_aft, a."spe_AB_0"-b."spe_AB_0" as AB_24_dif, a."spe_AB_0"/b."spe_AB_0" as AB_24_rel,' \
    ' b."spe_BA_0" as BA_24_bef, a."spe_BA_0" as BA_24_aft, a."spe_BA_0"-b."spe_BA_0" as BA_24_dif, a."spe_BA_0"/b."spe_BA_0" as BA_24_rel' \
    ' from {}.{} b, {}.{} a where b.id=a.id'.format(schema_name,newDbName,schema_name,newDbName, schema_name,before_db,schema_name,after_db)
    print (query)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    print('Realtion {} created successfully.'.format(newDbName))
    if to_shp:
        exportDb2Shp(newDbName)
    if to_csv:
        exportDb2Csv(newDbName)
    return newDbName



def createComperativeIsManLayerVer2(before_db,after_db,newDbName, to_shp=False, to_csv=False, schema_name='processed_ltrs'):
    query = 'drop table if exists {}.{};\
    create table {}.{} as select b.id,b.geom,b.length,b.dir,b.street,b.fromleft,b.toleft,b.fromright,b.toright,' \
            'b.streetcode,b.objid,b.userid,b.fjunction,b.tjunction,b.length1,b.roadtype,b.oneway,b.f_zlev,b.t_zlev,' \
            'b.flanes,b.tlanes,b.intended,b.cityname,b.citycode,b.seconds,b.fromspeedl,b.tospeedl,b.aprxspeedl' \
            ',b.autonomy,b.regulation,b.roadnamemz,b.directionm,b.status,b.roadfuncti,b.flag,b.identifier,b.roaddirect,' \
            'b.clearstree,b.reverstree,b.clearcityn,b.revercityn,b.from_main_id,b.to_main_id,b.from_id,b.to_id,' \
            ' b."d_nos_ab" as bef_dns_AB,b."d_nos_ba" as bef_dns_BA,a."d_nos_ab" as aft_dns_AB, a."d_nos_ba" as aft_dns_BA,' \
            ' b."spe_AB_5" as AB_5_bef, a."spe_AB_5" as AB_5_aft, a."spe_AB_5"-b."spe_AB_5" as AB_5_dif, a."spe_AB_5"/b."spe_AB_5" as AB_5_rel,' \
            ' b."spe_BA_5" as BA_5_bef, a."spe_BA_5" as BA_5_aft, a."spe_BA_5"-b."spe_BA_5" as BA_5_dif, a."spe_BA_5"/b."spe_BA_5" as BA_5_rel,' \
            ' b."spe_AB_6" as AB_6_bef, a."spe_AB_6" as AB_6_aft, a."spe_AB_6"-b."spe_AB_6" as AB_6_dif, a."spe_AB_6"/b."spe_AB_6" as AB_6_rel,' \
            ' b."spe_BA_6" as BA_6_bef, a."spe_BA_6" as BA_6_aft, a."spe_BA_6"-b."spe_BA_6" as BA_6_dif, a."spe_BA_6"/b."spe_BA_6" as BA_6_rel,' \
            ' b."spe_AB_7" as AB_7_bef, a."spe_AB_7" as AB_7_aft, a."spe_AB_7"-b."spe_AB_7" as AB_7_dif, a."spe_AB_7"/b."spe_AB_7" as AB_7_rel,' \
            ' b."spe_BA_7" as BA_7_bef, a."spe_BA_7" as BA_7_aft, a."spe_BA_7"-b."spe_BA_7" as BA_7_dif, a."spe_BA_7"/b."spe_BA_7" as BA_7_rel,' \
            ' b."spe_AB_8" as AB_8_bef, a."spe_AB_8" as AB_8_aft, a."spe_AB_8"-b."spe_AB_8" as AB_8_dif, a."spe_AB_8"/b."spe_AB_8" as AB_8_rel,' \
            ' b."spe_BA_8" as BA_8_bef, a."spe_BA_8" as BA_8_aft, a."spe_BA_8"-b."spe_BA_8" as BA_8_dif, a."spe_BA_8"/b."spe_BA_8" as BA_8_rel,' \
            ' b."spe_AB_9" as AB_9_bef, a."spe_AB_9" as AB_9_aft, a."spe_AB_9"-b."spe_AB_9" as AB_9_dif, a."spe_AB_9"/b."spe_AB_9" as AB_9_rel,' \
            ' b."spe_BA_9" as BA_9_bef, a."spe_BA_9" as BA_9_aft, a."spe_BA_9"-b."spe_BA_9" as BA_9_dif, a."spe_BA_9"/b."spe_BA_9" as BA_9_rel,' \
            ' b."spe_AB_10" as AB_10_bef, a."spe_AB_10" as AB_10_aft, a."spe_AB_10"-b."spe_AB_10" as AB_10_dif, a."spe_AB_10"/b."spe_AB_10" as AB_10_rel,' \
            ' b."spe_BA_10" as BA_10_bef, a."spe_BA_10" as BA_10_aft, a."spe_BA_10"-b."spe_BA_10" as BA_10_dif, a."spe_BA_10"/b."spe_BA_10" as BA_10_rel,' \
            ' b."spe_AB_11" as AB_11_bef, a."spe_AB_11" as AB_11_aft, a."spe_AB_11"-b."spe_AB_11" as AB_11_dif, a."spe_AB_11"/b."spe_AB_11" as AB_11_rel,' \
            ' b."spe_BA_11" as BA_11_bef, a."spe_BA_11" as BA_11_aft, a."spe_BA_11"-b."spe_BA_11" as BA_11_dif, a."spe_BA_11"/b."spe_BA_11" as BA_11_rel,' \
            ' b."spe_AB_12" as AB_12_bef, a."spe_AB_12" as AB_12_aft, a."spe_AB_12"-b."spe_AB_12" as AB_12_dif, a."spe_AB_12"/b."spe_AB_12" as AB_12_rel,' \
            ' b."spe_BA_12" as BA_12_bef, a."spe_BA_12" as BA_12_aft, a."spe_BA_12"-b."spe_BA_12" as BA_12_dif, a."spe_BA_12"/b."spe_BA_12" as BA_12_rel,' \
            ' b."spe_AB_13" as AB_13_bef, a."spe_AB_13" as AB_13_aft, a."spe_AB_13"-b."spe_AB_13" as AB_13_dif, a."spe_AB_13"/b."spe_AB_13" as AB_13_rel,' \
            ' b."spe_BA_13" as BA_13_bef, a."spe_BA_13" as BA_13_aft, a."spe_BA_13"-b."spe_BA_13" as BA_13_dif, a."spe_BA_13"/b."spe_BA_13" as BA_13_rel,' \
            ' b."spe_AB_14" as AB_14_bef, a."spe_AB_14" as AB_14_aft, a."spe_AB_14"-b."spe_AB_14" as AB_14_dif, a."spe_AB_14"/b."spe_AB_14" as AB_14_rel,' \
            ' b."spe_BA_14" as BA_14_bef, a."spe_BA_14" as BA_14_aft, a."spe_BA_14"-b."spe_BA_14" as BA_14_dif, a."spe_BA_14"/b."spe_BA_14" as BA_14_rel,' \
            ' b."spe_AB_15" as AB_15_bef, a."spe_AB_15" as AB_15_aft, a."spe_AB_15"-b."spe_AB_15" as AB_15_dif, a."spe_AB_15"/b."spe_AB_15" as AB_15_rel,' \
            ' b."spe_BA_15" as BA_15_bef, a."spe_BA_15" as BA_15_aft, a."spe_BA_15"-b."spe_BA_15" as BA_15_dif, a."spe_BA_15"/b."spe_BA_15" as BA_15_rel,' \
            ' b."spe_AB_16" as AB_16_bef, a."spe_AB_16" as AB_16_aft, a."spe_AB_16"-b."spe_AB_16" as AB_16_dif, a."spe_AB_16"/b."spe_AB_16" as AB_16_rel,' \
            ' b."spe_BA_16" as BA_16_bef, a."spe_BA_16" as BA_16_aft, a."spe_BA_16"-b."spe_BA_16" as BA_16_dif, a."spe_BA_16"/b."spe_BA_16" as BA_16_rel,' \
            ' b."spe_AB_17" as AB_17_bef, a."spe_AB_17" as AB_17_aft, a."spe_AB_17"-b."spe_AB_17" as AB_17_dif, a."spe_AB_17"/b."spe_AB_17" as AB_17_rel,' \
            ' b."spe_BA_17" as BA_17_bef, a."spe_BA_17" as BA_17_aft, a."spe_BA_17"-b."spe_BA_17" as BA_17_dif, a."spe_BA_17"/b."spe_BA_17" as BA_17_rel,' \
            ' b."spe_AB_18" as AB_18_bef, a."spe_AB_18" as AB_18_aft, a."spe_AB_18"-b."spe_AB_18" as AB_18_dif, a."spe_AB_18"/b."spe_AB_18" as AB_18_rel,' \
            ' b."spe_BA_18" as BA_18_bef, a."spe_BA_18" as BA_18_aft, a."spe_BA_18"-b."spe_BA_18" as BA_18_dif, a."spe_BA_18"/b."spe_BA_18" as BA_18_rel,' \
            ' b."spe_AB_19" as AB_19_bef, a."spe_AB_19" as AB_19_aft, a."spe_AB_19"-b."spe_AB_19" as AB_19_dif, a."spe_AB_19"/b."spe_AB_19" as AB_19_rel,' \
            ' b."spe_BA_19" as BA_19_bef, a."spe_BA_19" as BA_19_aft, a."spe_BA_19"-b."spe_BA_19" as BA_19_dif, a."spe_BA_19"/b."spe_BA_19" as BA_19_rel,' \
            ' b."spe_AB_20" as AB_20_bef, a."spe_AB_20" as AB_20_aft, a."spe_AB_20"-b."spe_AB_20" as AB_20_dif, a."spe_AB_20"/b."spe_AB_20" as AB_20_rel,' \
            ' b."spe_BA_20" as BA_20_bef, a."spe_BA_20" as BA_20_aft, a."spe_BA_20"-b."spe_BA_20" as BA_20_dif, a."spe_BA_20"/b."spe_BA_20" as BA_20_rel,' \
            ' b."spe_AB_21" as AB_21_bef, a."spe_AB_21" as AB_21_aft, a."spe_AB_21"-b."spe_AB_21" as AB_21_dif, a."spe_AB_21"/b."spe_AB_21" as AB_21_rel,' \
            ' b."spe_BA_21" as BA_21_bef, a."spe_BA_21" as BA_21_aft, a."spe_BA_21"-b."spe_BA_21" as BA_21_dif, a."spe_BA_21"/b."spe_BA_21" as BA_21_rel,' \
            ' b."spe_AB_22" as AB_22_bef, a."spe_AB_22" as AB_22_aft, a."spe_AB_22"-b."spe_AB_22" as AB_22_dif, a."spe_AB_22"/b."spe_AB_22" as AB_22_rel,' \
            ' b."spe_BA_22" as BA_22_bef, a."spe_BA_22" as BA_22_aft, a."spe_BA_22"-b."spe_BA_22" as BA_22_dif, a."spe_BA_22"/b."spe_BA_22" as BA_22_rel,' \
            ' b."spe_AB_23" as AB_23_bef, a."spe_AB_23" as AB_23_aft, a."spe_AB_23"-b."spe_AB_23" as AB_23_dif, a."spe_AB_23"/b."spe_AB_23" as AB_23_rel,' \
            ' b."spe_BA_23" as BA_23_bef, a."spe_BA_23" as BA_23_aft, a."spe_BA_23"-b."spe_BA_23" as BA_23_dif, a."spe_BA_23"/b."spe_BA_23" as BA_23_rel,' \
            ' b."spe_AB_0" as AB_24_bef, a."spe_AB_0" as AB_24_aft, a."spe_AB_0"-b."spe_AB_0" as AB_24_dif, a."spe_AB_0"/b."spe_AB_0" as AB_24_rel,' \
            ' b."spe_BA_0" as BA_24_bef, a."spe_BA_0" as BA_24_aft, a."spe_BA_0"-b."spe_BA_0" as BA_24_dif, a."spe_BA_0"/b."spe_BA_0" as BA_24_rel' \
            ' from {}.{} b, {}.{} a where b.id=a.id'.format(schema_name,newDbName,schema_name,newDbName, schema_name,before_db,schema_name,after_db)
    print (query)
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    print('Realtion {} created successfully.'.format(newDbName))
    if to_shp:
        exportDb2Shp(newDbName)
    if to_csv:
        exportDb2Csv(newDbName)

def getRelevantDatesString(from_date,to_date,filter_type=4,stringOrList=0):
    # filter 1: mon-wed, filter 2: sun-thu, filter 3: thu, filter 4:sun-sat, 5:fri-sat
    # stringOrList=0 => string, =1=> list
    weekday_dict={6:1,0:2,1:3,2:4,3:5,4:6,5:7}
    from_date=datetime.strptime(from_date, '%Y-%m-%d')
    to_date=datetime.strptime(to_date, '%Y-%m-%d')
    legitDatesList=[]
    filter_dict={1:[2,4],2:[1,5],3:[5,5],4:[1,7],5:[6,7],6:[3,3]}
    delta=to_date-from_date
    for i in range(delta.days + 1):
        day = from_date + timedelta(days=i)
        if(filter_dict[filter_type][0]<= weekday_dict[day.weekday()]<=filter_dict[filter_type][1]):
            legitDatesList.append(day.date())
    if(stringOrList==1):
        return sorted(legitDatesList)
    if (len(legitDatesList) >0):
        query_string = '('
        for date in legitDatesList:
            query_string += "'" + str(date) + "',"
        query_string = query_string[:-1] + ")"
        return query_string
    print sorted(legitDatesList)

def createIntersectingStreetsLayer(route,street_layer):
    intersecting_streets_query = "drop table if exists intersecting_streets;" \
                                 " CREATE TABLE intersecting_streets as" \
        "(select m. * from (select distinct sg.the_geom from gtfs_shape_geoms sg," \
        " gtfs_routes r, gtfs_trips t where r.route_id= '{}' and r.route_id=t.route_id and t.shape_id=sg.shape_id) r," \
        " {} m where st_intersects(r.the_geom, m.geom)) ".format(route[0],street_layer)
    try:
        cur = conn.cursor()
        cur.execute(intersecting_streets_query)
        conn.commit()
    except:
        print intersecting_streets_query
        print "Can't create and fill Table 'intersecting_streets'"


conn = psycopg2.connect("dbname= 'postgis_siri' user='postgres' password='123qwe' host='localhost' port = 5432")
engine = create_engine('postgresql://postgres:123qwe@localhost:5432/postgis_siri')


def insertNetworkCreationDataToLog(repository_name,from_siri_date,to_siri_date,filter_type,street_layer,num_of_routes,start_time,end_time,siri_dbs_list,num_of_records,log_path='D://export//speed_network_log//',log_file_name='speed_network_log.csv'):
    is_file_found=False
    all_file_names = [f for f in listdir(log_path) if isfile(join(log_path, f))]
    for file in all_file_names:
        print file
        if(is_file_found==True):
            break
        else:
            if file==log_file_name:
                is_file_found=True
    if(is_file_found==False):
        fields = ['network_name','from_date','to_date','weekdays_checked','street_layer','num_of_routes_checked','start_timestamp','finish_typestamp','total_time_taken','siri_files_checked','num_of_siris_checked','No. or records in output file']
        with open(log_path+log_file_name, 'a') as f:
            writer = csv.writer(f)
            writer.writerow(fields)
            print("Log file does not exist. Creating log file '{}' at '{}'".format(log_file_name,log_path))
    fields_to_insert=[repository_name,from_siri_date,to_siri_date,retrieveWeekdaysByFilter(filter_type),street_layer,num_of_routes,start_time,end_time,end_time-start_time,siri_dbs_list,len(siri_dbs_list),num_of_records]
    with open(log_path+log_file_name, 'a') as f:
        writer = csv.writer(f)
    #try:
        writer.writerow(fields_to_insert)
    #    print("SUCCESS: Speed network creation added to log '{}' at '{}'".format(log_file_name,log_path))
    #except:
    #    print("FAIULRE: Speed network creation was not added")


def retrieveWeekdaysByFilter(filter_type):
    if(filter_type==1):
        weekdays_checked=['mon','tue','wed']
    elif(filter_type==2):
        weekdays_checked = ['sun','mon', 'tue', 'wed','thu']
    elif(filter_type==3):
        weekdays_checked = ['thu']
    else:
        weekdays_checked = ['sun','mon', 'tue', 'wed','thu','fri','sat']
    return weekdays_checked

def makeAllSirisUniteQuery(schema_name='public',from_date='2000-01-01',to_date='2040-12-31',string_to_search='siri_sm_res_monitor',where_clause=''):
    from_date=datetime.strptime(from_date,'%Y-%m-%d').date()
    to_date=datetime.strptime(to_date,'%Y-%m-%d').date()
    siris_in_db=getAllSirisInDb(schema_name,string_to_search)
    #print siris_in_db
    query='('
    for siri in siris_in_db:
        #siri=siri[0]
        #print siri
        #print siri
        tableFrom=datetime.strptime(siri[20:24]+'-'+siri[24:26]+'-'+siri[26:28],'%Y-%m-%d').date()
        #print tableFrom
        if(siri[-10:]=='_processed'):
            tableTo=datetime.strptime(siri[-21:-17]+'-'+siri[-17:-15]+'-'+siri[-15:-13],'%Y-%m-%d').date()
        else:
            tableTo=datetime.strptime(siri[-11:-7]+'-'+siri[-7:-5]+'-'+siri[-5:-3],'%Y-%m-%d').date()
        #print tableTo
        #print tableFrom, tableTo
        if ((tableFrom >= from_date and (tableFrom < to_date or (tableFrom == to_date and tableTo > to_date))) or (
                tableFrom <= from_date and (tableTo > from_date or tableTo == from_date))):
            query += '(select * from {}.{} {}) union '.format(schema_name,siri,where_clause)
            print siri
    query=query[:-6]+' ) s'
    return query

def makeAllLTRsUniteQuery(schema_name='public',from_date='2000-01-01',to_date='2040-12-31',string_to_search='ltr_everything_'):
    from_date=from_date.replace('-','_')
    to_date=to_date.replace('-','_')
    from_date=datetime.strptime(from_date,'%Y_%m_%d').date()
    to_date=datetime.strptime(to_date,'%Y_%m_%d').date()
    ltrs_in_db=sorted(getAllSirisInDb(schema_name,string_to_search))
    mekadem=0
    query='('
    for ltr in ltrs_in_db:
        table_date=datetime.strptime(ltr[-10-mekadem:-6-mekadem]+'_'+ltr[-5-mekadem:-3-mekadem]+'_'+ltr[-2-mekadem:len(ltr)-mekadem],'%Y_%m_%d').date()
        #print table_date
        if (from_date<=table_date<=to_date):
            print ltr
            query += '(select * from {}.{} ) union '.format(schema_name,ltr)
    query=query[:-6]+' )'
    return(query)

def stringQueryResultsAsList(query_feedback,add_before='',add_after='',delimiter=", "):
    string_result="("
    for query in query_feedback:
        string_result=string_result+add_before+query+add_after+delimiter
    return string_result[:-len(delimiter)]+")"

def makeAllSirisList(from_date='2000-01-01',to_date='2040-12-31'):
    from_date=datetime.strptime(from_date,'%Y-%m-%d').date()
    to_date=datetime.strptime(to_date,'%Y-%m-%d').date()
    siris_in_db=getAllSirisInDb()
    query='['
    for siri in siris_in_db:
        siri=siri[0]
        query +="'{}',".format(siri)
    query=query[:-1]+']'
    print(query)

def exportDailyTablesForWebsite(from_date,to_date,dates_filter=2,output_dir='D:\export\dumps_for_website'):
    dates_to_export = getRelevantDatesString(from_date,to_date, dates_filter, 1)
    for date in dates_to_export:
        print date
        pg_dump(turnRepository2linkAvgSpeedWithModifiedIsMan('is_man_modified','ltr_everything_{}'.format(str(date).replace('-', '_')), 2),output_dir)

def exportDailyLTRforWebsite(date,street_layer='is_man_modified',export_path='\\\\192.168.11.138//d$//tables_for_website_2//',):
    temp_dir = 'D://export//dumps_for_website//temp_dir//'
    try:
        exportDailyTablesForWebsite(date,date,4,temp_dir)
        dump_name='dsn_{}.dump'.format(date.replace('-', '_'))
        #move(temp_dir + dump_name, export_path + dump_name)
        #print ("File {} moved from {} to {}".format(dump_name,temp_dir,export_path))
    except:
        print ("Can't export ltr for date {}. Skipping.".format(date))

def exportDailyLTRforBackup(date,street_layer='is_man',export_path='\\\\mars//gl//GL_Transit//ltrs_backups//'):
    temp_dir = 'D://export//dumps_for_backup//temp_dir//'
    dump_name='ltr_everything_{}'.format(str(date).replace('-', '_'))
    if(DoesLtrExist(dump_name,'processed_ltrs')):
        pg_dump(dump_name,temp_dir,'processed_ltrs')
        move(temp_dir + dump_name+'.dump', export_path + dump_name+'.dump')
        print ("File {} moved from {} to {}".format(dump_name,temp_dir,export_path))
    else:
        print ("LTR {} does not exist.".format(dump_name))
    #except:
        print ("Can't export ltr for date {}. Skipping.".format(date))

def exportDailyLTRforExternalWebsiteByFTP(sourceFilePath,destinationDirectory, server, username, password):
    #myFTP = ftplib.FTP(server, username, password)
    myFTP = ftplib.FTP()
    #myFTP.connect(host='192.168.11.138', user='yohai', passwd='123qwe')
    myFTP.connect('192.168.11.138')
    myFTP.cwd(destinationDirectory)
    if os.path.isfile(sourceFilePath):
        fh = open(sourceFilePath, 'rb')
        myFTP.storbinary('STOR %s', fh)
        fh.close()
    else:
        print "Source File does not exist"


def exportDailyLTRforExternalWebsiteByParamiko(sourceFilePath,destinationDirectory, server, username, password):

    ssh = paramiko.SSHClient()
    #ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(server,22, username, password)

    print "connected successfully!"

    sftp = ssh.open_sftp()
    print sftp
    sftp.put(sourceFilePath,destinationDirectory)
    sftp.close()
    print "copied successfully!"

    ssh.close()
    exit()


def exportDailyLTRforExternalWebsiteBySubProcess(sourceFilePath,destinationDirectory, server, username, password):

    try:
        # Set scp and ssh data.
        connPrivateKey = '/home/user/myKey.pem'

        # Use scp to send file from local to host.
        scp = subprocess.Popen(
            ['scp', '-i', connPrivateKey, 'myFile.txt', '{}@{}:{}'.format(username, server, destinationDirectory)])
    except:
        print('ERROR: Connection to host failed!')