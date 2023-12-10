from datetime import datetime
class SiriManLink(object):
    def __init__(self,LinkID,RouteID,tripUniqueKey,firstTimeStamp,FromLongLat,ToLongLat,FromTime,ToTime,siriDistance,siriSpeed):
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
        self.siriDistance=float(siriDistance)
        self.siriSpeed = siriSpeed
        #self.siriSpeed = self.__GetSpeedFromTimeAndDistance__(self.FromTime, self.ToTime, float(self.siriDistance))

    def set_dir(self,dir):
        self.dir = dir

    def __GetSpeedFromTimeAndDistance__(self, FromTime, ToTime, siriDistance):
        FromTime= datetime.strptime(FromTime, '%Y-%m-%d %H:%M:%S')
        ToTime=datetime.strptime(ToTime, '%Y-%m-%d %H:%M:%S')
        sec = (abs((ToTime - FromTime).total_seconds()))
        #return (siriDistance / (abs((ToTime - FromTime).total_seconds())) * 3.6)
        if sec > 0:
            return (siriDistance/1000.0) / (sec/3600.0)
        else:
            return 0


    def __str__(self):
        #print('LinkID:{:>20}\nRouteID:{:>19}\ntripUniqueKey:{:>39}\nfirst Time Stamp:      {} \nFromLongLat:{:>20},{}\nToLongLat:{:>21},{}\nFromTime:              {}\nToTime:                {}\nDistance:              {}\nSpeed:{:>22}\n'.\
        #format(self.LinkID,self.RouteID,self.tripUniqueKey,self.firstTimeStamp,self.FromLong,self.FromLat,self.ToLong,self.ToLat,self.FromTime,self.ToTime,self.siriDistance,self.siriSpeed))
        pass

    def getDayPeriod(self,from_time):
        return 1
        from_date_time = datetime.strptime(from_time, '%Y-%m-%d %H:%M:%S') #2018-11-27 05:01:16
        from_time = datetime.strptime(from_time[-8:], '%H:%M:%S')
        if ('04:00:00' <= from_time <= '06:29:59'):
            return '4:00-6:29'
        elif ('06:30:00' <= from_time <= '08:29:59'):
            return '6:30-8:29'
        elif ('08:30:00' <= from_time <= '11:59:59'):
            return '8:30-11:59'
        elif ('12:00:00' <= from_time <= '14:59:59'):
            return '12:00-14:59'
        elif ('15:00:00' <= from_time <= '18:59:59'):
            return '15:00-18:59'
        elif ('19:00:00' <= from_time <= '23:59:59'):
            return '19:00-23:59'
        else:
            return '0:00-03:59'

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
        self.siriObjectList.append(self.getDayPeriod(self.FromTime))
        self.siriObjectList.append(datetime.strptime(self.FromTime, '%Y-%m-%d %H:%M:%S').hour),
        self.siriObjectList.append(self.dir)
        return self.siriObjectList


