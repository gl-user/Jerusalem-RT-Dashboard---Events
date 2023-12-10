__author__ = 'asafeh'
import os
import zipfile
from datetime import date, timedelta
from urllib.request import urlopen

import ssl

print ("Start updateGTFSFromHTTP...")
israel_gtfs_name = 'israel-public-transportation'
filename = 'israel-public-transportation.zip'
clusterFilename = 'ClusterToLine.zip'
tripFileName = 'TripIdToDate.zip'
ChargingRavKavFileName = "ChargingRavKav.zip"
TariffFileName = "Tariff.zip"
TariffFileName_2022 = "Tariff_2022.zip"
ZonesFileName = "zones.zip"
ZonesFileName_2022 = "zones_2022.zip"
local_gtfs_zip_dir='C:\\GTFS\\Download\\'
local_gtfs_zip_extract_dir="C:\\GTFS\\Download\\Extract\\"
os.chdir(local_gtfs_zip_dir)

context = ssl._create_unverified_context()
zipFile = urlopen("https://gtfs.mot.gov.il/gtfsfiles/"+filename, context=context)
with open(filename,'wb') as output:
  output.write(zipFile.read())

zip_ref = zipfile.ZipFile(filename, 'r')
zip_ref.extractall(local_gtfs_zip_extract_dir)
zip_ref.close()
todayDate = date.today()
now_str = str(todayDate.year) + '-' + str(todayDate.month) + '-' + str(todayDate.day)
os.chdir(local_gtfs_zip_dir)
os.rename(filename,israel_gtfs_name+'-'+now_str+'.zip')

zipFile = urlopen("https://gtfs.mot.gov.il/gtfsfiles/"+ChargingRavKavFileName, context=context)
with open(ChargingRavKavFileName,'wb') as output:
  output.write(zipFile.read())

zipFile = urlopen("https://gtfs.mot.gov.il/gtfsfiles/"+clusterFilename, context=context)
with open(clusterFilename,'wb') as output:
  output.write(zipFile.read())

zipFile = urlopen("https://gtfs.mot.gov.il/gtfsfiles/"+TariffFileName, context=context)
with open(TariffFileName,'wb') as output:
  output.write(zipFile.read())

zipFile = urlopen("https://gtfs.mot.gov.il/gtfsfiles/"+TariffFileName_2022, context=context)
with open(TariffFileName_2022,'wb') as output:
  output.write(zipFile.read())

zipFile = urlopen("https://gtfs.mot.gov.il/gtfsfiles/"+tripFileName, context=context)
with open(tripFileName,'wb') as output:
  output.write(zipFile.read())

zipFile = urlopen("https://gtfs.mot.gov.il/gtfsfiles/"+ZonesFileName_2022, context=context)
with open(ZonesFileName_2022,'wb') as output:
  output.write(zipFile.read())

os.rename(clusterFilename,'ClusterToLine-'+now_str+'.zip')
os.rename(tripFileName,'TripIdToDate-'+now_str+'.zip')
os.rename(ChargingRavKavFileName,'ChargingRavKav-'+now_str+'.zip')
os.rename(TariffFileName,'Tariff-'+now_str+'.zip')
os.rename(TariffFileName_2022,'Tariff_2022-'+now_str+'.zip')
os.rename(ZonesFileName_2022,'zones_2022-'+now_str+'.zip')
print ("End updateGTFSFromHTTP...")

