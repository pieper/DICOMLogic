import json
import logging
import pydicom
import random
import requests
import time


# for development
import sys
try:
    del sys.modules['DICOMLogic']
    del sys.modules['DICOMLogic.databases.DICOMDatabase']
    del sys.modules['DICOMLogic.databases.ctkSQLite']
    del sys.modules['DICOMLogic.stores.DICOMwebStore']
    import shutil
    shutil.rmtree("/opt/sr/python-install/lib/python3.9/site-packages/DICOMLogic")
except KeyError:
    pass
pip_install("--upgrade /Users/pieper/slicer/latest/DICOMLogic")
import DICOMLogic
import DICOMLogic.databases
import DICOMLogic.stores
import importlib
#importlib.reload(DICOMLogic.databases)
#importlib.reload(DICOMLogic.stores)

# url = "https://d33do7qe4w26qo.cloudfront.net/dicomweb"
url = "https://testing-proxy.canceridc.dev/current/viewer-only-no-downloads-see-tinyurl-dot-com-slash-3j3d9jyp/dicomWeb"

testStudyInstanceUID = "1.3.6.1.4.1.14519.5.2.1.6279.6001.224985459390356936417021464571"
testStudyInstanceUID = None

# applies when testStudyInstanceUID is None
#studyRequestLimit = 1000
studyRequestLimit = 100
#studyRequestOffset = 1000
studyRequestOffset = 0

dbDirectory = "/opt/tmp/db"
#db = DICOMLogic.databases.ctkSQLite(dbDirectory)
#store = DICOMLogic.stores.DICOMwebStore(db, url)
db = DICOMLogic.databases.ctkSQLite(dbDirectory)
store = DICOMLogic.stores.DICOMwebStore(db, url)

# get a list studyInstanceUIDs
studyInstanceUIDs = []
studiesURL = f"{url}/studies?limit={studyRequestLimit}&offset={studyRequestOffset}"
studiesRequest = requests.get(studiesURL)
studies = json.loads(studiesRequest.content)
for study in studies:
    studyDataset = pydicom.Dataset.from_json(study)
    studyInstanceUIDs.append(studyDataset.StudyInstanceUID)

if testStudyInstanceUID:
    studyInstanceUIDs = [testStudyInstanceUID]

for studyInstanceUID in studyInstanceUIDs:
    print(f"Storing {studyInstanceUID}")
    store.indexStudy(studyInstanceUID)
