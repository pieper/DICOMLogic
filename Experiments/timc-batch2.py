import json
import pydicom
import requests
import subprocess
import time

pip_install("--upgrade /Users/pieper/slicer/latest/DICOMLogic")

import DICOMLogic

startTime = time.time()

dbDirectory = "/opt/data/LNQ/timc-batch2/db"
overallStudyLimit = 10 ;# only request this many studies

# url = "https://d33do7qe4w26qo.cloudfront.net/dicomweb"
# TODO: this url is not for general use!
#url = "https://testing-proxy.canceridc.dev/current/viewer-only-no-downloads-see-tinyurl-dot-com-slash-3j3d9jyp/dicomWeb"
#url = "https://healthcare.googleapis.com/v1/projects/bwh-lnq-gcp-1620235879/locations/us-central1/datasets/sdp-performance-testing/dicomStores/sdp-performance-testing/dicomWeb"

url = "https://healthcare.googleapis.com/v1/projects/bwh-lnq-gcp-1620235879/locations/us-central1/datasets/time-batch2/dicomStores/time-batch2/dicomWeb"

def getGCPToken():
    command = "gcloud auth print-access-token"
    tokenProcess = slicer.util.launchConsoleProcess(command.split(" "))
    return(tokenProcess.stdout.read().strip())

def setStoreToken(store):
    """Set the bearer token for the store header and refresh is hourly"""
    token = getGCPToken()
    store.headers = {"Authorization": f"Bearer {token}"}
    print(f"new token: {store.headers}")
    msHalfHour = 1000 * 60 * 30
    qt.QTimer.singleShot(msHalfHour, lambda store=store: setStoreToken(store))


db = DICOMLogic.databases.ctkSQLite(
        dbDirectory,
        tagsToPrecache=slicer.dicomDatabase.tagsToPrecache,
        tagsToExcludeFromStorage=slicer.dicomDatabase.tagsToExcludeFromStorage
)
store = DICOMLogic.stores.DICOMwebStore(db, url)
setStoreToken(store)

# get a list studyInstanceUIDs
headers = {"Authorization": f"Bearer {getGCPToken()}"}
studyRequestLimit = 10
studyRequestOffset = 0
studyInstanceUIDs = []
while studyRequestOffset < overallStudyLimit:
    studiesURL = f"{url}/studies?limit={studyRequestLimit}&offset={studyRequestOffset}"
    studiesRequest = requests.get(studiesURL, headers=headers)
    if studiesRequest.content == b'':
        break
    studies = json.loads(studiesRequest.content)
    for study in studies:
        studyDataset = pydicom.Dataset.from_json(study)
        studyInstanceUIDs.append(studyDataset.StudyInstanceUID)
    studyInstanceUIDs.append(studyDataset.StudyInstanceUID)
    studyRequestOffset += studyRequestLimit
    print(f"{len(studyInstanceUIDs)} so far")

studiesInDB = []
db = slicer.dicomDatabase
for patient in db.patients():
    for study in db.studiesForPatient(patient):
        studiesInDB.append(study)

failedStudyInstanceUIDs = []
studyCount = 0
for studyInstanceUID in studyInstanceUIDs:
    studyCount += 1
    slicer.util.showStatusMessage(f"Storing {studyInstanceUID}, {studyCount} of {len(studyInstanceUIDs)}")
    slicer.app.processEvents()
    if studyInstanceUID in studiesInDB:
        continue
    setStoreToken(store)
    try:
        store.indexStudy(studyInstanceUID)
    except:
        print(f"indexing failed on {studyInstanceUID}")
        failedStudyInstanceUIDs.append(studyInstanceUID)

slicer.util.selectModule("DICOM")

if not hasattr(slicer.modules, "dicomURLHandlers"):
    slicer.modules.dicomURLHandlers = {}
slicer.modules.dicomURLHandlers["https"] = store

print(f"Index time = {time.time() - startTime}")
print(f"{len(failedStudyInstanceUIDs)} studies had a failure")

