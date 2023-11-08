import json
import logging
import pydicom
import random
import requests
import subprocess
import time

dbDirectory = "/opt/tmp/db"

# for development
print("Installing DICOMLogic:")
startTime = time.time()
import shutil, os
try:
    shutil.rmtree("/opt/sr/python-install/lib/python3.9/site-packages/DICOMLogic")
except FileNotFoundError:
    pass
shutil.rmtree(dbDirectory)
os.mkdir(dbDirectory)
pip_install("--upgrade /Users/pieper/slicer/latest/DICOMLogic")
import DICOMLogic
print(f"Install time = {time.time() - startTime}")

# url = "https://d33do7qe4w26qo.cloudfront.net/dicomweb"
# TODO: this url is not for general use!
url = "https://testing-proxy.canceridc.dev/current/viewer-only-no-downloads-see-tinyurl-dot-com-slash-3j3d9jyp/dicomWeb"

url = "https://healthcare.googleapis.com/v1/projects/bwh-lnq-gcp-1620235879/locations/us-central1/datasets/time-batch2/dicomStores/time-batch2/dicomWeb"
url = "https://healthcare.googleapis.com/v1/projects/bwh-lnq-gcp-1620235879/locations/us-central1/datasets/sdp-performance-testing/dicomStores/sdp-performance-testing/dicomWeb"
command = "gcloud auth print-access-token"
tokenProcess = subprocess.run(command, capture_output=True, shell=True, text=True)
token = tokenProcess.stdout.strip()

testStudyInstanceUID = "1.3.6.1.4.1.14519.5.2.1.6279.6001.224985459390356936417021464571"
testStudyInstanceUID = None

# applies when testStudyInstanceUID is None
#studyRequestLimit = 1000
studyRequestLimit = 10
#studyRequestOffset = 1000
studyRequestOffset = 0

db = DICOMLogic.databases.ctkSQLite(
        dbDirectory,
        tagsToPrecache=slicer.dicomDatabase.tagsToPrecache,
        tagsToExcludeFromStorage=slicer.dicomDatabase.tagsToExcludeFromStorage
)
headers = {"Authorization": f"Bearer {token}"}
store = DICOMLogic.stores.DICOMwebStore(db, url, headers=headers)

# get a list studyInstanceUIDs
studyInstanceUIDs = []
studiesURL = f"{url}/studies?limit={studyRequestLimit}&offset={studyRequestOffset}"
studiesRequest = requests.get(studiesURL, headers=headers)
studies = json.loads(studiesRequest.content)
for study in studies:
    studyDataset = pydicom.Dataset.from_json(study)
    studyInstanceUIDs.append(studyDataset.StudyInstanceUID)

if testStudyInstanceUID:
    studyInstanceUIDs = [testStudyInstanceUID]

for studyInstanceUID in studyInstanceUIDs:
    print(f"Storing {studyInstanceUID}")
    store.indexStudy(studyInstanceUID)

slicer.util.selectModule("DICOM")

if not hasattr(slicer.modules, "dicomURLHandlers"):
    slicer.modules.dicomURLHandlers = {}
slicer.modules.dicomURLHandlers["https"] = store
