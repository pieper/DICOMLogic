import json
import logging
import pydicom
import random
import requests
import time

dbDirectory = "/opt/tmp/db"

# for development
print("Installing DICOMLogic:")
startTime = time.time()
import shutil, os
shutil.rmtree("/opt/sr/python-install/lib/python3.9/site-packages/DICOMLogic")
shutil.rmtree(dbDirectory)
os.mkdir(dbDirectory)
pip_install("--upgrade /Users/pieper/slicer/latest/DICOMLogic")
import DICOMLogic
print(f"Install time = {time.time() - startTime}")


# initialize database
db = DICOMLogic.databases.ctkSQLite(
        dbDirectory,
        tagsToPrecache=slicer.dicomDatabase.tagsToPrecache,
        tagsToExcludeFromStorage=slicer.dicomDatabase.tagsToExcludeFromStorage
)

# dicom store for testing
# - requires AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY in env
datastoreId = "8cc611428069435e9f047957e4d15b22"
store = DICOMLogic.stores.DICOMAHIStore(db, datastoreId)

if not hasattr(slicer.modules, "dicomURLHandlers"):
    slicer.modules.dicomURLHandlers = {}
slicer.modules.dicomURLHandlers["ahi"] = store

print("Indexing database:")
startTime = time.time()
store.indexDatastore()
print(f"Indexing time = {time.time() - startTime}")

print("Opening database:")
startTime = time.time()
slicer.dicomDatabase.openDatabase(dbDirectory + "/ctkDICOM.sql")
print(f"Opening time = {time.time() - startTime}")

print("Updating Display:")
startTime = time.time()
slicer.dicomDatabase.updateDisplayedFields()
print(f"Updating time = {time.time() - startTime}")

print("Selecting module:")
startTime = time.time()
slicer.util.selectModule("DICOM")
print(f"Selecting time = {time.time() - startTime}")
print("Finish")
