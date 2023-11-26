import sys
import time

# Slicer default database location
dbDirectory = "/home/ubuntu/Documents/SlicerDICOMDatabase"

# - requires AWS_DEFAULT_REGION AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY in env
datastoreId = "8cc611428069435e9f047957e4d15b22"

startTime = time.time()
try:
    import DICOMLogic
except ModuleNotFoundError:
    print("Installing DICOMLogic:")
    pip_install("git+https://github.com/pieper/DICOMLogic")
    import DICOMLogic
    print(f"Install time = {time.time() - startTime}")

# initialize database
db = DICOMLogic.databases.ctkSQLite(
        dbDirectory,
        tagsToPrecache=slicer.dicomDatabase.tagsToPrecache,
        tagsToExcludeFromStorage=slicer.dicomDatabase.tagsToExcludeFromStorage
)

# dicom store for testing
store = DICOMLogic.stores.DICOMAHIStore(db, datastoreId)

if not hasattr(slicer.modules, "dicomURLHandlers"):
    slicer.modules.dicomURLHandlers = {}
slicer.modules.dicomURLHandlers["ahi"] = store

if sys.argv[1] == "--index":
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
