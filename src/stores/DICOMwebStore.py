import json
import logging
import pydicom
import requests

from DICOMLogic.stores.DICOMStore import DICOMStore

class DICOMwebStore(DICOMStore):

    def __init__(self, db, url, headers={}):
        self.db = db
        self.url = url
        self.headers = headers

    def indexInstance(self, instanceDataset):
        frameURL = f"{self.url}/studies/{instanceDataset.StudyInstanceUID}"
        frameURL += f"/series/{instanceDataset.SeriesInstanceUID}"
        frameURL += f"/instances/{instanceDataset.SOPInstanceUID}/frames/1"
        self.db.insert(instanceDataset, frameURL)

    def indexStudy(self, studyInstanceUID):
        """
        TODO: headers
        studyMetadataRequest = requests.get(metadataRequest, headers=self.headers)
        """
        metadataRequest = f"{self.url}/studies/{studyInstanceUID}/metadata"
        studyMetadataRequest = requests.get(metadataRequest)
        studyMetadata = json.loads(studyMetadataRequest.content)
        for instanceData in studyMetadata:
            instanceDataset = pydicom.Dataset.from_json(instanceData)
            self.indexInstance(instanceDataset)
