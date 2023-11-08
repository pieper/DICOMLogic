import copy
import gzip
import json
import logging
import numpy as np
import os
import pydicom
import requests
import time


try:
    import boto3
except ModuleNotFoundError:
    pip_install('boto3')
    import boto3

import ahi_retrieve as ahi

from DICOMLogic.stores.DICOMStore import DICOMStore

class DICOMAHIStore(DICOMStore):

    def __init__(self, db, datastoreId=None):
        self.db = db
        self.datastoreId = datastoreId

        # Initialize the module
        config = ahi.AHIRetrieveConfig()
        config.region = os.getenv("AWS_DEFAULT_REGION")
        config.awsAccessKeyId = os.getenv('AWS_ACCESS_KEY_ID')
        config.awsSecretAccessKey = os.getenv('AWS_SECRET_ACCESS_KEY')
        config.numDownloadThreads = 1
        config.numDecodeThreads = 10
        config.numDecodeThreads = 1
        config.logLevel = 4 # INFO
        config.logLevel = 6 # TRACE
        config.logLevel = 0 # None
        ahi.init(config)

        # Create handler
        self.handler = ahi.AHIRequestHandler()
        self.client = boto3.client("medical-imaging")

    def indexImageSet(self, imageSetMetadata):
        self.db.startBatchInsert()
        dataset = pydicom.Dataset()
        levels = ['Patient', 'Study']
        for level in levels:
            for tagName,value in imageSetMetadata[level]['DICOM'].items():
                vr = pydicom.datadict.dictionary_VR(tagName)
                if vr != 'SQ':
                    dataset[tagName] = pydicom.DataElement(tagName, vr, value)
        for seriesUID in imageSetMetadata['Study']['Series']:
            seriesMetadata = imageSetMetadata['Study']['Series'][seriesUID]
            for tagName,value in seriesMetadata['DICOM'].items():
                vr = pydicom.datadict.dictionary_VR(tagName)
                if vr != 'SQ':
                    dataset[tagName] = pydicom.DataElement(tagName, vr, value)
            for instanceUID in seriesMetadata["Instances"]:
                instanceDataset = copy.deepcopy(dataset)
                instanceMetadata = seriesMetadata['Instances'][instanceUID]
                instanceDICOMData = instanceMetadata['DICOM']
                for tagName,value in instanceDICOMData.items():
                    if pydicom.datadict.dictionary_has_tag(tagName):
                        vr = pydicom.datadict.dictionary_VR(tagName)
                        if vr != 'SQ':
                            instanceDataset[tagName] = pydicom.DataElement(tagName, vr, value)
                frameURL = f"ahi://{self.datastoreId}"
                frameURL += f"/{imageSetMetadata['ImageSetID']}"
                frameURL += f"/{instanceDataset.SeriesInstanceUID}"
                frameURL += f"/{instanceDataset.SOPInstanceUID}"
                if len(instanceMetadata['ImageFrames']) > 0:
                    frameURL += f"/{instanceMetadata['ImageFrames'][0]['ID']}"
                else:
                    frameURL += "/TODO-non-image-instance"
                self.db.insert(instanceDataset, frameURL)
        self.db.endBatchInsert()

    def indexDatastore(self):
        """
        Get all image sets in the AHI DICOM datastore and
        insert all the instances in to the database.
        """
        searchCriteria= {
            "filters" : [{
                "operator": "BETWEEN",
                "values":[
                    {"createdAt": "1985-04-12T23:20:50.52Z"},
                    {"createdAt": "2024-01-12T23:20:50.52Z"}
            ]}
        ]}
        response = self.client.search_image_sets(
                    datastoreId=self.datastoreId,
                    searchCriteria=searchCriteria)
        for imageSetsMetadataSummary in response['imageSetsMetadataSummaries']:
            metadataResponse = self.client.get_image_set_metadata(
                    datastoreId = self.datastoreId,
                    imageSetId = imageSetsMetadataSummary['imageSetId'])
            gzippedMetadata = metadataResponse['imageSetMetadataBlob'].read()
            imageSetJSON = gzip.decompress(gzippedMetadata)
            imageSetMetadata = json.loads(imageSetJSON)
            self.indexImageSet(imageSetMetadata)

    def startRequest(self, urls):
        """
        Retrieve frames based on URLs

        urls must all be from the same data store, study, and series.  I.e.
        same image set.
        """
        url0 = urls[0]
        _, _, datastoreId, imageSetId, seriesUID, sopInstanceID, imageFrameId = url0.split('/')
        ahiRequest = {}
        ahiRequest['DatastoreID'] = datastoreId
        ahiRequest['ImageSetID'] = imageSetId
        ahiRequest['Study'] = {'Series': {seriesUID: {'Instances': {}}}}
        self.urlsByImageFrameID = {}
        for url in urls:
            _, _, datastoreId, imageSetId, seriesUID, sopInstanceID, imageFrameId = url.split('/')
            ahiRequest['Study']['Series'][seriesUID]['Instances'][sopInstanceID] = {}
            ahiRequest['Study']['Series'][seriesUID]['Instances'][sopInstanceID]['ImageFrames'] = []
            frames = ahiRequest['Study']['Series'][seriesUID]['Instances'][sopInstanceID]['ImageFrames']
            frames.append({"ID": imageFrameId, "FrameSizeInBytes": 0})
            self.urlsByImageFrameID[imageFrameId] = url
        self.handler.request_frames(json.dumps(ahiRequest))

    def getFrames(self):
        """
        Returns any available frames corresponding to requested URLs.
        TODO: handle any error codes
        """
        framesByURL = {}
        responses = self.handler.get_frame_responses()
        for i in responses:
            data = np.array(i, copy = False)
            url = self.urlsByImageFrameID[i.imageFrameId]
            del(self.urlsByImageFrameID[i.imageFrameId])
            framesByURL[url] = data
        return(framesByURL)

    def requestFinished(self):
        # TODO: return self.handler.is_busy() == False
        return len(self.urlsByImageFrameID) == 0
