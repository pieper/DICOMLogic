import functools
import json
import logging
import numpy as np
import pydicom
import requests

try:
    import qt
except ModuleNotFoundError:
    pass

import DICOMLogic
from DICOMLogic.stores.DICOMStore import DICOMStore

class DICOMwebStore(DICOMStore):

    def __init__(self, db, url, headers={}):
        self.db = db
        self.url = url
        self.headers = headers
        self.framesByURL = {}
        try:
            import qt
            self.networkAccessManager = qt.QNetworkAccessManager()
            self.urlsByReply = {}
            self.networkAccessManager.connect("finished(QNetworkReply*)", self.handleQtReply)
            self.http2Allowed = True
            self._haveQT = True
        except ModuleNotFoundError:
            self._haveQT = True

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
        self.db.startBatchInsert()
        metadataRequest = f"{self.url}/studies/{studyInstanceUID}/metadata"
        studyMetadataRequest = requests.get(metadataRequest, headers=self.headers)
        studyMetadata = json.loads(studyMetadataRequest.content)
        for instanceData in studyMetadata:
            instanceDataset = pydicom.Dataset.from_json(instanceData)
            self.indexInstance(instanceDataset)
        self.db.endBatchInsert()

    def frameFromReplyContent(self, url, content):
        delimiter = b"\r\n\r\n"
        frameStart = content.find(delimiter) + len(delimiter)
        delimiter = b"\r\n"
        frameEnd = content.rfind(delimiter, 0, content.rfind(delimiter))
        frameContent = content[frameStart:frameEnd]
        try:
            frame = np.frombuffer(frameContent, dtype='int16')
            self.framesByURL[url] = frame
            return True
        except ValueError:
            return False

    def makeQtRequest(self, url):
        request = qt.QNetworkRequest(qt.QUrl(url))
        request.setAttribute(request.HTTP2AllowedAttribute, self.http2Allowed)
        for name,value in self.headers.items():
            request.setRawHeader(name, value)
        reply = self.networkAccessManager.get(request)
        self.urlsByReply[reply] = url

    def startRequest(self, urls):
        """
        Retrieve frames based on URLs

        urls must all be from the same data store, study, and series.  I.e.
        same image set.
        """
        for url in urls:
            if self._haveQT:
                self.makeQtRequest(url)
            else:
                urlResponse = requests.get(url, headers=self.headers)
                content = urlResponse.content
                try:
                    self.frameFromReplyContent(url, content)
                except:
                    print(f"failed for {url}")

    def handleQtReply(self, reply):
        if reply in self.urlsByReply:
            if reply.error() != qt.QNetworkReply.NoError:
                print(f"Error is {reply.error()}")
            url = self.urlsByReply[reply]
            del(self.urlsByReply[reply])
            content = reply.readAll().data()
            if not self.frameFromReplyContent(url, content):
                logging.debug(f"Resending request for {url}")
                self.makeQtRequest(url)

    def getFrames(self, requestedURLs):
        """
        Returns any available frames corresponding to requested URLs.
        Because the frames may have been requested by multiple requesters,
        only return the frames corresponding to the ones in the urls parameter
        and save the others for later.  Remove the urls for returned frames
        from the list of frames by url.
        TODO: handle any error codes
        """
        framesByURLForURLs = {}
        for url in self.framesByURL.keys():
            if url in requestedURLs:
                framesByURLForURLs[url] = self.framesByURL[url]
        for url in framesByURLForURLs:
            del(self.framesByURL[url])
        return(framesByURLForURLs)

    def requestFinished(self):
        if self._haveQT:
            return len(self.urlsByReply) == 0
        else:
            # for sync requests
            return True

    #
    # infrastructure for getting instance metadata from dicom store
    # here, file is really a frameURL since we are mocking the ctkDICOMDatabase API
    # so we use the method provide by the DICOMStore to fetch metadata
    # and store it here for faster access
    #

    @functools.lru_cache(maxsize=100)
    def seriesMetadata(self, seriesURL):
        seriesRequest = requests.get(seriesURL, headers=self.headers)
        seriesMetadata = json.loads(seriesRequest.content)
        tag = DICOMLogic.databases.DICOMDatabase.dicomTagNoComma("SOPInstanceUID")
        instanceMetadataByUID = {}
        for metadata in seriesMetadata:
            instanceUID = metadata[tag]["Value"][0]
            instanceMetadataByUID[instanceUID] = metadata
        return(instanceMetadataByUID)

    @functools.lru_cache(maxsize=10000)
    def instanceMetadata(self, frameURL):
        seriesURL = frameURL[:frameURL.find("instances")] + "metadata"
        instanceMetadataByUID = self.seriesMetadata(seriesURL)
        instanceUID = frameURL[frameURL.find("instances/"):].split("/")[1]
        return(instanceMetadataByUID[instanceUID])

    @functools.lru_cache(maxsize=10000)
    def fileValue(self, file, tag):
        metadata = self.instanceMetadata(file)
        tag = tag.replace(",", "").upper()
        value = ""
        if tag in metadata:
            value = metadata[tag]["Value"]
            if len(value) == 1:
                value = str(value[0])
            else:
                value = "\\".join(map(str, value))
        return(value)

    def fileValueExists(self, file, tag):
        return(self.fileValue(file, tag) != "")
