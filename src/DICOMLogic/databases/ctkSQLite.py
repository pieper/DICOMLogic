import datetime
import json
import logging
import numpy
import os
import pydicom
import random
import requests
import sqlite3
import time

from DICOMLogic.databases.DICOMDatabase import DICOMDatabase

class ctkSQLite(DICOMDatabase):
    """
    Reimplementation and generalization of ctkDICOMDatabase.
    This class can be used to populate and query a database
    following the ctkDICOMSchema for patient/study/series/instance
    level access and provides a TagCache for efficient access
    to a pre-defined set of instance tags.

    If needed, this code creates the sqlite databases for the
    metadata and the tagcache using the
    the ctkDICOMDatabase schemas that are hard-coded with the ctkDICOM
    library.  These need to be kept in sync manually to ensure
    compatibility.
    """

    SchemaURL = "https://raw.githubusercontent.com/commontk/CTK/master/Libs/DICOM/Core/Resources/dicom-schema.sql"
    SchemaVersion = "0.7.0"

    #/// Flag for tag cache to avoid
    # repeated searches for tags that do no exist
    TagNotInInstance = "__TAG_NOT_IN_INSTANCE__"
    #/// Flag for tag cache indicating that the
    # value really is the empty string
    ValueIsEmptyString = "__VALUE_IS_EMPTY_STRING__"
    # /// Tag exists in the instance and non-empty
    # but its value is not stored (e.g., because it is too long)
    ValueIsNotStored = "__VALUE_IS_NOT_STORED__"

    RequiredTags = ["PatientName", "PatientID", "PatientBirthDate",
                    "PatientSex", "StudyID", "StudyDate", "StudyTime",
                    "StudyDescription", "AccessionNumber", "ModalitiesInStudy",
                    "InstitutionName", "ReferringPhysicianName",
                    "PerformingPhysicianName", "SeriesInstanceUID",
                    "StudyInstanceUID", "SeriesNumber", "SeriesDate",
                    "SeriesTime", "SeriesDescription", "Modality",
                    "BodyPartExamined", "FrameOfReferenceUID",
                    "AcquisitionNumber", "ContrastBolusAgent",
                    "ScanningSequence", "EchoNumbers",
                    "TemporalPositionIdentifier", "SOPInstanceUID",
                    "SeriesInstanceUID", "ContentDate",
                    "Manufacturer", "PatientPosition"]

    DatabaseFileName = "ctkDICOM.sql"
    TagCacheDatabaseFileName = "ctkDICOMTagCache.sql"

    def __init__(self, dbDirectory,
                 tagsToPrecache = (),
                 tagsToExcludeFromStorage = ()):
        self.dbDirectory = dbDirectory
        self.databaseFilePath = os.path.join(self.dbDirectory,
                                             ctkSQLite.DatabaseFileName)
        self.tagCacheFilePath = os.path.join(self.dbDirectory,
                                             ctkSQLite.TagCacheDatabaseFileName)
        self.tagsToPrecache = tagsToPrecache
        self.tagsToExcludeFromStorage = tagsToExcludeFromStorage
        self.databaseInitialized = False
        self.dbConnection = None
        self.dbCursor = None
        self.patientsThisBatch = {}
        self.studiesThisBatch = []
        self.seriesThisBatch = []

    def initializeDatabase(self):
        if self.databaseInitialized:
            return True
        dbConnection  = sqlite3.connect(self.databaseFilePath)
        cursor = dbConnection.cursor()
        # populate the schema if needed
        try:
            cursor.execute("SELECT Version from SchemaInfo LIMIT 1")
            schemaVersion = cursor.fetchone()[0]
            if schemaVersion != ctkSQLite.SchemaVersion:
                msg = f"Database has wrong schema.  "
                msg += f"Expected {ctkSQLite.SchemaVersion}, but found {schemaVersion}."
                logging.error(msg)
                logging.error("Aborting initialization operation")
                return False
        except sqlite3.OperationalError:
            logging.warn("Initializing Database")
            schemaResponse = requests.get(ctkSQLite.SchemaURL)
            schema = schemaResponse.content.decode()
            cursor.executescript(schema)
            dbConnection.commit()
        self.databaseInitialized = True
        return True

    #staticmethod
    def uidsForDataset(ds):
        """
        replicates
            ctkDICOMDatabasePrivate::uidsForDataSet(
                QString& patientsName, QString& patientID,
                QString& studyInstanceUID)
        makes sure patientsName and patientID are valid in the dataset
        """
        if ds.PatientID == "" and ds.StudyInstanceUID != "":
            msg = f"Patient ID is empty, using studyInstanceUID"
            msg += f"{ds.studyInstanceUID} (%1) as patient ID"
            logging.warn(msg)
            ds.PatientID = ds.StudyInstanceUID
        if ds.PatientName == "" and ds.PatientID != "":
            ds.PatientName = ds.PatientID
        if ds.PatientName == "" \
                or ds.StudyInstanceUID == "" \
                or ds.PatientID == "":
            msg = "Required information (patient name, patient ID, "
            msg += "study instance UID) is missing from dataset"
            logging.error(msg)
            return False
        return True

    #staticmethod
    def compositePatientID(patientID, patientName, patientBirthDate):
        """
        replicates
            QString ctkDICOMDatabase::compositePatientID(
              const QString& patientID,
              const QString& patientsName,
              const QString& patientsBirthDate)
        provides a pseudo-unique patient identifier
        """
        return f"{patientID}-{patientBirthDate}-{patientName}"

    #staticmethod
    def stringList(*values):
        return [str(value) for value in values]


    def startBatchInsert(self):
        self.dbConnection  = sqlite3.connect(self.databaseFilePath)
        self.cursor = self.dbConnection.cursor()
        self.dbTagCacheConnection  = sqlite3.connect(self.tagCacheFilePath)
        self.cursorTagCache = self.dbTagCacheConnection.cursor()

    def endBatchInsert(self):
        self.dbConnection.commit()
        self.dbTagCacheConnection.commit()
        self.cursor = None
        self.cursorTagCache = None
        self.dbConnection = None
        self.dbTagCacheConnection = None
        self.patientsThisBatch = {}
        self.studiesThisBatch = []
        self.seriesThisBatch = []

    def cacheTags(self, cacheTagValues):
        import slicer
        slicer.modules.cacheTagValues = cacheTagValues
        # TODO: remove duplicates?
        try:
            self.cursorTagCache.execute("SELECT * from TagCache LIMIT 1")
        except sqlite3.OperationalError:
            logging.warn("Initializing TagCache")
            statement = "CREATE TABLE TagCache (SOPInstanceUID, Tag, Value, PRIMARY KEY (SOPInstanceUID, Tag))"
            self.cursorTagCache.execute(statement)
        self.cursorTagCache.executemany(f"""
            INSERT OR REPLACE INTO TagCache VALUES(?,?,?)
        """, cacheTagValues)

    def insert(self, ds, frameURL):
        """
        Insert dataset into database

        Returns True if insert completed
        """

        timeFormat = "%Y-%m-%dT%H:%M:%S" # TODO: need three milliseconds?
        timestamp = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

        for tag in ctkSQLite.RequiredTags:
            if tag not in ds:
                setattr(ds, tag, "")

        if not ctkSQLite.uidsForDataset(ds):
            # minimum information is missing, can't insert
            return False

        if not self.initializeDatabase():
            # can't work with database
            return False

        patientUID = ctkSQLite.compositePatientID(
                            ds.PatientID, ds.PatientName, ds.PatientBirthDate)

        # maybe insert patient
        patientIdentifiers = (ds.PatientName, ds.PatientID)
        if patientIdentifiers not in self.patientsThisBatch:
            self.cursor.execute(f"""
                SELECT UID FROM Patients WHERE PatientsName = ? AND PatientID = ?
            """, [str(value) for value in patientIdentifiers])
            dbResult = self.cursor.fetchone()
            if dbResult is None:
                self.cursor.execute(f"""
                    INSERT INTO Patients
                    ('UID', 'PatientsName', 'PatientID', 'PatientsBirthDate',
                     'PatientsBirthTime', 'PatientsSex', 'PatientsAge',
                     'PatientsComments', 'InsertTimestamp',
                     'DisplayedPatientsName', 'DisplayedNumberOfStudies',
                     'DisplayedFieldsUpdatedTimestamp')
                    VALUES(NULL, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)
                """, ctkSQLite.stringList(ds.PatientName, ds.PatientID,
                                          ds.PatientBirthDate,
                                          "", ds.PatientSex, "",
                                          "", timestamp))
                # get the newly created dbPatientID
                self.cursor.execute(f"""
                    SELECT UID FROM Patients WHERE PatientsName = ? AND PatientID = ?
                """, [str(value) for value in [ds.PatientName, ds.PatientID]])
                dbResult = self.cursor.fetchone()
                if dbResult is None:
                    logging.error("Error insterting patient")
                    return False
            dbPatientID = dbResult[0]
            self.patientsThisBatch[patientIdentifiers] = dbPatientID
        else:
            dbPatientID = self.patientsThisBatch[patientIdentifiers]

        # always insert instance and tags to cache
        try:
            self.cursor.execute(f"""
                INSERT OR REPLACE INTO Images
                ("SOPInstanceUID", "Filename", "SeriesInstanceUID",
                 "InsertTimestamp", "DisplayedFieldsUpdatedTimestamp")
                VALUES(?, ?, ?, ?, NULL)
            """, ctkSQLite.stringList(ds.SOPInstanceUID, frameURL,
                            ds.SeriesInstanceUID, timestamp))
        except sqlite3.IntegrityError:
            logging.warn('ignoring duplicate instance error')

        # populate the tag cache
        sopInstanceID = str(ds.SOPInstanceUID)
        cacheTagValues = []
        extraKeys = ["StudyInstanceUID", "BitsAllocated", "BitsStored",
                     "PixelRepresentation", "WindowCenter", "WindowWidth",
                     "RescaleIntercept", "RescaleSlope", "ContentDate",
                     "Manufacturer", "PatientPosition"]
        extraTags = [DICOMDatabase.dicomTagWithComma(k) for k in extraKeys]
        tagsToCache = self.tagsToPrecache + tuple(extraTags)
        for tag in tagsToCache:
            # db uses comma, pydicom does not but is upper
            dsTag = tag.replace(',','').upper()
            if dsTag in ds:
                value = ds[dsTag]._value
                if value == "":
                    value = ctkSQLite.ValueIsEmptyString
                elif ds[dsTag].VR == 'DS' and ds[dsTag].VM > 1:
                    value = "\\".join(map(str, list(value)))
            else:
                value = ctkSQLite.TagNotInInstance
            if tag in self.tagsToExcludeFromStorage:
                value = ctkSQLite.ValueIsNotStored
            cacheTagValues.append((sopInstanceID, tag.upper(), str(value)))
        self.cacheTags(cacheTagValues)

        # maybe insert Series
        if ds.SeriesInstanceUID in self.seriesThisBatch:
            # series is there, so study will be too
            return True
        else:
            self.cursor.execute(f"""
                SELECT * FROM Series WHERE SeriesInstanceUID = ?
            """, [str(value) for value in [ds.SeriesInstanceUID]])
            if self.cursor.fetchone() is None:
                self.cursor.execute(f"""
                    INSERT INTO Series
                    ('SeriesInstanceUID', 'StudyInstanceUID', 'SeriesNumber',
                     'SeriesDate', 'SeriesTime', 'SeriesDescription',
                     'Modality', 'BodyPartExamined', 'FrameOfReferenceUID',
                     'AcquisitionNumber', 'ContrastAgent', 'ScanningSequence',
                     'EchoNumber', 'TemporalPosition', 'InsertTimestamp')
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, ctkSQLite.stringList(ds.SeriesInstanceUID, ds.StudyInstanceUID,
                                ds.SeriesNumber, ds.SeriesDate, ds.SeriesTime,
                                ds.SeriesDescription, ds.Modality,
                                ds.BodyPartExamined, ds.FrameOfReferenceUID,
                                ds.AcquisitionNumber, ds.ContrastBolusAgent,
                                ds.ScanningSequence, ds.EchoNumbers,
                                ds.TemporalPositionIdentifier, timestamp))
            # series either already in db or was just inserted
            self.seriesThisBatch.append(ds.SeriesInstanceUID)

        # maybe insert Study
        if ds.StudyInstanceUID not in self.studiesThisBatch:
            self.cursor.execute(f"""
                SELECT * FROM Studies WHERE StudyInstanceUID = ?
            """, [str(value) for value in [ds.StudyInstanceUID]])
            if self.cursor.fetchone() is None:
                self.cursor.execute(f"""
                    INSERT INTO Studies
                    ('StudyInstanceUID', 'PatientsUID', 'StudyID',
                     'StudyDate', 'StudyTime', 'AccessionNumber',
                     'ModalitiesInStudy', 'InstitutionName',
                     'ReferringPhysician', 'PerformingPhysiciansName',
                     'StudyDescription', 'InsertTimestamp',
                     'DisplayedNumberOfSeries', 'DisplayedFieldsUpdatedTimestamp')
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)
                """, ctkSQLite.stringList(ds.StudyInstanceUID, dbPatientID,
                                ds.StudyID, ds.StudyDate, ds.StudyTime,
                                ds.AccessionNumber, ds.ModalitiesInStudy,
                                ds.InstitutionName, ds.ReferringPhysicianName,
                                ds.PerformingPhysicianName, ds.StudyDescription,
                                timestamp))
            self.studiesThisBatch.append(ds.StudyInstanceUID)
        return True
