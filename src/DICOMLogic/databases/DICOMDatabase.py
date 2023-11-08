import pydicom

class DICOMDatabase:
    """
    Abstract interface to implementations of
    databases of DICOM metadata about instances.
    Also contains some generally useful utilities.
    Modeled after the API of ctkDICOMDatabase
    """

    def __init__(self):
        pass

    def dicomTagNoComma(keyword : str):
        noCommaTag = "{:08X}".format(pydicom.datadict.tag_for_keyword(keyword))
        return noCommaTag

    def dicomTagWithComma(keyword : str):
        noCommaTag = DICOMDatabase.dicomTagNoComma(keyword)
        return f"{noCommaTag[0:4]},{noCommaTag[4:8]}"

    def insert(self, ds : pydicom.Dataset, frameURL : str):
        raise NotImplementedError("Method needs to be defined by subclass")
