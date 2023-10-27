# DICOMLogic
A python package of logic for working with dicom

This package was formed by factoring out or reimplementing and expanding on
code developed over the last several years by the [3D Slicer](https://slicer.org)
and [CommonTK](https://commontk.org) projects.

## Initial Use Cases

The initial driving application for this package is to:
* Populate an SQLite database according to the [ctkDICOM schema]([url](https://github.com/commontk/CTK/blob/master/Libs/DICOM/Core/Resources/dicom-schema.sql)) using metadata obtained from [DICOMweb]([url](https://www.dicomstandard.org/using/dicomweb)) or [AWS Health Imaging]([url](https://aws.amazon.com/healthimaging/)).
  * This may be generalized in the future to obtain metadata with pydicom or DIMSE.
  * This may be generalized to other database back ends in the future
* Provide the ability to load bulk data (such PixelData) from networked sources such as DICOMweb and AHI.
  * This may be generalized to load from local files in the future
* Provide adaptors to provide higher-level data structures assembled from the contents of DICOM instances.  These adapters will be modeled after the functionality of [3D Slicer's DICOM Plugins]([url](https://slicer.readthedocs.io/en/latest/user_guide/modules/dicom.html)) but with only python native dependencies.  The outputs of these adaptors can either be output in standard research formats like nrrd, or consumed directly by applications as python buffers with metadata.  The adaptors will provide DICOM consistency checks, such as checking that slices are parallel and equally spaced when exporting a volume.
  * Like the plugins in 3D Slicer, it will be possible to extend the adaptor infrastucture to support various ways of interpreting the DICOM data as needed in various application scenarios.

## Motivations
The 3D Slicer and CTK DICOM infrastructure developed over the years to handle many important processing
tasks such as managing a database of dicom instances, organizing collections of instances
for high level processing, and interacting with dicom archives via DIMSE and DICOMweb.
However these features were built on heavyweight C++ dependencies, including Qt, DCMTK, ITK, and VTK.

This package aims to extract or replicate the core logic from CTK and 3D Slicer into a python package
with few dependencies so that it can easily be used for purposes like managing a set of DICOM data
in a colab notebook and feeding it to a machine learning system.  A goal is to also use this
package inside 3D Slicer where possible so that the code that remains inside 3D Slicer is responsible
only for the parts that map the processed dicom into and out of 3D Slicer-specific datastructures such
as the MRML Scene database.

While being powerful and capble of handling a wide range of real-world DICOM tasks, such
as interpreting time-series volume acquisitions, radiotherapy plans, PET/CT scans, etc. the
3D Slicer / CTK DICOM infrastructure is in many ways hard-coded to work with DICOM instances
stored in binary files on a local hard disk (in so-called "part 10" format).  This DICOMLogic
package is designed to generalize the architecture so that DICOM data can come from a variety
of sources, such as DICOMweb, and with metadata stored in various databases for analysis
and pixel data loaded on demand.

## Usage

This package is currently under development.  Use it from github in Slicer with:
```
pip_install("git+https://github.com/pieper/DICOMLogic")
```
