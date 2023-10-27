# DICOMLogic
A python package of logic for working with dicom

This package was formed by factoring out or reimplementing and expanding on
code developed over the last several years by the [3D Slicer](https://slicer.org)
and [CommonTK](https://commontk.org) projects.

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

## Usage

This package is currently under development.  Use it from github in Slicer with:
```
pip_install("git+https://github.com/pieper/DICOMLogic")
```
