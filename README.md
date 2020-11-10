# Taric March end date
This application takes EU incremental Taric 3 XML files and converts
them into UK-ready ones by applying business rules to the various
objects to stop them applying to the UK after the critical date,
such that they do not conflict with the UK equivalents.

It is **critical** that the **envelope ID** of the original is maintained, as these need to
follow in sequence and need to start where the master file left off.

Usage
-----
There are two Python scripts that can be run, as follows:

* convert.py - used for converting individual files
* iterate.py - used for converting multiple files in one go

### Using convert.py

1. Most simply, use **py convert.py filename** where filename refers to an EU XML file in the 
    *xml_in* folder. This just converts a single EU file into a UK-specific equivalent
    without attempting to merge any UK-specific content into the file.

2. To merge additional files into the converted EU file, additional arguments can be specified
    on convert.py, as follows:

  py convert.py filename filename2 filename3 filename4

  * where filename2 and filename3 are Taric 3 XML files located in the **../migrate-reference-data/xml** folder.

  * Up to 3 additional files can be merged

### Using iterate.py

This Python script is used to iterate through multiple files in the **xml_in** folder. It runs **convert.py** multiple times according to what files are in the **xml_in** folder.