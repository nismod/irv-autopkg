# Job Queue Spike

Plan to use the file structure as the source of truth about whats processed - we wont try to sync this into the DB

All we'll have is the job queue results to tell us about job processing

TODO:

* Processor for setting up top-level structure (also methods for checking existance of this structure)
* ABC / Base methods for checking existance of files at the given processors version (to be used during processing and GET API calls)
* ABC / Base methods for generating file structure
* ABC Run method for e2e processing
* Non ABC methods for the actual raster processing
* Multiple Backend classes - localfs, S3, Zenodo?