# LargeFileZipperAndUploader

### Description

This programme continuously zipping a large file and uploading it 
to S3 in a memory-efficient manner using streams and multithreading.
It  compresses the file on-the-fly and utilizes 
Amazon S3's multipart upload to handle the large size.
### Configuration 

* Clone the project
* configure your credential aws " access key and secret key in your .aws credentail localy
* set your largeFilePath, bucketName, keyName( (with the name you want for your file in the s3 destination)) in the code 

