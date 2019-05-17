# awsFileDownload
This is an example class to demonstrate how we can download compressed file from aws and persist the whole data frame into database with less than 10-15 lines of code.

In this working example,created wrapper classes over aws S3 File system, sqlalchemy and turbodbc. For more details on these modules please 
refer to their documentation but with these wrappers most likely you won't need that. One other observation using turbodbc over sqlalchemy is that, turbodbc is 50-60 times faster than sqlalchemy.



