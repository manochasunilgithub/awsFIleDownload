# awsFileDownload
This is an example class to demonstrate how we can download compressed file from aws and persist the whole data frame into database with less than 10-15 lines of code.

In this working example,created wrapper classes over aws S3 File system, sqlalchemy and turbodbc. For more details on these modules please 
refer to their documentation but with these wrappers most likely you won't need that. One other observation using turbodbc over sqlalchemy is that, turbodbc is 50-60 times faster than sqlalchemy.

One thing to note that turbodb does not like null values and is not very explicit what record we are having an error and it could be nightmare to trouble shoot these errors.

Trouble shooting tips:
 1. Make sure column names of data frame matches the table columns
 2. remove extra spaces from the columns
 3. My file had spaces whereas db schema had _ so I transformed the data
 4. turbo db does not like null so drop them
 
 df.columns = df.columns.str.strip()  # remove white spaces around column names
 df.columns = df.columns.str.replace(' ', '_')
 df = df.replace('', np.nan)  # map nans, to drop NAs rows and columns later
 df = df.dropna(how='all', axis=0)  # remove rows containing only NAs
 df = df.dropna(how='all', axis=1)  # remove columns containing only NAs
