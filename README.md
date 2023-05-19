# Transfer-Service
The ODS Transfer Service

## Technology Stack
- Eureka <sub>(not required)</sub>
- CockroachDb<sub>(required)</sub>
- RabbitMQ <sub>(not required)</sub>
- Pmeter <sub>(not required)</sub>
- InfluxDB <sub>(not required)</sub>
to enable or disable any of these look in the application.properties file.
## Architecture
Let's begin with the understanding of what this service does. Strictly speaking the Transfer-Service moves data using many threads.
Before we get into the internals lets look at an api example.
```
{
    "ownerId": "jacobgol@buffalo.edu", <- the users ods email of the user that is submitting the request
    "source": {
        "type": "vfs", <- dropbox, gdrive, sftp, ftp, box, s3, http, vfs, scp
        "vfsSourceCredential": {

        },
        "parentInfo": {
            "path": "/Users/jacobgoldverg/UBPINTOS", <- the folder to find all the files in the infoList
            "id": "/Users/jacobgoldverg/UBPINTOS" <- the folder to find all the files in the infoList
        },
        "infoList": [
        //each json element here is a separate file
            {
                "path": "absolute path of the file",
                "size": 11326976, <- in bytes
                "chunkSize": 11326976, <- the chunk size to use for this file
                "id" : "apache-maven-3.6.3-bin.tar" <- the name of the file
            },
            {
                "path": "go1.16beta1.darwin-amd64.tar", 
                "size": 411944960,
                "chunkSize": 411944960,
                "id" : "go1.16beta1.darwin-amd64.tar"                
            }
        ]
    },
    "destination": {
        "type": "scp", <- dropbox, gdrive, sftp, ftp, box, s3, gftp, http, vfs, scp
        "vfsDestCredential": {
            "username": "ubuntu",
            "secret": "", <- this should contain the password or the pem file
            "uri": "ipaddr:22", <- the ip address: port of the destination
            "encryptedSecret": "" <- do not use this ever
        },
        "parentInfo": {
            "path": "destination path to put data/",
            "id": "destination path to put data/"
        }
    },
    "options": {
        "concurrencyThreadCount": 3,
        "parallelThreadCount":4,
        "pipeSize": 1,
        "compress": false,
        "retry" : 1
    }
}
```
Let's begin with some verbiage clarification:
1. vfsDestCredential, vfsSourceCredential, oauthDestCredential, oauthSourceCredential are two sets of credentials to be found to access the source and the destination respectively. There is never a case where the api should have vfs and oauth, its XOR only for both source or destinatino depending on what kind of remote endpoint you are attempting to access.
2. Options, these options are fully described in the ODS-Starter documentation so please reference that.
3. Source: represents the source where we are downloading data from
4. Destination: represents the location where we are uploading data too.
5. OwnerId: This is a required field that corresponds to the user email of the onedatashare.org account

## How to run this locally.
Things to install:
1. Install Java 11
2. Install maven
3. Get the boot.sh and certs files from Jacob.

## The way this works

Please read Language Domain of Spring batch first.
Using the above json as long as you replace the values appropriately you can run a transfer.
General things to know: 1 step = 1 file, ODS parallelism= strapping a thread pool to a step, ODS concurrency= splitting steps across a thread pool.
Connections are important, for us that entails pooling the clients, which honestly might not be the best idea for the cloud provider clients, but it works and compares/beats to RClone so.


So to start this service receives a message. Either as a request through the controller or the RabbitMQ consumer.
Once we get a request, we process which means running the code in JobControl.java, which all it really does it set up the Job object with the various steps.
This is where we apply a concurrency, parallelism and pipelining(commit-count, number of read calls to 1 write) by splitting the execution of many steps across a thread pool. Once we are done defining a Job we launch the job in the JobLauncher.
Once the job starts spring batch actually keeps track of the read, skip, write, ,,, counts in CockroachDB. Which means we can run many Transfer-Services that use the same Job table.

Once we have created 

### Helpful Links 

1. [Spring Batch Docs](https://docs.spring.io/spring-batch/docs/current/reference/html/)
   Sections: 
    - (Language Domain)[https://docs.spring.io/spring-batch/docs/current/reference/html/domain.html#domainLanguageOfBatch]
    - (Multi Threaded Steps)[https://docs.spring.io/spring-batch/docs/current/reference/html/scalability.html#multithreadedStep]
    - (Parallel Steps)[https://docs.spring.io/spring-batch/docs/current/reference/html/scalability.html#scalabilityParallelSteps]
    - few others as well but no point in listing them all
    
2. [AWS Java Docs](https://github.com/aws/aws-sdk-java-v2)
    Sections
   - Only worth reading about S3 stuff nothing else.
    
3. [Jsch Examples](http://www.jcraft.com/jsch/examples/)
    Here is the thing about Jsch THERE ARE NO DOCS. I know I hate it too sorry. So we have to go off examples and 
    stackoverflow BUT its actually a damn good library b/c it works similarly to how you would expect the ssh protocol to work
    Sections
    - Only read on stuff about Scp and Sftp, unless you are doing a remote execution which case 
      I would prob use Shell or Exec docs
      
4. [Dropbox Github](https://github.com/dropbox/dropbox-sdk-java)
   
5. [Box Docs](http://opensource.box.com/box-java-sdk/)

6. [Google Drive Docs](https://developers.google.com/drive/api/quickstart/java)
   
7. [Influx Docs](https://github.com/influxdata/influxdb-java)

8. [Spring JPA](https://docs.spring.io/spring-data/jpa/docs/current/reference/html/)
    I would only look this over if you plan on working with the Data otherwise not very necessary

## Features Implemented - Spring 2023

| No.| Feature                                                     | Branch name                                                                                           |
|----|-------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| 1  | HTTP Pipelining for Reader using Apache HTTP client library | [origin/vn/httppipelining](https://github.com/didclab/Transfer-Service/tree/origin/vn/httppipelining) |
| 2  | Added support for WebDAV reader and writer                  | [origin/vn/webdav](https://github.com/didclab/Transfer-Service/tree/origin/vn/webdav)                 |
