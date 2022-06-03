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
