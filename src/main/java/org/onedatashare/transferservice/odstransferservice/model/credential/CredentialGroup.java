package org.onedatashare.transferservice.odstransferservice.model.credential;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;

import java.util.Arrays;
import java.util.HashSet;

public class CredentialGroup {
        public static final HashSet<EndpointType> ACCOUNT_CRED_TYPE = new HashSet<>(Arrays.asList(EndpointType.s3, EndpointType.ftp, EndpointType.http, EndpointType.sftp));

        public static final HashSet<EndpointType> OAUTH_CRED_TYPE = new HashSet<>(Arrays.asList(EndpointType.box, EndpointType.dropbox, EndpointType.gdrive, EndpointType.gftp));
}
