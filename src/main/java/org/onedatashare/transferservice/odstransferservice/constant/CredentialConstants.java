package org.onedatashare.transferservice.odstransferservice.constant;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;

import java.util.HashSet;

public class CredentialConstants {
    public static final HashSet<EndpointType> ACCOUNT_CRED_TYPE = new HashSet<>(Arrays.asList(new EndpointType[]{
            EndpointType.s3, EndpointType.ftp, EndpointType.http, EndpointType.sftp
    }));

    public static final HashSet<EndpointType> OAUTH_CRED_TYPE = new HashSet<>(Arrays.asList(new EndpointType[]{
            EndpointType.box, EndpointType.dropbox, EndpointType.gdrive, EndpointType.gridftp
    }));
}
