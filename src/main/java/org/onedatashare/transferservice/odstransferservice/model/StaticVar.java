package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Getter;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.OAuthEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.ws.Endpoint;
import java.util.HashMap;
import java.util.Map;


public class StaticVar {
    static Logger logger = LoggerFactory.getLogger(StaticVar.class);
    @Getter
    @Setter
    static Map<String, Long> hm = new HashMap<>();

    //TODO: Must remove these and find way to pass to reader and writer
    @Getter
    @Setter
    private static AccountEndpointCredential vfsSource;
    @Getter
    @Setter
    private static AccountEndpointCredential vfsDest;
    @Getter
    @Setter
    private static OAuthEndpointCredential oauthSource;
    @Getter
    @Setter
    private static OAuthEndpointCredential oauthDest;
    public static int sourceFlag = 0; //If 1 then the credential for the job is an Account Type if its a 2 then the source credential is oauthSource
    public static int destFlag = 0; //If 1 then the credential for the job is an Account Type if its a 2 then the source credential is oauthSource

    public static String sPass = "";
    public static String dPass = "";

    public static void setVfsSource(AccountEndpointCredential cred){
        vfsSource = cred;
        sourceFlag = 1;
    }

    public static void setOauthSource(OAuthEndpointCredential oauth){
        oauthSource = oauth;
        sourceFlag = 2;
    }

    public static void setVfsDest(AccountEndpointCredential cred){
        vfsDest = cred;
        destFlag = 1;
    }
    public static void setOauthDest(OAuthEndpointCredential oauth){
        oauthDest = oauth;
        destFlag = 2;
    }

    public static EndpointCredential getSourceCred(){
        if(vfsSource != null){
            sourceFlag = 1;
            return vfsSource;
        }else if(oauthSource != null){
            sourceFlag = 2;
            return oauthSource;
        }
        return null;
    }

    public static EndpointCredential getDestCred(){
        if(vfsDest != null){
            destFlag = 1;
            return vfsDest;
        }else if(oauthDest != null){
            destFlag = 2;
            return oauthDest;
        }
        return null;
    }

    public static void clearAllStaticVar() {
        logger.info("Resetting all static variable");
        hm.clear();
        vfsDest = null;
        vfsSource = null;
        oauthDest = null;
        oauthSource = null;
        sPass = "";
        dPass = "";
    }
}
