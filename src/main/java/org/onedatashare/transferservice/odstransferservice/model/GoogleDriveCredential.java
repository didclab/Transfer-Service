//package org.onedatashare.transferservice.odstransferservice.model;
//
//import lombok.*;
//
//import javax.validation.constraints.NotNull;
//import java.util.List;
//
//@Data
//@NoArgsConstructor
//@Builder
//public class GoogleDriveCredential {
//    private DriveKey installed;
//
//    @Override
//    public String toString() {
//        return "GoogleDriveCredential{" +
//                "installed=" + installed +
//                '}';
//    }
//
//    @Data
//    @NoArgsConstructor
//    @Builder
//    public static class DriveKey {
//        @NotNull private String clientID;
//        @NotNull private String projectID;
//        @NotNull private String auth_uri;
//        @NotNull private String token_uri;
//        @NotNull private String auth_provider_x509_cert_url;
//        @NotNull private String client_secret;
//        @NotNull private List<String> redirect_uris;
//    }
//}
//
//
