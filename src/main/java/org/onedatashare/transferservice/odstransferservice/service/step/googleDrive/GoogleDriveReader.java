//package org.onedatashare.transferservice.odstransferservice.service.step.googleDrive;
//
//import com.google.api.client.auth.oauth2.Credential;
//import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
//import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
//import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
//import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
//import com.google.api.client.http.javanet.NetHttpTransport;
//import com.google.api.client.json.JsonFactory;
//import com.google.api.client.json.jackson2.JacksonFactory;
//import com.google.api.client.util.store.FileDataStoreFactory;
//import com.google.api.services.drive.DriveScopes;
//import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
//import org.onedatashare.transferservice.odstransferservice.model.GoogleDriveCredential;
//import org.springframework.batch.core.StepExecution;
//import org.springframework.batch.core.annotation.BeforeStep;
//import org.springframework.batch.item.file.ResourceAwareItemReaderItemStream;
//import org.springframework.batch.item.support.AbstractItemCountingItemStreamItemReader;
//import org.springframework.beans.factory.InitializingBean;
//import org.springframework.core.io.Resource;
//
//import java.io.*;
//import java.nio.charset.StandardCharsets;
//import java.util.Collections;
//import java.util.List;
//
//import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;
//
//public class GoogleDriveReader extends AbstractItemCountingItemStreamItemReader<DataChunk> implements ResourceAwareItemReaderItemStream<DataChunk>, InitializingBean {
//
//    private static final String APPLICATION_NAME = "OneDataShare Google Drive API Java";
//    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
//    private static final String TOKENS_DIRECTORY_PATH = "tokens";
//    private static final List<String> SCOPES = Collections.singletonList(DriveScopes.DRIVE);
//    private static final String CREDENTIALS_FILE_PATH = "credentials.json";
//    private String clientID = " ";
//    private String clientPassword = " ";
//    private String credential = " ";
//    private List<String> redirectUris = null;
//
//    @BeforeStep
//    public void beforeStep(StepExecution stepExecution) {
//        String[] driveClientId = stepExecution.getJobParameters().getString(SOURCE_ACCOUNT_ID_PASS).split(":");
//        clientID = driveClientId[0];
//        clientPassword = driveClientId[1];
//        redirectUris.add("urn:ietf:wg:oauth:2.0:oob");
//        redirectUris.add("http://localhost");
//        credential = createCredentialJson(clientID,clientPassword);
//    }
//
//    @Override
//    public void setResource(Resource resource) {
//
//    }
//
//    @Override
//    protected DataChunk doRead() throws Exception {
//        return null;
//    }
//
//    @Override
//    protected void doOpen() throws Exception {
//
//    }
//
//    @Override
//    protected void doClose() throws Exception {
//
//    }
//
//    @Override
//    public void afterPropertiesSet() throws Exception {
//
//    }
//
//    /**
//     * Creates an authorized Credential object.
//     * @param HTTP_TRANSPORT The network HTTP Transport.
//     * @return An authorized Credential object.
//     * @throws IOException If the credentials.json file cannot be found.
//     */
//    private Credential getCredentials(final NetHttpTransport HTTP_TRANSPORT) throws IOException {
//        InputStream in = new ByteArrayInputStream(credential.getBytes(StandardCharsets.UTF_8));
//        if (in == null) {
//            throw new FileNotFoundException("Resource not found: " + CREDENTIALS_FILE_PATH);
//        }
//        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));
//
//        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
//                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
//                .setDataStoreFactory(new FileDataStoreFactory(new java.io.File(TOKENS_DIRECTORY_PATH)))
//                .setAccessType("offline")
//                .build();
//        LocalServerReceiver receiver = new LocalServerReceiver.Builder().setPort(8888).build();
//        return new AuthorizationCodeInstalledApp(flow, receiver).authorize("user");
//    }
//
//    private String createCredentialJson(String clientID,String clientPassword) {
//        GoogleDriveCredential.DriveKey driveKey = GoogleDriveCredential.DriveKey.builder().clientID(clientID)
//                .projectID(PROJECT_ID)
//                .auth_uri(AUTH_URI)
//                .token_uri(TOKEN_URI)
//                .auth_provider_x509_cert_url(AUTH_PROVIDER_x509_CERT_URL)
//                .client_secret(clientPassword)
//                .redirect_uris(redirectUris).build();
//        GoogleDriveCredential googleDriveCredential = GoogleDriveCredential.builder().installed(driveKey).build();
//        return  googleDriveCredential.toString();
//    }
//}
