//package org.onedatashare.transferservice.odstransferservice.service.step;
//
//import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
//import com.google.api.client.http.javanet.NetHttpTransport;
//import com.google.api.client.json.JsonFactory;
//import com.google.api.client.json.jackson2.JacksonFactory;
//import com.google.api.services.drive.Drive;
//import org.springframework.batch.core.configuration.annotation.StepScope;
//import org.springframework.batch.item.file.FlatFileItemReader;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.context.annotation.Bean;
//import org.springframework.stereotype.Component;
//
//import java.net.MalformedURLException;
//
//@Component
//public class GoogleDriveItemReader {
//    private static final String APPLICATION_NAME = "Google Drive API Java Quickstart";
//    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
//    @StepScope
//    @Bean
//    public FlatFileItemReader flatFileItemReader(@Value("#{jobParameters['fileName']}") String fName,
//                                                 @Value("#{jobParameters['sourceAccountIdPass']}") String sAccountIdPass,
//                                                 @Value("#{jobParameters['sourceBasePath']}") String sBasePath) throws MalformedURLException {
//
//        FlatFileItemReader<byte[]> reader = new FlatFileItemReader<>();
////        final NetHttpTransport HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
////        Drive service = new Drive.Builder(HTTP_TRANSPORT,JSON_FACTORY,)
//        return reader;
//    }
//}
