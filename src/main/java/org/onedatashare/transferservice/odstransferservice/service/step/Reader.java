package org.onedatashare.transferservice.odstransferservice.service.step;

import org.apache.commons.net.ftp.FTPFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import org.apache.commons.net.ftp.FTPClient;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;

@Component
public class Reader {

    Logger logger = LoggerFactory.getLogger(Reader.class);

    @StepScope
    @Bean
    public FlatFileItemReader flatFileItemReader(@Value("#{jobParameters['fileName']}") String fName,
                                                  @Value("#{jobParameters['sourceAccountIdPass']}") String sAccountIdPass,
                                                  @Value("#{jobParameters['sourceBasePath']}") String sBasePath) throws MalformedURLException {
        FTPClient client = new FTPClient();
        FlatFileItemReader<byte[]> reader = new FlatFileItemReader<>();
        logger.info("Inside Flat reader");
        UrlResource urlResource = new UrlResource(sBasePath.substring(0, 6) + sAccountIdPass + "@" + sBasePath.substring(6) + fName);
        reader.setResource(urlResource);
        reader.setLineMapper((line, lineNumber) -> {
            //System.out.println(lineNumber);
            line += "\n";
            return line.getBytes();
        });
//        try {
//            client.connect("localhost",2121);
//            client.login("user","pass");
//            FTPFile[] ftpFiles = client.listDirectories("");
//
//            for(FTPFile file:ftpFiles){
//                InputStream iStream=client.retrieveFileStream(file.getName());
//                File fileName = File.createTempFile("tmp", null);
//                System.out.println(file.getName());
//            }
//        }
//        catch (Exception ex){
//            ex.printStackTrace();
//        }

        return reader;
    }
}