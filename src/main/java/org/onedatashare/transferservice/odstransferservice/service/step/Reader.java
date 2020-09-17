package org.onedatashare.transferservice.odstransferservice.service.step;

import org.apache.commons.net.ftp.FTPClient;
import org.onedatashare.transferservice.odstransferservice.controller.TransferController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Comparator;

@Component
public class Reader {

    Logger logger = LoggerFactory.getLogger(Reader.class);


    @StepScope
    @Bean
    public FlatFileItemReader flatFileItemReader(@Value("#{jobParameters['fileName']}") String fName,
                                                  @Value("#{jobParameters['sourceAccountIdPass']}") String sAccountIdPass,
                                                  @Value("#{jobParameters['sourceBasePath']}") String sBasePath) throws IOException {
        logger.info("Inside Flat reader");

        FlatFileItemReader<byte[]> reader = new FlatFileItemReader<>();
        reader.setResource(new UrlResource(sBasePath.substring(0, 6) + sAccountIdPass + "@" + sBasePath.substring(6) + fName));
        reader.setLineMapper(new LineMapper<byte[]>() {
            @Override
            public byte[] mapLine(String line, int lineNumber) throws Exception {
                //System.out.println(lineNumber);
                line += "\n";
                return line.getBytes();
            }
        });
        return reader;
    }
}