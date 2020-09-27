package org.onedatashare.transferservice.odstransferservice.service.step;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.MalformedURLException;

@Component
public class Reader {

    Logger logger = LoggerFactory.getLogger(Reader.class);

    @StepScope
    @Bean
    public FlatFileItemReader flatFileItemReader(@Value("#{jobParameters['fileName']}") String fName,
                                                  @Value("#{jobParameters['sourceAccountIdPass']}") String sAccountIdPass,
                                                  @Value("#{jobParameters['sourceBasePath']}") String sBasePath) throws MalformedURLException {
        FlatFileItemReader<byte[]> reader = new FlatFileItemReader<>();
            logger.info("Inside Flat reader");
            UrlResource urlResource = new UrlResource(sBasePath.substring(0, 6) + sAccountIdPass + "@" + sBasePath.substring(6) + fName);
            reader.setResource(urlResource);
            reader.setLineMapper((line, lineNumber) -> {
                //System.out.println(lineNumber);
                line += "\n";
                return line.getBytes();
            });

        return reader;
    }
}