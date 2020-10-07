package org.onedatashare.transferservice.odstransferservice.service.step;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.onedatashare.transferservice.odstransferservice.model.DataChunk;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileUrlResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

@Component
public class Reader {

    Logger logger = LoggerFactory.getLogger(Reader.class);

//    @StepScope
//    @Bean
//    public FlatFileItemReader flatFileItemReader(@Value("#{jobParameters['fileName']}") String fName,
//                                                  @Value("#{jobParameters['sourceAccountIdPass']}") String sAccountIdPass,
//                                                  @Value("#{jobParameters['sourceBasePath']}") String sBasePath) throws IOException {
//        logger.info("Inside Flat reader");
//
//        FlatFileItemReader<byte[]> reader = new FlatFileItemReader<>();
//        reader.setResource(new UrlResource(sBasePath.substring(0, 6) + sAccountIdPass + "@" + sBasePath.substring(6) + fName));
//        reader.setLineMapper(new LineMapper<byte[]>() {
//            @Override
//            public byte[] mapLine(String line, int lineNumber) throws Exception {
//                //System.out.println(lineNumber);
//                line += "\n";
//                return line.getBytes();
//            }
//        });
//        return reader;
//    }
    @StepScope
    @SneakyThrows
    @Bean
    public CustomReader customReader(@Value("#{jobParameters['sourceBasePath']}") String basePath, @Value("#{jobParameters['sourceCredential']}") String accountId, @Value("#{jobParameters['INFO_LIST']}") String infoList){
        List<EntityInfo> fileList = new ObjectMapper().readValue(infoList, new TypeReference<List<EntityInfo>>(){});
        CustomReader<DataChunk> reader = new CustomReader<>();
        for(EntityInfo info: fileList){
            String fileName = info.getPath();
            reader.setResource(new UrlResource(basePath.substring(0,6)+accountId+"@" + basePath.substring(6) + fileName));
        }
        return reader;
    }

//    @StepScope
//    @Bean
//    public CustomReader customReader(@Value("#{jobParameters['fileName']}") String fName,
//                                     @Value("#{jobParameters['sourceAccountIdPass']}") String sAccountIdPass,
//                                     @Value("#{jobParameters['sourceBasePath']}") String sBasePath) throws IOException {
//        logger.info("Inside CustomReader");
//        CustomReader<byte[]> reader = new CustomReader<>();
//        logger.info(sBasePath.substring(0, 6) + sAccountIdPass + "@" + sBasePath.substring(6) + fName);
//        reader.setResource(new UrlResource(sBasePath.substring(0, 6) + sAccountIdPass + "@" + sBasePath.substring(6) + fName));
//        return reader;
//    }
}