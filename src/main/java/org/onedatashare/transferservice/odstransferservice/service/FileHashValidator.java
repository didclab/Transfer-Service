package org.onedatashare.transferservice.odstransferservice.service;

import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author deepika
 */
@Component
@Getter
@Setter
public class FileHashValidator {

    Logger logger = LoggerFactory.getLogger(FileHashValidator.class);

    //todo - check if transfer is at file level or not
//    Map<String, String> readerHash;
//    Map<String, String> writerHash;

    String readerHash;
    String writerHash;


    public FileHashValidator() {
//        readerHash = new HashMap<>();
//        writerHash = new HashMap<>();
        readerHash = Strings.EMPTY;
        writerHash = Strings.EMPTY;
    }

    public void remove(){
        readerHash = Strings.EMPTY;
        writerHash = Strings.EMPTY;
        //readerHash = null;
        //writerHash = null;
    }
}
