package org.onedatashare.transferservice.odstransferservice.service;

import com.amazonaws.util.Base64;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.codec.binary.Hex;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.security.MessageDigest;
import java.util.Arrays;

/**
 * @author deepika
 */
@Component
@Getter
@Setter
public class FileHashValidator {

    Logger logger = LoggerFactory.getLogger(FileHashValidator.class);

    MessageDigest readerMessageDigest; //todo - should we create instance here?
    MessageDigest writerMessageDigest;

    String readerHash;
    String writerHash;


    public FileHashValidator() {
        readerHash = Strings.EMPTY;
        writerHash = Strings.EMPTY;
    }

    public void remove(){
        readerMessageDigest.reset();
        writerMessageDigest.reset();
    }

    public boolean check(){
        byte[] read = readerMessageDigest.digest();
        byte[] write =writerMessageDigest.digest();
//        logger.info("read: " + Arrays.toString(read));
//        logger.info("write: " + Arrays.toString(write));
//        logger.info("Reader Hash: " + new String(Hex.encodeHex(read)));
//        logger.info("Writer Hash: " + new String(Hex.encodeHex(write)));

        logger.info("Reader Hash: " + Base64.encodeAsString(read));
        logger.info("Writer Hash: " + Base64.encodeAsString(write));
        //return readerHash.equals(writerHash);
        return MessageDigest.isEqual(read,write);
    }
}
