package org.onedatashare.transferservice.odstransferservice.service;

import com.amazonaws.util.Base64;
import com.amazonaws.util.StringUtils;
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

    String algorithm;
    boolean verify;

    public FileHashValidator() {
        readerHash = Strings.EMPTY;
        writerHash = Strings.EMPTY;
    }

    public void remove(){
        if(readerMessageDigest!=null) readerMessageDigest.reset();
        if(writerMessageDigest!=null) writerMessageDigest.reset();
        readerHash = Strings.EMPTY;
        writerHash = Strings.EMPTY;
    }

    public boolean check(){
        if(!StringUtils.isNullOrEmpty(readerHash) && !StringUtils.isNullOrEmpty(writerHash)){
            logger.info(String.format("Reader hash (%s) matches writer hash (%s)"), readerHash, writerHash);
            return readerHash.equals(writerHash);
        }
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
