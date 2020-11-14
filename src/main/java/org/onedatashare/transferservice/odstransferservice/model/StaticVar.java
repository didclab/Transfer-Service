package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Getter;
import lombok.Setter;
import org.onedatashare.transferservice.odstransferservice.service.step.sftp.SFTPReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class StaticVar {
    static Logger logger = LoggerFactory.getLogger(StaticVar.class);
    @Getter
    @Setter
    static Map<String, Long> hm = new HashMap<>();

    //TODO: Must remove these and find way to pass to reader and writer
    public static String sPass = "";
    public static String dPass = "";

    public static void clearAllStaticVar() {
        logger.info("Resetting all static variable");
        hm = null;
        sPass = "";
        dPass = "";
    }
}
