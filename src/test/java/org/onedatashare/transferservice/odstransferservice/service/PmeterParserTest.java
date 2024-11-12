package org.onedatashare.transferservice.odstransferservice.service;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class PmeterParserTest {

    PmeterParser testObj;

    @Test
    public void parsePmeter() {
        testObj = new PmeterParser();
        try {
            String interfaceToUse = testObj.discoverActiveNetworkInterface();
            Assert.assertEquals("en0", interfaceToUse);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
