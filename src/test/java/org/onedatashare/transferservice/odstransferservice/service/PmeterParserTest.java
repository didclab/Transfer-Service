package org.onedatashare.transferservice.odstransferservice.service;

import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.env.Environment;

import java.io.IOException;


public class PmeterParserTest {

    PmeterParser testObj;

    @MockBean
    Environment environment;

    @Test
    public void testPmeterNicDefaultEmpty() throws IOException {
        this.environment = Mockito.mock(Environment.class);
        Mockito.when(environment.getProperty("pmeter.nic", "")).thenReturn("");
        testObj = new PmeterParser(this.environment);
        testObj.init();
        Assert.assertEquals("en0",this.testObj.pmeterNic);
    }

    @Test
    public void testPmeterNicGivenValue() throws IOException {
        this.environment = Mockito.mock(Environment.class);
        Mockito.when(environment.getProperty("pmeter.nic", "")).thenReturn("en0");
        testObj = new PmeterParser(this.environment);
        testObj.init();
        Assert.assertEquals("en0",this.testObj.pmeterNic);
    }
}
