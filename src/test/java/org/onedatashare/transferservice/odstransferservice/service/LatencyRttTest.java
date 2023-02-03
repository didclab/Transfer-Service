package org.onedatashare.transferservice.odstransferservice.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class LatencyRttTest {

    LatencyRtt testObj;

    @BeforeEach
    public void init(){
        this.testObj = new LatencyRtt();
    }

    @Test
    public void rttReturnsZeroWithNoHost() throws IOException {
        double value = testObj.rttCompute("",0);
        Assertions.assertEquals(0.0, value);
    }

    @Test
    public void rttTestWithGoogleCred() throws IOException {
        double value = testObj.rttCompute("drive.google.com", 80);
        Assertions.assertTrue(value > 0.0);
    }

    @Test
    public void rttTestWithS3Uri() throws IOException {
        double value = testObj.rttCompute("https://jacobstestbucket.s3.us-east-2.amazonaws.com",80);
        Assertions.assertTrue(value > 0.0);
    }

    @Test
    public void rttTestWithDropbBox() throws IOException {
        double value = testObj.rttCompute("dropbox.com", 80);
        Assertions.assertTrue(value > 0.0);
    }

    @Test
    public void rttTestWithBox() throws IOException {
        double value = testObj.rttCompute("box.com", 80);
        Assertions.assertTrue(value > 0.0);
    }

    @Test
    public void rttTestWithFTP() throws IOException {
        double value = testObj.rttCompute("test.rebex.net", 21);
        Assertions.assertTrue(value > 0.0);
    }

    /**
     * This test covers scp, sftp as both run over port 22.
     * @throws IOException
     */
    @Test
    public void rttTestWithSFTP() throws IOException {
        double value = testObj.rttCompute("3.144.15.90", 22);
        Assertions.assertTrue(value > 0.0);
    }

}
