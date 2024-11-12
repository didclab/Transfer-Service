package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.onedatashare.transferservice.odstransferservice.constant.ODSConstants;
import org.onedatashare.transferservice.odstransferservice.model.FileTransferNodeMetaData;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.core.env.Environment;

import java.util.UUID;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class FileTransferNodeRegistrationServiceTest {

    @Mock
    private Environment environment;

    @Mock
    private JobExecution jobExecution;

    @Mock
    private JobParameters jobParameters;

    private HazelcastInstance hazelcastInstance;
    String appName = "testAppName@random.org";

    private IMap<String, HazelcastJsonValue> fileTransferNodeMap;

    FileTransferNodeRegistrationService testObj;
    ObjectMapper objectMapper = new ObjectMapper();

    private UUID testJobUuid = UUID.randomUUID();

    public FileTransferNodeRegistrationServiceTest() {
        this.hazelcastInstance = Hazelcast.newHazelcastInstance();
        this.fileTransferNodeMap = this.hazelcastInstance.getMap("testNodeRegistrationMap");
    }

    @BeforeEach
    public void setUp() {
        when(environment.getProperty("spring.application.name")).thenReturn(this.appName);
        when(environment.getProperty("ods.user")).thenReturn("testUser");
        testObj = new FileTransferNodeRegistrationService(hazelcastInstance, fileTransferNodeMap, environment, this.objectMapper);
    }

    @Test
    public void testInitialNodeRegistrationInMap() throws JsonProcessingException {
        testObj.updateRegistrationInHazelcast(null);
        Assert.assertEquals(this.fileTransferNodeMap.containsKey(this.appName), true);
        HazelcastJsonValue jsonValue = this.fileTransferNodeMap.get(this.appName);
        FileTransferNodeMetaData testData = this.objectMapper.readValue(jsonValue.getValue(), FileTransferNodeMetaData.class);
        Assert.assertNotNull(testData);
        Assert.assertEquals(this.hazelcastInstance.getLocalEndpoint().getUuid(), testData.getNodeUuid());
        Assert.assertEquals("testAppName", testData.getNodeName());
        Assert.assertEquals("testUser", testData.getOdsOwner());
        Assert.assertEquals(-1L, testData.getJobId());
        Assert.assertEquals(new UUID(0, 0), testData.getJobUuid());
        Assert.assertEquals(false, testData.getRunningJob());
        Assert.assertEquals(true, testData.getOnline());
    }

    @Test
    public void testRegisterWithJobExecution() throws JsonProcessingException {
        testObj.updateRegistrationInHazelcast(this.jobExecution);
        when(jobExecution.getJobId()).thenReturn(1L);
        when(jobExecution.getJobParameters()).thenReturn(this.jobParameters);
        when(this.jobParameters.getString(ODSConstants.JOB_UUID)).thenReturn(this.testJobUuid.toString());
        testObj.updateRegistrationInHazelcast(this.jobExecution);
        HazelcastJsonValue jsonValue = this.fileTransferNodeMap.get(this.appName);
        FileTransferNodeMetaData testData = this.objectMapper.readValue(jsonValue.getValue(), FileTransferNodeMetaData.class);
        Assert.assertNotNull(testData);
        Assert.assertEquals(true, testData.getRunningJob());
        Assert.assertEquals(this.testJobUuid, testData.getJobUuid());
    }

    @Test
    public void testDeRegisterNodeFromMap() throws JsonProcessingException {
        testObj.updateRegistrationInHazelcast(null);
        testObj.updateRegistrationInHazelcast(this.jobExecution);
        HazelcastJsonValue jsonValue = this.fileTransferNodeMap.get(this.appName);
        FileTransferNodeMetaData testData = objectMapper.readValue(jsonValue.getValue(), FileTransferNodeMetaData.class);
        Assert.assertNotNull(testData);
        Assert.assertEquals(false, testData.getRunningJob());
        Assert.assertEquals(false, testData.getOnline());

    }

}
