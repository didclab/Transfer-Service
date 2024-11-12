package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.TransferOptions;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.UUID;

public class CarbonJobMeasureTest {

    CarbonJobMeasure testObj;
    static IMap<UUID, HazelcastJsonValue> carbonIntensityMap;
    static IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap;
    static ObjectMapper objectMapper;
    @Mock
    PmeterParser pmeterParser;

    @BeforeAll
    public static void beforeAllTests() {
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance();
        carbonIntensityMap = hazelcastInstance.getMap("carbon-intensity-map");
        fileTransferScheduleMap = hazelcastInstance.getMap("file-transfer-schedule-map");
        objectMapper = new ObjectMapper();
    }

    @BeforeEach
    public void beforeEachTest() {
        testObj = new CarbonJobMeasure(carbonIntensityMap, fileTransferScheduleMap, pmeterParser, objectMapper);
        ReflectionTestUtils.setField(testObj, "appName", "odsNode");
        ReflectionTestUtils.setField(testObj, "odsUser", "odsNode");
    }

    @Test
    public void testEmptyMapsDefault() {
        testObj.measureCarbonOfPotentialJobs();
        Assert.assertEquals(0, carbonIntensityMap.size());
    }

    @Test
    public void testOneJobInMapForThisNode() throws JsonProcessingException {
        TransferJobRequest jobRequest = new TransferJobRequest();
        jobRequest.setJobUuid(UUID.randomUUID());
        jobRequest.setOptions(new TransferOptions());
        jobRequest.setSource(new TransferJobRequest.Source());
        jobRequest.setDestination(new TransferJobRequest.Destination());
        jobRequest.setOwnerId("jgoldverg@gmail.com");
        jobRequest.setTransferNodeName("odsNode");
        String jsonJob = objectMapper.writeValueAsString(jobRequest);
        fileTransferScheduleMap.put(jobRequest.getJobUuid(), new HazelcastJsonValue(jsonJob));
        testObj.measureCarbonOfPotentialJobs();
        Assert.assertEquals(1, carbonIntensityMap.size());
    }


}
