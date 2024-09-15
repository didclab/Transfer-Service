package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import org.onedatashare.transferservice.odstransferservice.model.CarbonIpEntry;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Service
public class CarbonJobMeasure {

    private final IMap<String, HazelcastJsonValue> carbonIntensityMap;
    private final IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap;
    private final PredicateBuilder.EntryObject entryObj;
    private final PmeterParser pmeterParser;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(CarbonJobMeasure.class);

    @Value("spring.application.name")
    private String appName;

    public CarbonJobMeasure(IMap<String, HazelcastJsonValue> carbonIntensityMap, IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap, PmeterParser pmeterParser, ObjectMapper objectMapper) {
        this.carbonIntensityMap = carbonIntensityMap;
        this.fileTransferScheduleMap = fileTransferScheduleMap;
        this.entryObj = Predicates.newPredicateBuilder().getEntryObject();
        this.pmeterParser = pmeterParser;
        this.objectMapper = objectMapper;
    }

    @Scheduled(cron = "0 0/10 * * * ?")
    public void measureCarbonOfPotentialJobs() {
        Collection<HazelcastJsonValue> scheduledJobsJson = this.fileTransferScheduleMap.values(this.entryObj.get("options.transferNodeName").equal(this.appName));
        scheduledJobsJson.forEach(hazelcastJsonValue -> {
            try {
                TransferJobRequest transferJobRequest = this.objectMapper.readValue(hazelcastJsonValue.getValue(), TransferJobRequest.class);
                String sourceIp = "";
                if (transferJobRequest.getSource().getVfsSourceCredential() != null) {
                    sourceIp = ODSUtility.uriFromEndpointCredential(transferJobRequest.getSource().getVfsSourceCredential(), transferJobRequest.getSource().getType());
                } else {
                    sourceIp = ODSUtility.uriFromEndpointCredential(transferJobRequest.getSource().getOauthSourceCredential(), transferJobRequest.getSource().getType());
                }
                String destIp = "";
                if (transferJobRequest.getDestination().getVfsDestCredential() != null) {
                    destIp = ODSUtility.uriFromEndpointCredential(transferJobRequest.getDestination().getVfsDestCredential(), transferJobRequest.getDestination().getType());
                } else {
                    destIp = ODSUtility.uriFromEndpointCredential(transferJobRequest.getDestination().getOauthDestCredential(), transferJobRequest.getDestination().getType());
                }
                List<CarbonIpEntry> sourceCarbonPerIp = this.pmeterParser.carbonPerIp(sourceIp);
                sourceCarbonPerIp.addAll(this.pmeterParser.carbonPerIp(destIp));
                this.carbonIntensityMap.put(transferJobRequest.getOwnerId() + "-" + transferJobRequest.getTransferNodeName() + "-" + transferJobRequest.getJobUuid().toString(), new HazelcastJsonValue(this.objectMapper.writeValueAsString(sourceCarbonPerIp)));
            } catch (JsonProcessingException e) {
                logger.error("Failed to parse job: {} \n Error received: \t {}", hazelcastJsonValue.getValue(), e.getMessage());
            } catch (IOException e) {
                logger.error("Failed to measure ip: {} \n Error received: \t {}", hazelcastJsonValue.getValue(), e.getMessage());
            }
        });
    }

}
