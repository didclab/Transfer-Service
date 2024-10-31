package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import org.onedatashare.transferservice.odstransferservice.model.CarbonIntensityMapKey;
import org.onedatashare.transferservice.odstransferservice.model.CarbonIpEntry;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class CarbonJobMeasure {

    private final IMap<HazelcastJsonValue, HazelcastJsonValue> carbonIntensityMap;
    private final IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap;
    private final PredicateBuilder.EntryObject entryObj;
    private final PmeterParser pmeterParser;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(CarbonJobMeasure.class);

    @Value("spring.application.name")
    private String appName;

    @Value("ods.user")
    private String odsUser;

    public CarbonJobMeasure(IMap<HazelcastJsonValue, HazelcastJsonValue> carbonIntensityMap, IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap, PmeterParser pmeterParser, ObjectMapper objectMapper) {
        this.carbonIntensityMap = carbonIntensityMap;
        this.fileTransferScheduleMap = fileTransferScheduleMap;
        this.entryObj = Predicates.newPredicateBuilder().getEntryObject();
        this.pmeterParser = pmeterParser;
        this.objectMapper = objectMapper;
    }

    public List<TransferJobRequest> getPotentialJobsFromMap() {
        Predicate<UUID, HazelcastJsonValue> potentialJobs;
        if (odsUser.equals("onedatashare")) {
            potentialJobs = this.entryObj.get("options.transferNodeName").equal(this.appName).or(this.entryObj.get("options.transferNodeName").equal(""));
        } else {
            potentialJobs = this.entryObj.get("options.transferNodeName").equal(appName).or(this.entryObj.get("source.credId").equal(appName)).or(this.entryObj.get("destination.credId").equal(appName));
        }

        Collection<HazelcastJsonValue> jsonJobs = this.fileTransferScheduleMap.values(potentialJobs);
        return jsonJobs.stream().map(hazelcastJsonValue -> {
            try {
                return this.objectMapper.readValue(hazelcastJsonValue.getValue(), TransferJobRequest.class);
            } catch (JsonProcessingException ignored) {
            }
            return null;
        }).collect(Collectors.toList());
    }

    @Scheduled(cron = "0 0/10 * * * ?")
    public void measureCarbonOfPotentialJobs() {
        List<TransferJobRequest> potentialJobs = getPotentialJobsFromMap();
        potentialJobs.forEach(transferJobRequest -> {
            try {
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
                CarbonIntensityMapKey mapKey = new CarbonIntensityMapKey(transferJobRequest.getOwnerId(), transferJobRequest.getTransferNodeName(), transferJobRequest.getJobUuid(), LocalDateTime.now());
                this.carbonIntensityMap.put(new HazelcastJsonValue(this.objectMapper.writeValueAsString(mapKey)), new HazelcastJsonValue(this.objectMapper.writeValueAsString(sourceCarbonPerIp)));
            } catch (JsonProcessingException e) {
                logger.error("Failed to parse job: {} \n Error received: \t {}", transferJobRequest.toString(), e.getMessage());
            } catch (IOException e) {
                logger.error("Failed to measure ip: {} \n Error received: \t {}", transferJobRequest.toString(), e.getMessage());
            }
        });
    }

}
