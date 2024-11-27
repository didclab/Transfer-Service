package org.onedatashare.transferservice.odstransferservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import jakarta.annotation.PostConstruct;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.CarbonIpEntry;
import org.onedatashare.transferservice.odstransferservice.model.CarbonMeasurement;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class CarbonJobMeasure {

    private final IMap<UUID, HazelcastJsonValue> carbonIntensityMap;
    private final IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap;
    private final PredicateBuilder.EntryObject entryObj;
    private final PmeterParser pmeterParser;
    private final ObjectMapper objectMapper;
    private final Logger logger = LoggerFactory.getLogger(CarbonJobMeasure.class);
    private boolean odsConnector;

    @Value("${spring.application.name}")
    private String appName;

    @Value("${ods.user}")
    private String odsUser;

    public CarbonJobMeasure(IMap<UUID, HazelcastJsonValue> carbonIntensityMap, IMap<UUID, HazelcastJsonValue> fileTransferScheduleMap, PmeterParser pmeterParser, ObjectMapper objectMapper) {
        this.carbonIntensityMap = carbonIntensityMap;
        this.fileTransferScheduleMap = fileTransferScheduleMap;
        this.entryObj = Predicates.newPredicateBuilder().getEntryObject();
        this.pmeterParser = pmeterParser;
        this.objectMapper = objectMapper;
        this.odsConnector = false;
    }

    @PostConstruct
    public void init() {
        //set ODS Connector
        if(this.odsUser.equals("OneDataShare") || this.appName.equals("ODSTransferService")) {
            this.odsConnector = true;
        }

    }

    public List<TransferJobRequest> getPotentialJobsFromMap() {
        Predicate<UUID, HazelcastJsonValue> potentialJobs;
        if (this.odsConnector) {
            logger.info("{}} Querying Hazelcast for jobs", this.appName);
            potentialJobs = this.entryObj.get("transferNodeName").equal("");
        } else {
            logger.info("ODS Connector: {} Querying Hazelcast for jobs", this.appName);
            potentialJobs = this.entryObj.get("transferNodeName").equal(appName).or(this.entryObj.get("source.credId").equal(appName)).or(this.entryObj.get("destination.credId").equal(appName));
        }

        Collection<HazelcastJsonValue> jsonJobs = this.fileTransferScheduleMap.values(potentialJobs);
        return jsonJobs.stream().map(hazelcastJsonValue -> {
            try {
                return this.objectMapper.readValue(hazelcastJsonValue.getValue(), TransferJobRequest.class);
            } catch (JsonProcessingException e) {
                logger.error("Json Processing Exception: {}\n With message: {}", e, e.getMessage());
            }
            return null;
        }).collect(Collectors.toList());
    }

    @Scheduled(cron = "0 * * * * *")
    public void measureCarbonOfPotentialJobs() {
        List<TransferJobRequest> potentialJobs = getPotentialJobsFromMap();
        logger.info("Potential jobs from ODS to run: {}", potentialJobs);
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
                List<CarbonIpEntry> totalEntries = new ArrayList<>();
                if (!transferJobRequest.getSource().getType().equals(EndpointType.vfs)) {
                    totalEntries.addAll(this.pmeterParser.carbonPerIp(sourceIp));
                }
                if (transferJobRequest.getDestination().getType().equals(EndpointType.vfs)) {
                    totalEntries.addAll(this.pmeterParser.carbonPerIp(destIp));
                }
                CarbonMeasurement carbonMeasurement = new CarbonMeasurement();
                carbonMeasurement.setTimeMeasuredAt(LocalDateTime.now());
                carbonMeasurement.setJobUuid(transferJobRequest.getJobUuid());
                carbonMeasurement.setOwnerId(transferJobRequest.getOwnerId());
                carbonMeasurement.setTransferNodeName(transferJobRequest.getTransferNodeName());
                carbonMeasurement.setTraceRouteCarbon(totalEntries);
                HazelcastJsonValue jsonValue = new HazelcastJsonValue(this.objectMapper.writeValueAsString(carbonMeasurement));
                UUID randomUUID = UUID.randomUUID();
                this.carbonIntensityMap.put(randomUUID, jsonValue);
                logger.info("Created Carbon entry with Key={} and Value={}", randomUUID, jsonValue.getValue());
            } catch (JsonProcessingException e) {
                logger.error("Failed to parse job: {} \n Error received: \t {}", transferJobRequest.toString(), e.getMessage());
            } catch (IOException e) {
                logger.error("Failed to measure ip: {} \n Error received: \t {}", transferJobRequest.toString(), e.getMessage());
            }
        });
    }

}
