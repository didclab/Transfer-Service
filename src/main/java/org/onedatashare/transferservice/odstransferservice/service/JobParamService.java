package org.onedatashare.transferservice.odstransferservice.service;

import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.metrics.CarbonScore;
import org.onedatashare.transferservice.odstransferservice.utility.ODSUtility;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.LocalDateTime;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.*;

@Service
public class JobParamService {

    Logger logger = LoggerFactory.getLogger(JobParamService.class);

    @Value("${spring.application.name}")
    private String appName;

    PmeterParser pmeterParser;

    public JobParamService(PmeterParser pmeterParser) {
        this.pmeterParser = pmeterParser;
    }

    /**
     * Here we are adding basically the whole request except for sensitive credentials to the Job Params table.
     * B/C we do not add
     *
     * @param builder
     * @param request
     * @return
     */
    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        logger.info("Setting job Parameters");
        EndpointType sourceType = request.getSource().getType();
        EndpointType destType = request.getDestination().getType();
        builder.addLocalDateTime(TIME, LocalDateTime.now());
        builder.addString(OWNER_ID, request.getOwnerId());
        builder.addString(SOURCE_BASE_PATH, request.getSource().getFileSourcePath());
        builder.addString(SOURCE_CREDENTIAL_ID, request.getSource().getCredId());
        builder.addString(SOURCE_CREDENTIAL_TYPE, sourceType.toString());
        builder.addString(DEST_BASE_PATH, request.getDestination().getFileDestinationPath());
        builder.addString(DEST_CREDENTIAL_ID, request.getDestination().getCredId());
        builder.addString(DEST_CREDENTIAL_TYPE, destType.toString());
        if (request.getJobUuid() != null) {
            builder.addString(JOB_UUID, request.getJobUuid().toString());
        }
        //here we are adding the starting optimization parameters to JobParameters
        builder.addLong(CONCURRENCY, (long) request.getOptions().getConcurrencyThreadCount());
        builder.addLong(PARALLELISM, (long) request.getOptions().getParallelThreadCount());
        builder.addLong(PIPELINING, (long) request.getOptions().getPipeSize());
        builder.addString(COMPRESS, String.valueOf(request.getOptions().getCompress()));
        builder.addLong(RETRY, (long) request.getOptions().getRetry());
        builder.addString(APP_NAME, this.appName);
        builder.addString(OPTIMIZER, request.getOptions().getOptimizer());
        builder.addLong(FILE_COUNT, (long) request.getSource().getInfoList().size());
        long totalSize = 0L;
        for (EntityInfo fileInfo : request.getSource().getInfoList()) {
            builder.addString(fileInfo.getId(), fileInfo.toString());
            totalSize += fileInfo.getSize();
        }
        builder.addLong(JOB_SIZE, totalSize);
        double value = 0;
        if (!request.getSource().getInfoList().isEmpty()) {
            value = totalSize / (double) request.getSource().getInfoList().size();
        }
        builder.addLong(FILE_SIZE_AVG, (long) value);

        //adding the source host and source port to use for RTT & Latency measurements.
        if (request.getSource().getVfsSourceCredential() != null) {
            String sourceIp = ODSUtility.uriFromEndpointCredential(request.getSource().getVfsSourceCredential(), sourceType);
            builder.addString(SOURCE_HOST, sourceIp);
            builder.addLong(SOURCE_PORT, (long) this.portFromEndpointCredential(request.getSource().getVfsSourceCredential(), sourceType));
            CarbonScore score = this.pmeterParser.carbonAverageTraceRoute(sourceIp);
            logger.info("Source Carbon Score: {}", score.avgCarbon);
            builder.addLong(CARBON_SCORE_SOURCE, (long) score.avgCarbon);
        } else if (request.getSource().getOauthSourceCredential() != null) {
            builder.addString(SOURCE_HOST, ODSUtility.uriFromEndpointCredential(request.getSource().getOauthSourceCredential(), sourceType));
            builder.addLong(SOURCE_PORT, (long) this.portFromEndpointCredential(request.getSource().getOauthSourceCredential(), sourceType));
        }
        if (request.getDestination().getVfsDestCredential() != null) {
            String destIp = ODSUtility.uriFromEndpointCredential(request.getDestination().getVfsDestCredential(), destType);
            builder.addString(DEST_HOST, destIp);
            builder.addLong(DEST_PORT, (long) this.portFromEndpointCredential(request.getDestination().getVfsDestCredential(), destType));
            CarbonScore score = this.pmeterParser.carbonAverageTraceRoute(destIp);
            logger.info("Destination Carbon Score: {}", score.avgCarbon);
            builder.addLong(CARBON_SCORE_DEST, (long) score.avgCarbon);
        } else if (request.getDestination().getOauthDestCredential() != null) {
            builder.addString(DEST_HOST, ODSUtility.uriFromEndpointCredential(request.getDestination().getOauthDestCredential(), destType));
            builder.addLong(DEST_PORT, (long) this.portFromEndpointCredential(request.getDestination().getOauthDestCredential(), destType));
        }
        return builder.toJobParameters();
    }


    public int portFromEndpointCredential(EndpointCredential credential, EndpointType type) {
        switch (type) {
            case http:
            case scp:
            case sftp:
            case ftp:
                AccountEndpointCredential ac = (AccountEndpointCredential) credential;
                return URI.create(ac.getUri()).getPort();
            case gdrive:
            case dropbox:
            case box:
            case s3:
                return 80;
            default:
                return 0;
        }
    }
}
