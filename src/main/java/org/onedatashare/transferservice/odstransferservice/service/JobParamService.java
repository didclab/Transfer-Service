package org.onedatashare.transferservice.odstransferservice.service;

import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.CHUNK_SIZE;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.COMPRESS;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.CONCURRENCY;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_BASE_PATH;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.DEST_CREDENTIAL_ID;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.OWNER_ID;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PARALLELISM;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PIPELINING;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.PRIORITY;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.RETRY;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_BASE_PATH;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.SOURCE_CREDENTIAL_ID;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.TIME;

import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.stereotype.Service;

@Service
public class JobParamService {

    Logger logger = LoggerFactory.getLogger(JobParamService.class);

    public JobParameters translate(JobParametersBuilder builder, TransferJobRequest request) {
        logger.info("Setting job Parameters");
        builder.addLong(TIME, System.currentTimeMillis());
        builder.addString(OWNER_ID, request.getOwnerId());
        builder.addString(PRIORITY, String.valueOf(request.getPriority()));
        builder.addString(CHUNK_SIZE, String.valueOf(request.getChunkSize()));
        builder.addString(SOURCE_BASE_PATH, request.getSource().getParentInfo().getPath());
        builder.addString(DEST_BASE_PATH, request.getDestination().getParentInfo().getPath());
        builder.addString(SOURCE_CREDENTIAL_ID, request.getSource().getCredId());
        builder.addString(DEST_CREDENTIAL_ID, request.getDestination().getCredId());
        builder.addString(COMPRESS, String.valueOf(request.getOptions().getCompress()));
        builder.addLong(CONCURRENCY, (long) request.getOptions().getConcurrencyThreadCount());
        builder.addLong(PARALLELISM, (long) request.getOptions().getParallelThreadCount());
        builder.addLong(PIPELINING, (long) request.getOptions().getPipeSize());
        builder.addLong(RETRY, (long) request.getOptions().getRetry());
        for(EntityInfo fileInfo : request.getSource().getInfoList()){
            builder.addString(fileInfo.getId(), fileInfo.toString());
        }
        return builder.toJobParameters();
    }
}
