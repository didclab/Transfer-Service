package org.onedatashare.transferservice.odstransferservice.consumer;

import com.google.gson.Gson;
import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.CrudService;
import org.onedatashare.transferservice.odstransferservice.service.JobControl;
import org.onedatashare.transferservice.odstransferservice.service.JobParamService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author deepika
 */
@Component
public class ConsumerUtility {

    Logger logger = LoggerFactory.getLogger(ConsumerUtility.class);

    @Autowired
    JobControl jc;

    @Autowired
    JobLauncher asyncJobLauncher;

    @Autowired
    JobParamService jobParamService;

    @Autowired
    CrudService crudService;
    public void process(final TransferJobRequest request) throws Exception{
        JobParameters parameters = jobParamService.translate(new JobParametersBuilder(), request);
        crudService.insertBeforeTransfer(request);
        jc.setRequest(request);
        asyncJobLauncher.run(jc.concurrentJobDefinition(), parameters);
    }
}
