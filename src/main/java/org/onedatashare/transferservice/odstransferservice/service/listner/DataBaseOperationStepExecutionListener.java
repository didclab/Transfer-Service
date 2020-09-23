package org.onedatashare.transferservice.odstransferservice.service.listner;

import org.onedatashare.transferservice.odstransferservice.model.MetaDataDTO;
import org.onedatashare.transferservice.odstransferservice.service.DatabaseService.MetaDataServiceImplementation;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;

public class DataBaseOperationStepExecutionListener implements StepExecutionListener {
    @Autowired
    MetaDataServiceImplementation metaDataServiceImplementation;
    @Override
    public void beforeStep(StepExecution stepExecution) {
        System.out.println("Step Starting");
        //Parameters to be filled out at the start of the Step
        JobParameters jobParameters = stepExecution.getJobParameters();
        System.out.println("ID: "+jobParameters.getString("id"));
        //Job ID
        String id = jobParameters.getString("id");
        //Source of the End Point
        String source = jobParameters.getString("source");
        //Destination of the End Point
        String destination = jobParameters.getString("destination");
        //Need a logic to add the chunk size in Job Parameters
        long chunkSize = 0;
        //Need a Logic to get extension
        String extension = "pdf";
        MetaDataDTO metaDataDTO = MetaDataDTO.builder().id(id).source(source).destination(destination).type(extension).build();
        //MetaDataDTO metaDataDTO = new MetaDataDTO(id,source,destination,extension,0,"concurrency",0,0);
        metaDataServiceImplementation.saveOrUpdate(metaDataDTO);
        System.out.println("I am done writing stuff in DB");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        System.out.println("Step Executed");
        //Parameters to be filled out at the end of the Step
        JobParameters jobParameters = stepExecution.getJobParameters();
        //Parameters to be filled out at the start of the Step
        //Job ID
        String id = jobParameters.getString("id");
        //Need a logic to calculate Speed
        //Query From the Job Table
        //Get Start and End Time
        //Divide by the Chunk Size
        int transferSpeed = 0;
        //Need a Logic to read Optimization Parameter
        String optimization = "concurrency";
        //Need a Logic to calculate Total Time
        //Query From the Job Table
        //Get Start and End Time
        //Get the difference
        int totalTime = 0;
        MetaDataDTO metaDataDTO = MetaDataDTO.builder().id(id).speed(transferSpeed).optimizations(optimization).time(totalTime).build();
        metaDataServiceImplementation.saveOrUpdate(metaDataDTO);
        return null;
    }
}
