package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transfer/update")
public class DynamicController {
    Logger logger = LoggerFactory.getLogger(DynamicController.class);
    
    @RequestMapping(value = "/parallelpool", method = RequestMethod.POST)
    public ResponseEntity<String> updateParallelThreadPoolSize(@RequestBody int poolSize) {
        logger.info("Hit the inside of updateParallelThreadPoolSize");
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @RequestMapping(value = "/concurrentpool")
    public ResponseEntity<String> updateConcurrentThreadPoolSize(@RequestBody int concurrentSize){
        logger.info("Hit the inside of updateConcurrentThreadPoolSize");
        return new ResponseEntity<>(HttpStatus.OK);
    }

    @RequestMapping(value = "/pipelinesize", method = RequestMethod.POST)
    public ResponseEntity<String> updatePipelineSize(@RequestBody int pipeSize){
        logger.info("Hit the inside of updateConcurrentThreadPoolSize");
        return new ResponseEntity<>(HttpStatus.OK);
    }
}
