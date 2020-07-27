package org.onedatashare.transferservice.odstransferservice.controller;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * Transfer controller with to initiate transfer request
 */
@RestController
@RequestMapping("/api/transfer")
public class TransferController {

    Logger logger = LoggerFactory.getLogger(TransferController.class);

    @Autowired
    TransferService transferService;

    @RequestMapping(value = "/start", method = RequestMethod.POST)
    public ResponseEntity<String> start(@RequestBody TransferJobRequest request) {
        logger.info("Inside TransferController");
        return transferService.submit(request);
    }
}
