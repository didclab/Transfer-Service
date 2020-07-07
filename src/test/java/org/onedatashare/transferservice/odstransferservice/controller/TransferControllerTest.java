package org.onedatashare.transferservice.odstransferservice.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.onedatashare.transferservice.odstransferservice.model.core.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.core.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.request.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.ArrayList;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Transfer Controller Test
 */
@RunWith(MockitoJUnitRunner.class)
public class TransferControllerTest {

    Logger logger = LoggerFactory.getLogger(TransferControllerTest.class);

    @InjectMocks
    private TransferController transferController;

    private MockMvc mockMvc;

    @Mock
    TransferService transferService;


    @Before
    public void setup() {
        logger.info("Inside TransferControllerTest setup function");
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(transferController).build();
        when(transferService.submit(any(TransferJobRequest.class))).thenReturn(new ResponseEntity<>("Test", HttpStatus.OK));
    }

    @Test
    public void startTestWithWrongAPI() throws Exception {
        logger.info("Inside startTestWithWrongAPI");
        mockMvc.perform(post("/wrongAPI")).andExpect(status().is4xxClientError());
        //when(transferController.start(any(TransferJobRequest.class))).thenReturn(new ResponseEntity<>("Test", HttpStatus.OK););
    }

    @Test
    public void startTest() throws Exception {
        logger.info("Inside startTest");
        mockMvc.perform(post("/api/transfer/start").contentType(MediaType.APPLICATION_JSON).content(toJson(createJobRequest()))).
                andExpect(status().isOk());
    }

    public String toJson(final Object obj) {
        logger.info("Converting obj to json");
        try {
            return new ObjectMapper().writeValueAsString(obj);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TransferJobRequest createJobRequest() {
        logger.info("creating sample JobRequest");
        TransferJobRequest transferJobRequest = new TransferJobRequest();
        transferJobRequest.setPriority(1);
        transferJobRequest.setId("testId");
        transferJobRequest.setOwnerId("testOwnerId");
        transferJobRequest.setDestination(new TransferJobRequest.Destination(EndpointType.box, "testCredId", new EntityInfo("testId", "testPath", 2l)));
        transferJobRequest.setSource(new TransferJobRequest.Source(EndpointType.box, "testCredId", new EntityInfo("testId", "testPath", 2l), new ArrayList<>()));
        return transferJobRequest;
    }

}