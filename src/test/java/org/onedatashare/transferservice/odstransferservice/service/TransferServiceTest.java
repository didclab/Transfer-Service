package org.onedatashare.transferservice.odstransferservice.service;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

/**
 * Transfer ServiceTest
 */
@RunWith(MockitoJUnitRunner.class)
public class TransferServiceTest {

    Logger logger = LoggerFactory.getLogger(TransferServiceTest.class);

    @InjectMocks
    TransferService transferService;

    private MockMvc mockMvc;

    @Before
    public void setup() {
        logger.info("Inside TransferServiceTest setup function");
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(transferService).build();
    }

    @Test
    public void submit(){
        //submit test
    }

}
