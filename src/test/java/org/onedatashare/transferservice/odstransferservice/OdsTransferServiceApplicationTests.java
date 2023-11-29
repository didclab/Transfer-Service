package org.onedatashare.transferservice.odstransferservice;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.onedatashare.transferservice.odstransferservice.controller.TransferController;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.*;

@SpringBootTest
class OdsTransferServiceApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	public void testPause() throws Exception {
		// setup
		long jobId = 5L;
		Set<Long> jobIds = new HashSet<>();
		TransferController transferController = new TransferController(jobIds);

		JobExecution jobExecution = mock(JobExecution.class);
		JobExplorer jobExplorer = mock(JobExplorer.class);
		JobOperator jobOperator = mock(JobOperator.class);
		Mockito.when(jobExplorer.getJobExecution(jobId)).thenReturn(jobExecution);
		Mockito.when(jobExecution.isRunning()).thenReturn(Boolean.TRUE);

		//action
		transferController.pause();

		//assert
		verify(jobOperator, times(1)).stop(jobId);
	}

}
