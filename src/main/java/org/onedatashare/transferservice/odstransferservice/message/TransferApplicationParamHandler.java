package org.onedatashare.transferservice.odstransferservice.message;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.onedatashare.transferservice.odstransferservice.model.optimizer.TransferApplicationParams;
import org.onedatashare.transferservice.odstransferservice.pools.ThreadPoolContract;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.stereotype.Service;

@Service
public class TransferApplicationParamHandler implements MessageHandler{

    private final ObjectMapper mesageObjectMapper;
    private final ThreadPoolContract threadPool;
    Logger logger = LoggerFactory.getLogger(TransferApplicationParamHandler.class);

    public TransferApplicationParamHandler(ObjectMapper messageObjectMapper, ThreadPoolContract threadPool){
        this.mesageObjectMapper = messageObjectMapper;
        this.threadPool = threadPool;
    }

    @Override
    public void messageHandler(Message message) throws JsonProcessingException {
        String jsonStr = new String(message.getBody());
        TransferApplicationParams params = mesageObjectMapper.readValue(jsonStr, TransferApplicationParams.class);
        logger.info("Parsed TransferApplicationParams: {}", params);
        this.threadPool.applyOptimizer(params.getConcurrency(), params.getParallelism());

    }
}
