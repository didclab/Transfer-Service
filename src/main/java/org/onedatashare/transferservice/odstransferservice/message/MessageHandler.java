package org.onedatashare.transferservice.odstransferservice.message;

import com.hazelcast.core.HazelcastJsonValue;

import java.io.IOException;

public interface MessageHandler {
    void messageHandler(HazelcastJsonValue jsonMsg) throws IOException;
}
