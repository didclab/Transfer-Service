package org.onedatashare.transferservice.odstransferservice.model.metrics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Data;

import java.util.UUID;

import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.*;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.CHUNK_SIZE;
import static org.onedatashare.transferservice.odstransferservice.constant.ODSConstants.JOB_UUID;

@Data
@Measurement(name = "transfer_data")
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataInflux {

    @JsonProperty(value = NETWORK_INTERFACE)
    @Column(name = NETWORK_INTERFACE)
    private String networkInterface = "";

    @JsonProperty(value = ODS_USER)
    @Column(name = ODS_USER, tag = true)
    private String odsUser = "";

    @JsonProperty(value = TRANSFER_NODE_NAME)
    @Column(name = TRANSFER_NODE_NAME, tag = true)
    private String transferNodeName = "";

    @JsonProperty(value = ACTIVE_CORE_COUNT)
    @Column(name = ACTIVE_CORE_COUNT)
    private Integer coreCount = Runtime.getRuntime().availableProcessors();

    @JsonProperty(value = CPU_FREQUENCY_MAX)
    @Column(name = CPU_FREQUENCY_MAX)
    private Double cpu_frequency_max = 0.0;

    @JsonProperty(value = CPU_FREQUENCY_CURRENT)
    @Column(name = CPU_FREQUENCY_CURRENT)
    private Double cpu_frequency_current = 0.0;

    @JsonProperty(value = CPU_FREQUENCY_MIN)
    @Column(name = CPU_FREQUENCY_MIN)
    private Double cpu_frequency_min = 0.0;

    @JsonProperty(value = CPU_ARCHITECTURE)
    @Column(name = CPU_ARCHITECTURE)
    private String cpuArchitecture = "";

    @JsonProperty(value = PACKET_LOSS_RATE)
    @Column(name = PACKET_LOSS_RATE)
    private Double packetLossRate = 0.0;
    //NIC values
    @JsonProperty(value = BYTES_SENT)
    @Column(name = BYTES_SENT)
    private Long bytesSent = 0L;

    @JsonProperty(value = BYTES_RECEIVED)
    @Column(name = BYTES_RECEIVED)
    private Long bytesReceived = 0L;

    @JsonProperty(value = PACKETS_SENT)
    @Column(name = PACKETS_SENT)
    private Long packetSent = 0L;

    @JsonProperty(value = PACKETS_RECEIVED)
    @Column(name = PACKETS_RECEIVED)
    private Long packetReceived = 0L;

    @JsonProperty(value = DROP_IN)
    @Column(name = DROP_IN)
    private Long dropin = 0L;

    @JsonProperty(value = DROP_OUT)
    @Column(name = DROP_OUT)
    private Long dropout = 0L;

    @JsonProperty(value = NIC_MTU)
    @Column(name = NIC_MTU)
    private Integer nicMtu = 0;

    @JsonProperty(value = NIC_SPEED)
    @Column(name = NIC_SPEED)
    private Integer nicSpeed = 0;

    @JsonProperty(value = LATENCY)
    @Column(name = LATENCY)
    private Double latency = 0.0;

    @JsonProperty(value = RTT)
    @Column(name = RTT)
    private Double rtt = 0.0;

    @Column(name = SOURCE_RTT)
    private Double sourceRtt = 0.0;

    @Column(name = SOURCE_LATENCY)
    private Double sourceLatency = 0.0;

    @Column(name = DESTINATION_RTT)
    private Double destinationRtt = 0.0;

    @Column(name = DEST_LATENCY)
    private Double destLatency = 0.0;

    @JsonProperty(value = ERROR_IN)
    @Column(name = ERROR_IN)
    private Long errin = 0L;

    @JsonProperty(value = ERROR_OUT)
    @Column(name = ERROR_OUT)
    private Long errout = 0L;

    //Job Values
    @Column(name = JOB_ID, tag = true)
    private Long jobId = 0L;
    @Column(name = READ_THROUGHPUT)
    private Double readThroughput = 0.0;
    @Column(name = WRITE_THROUGHPUT)
    private Double writeThroughput = 0.0;
    @Column(name = BYTES_UPLOADED)
    private Long bytesWritten = 0L;
    @Column(name = BYTES_DOWNLOADED)
    private Long bytesRead = 0L;
    @Column(name = CONCURRENCY)
    private Integer concurrency = 0;

    @Column(name = PARALLELISM)
    private Integer parallelism = 0;
    @Column(name = PIPELINING)
    private Integer pipelining = 0;
    @Column(name = MEMORY)
    private Long memory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    @Column(name = MAX_MEMORY)
    private Long maxMemory = Runtime.getRuntime().maxMemory();
    @Column(name = FREE_MEMORY)
    private Long freeMemory = Runtime.getRuntime().freeMemory();
    @Column(name = ALLOCATED_MEMORY)
    private Long allocatedMemory = Runtime.getRuntime().totalMemory();
    @Column(name = JOB_SIZE)
    private Long jobSize = 0L;
    @Column(name = AVERAGE_FILE_SIZE)
    private Long avgFileSize = 0L;

    @Column(name = SOURCE_TYPE, tag = true)
    private String sourceType = "";
    @Column(name = SOURCE_CRED_ID, tag = true)
    private String sourceCredId = "";

    @Column(name = DESTINATION_TYPE, tag = true)
    private String destType = "";
    @Column(name = DESTINATION_CRED_IT, tag = true)
    private String destCredId = "";

    @Column(name = CHUNK_SIZE)
    private Long chunksize = 0L;

    @Column(name = JOB_UUID, tag = true)
    private UUID jobUuid;

    @Column(name = IS_RUNNING)
    private Boolean isRunning = false;
}
