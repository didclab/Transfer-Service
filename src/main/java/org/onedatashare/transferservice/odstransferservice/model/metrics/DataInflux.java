package org.onedatashare.transferservice.odstransferservice.model.metrics;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.ACTIVE_CORE_COUNT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.ALLOCATED_MEMORY;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.AVERAGE_JOB_SIZE;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.BYTES_RECEIVED;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.BYTES_RECEIVED_DELTA;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.BYTES_SENT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.BYTES_SENT_DELTA;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.COMPRESSION;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.CONCURRENCY;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.CPU_ARCHITECTURE;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.CPU_FREQUENCY_CURRENT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.CPU_FREQUENCY_MAX;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.CPU_FREQUENCY_MIN;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.DESTINATION_TYPE;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.DROP_IN;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.DROP_OUT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.ENERGY_CONSUMED;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.ERROR_IN;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.ERROR_OUT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.FREE_MEMORY;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.JOB_ID;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.JOB_SIZE;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.LATENCY;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.LINK_CAPACITY;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.MAX_MEMORY;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.MEMORY;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.NETWORK_INTERFACE;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.NIC_MTU;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.NIC_SPEED;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.ODS_USER;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.PACKETS_RECEIVED;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.PACKETS_RECEIVED_DELTA;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.PACKETS_SENT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.PACKETS_SENT_DELTA;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.PACKET_LOSS_RATE;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.PARALLELISM;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.PIPELINING;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.P_METER_END_TIME;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.P_METER_START_TIME;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.RTT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.SOURCE_TYPE;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.THROUGHPUT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.TOTAL_BYTES_SENT;
import static org.onedatashare.transferservice.odstransferservice.constant.DataInfluxConstants.TRANSFER_NODE_NAME;

@Data
@Measurement(name = "transfer_data")
@JsonIgnoreProperties(ignoreUnknown = true)
@Component
public class DataInflux {

    @JsonProperty(value = NETWORK_INTERFACE)
    private String networkInterface;

    @JsonProperty(value = ODS_USER)
    private String odsUser;

    @JsonProperty(value = TRANSFER_NODE_NAME)
    private String transferNodeName;

    @JsonProperty(value = ACTIVE_CORE_COUNT)
    private Double coreCount;

    @JsonProperty(value = CPU_FREQUENCY_MAX)
    private Double cpu_frequency_max;

    @JsonProperty(value = CPU_FREQUENCY_CURRENT)
    private Double cpu_frequency_current;

    @JsonProperty(value = CPU_FREQUENCY_MIN)
    private Double cpu_frequency_min;

    @JsonProperty(value = ENERGY_CONSUMED)
    private Double energyConsumed;

    @JsonProperty(value = CPU_ARCHITECTURE)
    private String cpuArchitecture;

    @JsonProperty(value = PACKET_LOSS_RATE)
    private Double packetLossRate;

    @JsonProperty(value = LINK_CAPACITY)
    private Double linkCapacity;

    //NIC values
    @JsonProperty(value = BYTES_SENT)
    private Long bytesSent;

    @JsonProperty(value = BYTES_RECEIVED)
    private Long bytesReceived;

    @JsonProperty(value = PACKETS_SENT)
    private Long packetSent;

    @JsonProperty(value = PACKETS_RECEIVED)
    private Long packetReceived;

    @JsonProperty(value = DROP_IN)
    private Long dropin;

    @JsonProperty(value = DROP_OUT)
    private Long dropout;

    @JsonProperty(value = NIC_SPEED)
    private Long nicSpeed;

    @JsonProperty(value = NIC_MTU)
    private Integer nicMtu;

    //2022-06-01 10:41:15.123591
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    @JsonProperty(value = "start_time")
    private LocalDateTime startTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    @JsonProperty(value = "end_time")
    private LocalDateTime endTime;

    @JsonProperty(value = "latency")
    private Double latency;

    @JsonProperty(value = RTT)
    private Double rtt;

    @JsonProperty(value = ERROR_IN)
    private Long errin;

    @JsonProperty(value = ERROR_OUT)
    private Long errout;

    //Job Values
    private Long jobId;

    private Double throughput;

    private Integer concurrency;

    private Integer parallelism;

    private Integer pipelining;

    private Long memory;

    private Long maxMemory;

    private Long freeMemory;

    private Long jobSize;

    private Long avgFileSize;

    private Long dataBytesSent;

    private Boolean compression;

    private Long allocatedMemory;

    private String sourceType;

    private String destType;
}
