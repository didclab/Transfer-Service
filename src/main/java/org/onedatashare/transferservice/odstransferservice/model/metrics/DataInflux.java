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
    @Column(name = NETWORK_INTERFACE)
    private String networkInterface;

    @JsonProperty(value = ODS_USER)
    @Column(name = ODS_USER, tag = true)
    private String odsUser;

    @JsonProperty(value = TRANSFER_NODE_NAME)
    @Column(name = TRANSFER_NODE_NAME)
    private String transferNodeName;

    @JsonProperty(value = ACTIVE_CORE_COUNT)
    @Column(name = ACTIVE_CORE_COUNT)
    private Double coreCount;

    @JsonProperty(value = CPU_FREQUENCY_MAX)
    @Column(name = CPU_FREQUENCY_MAX)
    private Double cpu_frequency_max;

    @JsonProperty(value = CPU_FREQUENCY_CURRENT)
    @Column(name = CPU_FREQUENCY_CURRENT)
    private Double cpu_frequency_current;

    @JsonProperty(value = CPU_FREQUENCY_MIN)
    @Column(name = CPU_FREQUENCY_MIN)
    private Double cpu_frequency_min;

    @JsonProperty(value = ENERGY_CONSUMED)
    @Column(name = ENERGY_CONSUMED)
    private Double energyConsumed;

    @JsonProperty(value = CPU_ARCHITECTURE)
    @Column(name = CPU_ARCHITECTURE)
    private String cpuArchitecture;

    @JsonProperty(value = PACKET_LOSS_RATE)
    @Column(name = PACKET_LOSS_RATE)
    private Double packetLossRate;

    @JsonProperty(value = LINK_CAPACITY)
    @Column(name = LINK_CAPACITY)
    private Double linkCapacity;

    /* Delta values*/
    @Column(name = BYTES_SENT_DELTA)
    private Long bytesSentDelta;

    @Column(name = BYTES_RECEIVED_DELTA)
    private Long bytesReceivedDelta;

    @Column(name = PACKETS_SENT_DELTA)
    private Long packetsSentDelta;

    @Column(name = PACKETS_RECEIVED_DELTA)
    private Long packetsReceivedDelta;

    //NIC values
    @JsonProperty(value = BYTES_SENT)
    @Column(name = "bytes_sent")
    private Long bytesSent;

    @JsonProperty(value = BYTES_RECEIVED)
    @Column(name = "bytes_recv")
    private Long bytesReceived;

    @JsonProperty(value = PACKETS_SENT)
    @Column(name = "packets_sent")
    private Long packetSent;

    @JsonProperty(value = PACKETS_RECEIVED)
    @Column(name = "packets_recv")
    private Long packetReceived;

    @JsonProperty(value = DROP_IN)
    @Column(name = "dropin")
    private Double dropin;

    @JsonProperty(value = DROP_OUT)
    @Column(name = "dropout")
    private Double dropout;

    @JsonProperty(value = NIC_SPEED)
    @Column(name = "nic_speed")
    private Double nicSpeed;

    @JsonProperty(value = NIC_MTU)
    @Column(name = "nic_mtu")
    private Double nicMtu;

    //2022-06-01 10:41:15.123591
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    @JsonProperty(value = "start_time")
    @Column(name = P_METER_START_TIME)
    private LocalDateTime startTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    @JsonProperty(value = "end_time")
    @Column(name = P_METER_END_TIME)
    private LocalDateTime endTime;

    @JsonProperty(value = "latency")
    @Column(name = LATENCY)
    private Double latency;

    @JsonProperty(value = RTT)
    @Column(name = "rtt")
    private Double rtt;

    @JsonProperty(value = ERROR_IN)
    @Column(name = ERROR_OUT)
    private Double errin;

    @JsonProperty(value = ERROR_OUT)
    @Column(name = ERROR_OUT)
    private Double errout;

    //Job Values
    @Column(name = JOB_ID)
    private Long jobId;

    @Column(name = THROUGHPUT)
    private Double throughput;

    @Column(name = CONCURRENCY)
    private Integer concurrency;

    @Column(name = PARALLELISM)
    private Integer parallelism;

    @Column(name = PIPELINING)
    private Integer pipelining;

    @Column(name = MEMORY)
    private Long memory;

    @Column(name = MAX_MEMORY)
    private Long maxMemory;

    @Column(name = FREE_MEMORY)
    private Long freeMemory;

    @Column(name = JOB_SIZE)
    private Long jobSize;

    @Column(name = AVERAGE_JOB_SIZE)
    private Long avgFileSize;

    @Column(name = TOTAL_BYTES_SENT)
    private Long dataBytesSent;

    @Column(name = COMPRESSION)
    private Boolean compression;

    @Column(name = ALLOCATED_MEMORY)
    private Long allocatedMemory;

    @Column(name = SOURCE_TYPE)
    private String sourceType;

    @Column(name = DESTINATION_TYPE)
    private String destType;
}
