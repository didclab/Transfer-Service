package org.onedatashare.transferservice.odstransferservice.model.metrics;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

@Data
@Measurement(name = "transfer_data")
@JsonIgnoreProperties(ignoreUnknown = true)
@Component
public class DataInflux {

    @JsonProperty(value = "interface")
    @Column(name = "interface")
    private String networkInterface;

    @JsonProperty(value = "ods_user")
    @Column(name = "ods_user")
    private String odsUser;

    @JsonProperty(value = "transfer_node_name")
    @Column(name = "transfer_node_name")
    private String transferNodeName;

    @JsonProperty(value = "active_core_count")
    @Column(name = "active_core_count")
    private Double coreCount;

    @JsonProperty(value = "cpu_frequency_max")
    @Column(name = "cpu_frequency_max")
    private Double cpu_frequency_max;

    @JsonProperty(value = "cpu_frequency_current")
    @Column(name = "cpu_frequency_current")
    private Double cpu_frequency_current;

    @JsonProperty(value = "cpu_frequency_min")
    @Column(name = "cpu_frequency_min")
    private Double cpu_frequency_min;


    @JsonProperty(value = "energy_consumed")
    @Column(name = "energy_consumed")
    private Double energyConsumed;

    @JsonProperty(value = "cpu_arch")
    @Column(name = "cpu_arch")
    private String cpuArchitecture;

    @JsonProperty(value = "packet_loss_rate")
    @Column(name = "packet_loss_rate")
    private Double packetLossRate;

    @JsonProperty(value = "link_capacity")
    @Column(name = "link_capacity")
    private Double linkCapacity;

    /* Delta values*/
    @Column(name = "bytes_sent_delta")
    private Long bytesSentDelta;

    @Column(name = "bytes_received_delta")
    private Long bytesReceivedDelta;

    @Column(name = "packets_sent_delta")
    private Long packetsSentDelta;

    @Column(name = "packets_received_delta")
    private Long packetsReceivedDelta;

    //NIC values

    @JsonProperty(value = "bytes_sent")
    @Column(name = "bytes_sent")
    private Long bytesSent;

    @JsonProperty(value = "bytes_recv")
    @Column(name = "bytes_recv")
    private Long bytesReceived;

    @JsonProperty(value = "packets_sent")
    @Column(name = "packets_sent")
    private Long packetSent;

    @JsonProperty(value = "packets_recv")
    @Column(name = "packets_recv")
    private Long packetReceived;

    @JsonProperty(value = "dropin")
    @Column(name = "dropin")
    private Double dropin;

    @JsonProperty(value = "dropout")
    @Column(name = "dropout")
    private Double dropout;

    @JsonProperty(value = "nic_speed")
    @Column(name = "nic_speed")
    private Double nicSpeed;

    @JsonProperty(value = "nic_mtu")
    @Column(name = "nic_mtu")
    private Double nicMtu;

    //2022-06-01 10:41:15.123591
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    @JsonProperty(value = "start_time")
    @Column(name = "start_time")
    private LocalDateTime startTime;

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSSSSS")
    @JsonProperty(value = "end_time")
    @Column(name = "end_time")
    private LocalDateTime endTime;

    @JsonProperty(value = "latency")
    @Column(name = "latency")
    private Double latency;

    @JsonProperty(value = "rtt")
    @Column(name = "rtt")
    private Double rtt;

    @JsonProperty(value = "errin")
    @Column(name = "errin")
    private Double errin;

    @JsonProperty(value = "errout")
    @Column(name = "errout")
    private Double errout;

    //Job Values

    @Column(name = "jobId")
    private Long jobId;

    @Column(name = "throughput")
    private Double throughput;

    @Column(name = "concurrency")
    private Integer concurrency;

    @Column(name = "parallelism")
    private Integer parallelism;

    @Column(name = "pipelining")
    private Integer pipelining;

    @Column(name = "memory")
    private Long memory;

    @Column(name = "maxMemory")
    private Long maxMemory;

    @Column(name = "freeMemory")
    private Long freeMemory;

    @Column(name ="jobSize")
    private Long jobSize;

    @Column(name="avgJobSize")
    private Long avgFileSize;

    @Column(name="totalBytesSent")
    private Long dataBytesSent;

    @Column(name="compression")
    private Boolean compression;

    @Column(name="allocatedMemory")
    private Long allocatedMemory;

    @Column(name="sourceType")
    private String sourceType;

    @Column(name="destType")
    private String destType;
}
