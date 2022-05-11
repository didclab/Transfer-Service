package org.onedatashare.transferservice.odstransferservice.model.metrics;

import com.google.gson.annotations.SerializedName;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

@Data
@Measurement(name= "data")
@Component
public class DataInflux {
    @SerializedName(value="interface")
    @Column(name= "interface")
    private String networkInterface;

    @SerializedName(value="ods_user")
    @Column(name= "ods_user")
    private String odsUser;

    @SerializedName(value="active_core_count")
    @Column(name= "active_core_count")
    private float coreCount;

    @SerializedName(value="cpu_frequency")
    private double[] cpuFrequency;

    @Column(name= "current_cpu_frequency")
    private double currCpuFrequency;

    @Column(name= "max_cpu_frequency")
    private double maxCpuFrequency;

    @Column(name= "min_cpu_frequency")
    private double minCpuFrequency;

    @SerializedName(value="energy_consumed")
    @Column(name= "energy_consumed")
    private float energyConsumed;

    @SerializedName(value="cpu_arch")
    @Column(name="cpu_arch")
    private String cpuArchitecture;

    @SerializedName(value="rtt")
    @Column(name= "rtt")
    private float rtt;

    @SerializedName(value="bandwidth")
    @Column(name= "bandwidth")
    private double bandwidth;

    @SerializedName(value="bandwidth_delay_product")
    @Column(name= "bandwidth_delay_product")
    private float bandwidthDelayProduct;

    @SerializedName(value="packet_loss_rate")
    @Column(name= "packet_loss_rate")
    private float packetLossRate;

    @SerializedName(value="link_capacity")
    @Column(name= "link_capacity")
    private float linkCpacity;

    @SerializedName(value="bytes_sent")
    @Column(name= "bytes_sent")
    private long bytesSent;

    /* Delta values*/
    @Column(name= "bytes_sent_delta")
    private long bytesSentDelta;

    @Column(name= "bytes_received_delta")
    private long bytesReceivedDelta;

    @Column(name= "packets_sent_delta")
    private long packetsSentDelta;

    @Column(name= "packets_received_delta")
    private long packetsReceivedDelta;

    @SerializedName(value="bytes_recv")
    @Column(name= "bytes_recv")
    private long bytesReceived;

    @SerializedName(value="packets_sent")
    @Column(name= "packets_sent")
    private long packetSent;

    @SerializedName(value="packets_recv")
    @Column(name= "packets_recv")
    private long packetReceived;

    @SerializedName(value="dropin")
    @Column(name= "dropin")
    private float dropin;

    @SerializedName(value="dropout")
    @Column(name= "dropout")
    private float dropout;

    @SerializedName(value="nic_speed")
    @Column(name= "nic_speed")
    private double nicSpeed;

    @SerializedName(value="nic_mtu")
    @Column(name= "nic_mtu")
    private double nicMtu;

    @SerializedName(value="start_time")
    @Column(name= "start_time")
    private Timestamp startTime;

    @SerializedName(value="end_time")
    @Column(name= "end_time")
    private Timestamp endTime;

    @SerializedName(value="latency")
    private float[] latencyArr;

    @Column(name= "latency")
    private float latencyVal;

    @SerializedName(value="errin")
    @Column(name= "errin")
    private float errin;

    @SerializedName(value="errout")
    @Column(name= "errout")
    private float errout;

    @Column(name= "jobId")
    private String jobId;

    @Column(name= "throughput")
    private double throughput;

    @Column(name= "concurrency")
    private int concurrency;

    @Column(name= "parallelism")
    private int parallelism;

    @Column(name= "pipelining")
    private int pipelining;

    @Column(name= "cpus")
    private int cpus;

    @Column(name= "memory")
    private long memory;

    @Column(name="maxMemory")
    private long maxMemory;

    @Column(name="freeMemory")
    private long freeMemory;
}
