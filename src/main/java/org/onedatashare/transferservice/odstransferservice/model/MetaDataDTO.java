package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import javax.persistence.*;

@Entity
@Table(name = "job_info_3")
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class MetaDataDTO {
    @Id
    //Job ID
    private String id;
    //Source EndPoint
    private String source;
    //Destination EndPoint
    private String destination;
    //Transfer Speed
    private int speed;
    //What kind of optimization used
    private String optimizations;
    //Chunk Size during transfer
    private int chunks;

    //compress
    private String compress;
    //concurrency
    private long concurrency;
    //parallelism
    private long parallelism;
    //pipelining
    private long pipelining;
    //retry
    private long retry;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public String getDestination() {
        return destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public int getSpeed() {
        return speed;
    }

    public void setSpeed(int speed) {
        this.speed = speed;
    }

    public String getOptimizations() {
        return optimizations;
    }

    public void setOptimizations(String optimizations) {
        this.optimizations = optimizations;
    }

    public int getChunks() {
        return chunks;
    }

    public void setChunks(int chunks) {
        this.chunks = chunks;
    }

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }

    public long getConcurrency() {
        return concurrency;
    }

    public void setConcurrency(long concurrency) {
        this.concurrency = concurrency;
    }

    public long getParallelism() {
        return parallelism;
    }

    public void setParallelism(long parallelism) {
        this.parallelism = parallelism;
    }

    public long getPipelining() {
        return pipelining;
    }

    public void setPipelining(long pipelining) {
        this.pipelining = pipelining;
    }

    public long getRetry() {
        return retry;
    }

    public void setRetry(long retry) {
        this.retry = retry;
    }
}
