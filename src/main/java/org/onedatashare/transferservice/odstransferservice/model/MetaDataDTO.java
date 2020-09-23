package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "job_info")
@AllArgsConstructor
@Builder
public class MetaDataDTO {
    @Id
    @NonNull
    //Job ID
    private String id;
    //Source EndPoint
    private String source;
    //Destination EndPoint
    private String destination;
    //Type of the File
    private String documentType;
    //Transfer Speed
    private long transferSpeed;
    //What kind of optimization used
    private String optimizationParameters;
    //Chunk Size during transfer
    private long chunkSize;
    //Total time to transfer
    private long totalTime;

    public MetaDataDTO() {
    }

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

    public String getDocumentType() {
        return documentType;
    }

    public void setDocumentType(String documentType) {
        this.documentType = documentType;
    }

    public long getTransferSpeed() {
        return transferSpeed;
    }

    public void setTransferSpeed(long transferSpeed) {
        this.transferSpeed = transferSpeed;
    }

    public String getOptimizationParameters() {
        return optimizationParameters;
    }

    public void setOptimizationParameters(String optimizationParameters) {
        this.optimizationParameters = optimizationParameters;
    }

    public long getChunkSize() {
        return chunkSize;
    }

    public void setChunkSize(long chunkSize) {
        this.chunkSize = chunkSize;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }

    @Override
    public String toString() {
        return "MetaDataDTO{" +
                "id='" + id + '\'' +
                ", source='" + source + '\'' +
                ", destination='" + destination + '\'' +
                ", documentType='" + documentType + '\'' +
                ", transferSpeed=" + transferSpeed +
                ", optimizationParameters='" + optimizationParameters + '\'' +
                ", chunkSize=" + chunkSize +
                ", totalTime=" + totalTime +
                '}';
    }
}
