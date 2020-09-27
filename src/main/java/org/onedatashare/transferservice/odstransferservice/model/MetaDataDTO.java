package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "job_info")
@AllArgsConstructor
@Builder
@NoArgsConstructor
public class MetaDataDTO {
    @Id
    //Job ID
    @Column(name = "id")
    private String id;
    //Source EndPoint
    @Column(name = "source")
    private String source;
    //Destination EndPoint
    @Column(name = "destination")
    private String destination;
    //Type of the File
    @Column(name = "type")
    private String type;
    //Transfer Speed
    @Column(name = "speed")
    private int speed;
    //What kind of optimization used
    @Column(name = "optimizations")
    private String optimizations;
    //Chunk Size during transfer
    @Column(name = "chunks")
    private int chunks;
    //Total time to transfer
    @Column(name = "time")
    private int time;


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

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
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

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "MetaDataDTO{" +
                "id='" + id + '\'' +
                ", source='" + source + '\'' +
                ", destination='" + destination + '\'' +
                ", type='" + type + '\'' +
                ", speed=" + speed +
                ", optimizations='" + optimizations + '\'' +
                ", chunks=" + chunks +
                ", time=" + time +
                '}';
    }
}
