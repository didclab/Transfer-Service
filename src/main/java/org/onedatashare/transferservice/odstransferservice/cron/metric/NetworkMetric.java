package org.onedatashare.transferservice.odstransferservice.cron.metric;

import com.google.api.client.json.Json;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.vladmihalcea.hibernate.type.json.JsonType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.Type;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * @author deepika
 */
@Entity
@Table(name = "network_metric")
//@Table(name = "job_info_3") //todo - replace
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class NetworkMetric {

    @Id
    Long id;

//    String date;

//    @Type(type = "json")
//    @Column(name = "data", columnDefinition = "jsonb")
//    String data;
    
}
