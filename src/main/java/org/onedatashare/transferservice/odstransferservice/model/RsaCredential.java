package org.onedatashare.transferservice.odstransferservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Table(name = "RsaCred")
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RsaCredential {
    @Id
    //Job ID
    private String id;
    //Source EndPoint
    private String key;
}
