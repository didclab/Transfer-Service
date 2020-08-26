package org.onedatashare.transferservice.odstransferservice.model;

import lombok.Builder;
import lombok.Data;
import org.onedatashare.transferservice.odstransferservice.Enum.EndpointType;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Tap;
import org.onedatashare.transferservice.odstransferservice.service.tap.TapFactory;

@Data
public class ReaderDTO {
    Tap tap;
    TransferJobRequest.Source source;
    String priority;
    EndpointCredential credential;
    EndpointType type;
}
