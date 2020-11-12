package org.onedatashare.transferservice.odstransferservice.service.DatabaseService;

import org.onedatashare.transferservice.odstransferservice.DataRepository.RsaCredRepository;
import org.onedatashare.transferservice.odstransferservice.model.RsaCredential;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class RsaCredInterfaceImpl implements RsaCredInterface {

    @Autowired
    RsaCredRepository rsaCredRepository;

    @Override
    public RsaCredential saveOrUpdate(RsaCredential rsaCredential) {
        System.out.println("------------------------------------Updating: " + rsaCredential.toString());
        try {
            return rsaCredRepository.save(rsaCredential);
        } catch (Exception ex) {
            ex.getMessage();
        }
        return null;
    }

    @Override
    public String findById(String id) throws Exception {
        System.out.println("findById---for :" + id);
        RsaCredential rsaCredential = rsaCredRepository.findById(id).orElseThrow(() -> new Exception("not found"));
        return rsaCredential.getKey();
    }
}
