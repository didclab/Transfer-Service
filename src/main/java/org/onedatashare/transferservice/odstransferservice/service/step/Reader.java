//package org.onedatashare.transferservice.odstransferservice.service.step;
//
//import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
//import org.onedatashare.transferservice.odstransferservice.service.TransferService;
//import org.springframework.batch.item.ItemReader;
//import org.springframework.batch.item.file.FlatFileItemReader;
//import org.springframework.batch.item.file.MultiResourceItemReader;
//import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.core.io.FileSystemResource;
//import org.springframework.core.io.Resource;
//
//public class Reader implements ItemReader<String> {
//
//    TransferJobRequest transferJobRequest = TransferService.getTransferJobRequest();
//
//    @Override
//    public String read() {
//        FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
//        reader.setResource(new FileSystemResource(transferJobRequest.getSource().getInfo().getPath() + transferJobRequest.getSource().getInfoList().get(0)));
//        reader.setRecordSeparatorPolicy(new RecordSeparatorPolicy() {
//            @Override
//            public boolean isEndOfRecord(String s) {
//                if(s.length() == 10)
//                    return true;
//                return false;
//            }
//
//            @Override
//            public String postProcess(String s) {
//                return s;
//            }
//
//            @Override
//            public String preProcess(String s) {
//                return s;
//            }
//        });
//
//
//        return reader;
//    }
//}
