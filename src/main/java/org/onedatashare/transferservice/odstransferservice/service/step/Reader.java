//package org.onedatashare.transferservice.odstransferservice.service.step;
//
//import org.springframework.batch.item.ItemReader;
//import org.springframework.batch.item.file.FlatFileItemReader;
//import org.springframework.batch.item.file.MultiResourceItemReader;
//
//
//import java.net.URL;
//
//public class Reader implements ItemReader<String> {
//
//    private MultiResourceItemReader<String> delegate;
//
//    public Reader(MultiResourceItemReader<String> delegate) {
//        this.delegate = delegate;
//    }
//
//    @Override
//    public String read() throws Exception {
//        return delegate.read();
//    }
//}
//
//
//
////
////        FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
////        reader.setResource(new FileSystemResource(transferJobRequest.getSource().getInfo().getPath() + transferJobRequest.getSource().getInfoList().get(0)));
////        reader.setRecordSeparatorPolicy(new RecordSeparatorPolicy() {
////            @Override
////            public boolean isEndOfRecord(String s) {
////                if(s.length() == 10)
////                    return true;
////                return false;
////            }
////
////            @Override
////            public String postProcess(String s) {
////                return s;
////            }
////
////            @Override
////            public String preProcess(String s) {
////                return s;
////            }
////        });
////
////
////        return reader.read();
