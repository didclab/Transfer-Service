package org.onedatashare.transferservice.odstransferservice.service.step;

import org.onedatashare.transferservice.odstransferservice.model.TransferJobRequest;
import org.onedatashare.transferservice.odstransferservice.service.TransferService;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Component
public class Writer implements ItemWriter<String> {

    private TransferJobRequest transferJobRequest = TransferService.getTransferJobRequest();

    @Override
    public void write(List<? extends String> list) throws Exception {

        //System.out.println(list);
        String directory = System.getProperty("user.home");
        File file = new File(directory + "//tempOutput//firstTransferTest.txt");
        FileWriter fileWriter = new FileWriter(file,true);
        PrintWriter writer = new PrintWriter(fileWriter,true);
        for(String str : list){
            writer.println(str);
        }
        writer.close();
//        Files.write(Paths.get(directory + "//firstTransferTest.txt"), list);

//        try (FileOutputStream fos = new FileOutputStream("pathname")) {
//            fos.write(list);
//        }
    }

}
