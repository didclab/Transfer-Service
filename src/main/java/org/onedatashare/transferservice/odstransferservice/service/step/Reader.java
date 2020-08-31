package org.onedatashare.transferservice.odstransferservice.service.step;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.batch.item.file.separator.RecordSeparatorPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import java.net.MalformedURLException;

@Component
public class Reader {


    @StepScope
    @Bean
    public MultiResourceItemReader multiFileItemReader(@Value("#{jobParameters['listToTransfer']}") String list) throws MalformedURLException {
        System.out.println("Inside multi reader-----");
        MultiResourceItemReader<String> resourceItemReader = new MultiResourceItemReader<>();
        FlatFileItemReader<String> reader = new FlatFileItemReader<String>();
        //reader.setResource(new UrlResource("https://www.w3.org/TR/PNG/iso_8859-1.txt"));
        String[] resourceList = list.split("<::>");
        Resource[] temp = new Resource[resourceList.length];
        int i=0;
        for (String l : resourceList) {
            //System.out.println("http://techslides.com/demos/sample-videos/small.mp4");
//            System.out.println("https://www.w3.org/TR/PNG/iso_8859-1.txt");
            temp[i++]= new UrlResource("http://techslides.com/demos/sample-videos/small.mp4");
        }

        resourceItemReader.setResources(temp);
        resourceItemReader.setDelegate(reader);
        reader.setLineMapper(new PassThroughLineMapper());
//        reader.setRecordSeparatorPolicy(new RecordSeparatorPolicy() {
//            @Override
//            public boolean isEndOfRecord(String s) {
//                if (s.length() == 10)
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


        return resourceItemReader;
    }
}