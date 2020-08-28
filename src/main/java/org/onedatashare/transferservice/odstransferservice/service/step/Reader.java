package org.onedatashare.transferservice.odstransferservice.service.step;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
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
        String[] resourceList = list.split("<::>");
        Resource[] temp = new Resource[resourceList.length];
        int i=0;
        for (String l : resourceList) {
            System.out.println(l);
            temp[i++]= new UrlResource(l);
        }

        resourceItemReader.setResources(temp);
        resourceItemReader.setDelegate(reader);
        reader.setRecordSeparatorPolicy(new RecordSeparatorPolicy() {
            @Override
            public boolean isEndOfRecord(String s) {
                if (s.length() == 1000)
                    return true;
                return false;
            }

            @Override
            public String postProcess(String s) {
                return s;
            }

            @Override
            public String preProcess(String s) {
                return s;
            }
        });


        return resourceItemReader;
    }
}