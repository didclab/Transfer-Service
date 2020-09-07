package org.onedatashare.transferservice.odstransferservice.service.step;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.stereotype.Component;
import java.net.MalformedURLException;
import java.util.Comparator;

@Component
public class Reader {


    @StepScope
    @Bean
    public MultiResourceItemReader multiFileItemReader(@Value("#{jobParameters['listToTransfer']}") String list) throws MalformedURLException {
        System.out.println("Inside multi reader-----");
        MultiResourceItemReader<byte[]> resourceItemReader = new MultiResourceItemReader<>();
        FlatFileItemReader<byte[]> reader = new FlatFileItemReader<>();
        //reader.setResource(new UrlResource("https://www.w3.org/TR/PNG/iso_8859-1.txt"));
        String[] resourceList = list.split("<::>");
        Resource[] temp = new Resource[resourceList.length];
        int i=0;
        for (String l : resourceList) {
            //System.out.println("http://techslides.com/demos/sample-videos/small.mp4");
//            System.out.println("https://www.w3.org/TR/PNG/iso_8859-1.txt");
            System.out.println(l);
            temp[i++]= new UrlResource(l);
        }

        resourceItemReader.setResources(temp);
        resourceItemReader.setDelegate(reader);
        reader.setLineMapper(new LineMapper<byte[]>() {
            @Override
            public byte[] mapLine(String line, int lineNumber) throws Exception {
                System.out.println(lineNumber);
                return line.getBytes();
            }
        });
        resourceItemReader.setComparator(new Comparator<Resource>() {
            @Override
            public int compare(Resource o1, Resource o2) {
                return 0; // return in normal ordering
            }
        });
        return resourceItemReader;
    }
}