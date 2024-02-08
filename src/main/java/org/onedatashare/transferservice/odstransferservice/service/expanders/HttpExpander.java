package org.onedatashare.transferservice.odstransferservice.service.expanders;

import lombok.SneakyThrows;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.onedatashare.transferservice.odstransferservice.model.EntityInfo;
import org.onedatashare.transferservice.odstransferservice.model.credential.AccountEndpointCredential;
import org.onedatashare.transferservice.odstransferservice.model.credential.EndpointCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

public class HttpExpander implements FileExpander{

    private AccountEndpointCredential credential;
    Logger logger = LoggerFactory.getLogger(HttpExpander.class);

    @Override
    public void createClient(EndpointCredential credential) {
        this.credential = (AccountEndpointCredential) credential;
        logger.info(this.credential.toString());

    }

    @SneakyThrows
    @Override
    public List<EntityInfo> expandedFileSystem(List<EntityInfo> userSelectedResources, String basePath) {
        List<EntityInfo> filesToSend = new ArrayList<>();
        Stack<Element> directoriesToTraverse = new Stack<>();
        if (basePath.isEmpty()) basePath = "/";
        if (userSelectedResources.isEmpty()) { //we move the whole damn server
            logger.info("User resources is empty gonna just send the whole server I guess");
            Document doc = Jsoup.connect(this.credential.getUri() + basePath).get();
            Elements links = doc.select("body a");
            for (Element elem : links) {
                if (elem.text().endsWith("/")) { //directory to expand
                    directoriesToTraverse.push(elem);
                } else { //we have a file
                    filesToSend.add(fromElement(elem, 0));
                }
            }
        } else { //move only files/folders the user selected
            for (EntityInfo selectedFiles : userSelectedResources) {
                //we have a folder to transfer
                if(selectedFiles.getPath().endsWith("/")){
                    Document doc = Jsoup.connect(this.credential.getUri() + basePath + selectedFiles.getPath())
                            .ignoreContentType(true)
                            .get();
                    logger.info(doc.toString());
                    Elements links = doc.select("body a");
                    for (Element elem : links) {
                        if (elem.text().endsWith("/")) { //directory to expand
                            directoriesToTraverse.push(elem);
                        } else { //we have a file
                            filesToSend.add(fromElement(elem, selectedFiles.getChunkSize()));
                        }
                    }
                }else{
                    filesToSend.add(this.fileToInfo(this.credential.getUri() + Paths.get(basePath, selectedFiles.getPath()).toString(), selectedFiles.getChunkSize()));
                }
            }
        }
        //all of these have names that should be appended to the path
        while (!directoriesToTraverse.isEmpty()) {
            Element directory = directoriesToTraverse.pop();
            if (directory.text().contains("..") || directory.text().contains(".")) {
                continue;
            }
            logger.info(directory.baseUri() + directory.text());
            Document doc = Jsoup.connect(directory.baseUri() + basePath +directory.text()).get();
            Elements links = doc.select("body a");
            for (Element elem : links) {
                if (elem.text().endsWith("/")) { //directory to expand
                    directoriesToTraverse.push(elem);
                } else { //we have a file
                    filesToSend.add(fromElement(elem, 0));
                }
            }
        }
        return filesToSend;
    }

    public EntityInfo fromElement(Element elem, Integer chunkSize) throws IOException {
        EntityInfo fileInfo = new EntityInfo();
        if(chunkSize == 0){
            chunkSize = 16000000;
        }
        URL url = new URL(elem.absUrl("href"));
        long fileSize = url.openConnection().getContentLengthLong();
        Path path = Path.of(url.getPath());
        logger.info("File path: {}, name: {}, size: {},", path, elem.text(), fileSize);
        fileInfo.setId(elem.text());
        fileInfo.setSize(fileSize);
        fileInfo.setPath(path.toAbsolutePath().toString());
        fileInfo.setChunkSize(chunkSize);
        return fileInfo;
    }

    public EntityInfo fileToInfo(String strUrl, Integer chunkSize) throws IOException {
        if(chunkSize == 0){
            chunkSize = 16000000;
        }
        EntityInfo fileInfo = new EntityInfo();
        URL url = new URL(strUrl);
        URLConnection conn = url.openConnection();
        long fileSize = conn.getContentLengthLong();
        String fileName = Paths.get(conn.getURL().getFile()).getFileName().toString();
        logger.info("File path: {}, name: {}, size: {},", url.getPath(), fileName, fileSize);
        fileInfo.setId(fileName);
        fileInfo.setSize(fileSize);
        fileInfo.setPath(url.getPath());
        fileInfo.setChunkSize(chunkSize);
        return fileInfo;
    }
}
