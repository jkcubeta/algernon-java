package algernon;


import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import java.io.IOException;
import java.io.StringReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteData {

    private List<String> header;
    private List<Map<String,String>> data;
    private List<Element> tables;

    RemoteData(String keyId, String inputParam1, String inputParam2, String inputParam3){
        if(keyId != null) {
            String param1;
            String param2;
            String param3;
            if (inputParam1 == null) {
                param1 = "";
            } else {
                param1 = inputParam1;
            }
            if (inputParam2 == null) {
                param2 = "";
            } else {
                param2 = inputParam2;
            }
            if (inputParam3 == null) {
                param3 = "";
            } else {
                param3 = inputParam3;
            }
            data = new ArrayList<>();
            header = new ArrayList<>();
            tables = new ArrayList<>();
            URI uri = null;
            try {
                uri = new URIBuilder()
                        .setScheme("http")
                        .setHost("reportservices.crediblebh.com")
                        .setPath("/reports/ExportService.asmx/ExportXML")
                        .setParameter("connection", keyId)
                        .setParameter("start_date", "")
                        .setParameter("end_date", "")
                        .setParameter("custom_param1", param1)
                        .setParameter("custom_param2", param2)
                        .setParameter("custom_param3", param3)
                        .build();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            HttpGet httpGet = new HttpGet(uri);
            HttpClient httpclient = HttpClients.createDefault();
            HttpResponse response1 = null;
            try {
                response1 = httpclient.execute(httpGet);
            } catch (IOException e) {
                e.printStackTrace();
            }
            HttpEntity entity1 = response1.getEntity();
            SAXBuilder saxBuilder = new SAXBuilder();
            Document document;
            Document document2 = null;
            try {
                document = saxBuilder.build(entity1.getContent());
            } catch (IOException | UnsupportedOperationException | JDOMException e) {
                e.printStackTrace();
                throw new RuntimeException("credible server down, again...");
            }
            Element root = document.getRootElement();
            try {
                document2 = saxBuilder.build(new StringReader(root.getText()));
            } catch (IOException | UnsupportedOperationException | JDOMException e) {
                e.printStackTrace();
            }
            Element root2 = document2.getRootElement();
            List<Element> children = root2.getChildren();
            if (children.size() > 0) {
                List<Element> headerElements = children.get(0).getChildren();
                for (Element headerElement : headerElements) {
                    header.add(headerElement.getName());
                }
                for (Element child : children) {
                    Map<String, String> dataLine = new HashMap<>();
                    List<Element> fields = child.getChildren();
                    for (Element field : fields) {
                        tables.add(field);
                        String entryHeader = field.getName();
                        String entry = field.getText();
                        dataLine.put(entryHeader, entry);
                    }
                    data.add(dataLine);
                }
            }else{
                throw new ReplicationDatabaseConnectionError();
            }
        }
    }

    public List<Map<String,String>> getData() {
        return data;
    }

    public Map<String,Map<String,String>> getMapData(){
        Map<String,Map<String,String>> outputData = new HashMap<>();
        for(Map<String,String> dataLine : data){
            String likelyPKName = header.get(0);
            String likelyPKValue = dataLine.get(likelyPKName);
            outputData.put(likelyPKValue,dataLine);
        }
        return outputData;
    }

    public List<String> getHeader(){
        return header;
    }

    public List<Element> getTables(){
        return tables;
    }

}

