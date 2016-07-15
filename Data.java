package algernon;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
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
import java.util.List;

class Data {
    private String name;
    private ArrayList<String[]> data;

    Data(String keyId, String reportName,String inputParam1, String inputParam2) throws Exception {
        name = reportName;
        String decryptedKey = Main.keys.getKeyById(keyId);
        if(decryptedKey != null) {
            String param1;
            String param2;
            if(inputParam1==null){
                param1 = "";
            }else{
                param1 = inputParam1;
            }
            if(inputParam2==null){
                param2  = "";
            }else{
                param2=inputParam2;
            }
            name = reportName;
            data = new ArrayList<>();
            ArrayList<String> header = new ArrayList<>();
            URI uri = null;
            try {
                uri = new URIBuilder()
                        .setScheme("http")
                        .setHost("reportservices.crediblebh.com")
                        .setPath("/reports/ExportService.asmx/ExportXML")
                        .setParameter("connection", decryptedKey)
                        .setParameter("start_date", "")
                        .setParameter("end_date", "")
                        .setParameter("custom_param1", param1)
                        .setParameter("custom_param2", param2)
                        .setParameter("custom_param3", "")
                        .build();
            } catch (URISyntaxException e) {
                e.printStackTrace();
            }
            HttpGet httpGet = new HttpGet(uri);
            CloseableHttpClient httpclient = HttpClients.createDefault();
            CloseableHttpResponse response1 = null;
            try {
                response1 = httpclient.execute(httpGet);
            } catch (IOException e) {
                e.printStackTrace();
            }
            HttpEntity entity1 = response1.getEntity();
            SAXBuilder saxBuilder = new SAXBuilder();
            Document document = null;
            Document document2 = null;
            try {
                document = saxBuilder.build(entity1.getContent());
            } catch (IOException | UnsupportedOperationException | JDOMException e) {
                e.printStackTrace();
            }
            Element root = document.getRootElement();
            try {
                document2 = saxBuilder.build(new StringReader(root.getText()));
            } catch (IOException | UnsupportedOperationException | JDOMException e) {
                e.printStackTrace();
            }
            Element root2 = document2.getRootElement();
            List<Element> children = root2.getChildren();
            List<Element> headerElements = children.get(0).getChildren();
            for (Element headerElement : headerElements) {
                header.add(headerElement.getName());
            }
            for (int j = 0; j < children.size(); j++) {
                ArrayList<String> dataLine = new ArrayList<>();
                Element child = children.get(j);
                List<Element> fields = child.getChildren();
                for (int headerCount = 0; headerCount < header.size(); headerCount++) {
                    String entry = fields.get(headerCount).getText();
                    dataLine.add(entry);
                }
                String[] outputDataLine = new String[dataLine.size()];
                dataLine.toArray(outputDataLine);
                data.add(outputDataLine);
            }
            data.add(0, header.toArray(new String[header.size()]));
        }
    }

    Data(String name, ArrayList<ArrayList<String>> data){
        this.name = name;
        ArrayList<String []> outputData = new ArrayList<>();
        for(ArrayList<String> entry : data ){
            String[] entryArray = new String[entry.size()];
            entry.toArray(entryArray);
            outputData.add(entryArray);
        }
        this.data = outputData;
    }

    ArrayList<String[]> getData() {
        return data;
    }

    void setName(String name){
        this.name = name;
    }

    String getName(){
        return name;
    }

    String getSheetName(){
        return name;
    }

}

