package automation_framework;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.sql.*;
import java.sql.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Inspector {
    private static Connection con;

    public static void main(String[] args){
        Inspector inspector = new Inspector(args[0],args[1],args[2]);
        Map<List<Integer>,String> results;
        Map<List<Integer>,InsuranceResults> cleanedResults = new HashMap<>();
        Map<Integer,String[]> allParts = new HashMap<>();

        try {
            results = inspector.getScrubbedHtml();
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException("can't reach replication database");
        }
        for(List<Integer> key : results.keySet()) {
            Document document = Jsoup.parse(results.get(key));
            Elements tables = document.getElementsByTag("table");
            Element medicaidTable = tables.get(2);
            Element mcoTable = tables.get(3);
            Element medicareTable = tables.get(4);
            Element tplTable = tables.get(6);
            String rawMedicareText = medicareTable.text();
            String rawTplText = tplTable.text();
            String rawMedicaidText = medicaidTable.text();
            String rawMcoText = mcoTable.text();
            String rawResult = rawMedicaidText + " "+ rawMcoText +" " + rawMedicareText + " " + rawTplText;
            InsuranceResults result = new InsuranceResults(rawResult);
            if(result.other) {
                allParts.put(key.get(0),result.parts);
            }
            cleanedResults.put(key, result);
        }
        try {
            inspector.submitChanges(cleanedResults);
            for(Integer clientId : allParts.keySet()){
                System.out.println("couldn't classify this client's insurance, verify manually");
                System.out.println(clientId);
            }
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException("can't reach replication database");
        }
    }

    private Inspector(String remoteAddress,String username, String password){
        try {
            String address = "jdbc:mysql://" + remoteAddress + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(address, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("cannot connect to remote database, check configuration and try again");
        }
    }

    private Map<List<Integer>,String> getScrubbedHtml() throws SQLException{
        Map<List<Integer>,String> results = new HashMap<>();
        PreparedStatement ps = con.prepareStatement("SELECT client_id,max(scrub_id) as scrub_id,scrubbed_html FROM ScrubHistory WHERE scrubbed_html IS NOT NULL GROUP BY client_id;");
        ResultSet rs = ps.executeQuery();
        while(rs.next()){
            List<Integer> key = new ArrayList<>();
            Integer credibleId = rs.getInt(1);
            key.add(credibleId);
            Integer scrubId = rs.getInt(2);
            key.add(scrubId);
            String html = rs.getString(3);
            results.put(key,html);
        }
        return results;
    }

    private void submitChanges(Map<List<Integer>,InsuranceResults> results) throws SQLException{
        SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yyyy");
        PreparedStatement insertStatement = con.prepareStatement("INSERT IGNORE INTO ScrubResult (client_id, scrub_id, program_desc, program_code, mco_name, is_medicaid, is_medicare, is_mco, is_other, is_uninsured, is_ltc, begin_date, end_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);");
        for(List<Integer> key : results.keySet()){
            Integer clientId = key.get(0);
            Integer scrubId = key.get(1);
            InsuranceResults result = results.get(key);
            java.sql.Date beginDate;
            java.sql.Date endDate;
            if(result.beginDate != null) {
                try {
                    beginDate = new Date(sdf.parse(result.beginDate).getTime());
                } catch (ParseException e) {
                    throw new RuntimeException("can't parse date using pattern yyyy-mm-dd");
                }
            }else{
                beginDate = new Date(0);
            }
            if(result.endDate != null) {
                try {
                    endDate = new Date(sdf.parse(result.endDate).getTime());
                } catch (ParseException e) {
                    throw new RuntimeException("can't parse date using pattern yyyy-mm-dd");
                }
            }else{
                endDate = new Date(0);
            }
            insertStatement.setInt(1,clientId);
            insertStatement.setInt(2,scrubId);
            insertStatement.setString(3,result.program);
            insertStatement.setString(4,result.programCode);
            insertStatement.setString(5,result.mcoName);
            insertStatement.setBoolean(6,result.medicaid);
            insertStatement.setBoolean(7,result.medicare);
            insertStatement.setBoolean(8,result.mco);
            insertStatement.setBoolean(9,result.other);
            insertStatement.setBoolean(10,result.uninsured);
            insertStatement.setBoolean(11,result.ltc);
            insertStatement.setDate(12,beginDate);
            insertStatement.setDate(13,endDate);
            insertStatement.addBatch();
        }
        insertStatement.executeBatch();
        System.out.println("completed");
    }

}

class InsuranceResults{
    Boolean uninsured = false;
    Boolean medicaid = false;
    Boolean mco = false;
    Boolean medicare = false;
    Boolean ltc = false;
    Boolean other = true;
    String program;
    String programCode;
    String mcoName;
    String status;
    String beginDate;
    String endDate;
    String qmbCode;
    String[] parts;

    InsuranceResults(String rawResults) {
        String shortenedResults = removeServices(rawResults);
        parts = standardize(shortenedResults);
        if(parts.length <= 3 || parts.length >= 20){
            other = true;
        //assess for uninsured
        }else if(parts.length == 4) {
            if (parts[1].contains("N/A") && parts[2].contains("N/A") && parts[3].contains("N/A")) {
                uninsured = true;
                other = false;
            }
        //check for malformed straight medicare
        }else if(parts.length == 7){
            if(parts[4].contains("A HIC Number:")){
                this.medicare = true;
                this.other = false;
            }else{
                this.other = true;
            }
        //check for straight medicaid
        }else if(parts.length == 9 ) {
            if (parts[7].contains("N/A") && parts[8].contains("N/A")) {
                String program = parts[1].replace(String.valueOf((char) 160), "");
                String programCode = parts[2].replace(String.valueOf((char) 160), "");
                String status = parts[3].replace(String.valueOf((char) 160), "");
                String beginDate = parts[4].replace(String.valueOf((char) 160), "");
                String endDate = parts[5].replace(String.valueOf((char) 160), "");
                String qmbCode = parts[6].replace(String.valueOf((char) 160), "");
                this.program = program.trim();
                this.programCode = programCode.trim();
                this.status = status.trim();
                this.beginDate = beginDate.trim();
                this.endDate = endDate.trim();
                this.qmbCode = qmbCode.trim();
                this.uninsured = false;
                this.other = false;
            } else {
                this.other = true;
            }
        //check for Medicare with no program
        }else if(parts.length == 10) {
            if (parts[4].contains("A HIC Number:")) {
                this.medicare = true;
                this.other = false;
            }
        }else if(parts.length == 11){
            if(parts[10].contains("Provider Name")){
                this.ltc = true;
                this.other = false;
                this.program = parts[1].replace(String.valueOf((char) 160), "").trim();
                this.programCode = parts[2].replace(String.valueOf((char) 160), "").trim();
                this.status = parts[3].replace(String.valueOf((char) 160), "").trim();
                this.beginDate = parts[4].replace(String.valueOf((char) 160), "").trim();
                this.endDate = parts[5].replace(String.valueOf((char) 160), "").trim();
            }else{
                this.other = true;
            }
        //check for MCO
        }else if(parts.length == 13) {
            if (parts[7].contains("MCO")) {
                this.mco = true;
                this.mcoName = parts[10].replace(String.valueOf((char) 160), "").trim();
                String program = parts[1].replace(String.valueOf((char) 160), "");
                String programCode = parts[2].replace(String.valueOf((char) 160), "");
                String status = parts[3].replace(String.valueOf((char) 160), "");
                String beginDate = parts[4].replace(String.valueOf((char) 160), "");
                String endDate = parts[5].replace(String.valueOf((char) 160), "");
                String qmbCode = parts[6].replace(String.valueOf((char) 160), "");
                this.program = program.trim();
                this.programCode = programCode.trim();
                this.status = status.trim();
                this.beginDate = beginDate.trim();
                this.endDate = endDate.trim();
                this.qmbCode = qmbCode.trim();
                this.uninsured = false;
                this.other = false;
            } else {
                this.other = true;
            }
        //check for malformed managed medicare
        }else if(parts.length == 16){
            this.mco = parts[7].contains("MCO");
            this.medicare = parts[13].contains("A HIC Number:");
            if(this.medicare || this.mco){
                this.mco = true;
                this.mcoName = parts[10].replace(String.valueOf((char) 160), "").trim();
                String program = parts[1].replace(String.valueOf((char) 160), "");
                String programCode = parts[2].replace(String.valueOf((char) 160), "");
                String status = parts[3].replace(String.valueOf((char) 160), "");
                String beginDate = parts[4].replace(String.valueOf((char) 160), "");
                String endDate = parts[5].replace(String.valueOf((char) 160), "");
                String qmbCode = parts[6].replace(String.valueOf((char) 160), "");
                this.program = program.trim();
                this.programCode = programCode.trim();
                this.status = status.trim();
                this.beginDate = beginDate.trim();
                this.endDate = endDate.trim();
                this.qmbCode = qmbCode.trim();
                this.uninsured = false;
                this.other = false;
            }else{
                this.other = true;
            }
        //check for  managed medicare
        }else if(parts.length == 19) {
            this.medicare = parts[13].contains("A HIC Number:");
            //check for MCO managed Medicare
            this.mco = parts[10].contains("MCO");
            if(this.medicare || this.mco){
                this.mco = true;
                this.mcoName = parts[10].replace(String.valueOf((char) 160), "").trim();
                String program = parts[1].replace(String.valueOf((char) 160), "");
                String programCode = parts[2].replace(String.valueOf((char) 160), "");
                String status = parts[3].replace(String.valueOf((char) 160), "");
                String beginDate = parts[4].replace(String.valueOf((char) 160), "");
                String endDate = parts[5].replace(String.valueOf((char) 160), "");
                String qmbCode = parts[6].replace(String.valueOf((char) 160), "");
                this.program = program.trim();
                this.programCode = programCode.trim();
                this.status = status.trim();
                this.beginDate = beginDate.trim();
                this.endDate = endDate.trim();
                this.qmbCode = qmbCode.trim();
                this.uninsured = false;
                this.other = false;
            }else{
                this.other = true;
            }
        }else{
            this.other = true;
        }
    }

    private String removeServices(String rawResults){
        Integer removeStart = rawResults.indexOf("Service types");
        if(removeStart >0) {
            Integer mcoRemoveEnd = rawResults.indexOf("Service Management");
            Integer tplRemoveEnd = rawResults.indexOf("Third Party Liability");
            Integer medicareRemoveEnd = rawResults.indexOf("Medicare Information");
            Integer ltcRemoveEnd = rawResults.indexOf("Long Term Care");
            List<Integer> ends = new ArrayList<>();
            ends.add(medicareRemoveEnd);
            ends.add(tplRemoveEnd);
            ends.add(ltcRemoveEnd);
            ends.add(mcoRemoveEnd);
            ends.add(rawResults.length());
            ends.removeAll(Collections.singleton(-1));
            Collections.sort(ends);
            Integer end = ends.get(0);
            if (end > removeStart) {
                try {
                    String removeTarget = rawResults.substring(removeStart,end);
                    rawResults = rawResults.replace(removeTarget, "");
                } catch (IndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
            }
        }
        return rawResults;
    }

    private String[] standardize(String rawResults){
        rawResults = rawResults.replace("Plan Coverage Information Plan Coverage:","x");
        rawResults = rawResults.replace("Plan Coverage Information","x");
        rawResults = rawResults.replace("Program Code:","x");
        rawResults = rawResults.replace("Eligibility or Benefit Information:","x");
        rawResults = rawResults.replace("Medicare Information","x");
        rawResults = rawResults.replace("Third Party Liability Information","x");
        rawResults = rawResults.replace("Begin Date:","x");
        rawResults = rawResults.replace("End Date:","x");
        rawResults = rawResults.replace("QMB Indicator:","x");
        rawResults = rawResults.replace("Service Management Service Management Type:","x");
        rawResults = rawResults.replace("Provider:","x");
        rawResults = rawResults.replace("Service types","x");
        rawResults = rawResults.replace("Part A/B Indicator:","x");
        rawResults = rawResults.replace("Service Management","x");
        rawResults = rawResults.replace("Long Term Care Information","x");

        return rawResults.split("x");
    }

}

