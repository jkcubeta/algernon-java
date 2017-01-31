package inspector;

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

    public static void main(String[] args) {
        Inspector inspector = new Inspector(args[0], args[1], args[2]);
        HashMap<List<Integer>,InsuranceResults> cleanedResults = new HashMap<>();
        HashMap<Integer,String[]> allParts = new HashMap<>();

        Map<List<Integer>,String> results;
        try {
            results = inspector.getScrubbedHtml();
        } catch (SQLException var19) {
            var19.printStackTrace();
            throw new RuntimeException("can\'t reach replication database");
        }

        for(List<Integer> clientId : results.keySet()) {
            Document document = Jsoup.parse(results.get(clientId));
            Elements tables = document.getElementsByTag("table");
            Element medicaidTable = tables.get(2);
            Element mcoTable = tables.get(3);
            Element medicareTable = tables.get(4);
            Element tplTable = tables.get(6);
            String rawMedicareText = medicareTable.text();
            String rawTplText = tplTable.text();
            String rawMedicaidText = medicaidTable.text();
            String rawMcoText = mcoTable.text();
            String rawResult = rawMedicaidText + " " + rawMcoText + " " + rawMedicareText + " " + rawTplText;
            InsuranceResults result = new InsuranceResults(rawResult);
            if(result.other) {
                allParts.put(clientId.get(0), result.parts);
            }
            cleanedResults.put(clientId, result);
        }

        try {
            inspector.submitChanges(cleanedResults);
            for(Integer clientId : allParts.keySet()){
                System.out.println("couldn\'t classify this client\'s insurance, verify manually");
                System.out.println(clientId);
            }

        } catch (SQLException var20) {
            var20.printStackTrace();
            throw new RuntimeException("can\'t reach replication database");
        }
    }

    private Inspector(String remoteAddress, String username, String password) {
        try {
            String e = "jdbc:mysql://" + remoteAddress + "?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC";
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(e, username, password);
        } catch (ClassNotFoundException var5) {
            var5.printStackTrace();
        } catch (SQLException var6) {
            var6.printStackTrace();
            throw new RuntimeException("cannot connect to remote database, check configuration and try again");
        }

    }

    private Map<List<Integer>, String> getScrubbedHtml() throws SQLException {
        HashMap<List<Integer>,String> results = new HashMap<>();
        PreparedStatement ps = con.prepareStatement("SELECT client_id,max(scrub_id) as scrub_id,scrubbed_html FROM ScrubHistory WHERE scrubbed_html IS NOT NULL GROUP BY client_id;");
        ResultSet rs = ps.executeQuery();

        while(rs.next()) {
            List<Integer> key = new ArrayList<>();
            Integer credibleId = rs.getInt(1);
            key.add(credibleId);
            Integer scrubId = rs.getInt(2);
            key.add(scrubId);
            String html = rs.getString(3);
            results.put(key, html);
        }

        return results;
    }

    private void submitChanges(Map<List<Integer>, InsuranceResults> results) throws SQLException {
        SimpleDateFormat sdf = new SimpleDateFormat("mm/dd/yyyy");
        PreparedStatement insertStatement = con.prepareStatement("INSERT IGNORE INTO ScrubResult (client_id, scrub_id, program_desc, program_code, mco_name, is_medicaid, is_medicare, is_mco, is_other, is_uninsured, is_ltc, begin_date, end_date) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?);");
        for (Object o : results.keySet()) {
            List key = (List) o;
            Integer clientId = (Integer) key.get(0);
            Integer scrubId = (Integer) key.get(1);
            InsuranceResults result = results.get(key);
            Date beginDate;
            if (result.beginDate != null) {
                try {
                    beginDate = new Date(sdf.parse(result.beginDate).getTime());
                } catch (ParseException var12) {
                    throw new RuntimeException("can\'t parse date using pattern yyyy-mm-dd");
                }
            } else {
                beginDate = new Date(0L);
            }

            Date endDate;
            if (result.endDate != null) {
                try {
                    endDate = new Date(sdf.parse(result.endDate).getTime());
                } catch (ParseException var13) {
                    throw new RuntimeException("can\'t parse date using pattern yyyy-mm-dd");
                }
            } else {
                endDate = new Date(0L);
            }

            insertStatement.setInt(1, clientId);
            insertStatement.setInt(2, scrubId);
            insertStatement.setString(3, result.program);
            insertStatement.setString(4, result.programCode);
            insertStatement.setString(5, result.mcoName);
            insertStatement.setBoolean(6, result.medicaid);
            insertStatement.setBoolean(7, result.medicare);
            insertStatement.setBoolean(8, result.mco);
            insertStatement.setBoolean(9, result.other);
            insertStatement.setBoolean(10, result.uninsured);
            insertStatement.setBoolean(11, result.ltc);
            insertStatement.setDate(12, beginDate);
            insertStatement.setDate(13, endDate);
            insertStatement.addBatch();
        }

        insertStatement.executeBatch();
        System.out.println("completed");
    }
}
