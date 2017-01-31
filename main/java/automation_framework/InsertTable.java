package automation_framework;

import java.sql.SQLException;
import java.util.List;

public class InsertTable {
    private AlgyWhisperer algyWhisperer;
    private EncryptedKeys keys;
    private List<String> pkColumnNames;
    String tableName;
    PrimaryKey topLocalPrimaryKey;
    PrimaryKey topRemotePrimaryKey;

    public static void main(String[] args){
        InsertTable table = new InsertTable("ClientVisit",true);
    }

    InsertTable(String tableName,Boolean synched){
        keys = new EncryptedKeys();
        algyWhisperer = new AlgyWhisperer("jcubeta","v!Rg22044","mbi-cloud-test.cnv3iqiknsnm.us-east-1.rds.amazonaws.com","algernon_cloud");
        this.tableName = tableName;
        try {
            topLocalPrimaryKey = algyWhisperer.getTopPk(tableName);
            //topRemotePrimaryKey = getTopRemotePk(tableName);
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException("can't connect to remote database, check configuration and try again");
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("can't get into credible database");
        }
    }


}
