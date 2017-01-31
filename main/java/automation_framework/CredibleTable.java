package automation_framework;

import java.util.Map;

interface CredibleTable {

    String getTableName();
    Map<String,String> getHeader();
    Map<Map<String,Integer>,TableRow> getTableRows();
}
