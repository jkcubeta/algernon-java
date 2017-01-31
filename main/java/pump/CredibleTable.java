package pump;

import java.util.Map;

interface CredibleTable {

    String getTableName();
    Header getHeader();
    Map<PrimaryKey, TableRow> getTableRows();
}
