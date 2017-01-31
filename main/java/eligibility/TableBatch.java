package eligibility;

import algernon.AlgyEligibilityWhisperer;
import algernon.ReplicationDatabaseConnectionError;
import pump.Log;
import security.DatabaseCredentials;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

class TableBatch {

    private PreparedStatement resultStatement;
    private Integer maxRows;
    private Log log;
    private Integer resultCount = 0;

    TableBatch(DatabaseCredentials credentials, Integer maxRows){
        this.log = new Log();
        AlgyEligibilityWhisperer whisperer = new AlgyEligibilityWhisperer(credentials);
        this.maxRows = maxRows;
        try {
            this.resultStatement = whisperer.buildScrubHistoryInsert();
        }catch(SQLException e){
            throw new ReplicationDatabaseConnectionError();
        }
    }

    synchronized Boolean executeResultStatement() throws SQLException{
        try {
            log.log("firing clean up result batch for eligibility");
            resultStatement.executeBatch();
            return true;
        }catch(BatchUpdateException e){
            throw new ReplicationDatabaseConnectionError();
        }catch(Exception e){
            return false;
        }
    }

    synchronized Boolean addResultAndBatch(HtmlOutputRecord record) throws SQLException{
        addResult(record);
        if(resultCount >= maxRows){
            log.log("firing result batch for eligibility ("+maxRows+" records in batch)");
            try {
                resultStatement.executeBatch();
            }catch (BatchUpdateException e){
                throw new ReplicationDatabaseConnectionError();
            }
            resultCount = 0;
            return true;
        }else{
            return false;
        }
    }

    private PreparedStatement addResult(HtmlOutputRecord record) throws SQLException{
        resultStatement.setInt(1,record.getCredibleId());
        resultStatement.setTimestamp(2,record.getStartTime());
        resultStatement.setTimestamp(3,record.getEndTime());
        resultStatement.setString(4,record.getTableHtml());
        resultStatement.addBatch();
        resultCount++;
        return resultStatement;
    }

}