package algernon;

import java.sql.BatchUpdateException;

public class RemoteDatabaseConnectionError extends RuntimeException{

    public RemoteDatabaseConnectionError(){
        super("can't reach remote database, check internet connection and decrypted key and try again");
    }

    public RemoteDatabaseConnectionError(BatchUpdateException e){
        super("error occurred during execution of batch. error message: "+e.getMessage()+", executed "+e.getUpdateCounts().length+" updates prior to error ");
    }

    public RemoteDatabaseConnectionError(String e){
        super(e);
    }

}
