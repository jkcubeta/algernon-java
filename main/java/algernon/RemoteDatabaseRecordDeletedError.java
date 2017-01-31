package algernon;

public class RemoteDatabaseRecordDeletedError extends RuntimeException{

    public RemoteDatabaseRecordDeletedError(String fieldName){
        super("the field request: "+fieldName+" appears to have been deleted or otherwise tampered with, investigate manually");
    }
}
