package automation_framework;

class RemoteDatabaseRecordDeletedError extends RuntimeException{

    RemoteDatabaseRecordDeletedError(String fieldName){
        super("the field request: "+fieldName+" appears to have been deleted or otherwise tampered with, investigate manually");
    }
}
