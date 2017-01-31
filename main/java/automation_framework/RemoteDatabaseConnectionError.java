package automation_framework;

class RemoteDatabaseConnectionError extends RuntimeException{

    RemoteDatabaseConnectionError(){
        super("can't reach remote database, check internet connection and decrypted key and try again");
    }

}
