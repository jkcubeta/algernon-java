package security;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class DatabaseCredentials {

    private String username;
    private String password;
    private String address;
    private String dbName;
    private String internalKey;

    public DatabaseCredentials(String secretKey){
        EncryptedProperties properties = new EncryptedProperties(secretKey);
        Properties config = new Properties();
        ClassLoader loader = this.getClass().getClassLoader();
        try{
            InputStream configIn = loader.getResourceAsStream("config.properties");
            config.load(configIn);
            configIn.close();
        }catch(IOException e){
            throw new RuntimeException("can't find config file in rsc folder, check and try again");
        }
        this.username = properties.getProperty("algernon_username");
        this.password = properties.getProperty("algernon_password");
        this.internalKey = properties.getProperty("internal_key");
        this.address = config.getProperty("remote_replication_database");
        this.dbName = config.getProperty("remote_replication_schema");
    }

    public synchronized String getUsername(){
        return username;
    }

    public synchronized String getPassword() {
        return password;
    }

    public synchronized String getAddress(){
        return address+"/"+dbName;
    }

    public synchronized String getInternalKey(){
        return internalKey;
    }

}
