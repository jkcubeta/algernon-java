package automation_framework;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Logger;

public class Robot extends Thread {
	static AlgyWhisperer algyWhisperer;
    static String address;
    private static Boss boss;
    static Integer scrubThreadCount;
    static Integer tableThreadCount;
	static Logger logger;
	static Integer rosterGo;
	static Boolean redXGo;
	static EncryptedKeys keys;
	static String internalKey;
	static String dbUsername;
	static String dbPassword;
	static String dbName = "algernon_cloud";

	public static void main(String[] args){
		System.out.println("robot started");
		Robot.startUp();
		boss = new Boss();
		try {
			boss.startWork();
		}catch (Exception e){
			e.printStackTrace();
		}
	}

	private static void startUp(){
		System.setProperty("org.apache.commons.logging.simplelog.showdatetime", "true");
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.wire", "DEBUG");
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.impl.conn", "DEBUG");
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.impl.client", "DEBUG");
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http.client", "DEBUG");
		System.setProperty("org.apache.commons.logging.simplelog.log.org.apache.http", "DEBUG");
		Console console = new Console();
		String password = console.getPassword();
        address = console.getAddress();
        scrubThreadCount = console.getScrubThreadCount();
		redXGo = console.getRedXGo();
		tableThreadCount = console.getRosterGo();
		EncryptedProperties properties = Robot.loadProperties(password);
		internalKey = properties.getProperty("internal_key");
		dbUsername = properties.getProperty("algernon_username");
		dbPassword = properties.getProperty("algernon_password");
        //Probe probe = new Probe(internalKey,dbUsername,dbPassword,address,dbName);
		algyWhisperer = new AlgyWhisperer(dbUsername,dbPassword,address,dbName);
		/*access = null;
		try {
			access = new Access(dbUsername,dbPassword);
		}catch(SQLException e){
			e.printStackTrace();
			throw new RuntimeException("cannot find database with address and credentials given, please verify and try again");
		}
		*/
		try {
			keys = new EncryptedKeys();
		}catch(Exception e){
			e.printStackTrace();
		}

	}

	private static EncryptedProperties loadProperties(String password){
		String salt;
		EncryptedProperties properties = null;
		ClassLoader loader = Robot.class.getClassLoader();
		try {
			InputStream saltIn = loader.getResourceAsStream("salt.properties");
			Properties saltProperties = new Properties();
			saltProperties.load(saltIn);
			salt = saltProperties.getProperty("salt");
		}catch(IOException e) {
			throw new RuntimeException("troubles loading the salt, check the rsc file to confirm");
		}
		try{
			properties = new EncryptedProperties(password,salt);
			InputStream in = loader.getResourceAsStream("secrets.properties");
			if (in != null) {
				properties.load(in);
			} else {
				throw new FileNotFoundException("property file not found at classpath");
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return properties;
	}
}