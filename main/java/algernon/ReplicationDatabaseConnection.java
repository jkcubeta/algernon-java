package algernon;

import security.DatabaseCredentials;

import java.sql.*;

public class ReplicationDatabaseConnection {

    private Connection connection;

    public ReplicationDatabaseConnection(DatabaseCredentials credentials){
        String dbUsername = credentials.getUsername();
        String dbPassword = credentials.getPassword();
        String dbAddress = credentials.getAddress();
        try {
            String address = "jdbc:mysql://"+ dbAddress +"?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC&generateSimpleParameterMetaData=true&zeroDateTimeBehavior=convertToNull";
            Class.forName("com.mysql.cj.jdbc.Driver");
            this.connection = DriverManager.getConnection(address, dbUsername, dbPassword);
        }catch(ClassNotFoundException e){
            e.printStackTrace();
        }catch(SQLException e){
            e.printStackTrace();
            throw new RuntimeException("cannot connect to remote database, check configuration and try again");
        }

    }

    public PreparedStatement createPreparedStatement(String sql) throws SQLException{
        return connection.prepareStatement(sql);
    }

    public CallableStatement createCallableStatement(String sql) throws SQLException{
        return connection.prepareCall(sql);
    }

}
