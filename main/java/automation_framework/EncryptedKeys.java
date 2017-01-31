package automation_framework;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEParameterSpec;
import java.security.SecureRandom;
import java.sql.ResultSet;
import java.sql.SQLException;

class EncryptedKeys {

    private Cipher encrypter, decrypter;
    private static sun.misc.BASE64Encoder encoder;
    private static sun.misc.BASE64Decoder decoder;
    private static String dbName = Robot.dbName;

    EncryptedKeys(){
        encoder = new sun.misc.BASE64Encoder();
        decoder = new sun.misc.BASE64Decoder();
        try {
            encrypter = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");
            decrypter = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");
        }catch(Exception e){
            throw new RuntimeException("please verify encryption settings and try again");
        }
    }

    String getKeyById(String password,String keyId) throws Exception{
        String value = null;
        String salt = null;
        String decryptedValue = null;
        try {
            ResultSet keyRS = Robot.access.executeQuery("SELECT key_value_encrypted, salt FROM "+dbName+".`keys` WHERE key_id = " + keyId + ";");
            keyRS.next();
            value = keyRS.getString(1);
            salt = keyRS.getString(2);
        }catch(SQLException e){
            e.printStackTrace();
        }
        if(value!=null) {
            try {
                decryptedValue = decrypt(value, salt,password);
            } catch (Exception e) {
                throw new RuntimeException("Couldn't decrypt property");
            }
        }
        return decryptedValue;
    }

    String getKeyByName(String password, String keyName) throws Exception{
        String value = null;
        String salt = null;
        String decryptedValue = null;
        try {
            ResultSet keyRS = Robot.access.executeQuery("SELECT key_value_encrypted, salt FROM "+dbName+".`keys` WHERE key_name = '" + keyName + "';");
            keyRS.next();
            value = keyRS.getString(1);
            salt = keyRS.getString(2);
        }catch(SQLException e){
            e.printStackTrace();
        }
        if(value!=null) {
            try {
                decryptedValue =  decrypt(value, salt,password);
            } catch (Exception e) {
                throw new RuntimeException("Couldn't decrypt property");
            }
        }
        return decryptedValue;
    }

    String getKeyByTitle(String keyTitle) throws Exception{
        String salt = null;
        String decryptedValue = null;
        try {
            ResultSet keyRS = Robot.access.executeQuery("SELECT key_value_decrypted FROM "+dbName+".`keys` WHERE report_name = '" + keyTitle + "';");
            keyRS.next();
            decryptedValue = keyRS.getString(1);
        }catch(SQLException e){
            e.printStackTrace();
        }
        return decryptedValue;
    }

    String[] encryptString(String unencryptedValue,String password){
        byte[] salt = SecureRandom.getSeed(8);
        String encryptedValue;
        try {
            encryptedValue = encrypt(unencryptedValue, password, salt);
        }catch(Exception e){
            throw new RuntimeException("please check password and verify encryption settings");
        }
        String[] output = {encryptedValue,encoder.encode(salt)};
        return output;
    }

    String decryptString(String encryptedValue, String salt, String password){
        try {
            String decryptedValue = decrypt(encryptedValue, password, salt);
            return decryptedValue;
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("please check password and verify encryption settings");
        }
    }

    private synchronized String decrypt(String encryptedValue, String password, String storedSalt) throws Exception {
        byte[] salt = decoder.decodeBuffer(storedSalt);
        PBEParameterSpec ps = new PBEParameterSpec(salt, 20);
        SecretKeyFactory kf = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        SecretKey k = kf.generateSecret(new javax.crypto.spec.PBEKeySpec(password.toCharArray()));
        decrypter.init(Cipher.DECRYPT_MODE, k, ps);
        byte[] dec = decoder.decodeBuffer(encryptedValue);
        byte[] utf8 = decrypter.doFinal(dec);
        return new String(utf8, "UTF-8");
    }

    private synchronized String encrypt(String str, String password, byte[] salt) throws Exception {
        PBEParameterSpec ps = new PBEParameterSpec(salt, 20);
        SecretKeyFactory kf = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        SecretKey k = kf.generateSecret(new javax.crypto.spec.PBEKeySpec(password.toCharArray()));
        encrypter.init(Cipher.ENCRYPT_MODE, k, ps);
        byte[] utf8 = str.getBytes("UTF-8");
        byte[] enc = encrypter.doFinal(utf8);
        return encoder.encode(enc);
    }

}
