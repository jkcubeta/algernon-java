package security;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEParameterSpec;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class EncryptedProperties extends Properties {
    private static sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
    private static sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
    private Cipher encrypter, decrypter;

    public EncryptedProperties(String password){
        String storedSalt;
        ClassLoader loader = this.getClass().getClassLoader();
        try {
            InputStream saltIn = loader.getResourceAsStream("salt.properties");
            Properties saltProperties = new Properties();
            saltProperties.load(saltIn);
            storedSalt = saltProperties.getProperty("salt");
        }catch(IOException e) {
            throw new RuntimeException("troubles loading the salt, check the rsc file to confirm");
        }

        try {
            InputStream in = loader.getResourceAsStream("secrets.properties");
            if (in != null) {
                super.load(in);
            } else {
                throw new FileNotFoundException("property file not found at classpath");
            }
        }catch(Exception e){
            throw new RuntimeException("can't find secrets property file on classpath, or file is corrupt");
        }
        try {
            byte[] salt = decoder.decodeBuffer(storedSalt);
            PBEParameterSpec ps = new PBEParameterSpec(salt, 20);
            SecretKeyFactory kf = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
            SecretKey k = kf.generateSecret(new javax.crypto.spec.PBEKeySpec(password.toCharArray()));
            encrypter = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");
            decrypter = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");
            encrypter.init(Cipher.ENCRYPT_MODE, k, ps);
            decrypter.init(Cipher.DECRYPT_MODE, k, ps);
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("encryption failure, check to see that specified algorithm exists and that padding is set correctly");
        }
    }

    public String getProperty(String key) {
        try {
            return decrypt(super.getProperty(key));
        } catch( Exception e ) {
            throw new RuntimeException("incorrect password, please try again");
        }
    }

    public synchronized Object setProperty(String key, String value) {
        try {
            return super.setProperty(key, encrypt(value));
        } catch( Exception e ) {
            throw new RuntimeException("Couldn't encrypt property");
        }
    }

    private synchronized String decrypt(String str) throws Exception {
        byte[] dec = decoder.decodeBuffer(str);
        byte[] utf8 = decrypter.doFinal(dec);
        return new String(utf8, "UTF-8");
    }

    private synchronized String encrypt(String str) throws Exception {
        byte[] utf8 = str.getBytes("UTF-8");
        byte[] enc = encrypter.doFinal(utf8);
        return encoder.encode(enc);
    }

}