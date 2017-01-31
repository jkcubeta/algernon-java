package automation_framework;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEParameterSpec;
import java.util.Properties;

class EncryptedProperties extends Properties {
    private static sun.misc.BASE64Decoder decoder = new sun.misc.BASE64Decoder();
    private static sun.misc.BASE64Encoder encoder = new sun.misc.BASE64Encoder();
    private Cipher encrypter, decrypter;

    EncryptedProperties(String password, String storedSalt) throws Exception {
        byte[] salt = decoder.decodeBuffer(storedSalt);
        PBEParameterSpec ps = new PBEParameterSpec(salt, 20);
        SecretKeyFactory kf = SecretKeyFactory.getInstance("PBEWithMD5AndDES");
        SecretKey k = kf.generateSecret(new javax.crypto.spec.PBEKeySpec(password.toCharArray()));
        encrypter = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");
        decrypter = Cipher.getInstance("PBEWithMD5AndDES/CBC/PKCS5Padding");
        encrypter.init(Cipher.ENCRYPT_MODE, k, ps);
        decrypter.init(Cipher.DECRYPT_MODE, k, ps);
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