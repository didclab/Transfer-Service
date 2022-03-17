package org.onedatashare.transferservice.odstransferservice;

import com.amazonaws.util.Base64;
import org.apache.commons.codec.binary.Hex;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * @author deepika
 */
public class FileHashValidatorTest {

    @Test
    public void compareSameFile() throws NoSuchAlgorithmException {
        MessageDigest messageDigest1 = MessageDigest.getInstance("SHA-1");
        MessageDigest messageDigest2 = MessageDigest.getInstance("SHA-1");
        MessageDigest messageDigest3 = MessageDigest.getInstance("SHA-1");
        messageDigest1.update("test1".getBytes(StandardCharsets.UTF_8));
        messageDigest2.update("test1".getBytes(StandardCharsets.UTF_8));
        byte[] b1= messageDigest1.digest();
        //messageDigest2.reset(); --> reseting here changes the value
        byte[] b2= messageDigest2.digest();
        //messageDigest2.reset();
        byte[] b3= messageDigest3.digest();

        System.out.println(Arrays.toString(b1));
        System.out.println(Arrays.toString(b2));
        System.out.println(Arrays.toString(b3));
        System.out.println(Hex.encodeHex(b1));
        System.out.println(Hex.encodeHex(b2));
        System.out.println(Hex.encodeHex(b3));
        System.out.println(Arrays.equals(b1, b2));
    }


    @Test
    public void compareDifferentFiles() throws NoSuchAlgorithmException {
        MessageDigest messageDigest = MessageDigest.getInstance("SHA-1");

        // file hashing with DigestInputStream

        try (InputStream is = getClass().getClassLoader().getResourceAsStream("test.txt")) {
            byte[] buffer = new byte[1024];
            int read;
            while ((read = is.read(buffer)) != -1) {
                    messageDigest.update(buffer, 0, read);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//[-38, 57, -93, -18, 94, 107, 75, 13, 50, 85, -65, -17, -107, 96, 24, -112, -81, -40, 7, 9]

        // bytes to hex
        StringBuilder result = new StringBuilder();
        for (byte b : messageDigest.digest()) {
            result.append(String.format("%02x", b));
        }
        byte[] b = messageDigest.digest();
        System.out.println(Arrays.toString(b));
        System.out.println(Hex.encodeHex(b));
        System.out.println(result.toString());
    }

}
