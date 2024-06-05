package shard;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import javax.crypto.spec.OAEPParameterSpec;

/**
 * Hello world!
 *
 */
public class App 
{

    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    private static final int KEY_LENGTH = 5;
    private static final int MIN_VALUE_LENGTH = 16;
    private static final int MAX_VAL_LENGTH = 32;
    private static final int PAIRS_COUNT = 750;

    public static void main( String[] args )
    {
        final LSMShard lsmShard = new LSMShard();
        Map<String, String> randomValuePairs = generateRandomKeyValuePairs(PAIRS_COUNT);
        for (Map.Entry<String, String> entry : randomValuePairs.entrySet()) {
            lsmShard.put(entry.getKey(), entry.getValue());
        }
        System.out.println("Done");
    }


    public static Map<String, String> generateRandomKeyValuePairs(int count) {
        Map<String, String> map = new HashMap<>();
        Random random = new Random();
        Random valRandom = new Random();

        for (int i = 0; i < count; i++) {
            String key = generateRandomString(random, KEY_LENGTH, KEY_LENGTH);
            String value = generateRandomString(valRandom, MIN_VALUE_LENGTH, MAX_VAL_LENGTH);
            map.put(key, value);
        }

        return map;
    }

    private static String generateRandomString(Random random, int minlength, int maxLength) {
        int length;
        if (minlength == maxLength) {
            length = maxLength;
        } else {
            Random lengthRandom = new Random();
            length = lengthRandom.nextInt((maxLength - minlength)  + 1) + minlength;
        }
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            sb.append(CHARACTERS.charAt(random.nextInt(CHARACTERS.length())));
        }

        return sb.toString();
    }
}
