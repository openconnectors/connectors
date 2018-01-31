package org.openconnectors.token;

import java.time.Instant;

public class CryptoCompareUtils {

    public static final String separator = "~";
    /*
     * Change Flag 4 means no rate change. We can ignore these as we will catch
     * up volume changes on next "valid" message
     */
    public static final String unacceptedFlag = "4";
    /*
     * Type flag = 5 is for rates
     */
    public static final String acceptedType = "5";

    public static String hexStringToBinaryString(String hex) {
        return String.format("%16s", Integer.toBinaryString(Integer.parseInt(hex, 16))).replace(' ', '0');
    }

    public static boolean validateMask(String mask) {
        /*
         * we need price, volume24Hour and volume24HourTo every other record is
         * useless for us at this point every record by default has type,
         * market, fromSymbol, toSymbol and flag
         */

        if (mask.charAt(15) == '1' && mask.charAt(5) == '1' && mask.charAt(4) == '1') {
            return true;
        }

        return false;
    }

    public static LiveRate stringArrayToLiveRate(String[] items, String mask) {
        /*
         * here we replace lastUpdated value with current timestamp. Usually
         * difference is within 3 seconds range (depending on the network
         * channel).
         *
         * we can't rely on lastUpdated value provided by service since it
         * included very rarely and that means we would skip 90%+ of all records
         */

        // always provided at fixed position
        String exchangeName = items[1];
        String fromCurrency = items[2];
        String toCurrency = items[3];
        byte flag = Byte.parseByte(items[4]);

        // position may vary depending on mask
        double price = 0;
        long lastUpdate = Instant.now().getEpochSecond();
        double volume24h = 0;
        double volume24hTo = 0;

        int fixedOffset = 5; // 4 fixed fields + type
        int offset = 0;
        for (int i = mask.length() - 1; i > 0; i--) {
            if (mask.charAt(i) == '1') {
                if (i == 15) {
                    price = Double.parseDouble(items[fixedOffset + offset]);
                } else if (i == 5) {
                    volume24h = Double.parseDouble(items[fixedOffset + offset]);
                } else if (i == 4) {
                    volume24hTo = Double.parseDouble(items[fixedOffset + offset]);
                }

                offset++;
            }
        }

        LiveRate newLiveRate = new LiveRate(exchangeName, fromCurrency, toCurrency, flag, price, lastUpdate, volume24h,
            volume24hTo);

        return newLiveRate;
    }
}