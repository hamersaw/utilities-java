package io.blackpine.geohash;

public class Geohash {
    public static final char[] CODES16 = {'0', '1', '2', '3', '4',
        '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static String encode16(double latitude,
            double longitude, int length) {
        double minLat = -90.0;
        double maxLat = 90.0;
        double minLong = -180.0;
        double maxLong = 180.0;

        String geohash = "";
        for (int i=0; i<length; i++) {
            int index = 0;
            for (int j=0; j<4; j++) {
                index <<= 1;
                if (j % 2 == 0) {
                    double midLat = (minLat + maxLat) / 2.0;
                    if (latitude >= midLat) {
                        index |= 1;
                        minLat = midLat;
                    } else {
                        maxLat = midLat;
                    }
                } else {
                    double midLong = (minLong + maxLong) / 2.0;
                    if (longitude >= midLong) {
                        index |= 1;
                        minLong = midLong;
                    } else {
                        maxLong = midLong;
                    }
                }
            }

            geohash += CODES16[index];
        }

        return geohash;
    }

    public static void main(String[] args) {
        System.out.println(encode16(50.0, 70.0, 6));
    }
}
