public class InputUtils {
    public static String getMonth(String date) {
        return date.substring(0, 7);
    }

    /**
     * an enum representing 8 wind directions
     */
    public enum WindDirection {
        North,
        NorthEast,
        East,
        SouthEast,
        South,
        SouthWest,
        West,
        NorthWest
    }

    /**
     * get wind direction enum value by parsing the raw wind direction in degrees from 0 to 360
     * @param direction raw wind direction in degrees
     * @return WindDirection enum
     */
    public static WindDirection getWindDirection(float direction) {
        if (direction <= 33.75 || direction >= 326.25) {
            return WindDirection.North;
        } else if (direction >= 33.75 && direction <= 78.75) {
            return WindDirection.NorthEast;
        } else if (direction >= 78.75 && direction <= 123.75) {
            return WindDirection.East;
        } else if (direction >= 123.75 && direction <= 168.75) {
            return WindDirection.SouthEast;
        } else if (direction >= 168.75 && direction <= 213.75) {
            return WindDirection.South;
        } else if (direction <= 213.75 && direction <= 258.75) {
            return WindDirection.SouthWest;
        } else if (direction >= 258.75 && direction <= 303.75) {
            return WindDirection.West;
        } else {
            return WindDirection.NorthWest;
        }
    }
}
