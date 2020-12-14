package hiresim.scheduler.flow.hire;

import java.util.Arrays;

public enum HireLocalityHandlingMode {

    /**
     * the default mode as described in the paper
     */
    DEFAULT("default"),

    REMOVE("remove"),

    /**
     * use fixed min cost term
     */
    WEIGHT_MIN("min"),

    /**
     * use fixed max cost term
     */
    WEIGHT_MAX("max");

    public static HireLocalityHandlingMode getByName(String name) {
        return Arrays.stream(HireLocalityHandlingMode.values()).filter(mode -> mode.name.equals(name)).findAny().orElse(HireLocalityHandlingMode.DEFAULT);
    }

    private final String name;

    HireLocalityHandlingMode(String name) {
        this.name = name;
    }

}
