package hiresim.simulation.configuration;

import java.util.HashMap;

public enum SharedResourceMode {

    /**
     * when INC tasks of the same INC service run on the same switch, consider shared resource exactly once
     */
    CONSIDER_ONCE("once"),

    /**
     * when INC tasks of the same INC service run on the same switch, consider shared resource for each INC task individually
     */
    CONSIDER_FOREACH("foreach"),

    /**
     * do not consider shared resource demand
     */
    DEACTIVATED("never");

    public static SharedResourceMode getByName(String name) {

        for(SharedResourceMode mode : SharedResourceMode.values())
            if(mode.name.equals(name))
                return mode;

        throw new IllegalArgumentException("Mode " + name + " is not a valid mode!");
    }

    private final String name;

    SharedResourceMode(String name) {
        this.name = name;
    }

}
