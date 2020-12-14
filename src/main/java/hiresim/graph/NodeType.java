package hiresim.graph;

/**
 * Type of nodes in the flow solver {@code graph.Graph}
 */
public enum NodeType {

	SUPER_JOB_FLAVOR_SELECTOR("S"),

	JOB_FLAVOR_SELECTOR("F"),

	NETWORK_TASK_GROUP("GS"),
	SERVER_TASK_GROUP("GN"),

	TASK_GROUP_POSTPONE("P"),

	NETWORK_NODE_FOR_INC("NN"),
	NETWORK_NODE_FOR_SERVERS("NS"),

	MACHINE_NETWORK("MN"),
	MACHINE_SERVER("MS"),


	SINK("K");

	@Override
	public String toString() {
		return this.name;
	}

	private final String name;

	NodeType(String name) {
		this.name = name;
	}


	public static NodeType getByName(String name) {

		for (NodeType nodeType : NodeType.values())
			if (nodeType.name.equals(name))
				return nodeType;

		throw new IllegalArgumentException("NodeType " + name + " is not a valid NodeType!");
	}
}
