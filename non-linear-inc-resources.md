# Non-Linear Switch Resources

Switch task groups have two kind of resource demands: **shared** and **individual**.

## Individual resource demands

This refers to the resource demand each task has to run on a switch.

## Shared resource demands

We introduce *shared* resource demands, which need to be taken into account exactly only once at a time for the set of
INC services of a switch.

## How to adapt the simulator

Each (switch) task group gets two resource demand vectors, one for shared resource demand, one for individual resource
demand.

### Simple example with 1 switch property

So if two task groups encode switch property *A* (e.g., NetChain) and they are allocated on the very same switch, the
shared resource demand vector must be considered exactly once. If any of the two task groups "leaves" the switch, the
shared resource usage still remains. When the last task group of a switch property "leaves" a switch, the shared
resource usage gets freed.

### How it's implemented

The shared resource demand for one property can be set
via `Shared.setPropertyResourceDemand(property: SwitchProps, demand: Array[NumericalResource])` where `property` must be
an atomic property and `demand` the corresponding resource vector this property will take when running on a node. There
are several modes the shared resources can be taken into account. These modes are encoded by the
enum `SharedResourceMode`. The `DEACTIVATED` type is trivial and means that no shared resources should be considered at
all. The `CONSIDER_ONCE` mode encodes the behaviour that is described above. Finally, the
`CONSIDER_FOREACH` mode encodes a behaviour where each Task to be started on the machine consumes the shared resource
demand. These modes can be set via `SimulationConfiguration.SCHEDULER_SHARED_RESOURCE_MODE`. There are also some special
cell statistics that will provide insight in the actual demand on a machine and what has been allocated by the
scheduler. See `Statistics.md` for more details.     