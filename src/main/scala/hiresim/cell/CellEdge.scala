package hiresim.cell

import hiresim.shared.graph.Graph.NodeID

class CellEdge(val srcWithOffset: NodeID,
               val srcSwitch: Boolean = true,
               val dstWithOffset: NodeID,
               val dstSwitch: Boolean)
