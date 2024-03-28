"""Running Summerset on a variable number of c220g5 nodes in a single site.
"""

# Import the Portal object.
import geni.portal as portal

# Create a portal context, needed to defined parameters.
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

# Fixed parameters.
PHYS_NODE_TYPE = "c220g5"
DISK_IMAGE = (
    "urn:publicid:IDN+wisc.cloudlab.us+image+advosuwmadison-PG0:summerset.c220g5"
)

# Variable number of nodes.
pc.defineParameter(
    "nodeCount",
    "Number of nodes",
    portal.ParameterType.INTEGER,
    5,
)

# Retrieve the values the user specifies during instantiation.
params = pc.bindParameters()

# Check parameter validity.
if params.nodeCount < 1 or params.nodeCount > 9:
    pc.reportError(portal.ParameterError("#nodes must be >= 1 and <= 9", ["nodeCount"]))

pc.verifyParameters()

# Create link/lan.
if params.nodeCount > 1:
    if params.nodeCount == 2:
        lan = request.Link()
    else:
        lan = request.LAN()

# Process nodes, adding to link or lan.
for i in range(params.nodeCount):
    # Create a node and add it to the request
    name = "node" + str(i)
    node = request.RawPC(name)
    node.hardware_type = PHYS_NODE_TYPE
    node.disk_image = DISK_IMAGE
    # Add to lan
    if params.nodeCount > 1:
        iface = node.addInterface("eth1")
        lan.addInterface(iface)
        pass

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
