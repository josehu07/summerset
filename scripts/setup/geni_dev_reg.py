"""Running Summerset on a variable number of nodes in a single site.
"""

# Import the Portal object and ProtoGENI lib.
import geni.portal as portal  # type: ignore
import geni.rspec.pg as rspec  # type: ignore

# Primary partition's disk image.
DISK_IMAGE = "urn:publicid:IDN+wisc.cloudlab.us+image+advosuwmadison-PG0:summerset.dev"

# Create a portal context, needed to defined parameters.
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

# Variable number of nodes.
pc.defineParameter(
    "nodeCount",
    "Number of nodes",
    portal.ParameterType.INTEGER,
    5,
)

# Physical type for all nodes.
pc.defineParameter(
    "nodeType",
    "Physical node type",
    portal.ParameterType.STRING,
    "c220g5",
)

# Retrieve the values the user specifies during instantiation.
params = pc.bindParameters()

# Check parameter validity.
if params.nodeCount < 1 or params.nodeCount > 9:
    pc.reportError(portal.ParameterError("#nodes must be >= 1 and <= 9", ["nodeCount"]))

if params.nodeType.strip() == "":
    pc.reportError(portal.ParameterError("Physical node type is empty", ["nodeType"]))
tokens = params.nodeType.split(",")
if len(tokens) != 1:
    pc.reportError(portal.ParameterError("Only a single type is allowed", ["nodeType"]))

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
    node.hardware_type = params.nodeType
    node.disk_image = DISK_IMAGE
    # Add to lan
    if params.nodeCount > 1:
        iface = node.addInterface("eth1")
        lan.addInterface(iface)
        pass
    # Copy backup home directory back to '/home/smr'.
    node.addService(
        rspec.Execute(shell="bash", command="cp -r /opt/home-backup/. /home/smr/")
    )

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
