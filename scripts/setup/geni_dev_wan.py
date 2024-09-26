"""Running Summerset on a variable number of nodes across multiple sites.
"""

# Import the Portal object and ProtoGENI lib.
import geni.portal as portal  # type: ignore
import geni.rspec.pg as rspec  # type: ignore

# Primary partition's disk image.
DISK_IMAGE = "urn:publicid:IDN+wisc.cloudlab.us+image+advosuwmadison-PG0:summerset.dev"

# List of node types (chosen from different sites) to use.
NODE_TYPES_POOL = [
    "c220g5",  # Wisc
    "c6320",  # Clemson
    "xl170",  # Utah
    "rs620",  # Mass
]

# Create a portal context, needed to defined parameters.
pc = portal.Context()

# Create a Request object to start building the RSpec.
request = pc.makeRequestRSpec()

# Variable number of nodes.
pc.defineParameter(
    "nodeCount",
    "Number of nodes (sites assigned round-robinly)",
    portal.ParameterType.INTEGER,
    5,
)

# Retrieve the values the user specifies during instantiation.
params = pc.bindParameters()

# Check parameter validity.
if params.nodeCount < 1 or params.nodeCount > 25:
    pc.reportError(
        portal.ParameterError("#nodes must be >= 1 and <= 25", ["nodeCount"])
    )

pc.verifyParameters()

# Process nodes in semi round-robin assignment of sites.
for i in range(params.nodeCount):
    j = i
    if i >= len(NODE_TYPES_POOL):
        j = ((i - len(NODE_TYPES_POOL)) % (len(NODE_TYPES_POOL) - 1)) + 1
    # Create a node and add it to the request
    name = "node" + str(i)
    node = request.RawPC(name)
    node.hardware_type = NODE_TYPES_POOL[j]
    node.disk_image = DISK_IMAGE
    # Copy backup home directory back to '/home/smr'?
    # node.addService(
    #     rspec.Execute(shell="bash", command="cp -r /opt/home-backup/. /home/smr/")
    # )

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
