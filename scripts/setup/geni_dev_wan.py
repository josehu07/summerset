"""Running Summerset on a variable number of nodes across multiple sites (types hardcoded)."""

# Import the Portal object and ProtoGENI lib.
import geni.portal as portal  # type: ignore
import geni.rspec.pg as rspec  # type: ignore

# Primary partition's disk image map.
DISK_IMAGE = {
    "c220g5": "urn:publicid:IDN+wisc.cloudlab.us+image+advosuwmadison-PG0:smr.dev.wan",
    "xl170": "urn:publicid:IDN+wisc.cloudlab.us+image+advosuwmadison-PG0:smr.dev.wan",
    "c6320": "urn:publicid:IDN+clemson.cloudlab.us+image+advosuwmadison-PG0:smr.dev.wan",
    "rs620": "urn:publicid:IDN+wisc.cloudlab.us+image+advosuwmadison-PG0:smr.dev.wan",
    "r320": "urn:publicid:IDN+apt.emulab.net+image+advosuwmadison-PG0:smr.dev.wan",
}

# List of node types (chosen from different sites) to use.
NODE_TYPES_POOL = [
    "c220g5",  # Wisc
    "xl170",  # Utah
    "c6320",  # Clemson
    "rs620",  # Mass
    "r320",  # APT (close to Utah)
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

# Should site assignment be semi-round-robin (meaning Wisc skipped when wrapped around)?
pc.defineParameter(
    "semiRoundRobin",
    "True if semi-round-robin site assignment (first site skipped when wrapped around)",
    portal.ParameterType.BOOLEAN,
    False,
)

# Retrieve the values the user specifies during instantiation.
params = pc.bindParameters()

# Check parameter validity.
if params.nodeCount < 1 or params.nodeCount > 25:
    pc.reportError(
        portal.ParameterError("#nodes must be >= 1 and <= 25", ["nodeCount"])
    )

pc.verifyParameters()

# Process nodes in (normal or semi-) round-robin assignment of sites.
for i in range(params.nodeCount):
    j = i
    if i >= len(NODE_TYPES_POOL):
        if params.semiRoundRobin:
            j = ((i - len(NODE_TYPES_POOL)) % (len(NODE_TYPES_POOL) - 1)) + 1
        else:
            j = i % len(NODE_TYPES_POOL)
    # Create a node and add it to the request
    name = "node" + str(i)
    node = request.RawPC(name)
    node.hardware_type = NODE_TYPES_POOL[j]
    node.disk_image = DISK_IMAGE[node.hardware_type]
    # Copy backup home directory back to '/home/smr'?
    # node.addService(
    #     rspec.Execute(shell="bash", command="cp -r /opt/home-backup/. /home/smr/")
    # )

# Print the RSpec to the enclosing page.
pc.printRequestRSpec(request)
