import sys
import os
import shutil


REMOVE_PATHS = [
    "models",
    "publish/crossword",
    "scripts/crossword",
    "tla+/crossword",
    "src/protocols/crossword",
    "src/utils/linreg.rs",
    "src/utils/qdisc.rs",
    "publish/bodega",
    "scripts/bodega",
    "tla+/bodega",
    "src/protocols/bodega",
]
SIMPLE_TRIMS = [
    ".gitattributes",
    "scripts/distr_cluster.py",
    "scripts/local_cluster.py",
    "scripts/remote_killall.py",
    "src/lib.rs",
]
README = "README.md"
PROTOCOLS_MOD = "src/protocols/mod.rs"
UTILS_MOD = "src/utils/mod.rs"
REMOTE_HOSTS = "scripts/remote_hosts.toml"


def path_get_last_segment(path):
    if "/" not in path:
        return None
    eidx = len(path) - 1
    while eidx > 0 and path[eidx] == "/":
        eidx -= 1
    bidx = path[:eidx].rfind("/")
    bidx += 1
    return path[bidx : eidx + 1]


def check_proper_cwd():
    cwd = os.getcwd()
    if "summerset" not in path_get_last_segment(cwd) or not os.path.isdir("publish/"):
        print("ERROR: script must be run under top-level repo!")
        print("       example: python3 publish/public_repo_trim.py")
        sys.exit(1)


def num_leading_spaces(line):
    return len(line) - len(line.lstrip())


def remove_path(path):
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"  RM  {path}")
    else:
        print(f"  RM  {path}  NOT FOUND!")


def lines_and_fresh(path):
    if os.path.isfile(path):
        print(f"  TRIM  {path}")
        with open(path, "r") as f:
            lines = f.readlines()
        return lines, open(path, "w")
    else:
        print(f"  TRIM  {path}  NOT FOUND!")


def simple_trim(path):
    lines, file = lines_and_fresh(path)
    with file:
        for line in lines:
            line_lower = line.lower()
            if "crossword" in line_lower or "bodega" in line_lower:
                continue
            file.write(line)


def trim_readme(path):
    lines, file = lines_and_fresh(path)
    with file:
        in_pub, in_todo = False, False
        for line in lines:
            if line.strip() == "# Summerset":
                in_pub = True
            if line.strip() == "## TODO List":
                in_todo = True
            if in_todo and line.strip() == "---":
                in_todo = False
            if (not in_pub) or in_todo:
                continue
            line_lower = line.lower()
            if "crossword" in line_lower or "bodega" in line_lower:
                continue
            file.write(line)


def trim_protocols_mod(path):
    lines, file = lines_and_fresh(path)
    with file:
        in_func, outer_spaces, inner_spaces = (
            False,
            None,
            None,
        )
        for line in lines:
            if "new_server_replica" in line or "new_client_endpoint" in line:
                in_func, outer_spaces = True, num_leading_spaces(line)
            if in_func and ("Self::Crossword" in line or "Self::Bodega" in line):
                inner_spaces = num_leading_spaces(line)
            if inner_spaces is not None:
                if line.strip() == "}" and num_leading_spaces(line) == inner_spaces:
                    inner_spaces = None
                continue
            if outer_spaces is not None:
                if line.strip() == "}" and num_leading_spaces(line) == outer_spaces:
                    outer_spaces = None
                    in_func = False
            line_lower = line.lower()
            if "crossword" in line_lower or "bodega" in line_lower:
                continue
            file.write(line)


def trim_utils_mod(path):
    lines, file = lines_and_fresh(path)
    with file:
        for line in lines:
            line_lower = line.lower()
            if "linreg" in line_lower or "qdisc" in line_lower:
                continue
            file.write(line)


def trim_remote_hosts(path):
    lines, file = lines_and_fresh(path)
    with file:
        for line in lines:
            if line.count("=") == 1 and "@" in line:
                name = line[: line.index("=")].strip()
                line = f'{name} = "username@domain.com"\n'
            file.write(line)


if __name__ == "__main__":
    print("Removing...")
    for path in REMOVE_PATHS:
        remove_path(path)

    print("Trimming...")
    for path in SIMPLE_TRIMS:
        simple_trim(path)
    trim_readme(README)
    trim_protocols_mod(PROTOCOLS_MOD)
    trim_utils_mod(UTILS_MOD)
    trim_remote_hosts(REMOTE_HOSTS)

    print("Cargo fmt...")
    os.system("cargo fmt --all")
    print("  Done")

    print("Cargo build...")
    os.system("cargo build --workspace")
    print("  Done")

    print()
    print(f"REMEMBER to delete {sys.argv[0]} as well!!!")
