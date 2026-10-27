#!/usr/bin/env python3
"""Static code metrics for the six self-checkout implementations.

Size comes from tokei, complexity from rust-code-analysis-cli. Coupling and
cohesion are computed here from the Rust sources:

- a file-level module dependency graph built from `use` trees and qualified
  paths (`crate::`, `super::`, `self::`, child modules, workspace crates);
- fan-in/fan-out, Martin's afferent/efferent coupling and instability per
  crate, and MacCormack's propagation cost (density of the transitive closure);
- relational cohesion H = (R + 1) / N per crate;
- LCOM4 per type with an inherent impl (methods taking `self`, joined when they
  share a field or call each other).

Production code is every `.rs` file outside `tests/` and the `system-tests`
crate, with `#[cfg(test)]` modules removed.

Usage: python3 comparison/scripts/code_metrics.py  (writes comparison/data/code_metrics.json)
"""

import json
import re
import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "comparison" / "data" / "code_metrics.json"

ARCHS = [
    ("monolith", "Monolith"),
    ("layered", "Layered"),
    ("pipeline", "Pipeline"),
    ("service-based", "Service-based"),
    ("microservices", "Microservices"),
    ("event-based", "Event-based"),
]

TEST_CRATES = {"system-tests"}


def is_test_file(arch_dir: Path, path: Path) -> bool:
    rel = path.relative_to(arch_dir).parts
    return "tests" in rel or rel[0] in TEST_CRATES


TABLE_DOMAIN = {
    "items": "inventory",
    "stock_changes": "inventory",
    "reservations": "inventory",
    "reservation_lines": "inventory",
    "transactions": "basket",
    "lines": "basket",
    "catalog": "basket",
    "released_sagas": "basket",
    "scan_events": "basket",
    "sagas": "payment",
    "payments": "payment",
    "scans": "analytics",
    "cursor": "analytics",
    "popularity": "analytics",
}
SERVICE_DOMAIN = {
    "inventory-service": "inventory",
    "basket-service": "basket",
    "payment-service": "payment",
    "analytics-service": "analytics",
}


def strip_code(text: str) -> str:
    """Blank out comments, string and char literals, keeping offsets and newlines."""
    out = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if text.startswith("//", i):
            j = text.find("\n", i)
            j = n if j < 0 else j
            out.append(" " * (j - i))
            i = j
        elif text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            out.append(re.sub(r"[^\n]", " ", text[i:j]))
            i = j
        elif (m := re.match(r'b?r(#*)"', text[i:])) and (i == 0 or not (text[i - 1].isalnum() or text[i - 1] == "_")):
            end = '"' + m.group(1)
            j = text.find(end, i + m.end())
            j = n if j < 0 else j + len(end)
            out.append('"' + re.sub(r"[^\n]", " ", text[i + 1 : j - 1]) + '"')
            i = j
        elif c == '"':
            j = i + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            out.append('"' + re.sub(r"[^\n]", " ", text[i + 1 : j]) + '"')
            i = j + 1
        elif c == "'" and (m := re.match(r"'(\\.[^']*|[^\\'])'", text[i:])):
            out.append("' '" + " " * (m.end() - 3))
            i += m.end()
        else:
            out.append(c)
            i += 1
    return "".join(out)


def sql_tables(path: Path, known: set, span=None):
    """Tables read and written by SQL in a file's string literals (outside cfg(test)).

    `span` limits the scan to one (start, end) offset range, such as a function body.
    """
    raw = path.read_text()
    stripped = strip_code(raw)
    keep = [True] * len(raw)
    for m in re.finditer(r"#\[cfg\(test\)\]\s*mod\s+\w+\s*\{", stripped):
        end = matching_brace(stripped, m.end() - 1)
        for k in range(m.start(), end + 1):
            keep[k] = False
    lo, hi = span or (0, len(raw))
    strings = []
    for m in re.finditer(r'"[^"]*"', stripped):
        if keep[m.start()] and lo <= m.start() < hi:
            strings.append(raw[m.start() : m.end()])
    reads, writes = set(), set()
    for s in strings:
        for kw, table in re.findall(r"\b(DELETE FROM|INSERT INTO|INSERT OR \w+ INTO|UPDATE|FROM|JOIN|INTO)\s+(\w+)", s):
            if table not in known:
                continue
            (writes if kw.startswith(("DELETE", "INSERT", "UPDATE")) or kw == "INTO" else reads).add(table)
    return reads, writes


def matching_brace(text: str, open_idx: int) -> int:
    depth = 0
    for j in range(open_idx, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return j
    return len(text) - 1


def remove_cfg_test(code: str) -> str:
    while (m := re.search(r"#\[cfg\(test\)\]\s*mod\s+\w+\s*\{", code)):
        end = matching_brace(code, m.end() - 1)
        code = code[: m.start()] + re.sub(r"[^\n]", " ", code[m.start() : end + 1]) + code[end + 1 :]
    return code


# ---------------------------------------------------------------- crates/files


def workspace(arch_dir: Path):
    meta = json.loads(
        subprocess.run(
            ["cargo", "metadata", "--format-version", "1", "--offline", "--manifest-path", str(arch_dir / "Cargo.toml")],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    )
    members = set(meta["workspace_members"])
    crates = {}
    for pkg in meta["packages"]:
        if pkg["id"] not in members:
            continue
        lib = next((t for t in pkg["targets"] if "lib" in t["kind"]), None)
        bins = [t for t in pkg["targets"] if "bin" in t["kind"]]
        crates[pkg["name"]] = {
            "dir": Path(pkg["manifest_path"]).parent,
            "lib_name": lib["name"] if lib else None,
            "lib_root": Path(lib["src_path"]) if lib else None,
            "bin_roots": [Path(b["src_path"]) for b in bins],
            "deps": sorted({d["name"] for d in pkg["dependencies"] if d.get("kind") is None}),
        }
    names = set(crates)
    external = sorted({d for c in crates.values() for d in c["deps"] if d not in names and c["dir"].name not in TEST_CRATES})
    return crates, external, len(meta["packages"])


def module_files(crate):
    """Map module path tuples to files for the crate's lib (and bin roots)."""
    mods = {}

    def walk(path: Path, modpath: tuple, root_dir: Path):
        mods[modpath] = path
        code = strip_code(path.read_text())
        for m in re.finditer(r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*;", code, re.M):
            name = m.group(1)
            base = root_dir if path.name in ("lib.rs", "main.rs", "mod.rs") else path.parent / path.stem
            for cand in (base / f"{name}.rs", base / name / "mod.rs"):
                if cand.exists():
                    walk(cand, modpath + (name,), cand.parent if cand.name == "mod.rs" else base)
                    break

    if crate["lib_root"]:
        walk(crate["lib_root"], (), crate["lib_root"].parent)
    bins = {}
    for b in crate["bin_roots"]:
        sub = {}
        mods_before = dict(mods)
        mods.clear()
        walk(b, (), b.parent)
        sub = dict(mods)
        mods.clear()
        mods.update(mods_before)
        bins[b] = sub
    return mods, bins


def expand_use_tree(tree: str):
    """Expand `a::{b, c::{d, e}}` into ['a::b', 'a::c::d', 'a::c::e']."""
    tree = re.sub(r"\s+", "", tree)
    tree = re.sub(r"\bas\w+", "", tree)
    results = []

    def rec(prefix, s):
        if "{" not in s:
            results.append(prefix + s)
            return
        i = s.index("{")
        head = s[:i]
        inner = s[i + 1 : s.rindex("}")]
        parts, depth, cur = [], 0, ""
        for ch in inner:
            if ch == "," and depth == 0:
                parts.append(cur)
                cur = ""
                continue
            depth += ch == "{"
            depth -= ch == "}"
            cur += ch
        parts.append(cur)
        for p in parts:
            if p:
                rec(prefix + head, p)

    rec("", tree)
    return [r.rstrip(":") for r in results]


def dependency_graph(arch_dir: Path, crates):
    """Return nodes {file: crate} and directed edges between production files."""
    lib_by_name = {c["lib_name"]: name for name, c in crates.items() if c["lib_name"]}
    tables = {}
    for name, crate in crates.items():
        if crate["dir"].name in TEST_CRATES:
            continue
        lib_mods, bins = module_files(crate)
        tables[name] = (lib_mods, bins)

    nodes, edges, external_users = {}, set(), defaultdict(set)

    def resolve(crate_name, mods, path_segments):
        best = None
        for k in range(len(path_segments), -1, -1):
            key = tuple(path_segments[:k])
            if key in mods:
                best = mods[key]
                break
        return best

    for crate_name, (lib_mods, bins) in tables.items():
        scopes = [(lib_mods, lib_mods)] + [(b, b) for b in bins.values()]
        for mods, _ in scopes:
            for modpath, path in mods.items():
                if is_test_file(arch_dir, path):
                    continue
                nodes[path] = crate_name
                code = remove_cfg_test(strip_code(path.read_text()))
                child_mods = {k[-1] for k in mods if len(k) == len(modpath) + 1 and k[: len(modpath)] == modpath}
                refs = []
                for m in re.finditer(r"\buse\s+([^;]+);", code):
                    refs += expand_use_tree(m.group(1))
                for m in re.finditer(r"(?<![\w:])((?:crate|super|self|[a-z_][a-z0-9_]*)(?:::[A-Za-z_][A-Za-z0-9_]*)+)", code):
                    refs.append(m.group(1))
                for ref in refs:
                    segs = [s for s in ref.split("::") if s]
                    if not segs:
                        continue
                    head = segs[0]
                    target = None
                    if head == "crate":
                        target = resolve(crate_name, lib_mods if mods is lib_mods else mods, segs[1:])
                    elif head == "super":
                        target = resolve(crate_name, mods, list(modpath[:-1]) + segs[1:])
                    elif head == "self":
                        target = resolve(crate_name, mods, list(modpath) + segs[1:])
                    elif head in child_mods:
                        target = resolve(crate_name, mods, list(modpath) + segs)
                    elif head in lib_by_name and lib_by_name[head] in tables:
                        other = tables[lib_by_name[head]][0]
                        target = resolve(lib_by_name[head], other, segs[1:])
                    elif head not in ("std", "core", "alloc", "Self"):
                        external_users[head.replace("_", "-")].add(path)
                    if target and target != path:
                        edges.add((path, target))
    return nodes, edges, external_users


def propagation_cost(nodes, edges):
    idx = {n: i for i, n in enumerate(nodes)}
    adj = defaultdict(set)
    for a, b in edges:
        adj[idx[a]].add(idx[b])
    n = len(nodes)
    total = 0
    for s in range(n):
        seen, stack = {s}, [s]
        while stack:
            u = stack.pop()
            for v in adj[u]:
                if v not in seen:
                    seen.add(v)
                    stack.append(v)
        total += len(seen)
    return total / (n * n) if n else 0.0


def data_access(arch_dir: Path, nodes, edges, crates):
    """Per-module and per-deployable table access, with business-domain spread."""
    schemas = [s for s in arch_dir.rglob("schema.sql") if "target" not in s.parts]
    known = set()
    for schema in schemas:
        known |= set(re.findall(r"CREATE TABLE (?:IF NOT EXISTS )?(\w+)", schema.read_text()))
    qualify = len(schemas) > 1

    def q(path, tables):
        return {f"{path_crate(path)}.{t}" for t in tables} if qualify else set(tables)

    per_file, per_fn = {}, {}
    for path in nodes:
        reads, writes = sql_tables(path, known)
        if not (reads or writes):
            continue
        per_file[path] = (q(path, reads), q(path, writes))
        code = strip_code(path.read_text())
        for m in re.finditer(r"(?m)^pub fn (\w+)[^{;]*\{", code):
            end = matching_brace(code, m.end() - 1)
            r, w = sql_tables(path, known, (m.start(), end))
            per_fn[(path, m.group(1))] = (q(path, r), q(path, w))

    def bare(t):
        return t.split(".")[-1]

    def domains(tables):
        return sorted({TABLE_DOMAIN[bare(t)] for t in tables if bare(t) in TABLE_DOMAIN})

    module_of = defaultdict(set)
    for path in per_file:
        module_of[path.stem].add(path)

    deployables = {}
    for name, crate in crates.items():
        if crate["dir"].name in TEST_CRATES or not crate["bin_roots"] or name == "checkout-gateway":
            continue
        own_files = [f for f, c in nodes.items() if c == name]
        reads, writes = set(), set()
        for f in own_files:
            if f in per_file:
                reads |= per_file[f][0]
                writes |= per_file[f][1]
            code = strip_code(f.read_text())
            for mod, fn in re.findall(r"\b(\w+)::(\w+)\s*\(", code):
                for target in module_of.get(mod, ()):
                    if nodes[target] != name and (target, fn) in per_fn:
                        reads |= per_fn[(target, fn)][0]
                        writes |= per_fn[(target, fn)][1]
        own = SERVICE_DOMAIN.get(name)
        touched = reads | writes
        deployables[name] = {
            "reads": sorted(reads - writes),
            "writes": sorted(writes),
            "domains": domains(touched),
            "foreign_tables": sorted(t for t in touched if own and TABLE_DOMAIN.get(bare(t), own) != own),
            "foreign_writes": sorted(t for t in writes if own and TABLE_DOMAIN.get(bare(t), own) != own),
        }
    access = defaultdict(set)
    writers = defaultdict(set)
    for name, d in deployables.items():
        if name == "checkout-db-admin":
            continue
        for t in d["reads"] + d["writes"]:
            access[t].add(name)
        for t in d["writes"]:
            writers[t].add(name)
    modules = {
        str(p.relative_to(arch_dir)): {"reads": sorted(r - w), "writes": sorted(w), "domains": domains(r | w)}
        for p, (r, w) in per_file.items()
    }
    return {
        "tables": len(known),
        "tables_total": sum(len(re.findall(r"CREATE TABLE", s.read_text())) for s in schemas),
        "databases": len(schemas),
        "sql_modules": modules,
        "mean_domains_per_sql_module": sum(len(m["domains"]) for m in modules.values()) / len(modules),
        "deployables": deployables,
        "tables_shared_by_deployables": sorted(t for t, users in access.items() if len(users) > 1),
        "tables_written_by_several": sorted(t for t, users in writers.items() if len(users) > 1),
    }


# ---------------------------------------------------------------- LCOM4


def lcom4(files):
    """LCOM4 per type over all inherent impl blocks found in `files`."""
    types = defaultdict(lambda: {"methods": {}, "file": None})
    for path in files:
        code = remove_cfg_test(strip_code(path.read_text()))
        for m in re.finditer(r"(?m)^\s*impl(?:<[^>{]*>)?\s+([A-Z]\w*)(?:<[^>{]*>)?\s*\{", code):
            name = m.group(1)
            open_idx = m.end() - 1
            close = matching_brace(code, open_idx)
            body = code[open_idx + 1 : close]
            depth = 0
            pos = 0
            for fm in re.finditer(r"\bfn\s+(\w+)\s*(?:<[^>]*>)?\s*\(", body):
                pre = body[pos : fm.start()]
                depth += pre.count("{") - pre.count("}")
                pos = fm.start()
                if depth != 0:
                    continue
                args_end = body.find(")", fm.end())
                args = body[fm.end() : args_end]
                brace = body.find("{", args_end)
                semi = body.find(";", args_end)
                if brace < 0 or (0 <= semi < brace):
                    continue
                fn_end = matching_brace(body, brace)
                fn_body = body[brace : fn_end + 1]
                receiver = re.match(r"\s*(?:&\s*(?:'\w+\s*)?(?:mut\s+)?)?(?:mut\s+)?self\b", args) is not None
                fields = set(re.findall(r"\bself\s*\.\s*(\w+)\b(?!\s*\()", fn_body))
                calls = set(re.findall(r"\bself\s*\.\s*(\w+)\s*\(", fn_body)) | set(re.findall(r"\bSelf::(\w+)\s*\(", fn_body))
                key = (path_crate(path), name)
                types[key]["methods"][fm.group(1)] = {"receiver": receiver, "fields": fields, "calls": calls}
                types[key]["file"] = str(path)
                pos = brace
                depth += fn_body.count("{") - fn_body.count("}")
                pos = brace + len(fn_body)
    results = {}
    for (crate, name), info in types.items():
        methods = {k: v for k, v in info["methods"].items() if v["receiver"]}
        if len(methods) < 2:
            continue
        names = list(methods)
        parent = {k: k for k in names}

        def find(x):
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        for i, a in enumerate(names):
            for b in names[i + 1 :]:
                ma, mb = methods[a], methods[b]
                if ma["fields"] & mb["fields"] or b in ma["calls"] or a in mb["calls"]:
                    parent[find(a)] = find(b)
        components = len({find(k) for k in names})
        results[f"{crate}::{name}"] = {"methods": len(names), "lcom4": components, "file": info["file"]}
    return results


def path_crate(path: Path) -> str:
    """The crate directory a file belongs to (the directory holding its `src/`)."""
    parts = path.parts
    return parts[parts.index("src") - 1] if "src" in parts else path.parent.name


# ---------------------------------------------------------------- complexity


def rca(files):
    funcs, file_mi = [], []
    for path in files:
        raw = subprocess.run(
            ["rust-code-analysis-cli", "-m", "-O", "json", "-p", str(path)], capture_output=True, text=True, check=True
        ).stdout
        data = json.loads(raw)
        file_mi.append({"file": str(path), "mi": data["metrics"]["mi"]["mi_visual_studio"], "sloc": data["metrics"]["loc"]["sloc"]})

        def walk(space, owner=""):
            if space["kind"] == "function":
                m = space["metrics"]
                funcs.append(
                    {
                        "name": f"{owner}::{space['name']}" if owner else space["name"],
                        "file": str(path),
                        "cyclomatic": m["cyclomatic"]["sum"],
                        "cognitive": m["cognitive"]["sum"],
                        "sloc": m["loc"]["sloc"],
                    }
                )
            for child in space.get("spaces", []):
                walk(child, space["name"] if space["kind"] == "impl" else owner)

        walk(data)
    return funcs, file_mi


# ---------------------------------------------------------------- main


def tokei(arch_dir: Path, files):
    data = json.loads(subprocess.run(["tokei", "-o", "json", *map(str, files)], capture_output=True, text=True, check=True).stdout)
    rust = data.get("Rust", {"code": 0, "comments": 0, "blanks": 0})
    return {"code": rust["code"], "comments": rust["comments"], "blanks": rust["blanks"]}


def analyze(key, label):
    arch_dir = ROOT / key
    crates, external, lock_packages = workspace(arch_dir)
    all_rs = sorted(p for p in arch_dir.rglob("*.rs") if "target" not in p.parts)
    prod = [p for p in all_rs if not is_test_file(arch_dir, p)]
    test = [p for p in all_rs if is_test_file(arch_dir, p)]

    cfg_test_lines = 0
    for p in prod:
        code = strip_code(p.read_text())
        for m in re.finditer(r"#\[cfg\(test\)\]\s*mod\s+\w+\s*\{", code):
            end = matching_brace(code, m.end() - 1)
            cfg_test_lines += code[m.start() : end].count("\n") + 1

    nodes, edges, external_users = dependency_graph(arch_dir, crates)
    fan_out = defaultdict(int)
    fan_in = defaultdict(int)
    for a, b in edges:
        fan_out[a] += 1
        fan_in[b] += 1

    prod_crates = {n: c for n, c in crates.items() if c["dir"].name not in TEST_CRATES}
    crate_edges = {(nodes[a], nodes[b]) for a, b in edges if nodes[a] != nodes[b]}
    crate_metrics = {}
    for name in prod_crates:
        ca = len({a for a, b in crate_edges if b == name})
        ce = len({b for a, b in crate_edges if a == name})
        files = [f for f, c in nodes.items() if c == name]
        r = sum(1 for a, b in edges if nodes[a] == name and nodes[b] == name)
        crate_metrics[name] = {
            "files": len(files),
            "afferent": ca,
            "efferent": ce,
            "instability": ce / (ca + ce) if ca + ce else 0.0,
            "relational_cohesion": (r + 1) / len(files) if files else None,
            "internal_edges": r,
        }

    funcs, file_mi = rca(prod)
    lcom = lcom4(prod)
    types = sum(
        len(re.findall(r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(?:struct|enum|trait)\s+\w+", remove_cfg_test(strip_code(p.read_text()))))
        for p in prod
    )
    deployables = sorted(b.stem if b.stem != "main" else name for name, c in prod_crates.items() for b in c["bin_roots"])

    cyclo = [f["cyclomatic"] for f in funcs]
    cogn = [f["cognitive"] for f in funcs]
    sloc_weighted_mi = sum(f["mi"] * f["sloc"] for f in file_mi) / sum(f["sloc"] for f in file_mi)
    sqlite_users = sorted(str(p.relative_to(arch_dir)) for p in external_users.get("rusqlite", set()))
    axum_users = sorted(str(p.relative_to(arch_dir)) for p in external_users.get("axum", set()))
    kafka_users = sorted(str(p.relative_to(arch_dir)) for p in external_users.get("rdkafka", set()))

    prod_loc = tokei(arch_dir, prod)
    prod_loc["code"] -= cfg_test_lines
    return {
        "key": key,
        "label": label,
        "size": {
            "prod_loc": prod_loc["code"],
            "test_loc": tokei(arch_dir, test)["code"] + cfg_test_lines,
            "prod_files": len(prod),
            "crates": len(prod_crates),
            "deployables": len(deployables),
            "deployable_names": deployables,
            "functions": len(funcs),
            "types": types,
            "direct_external_deps": len(external),
            "external_deps": external,
            "lockfile_packages": lock_packages,
        },
        "complexity": {
            "cyclomatic_total": sum(cyclo),
            "cyclomatic_mean": sum(cyclo) / len(cyclo),
            "cyclomatic_max": max(cyclo),
            "cognitive_total": sum(cogn),
            "cognitive_mean": sum(cogn) / len(cogn),
            "cognitive_max": max(cogn),
            "functions_cc_over_10": sum(1 for c in cyclo if c > 10),
            "mi_vs_weighted": sloc_weighted_mi,
            "mi_vs_min": min(f["mi"] for f in file_mi),
            "largest_file_sloc": max(f["sloc"] for f in file_mi),
            "top_functions": sorted(funcs, key=lambda f: -f["cyclomatic"])[:5],
        },
        "coupling": {
            "modules": len(nodes),
            "edges": len(edges),
            "mean_fan_out": len(edges) / len(nodes),
            "max_fan_out": max(fan_out.values(), default=0),
            "max_fan_in": max(fan_in.values(), default=0),
            "propagation_cost": propagation_cost(list(nodes), edges),
            "cross_crate_edges": sum(1 for a, b in edges if nodes[a] != nodes[b]),
            "crates": crate_metrics,
            "rusqlite_modules": sqlite_users,
            "axum_modules": axum_users,
            "rdkafka_modules": kafka_users,
            "http_and_sql_modules": sorted(set(sqlite_users) & set(axum_users)),
            "edge_list": sorted((str(a.relative_to(arch_dir)), str(b.relative_to(arch_dir))) for a, b in edges),
        },
        "cohesion": {
            "lcom4_types": len(lcom),
            "lcom4_mean": sum(v["lcom4"] for v in lcom.values()) / len(lcom) if lcom else None,
            "lcom4_cohesive_share": sum(1 for v in lcom.values() if v["lcom4"] == 1) / len(lcom) if lcom else None,
            "lcom4_max": max((v["lcom4"] for v in lcom.values()), default=None),
            "lcom4": {k: {**v, "file": str(Path(v["file"]).relative_to(arch_dir))} for k, v in sorted(lcom.items())},
            "relational_cohesion_mean": (
                sum(c["relational_cohesion"] for c in crate_metrics.values()) / len(crate_metrics)
            ),
        },
        "data": data_access(arch_dir, nodes, edges, crates),
    }


def main():
    results = [analyze(k, l) for k, l in ARCHS]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(results, indent=2, default=str))
    for r in results:
        s, c, k, h = r["size"], r["complexity"], r["coupling"], r["cohesion"]
        print(
            f"{r['label']:14} loc={s['prod_loc']:5} test={s['test_loc']:5} files={s['prod_files']:3} crates={s['crates']} "
            f"deploy={s['deployables']} fns={s['functions']:3} types={s['types']:3} ext={s['direct_external_deps']} lock={s['lockfile_packages']} | "
            f"cc={c['cyclomatic_total']:.0f} ccavg={c['cyclomatic_mean']:.2f} ccmax={c['cyclomatic_max']:.0f} cog={c['cognitive_total']:.0f} "
            f">10:{c['functions_cc_over_10']} MI={c['mi_vs_weighted']:.1f} | mods={k['modules']} E={k['edges']} "
            f"fo={k['mean_fan_out']:.2f} PC={k['propagation_cost']:.2f} xcrate={k['cross_crate_edges']} sqlite={len(k['rusqlite_modules'])} | "
            f"LCOM4 n={h['lcom4_types']} mean={h['lcom4_mean']} cohesive={h['lcom4_cohesive_share']} H={h['relational_cohesion_mean']:.2f}"
        )


if __name__ == "__main__":
    main()
