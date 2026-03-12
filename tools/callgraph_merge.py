#!/usr/bin/env python
"""Build a merged call graph from static and runtime edges.

Usage examples:
  python tools/callgraph_merge.py
  python tools/callgraph_merge.py --trace "-m pytest -q"
  python tools/callgraph_merge.py --trace "-m pytest -q" --trace "-m workers.src.entry"
"""

from __future__ import annotations

import argparse
import atexit
import ast
import re
import runpy
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


EDGE_PATTERN = re.compile(
    r'^\s*(?:"(?P<src_q>[^"]+)"|(?P<src_u>[A-Za-z0-9_.]+))\s*->\s*'
    r'(?:"(?P<dst_q>[^"]+)"|(?P<dst_u>[A-Za-z0-9_.]+))'
)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _collect_python_files(source_dir: Path) -> list[str]:
    files: list[str] = []
    for path in source_dir.rglob("*.py"):
        parts = set(path.parts)
        if "__pycache__" in parts:
            continue
        files.append(str(path))
    files.sort()
    return files


def _resolve_pyan_command() -> list[str] | None:
    for name in ("pyan3", "pyan"):
        cmd = shutil.which(name)
        if cmd:
            return [cmd]
    # Fallback for venv-local executable not on PATH.
    scripts_dir = Path(sys.executable).resolve().parent
    for candidate in ("pyan3.exe", "pyan.exe", "pyan3", "pyan"):
        exe = scripts_dir / candidate
        if exe.exists():
            return [str(exe)]
    return None


def _run_static_callgraph(py_files: list[str], static_dot_path: Path) -> None:
    pyan_cmd = _resolve_pyan_command()
    if not pyan_cmd:
        raise RuntimeError("pyan3/pyan command not found. Install with: pip install pyan3")

    cmd = pyan_cmd + py_files + ["--uses", "--defines", "--grouped", "--colored", "--dot"]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            "pyan3 failed.\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    static_dot_path.write_text(result.stdout, encoding="utf-8")


def _read_dot_edges(dot_path: Path) -> set[tuple[str, str]]:
    edges: set[tuple[str, str]] = set()
    if not dot_path.exists():
        return edges
    for line in dot_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        m = EDGE_PATTERN.match(line)
        if m:
            src = m.group("src_q") or m.group("src_u")
            dst = m.group("dst_q") or m.group("dst_u")
            if src and dst:
                edges.add((src, dst))
    return edges


def _read_runtime_edges(tsv_path: Path) -> set[tuple[str, str]]:
    edges: set[tuple[str, str]] = set()
    if not tsv_path.exists():
        return edges
    for line in tsv_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 2:
            continue
        src, dst = parts[0].strip(), parts[1].strip()
        if src and dst:
            edges.add((src, dst))
    return edges


def _write_runtime_dot(runtime_edges: set[tuple[str, str]], runtime_dot_path: Path) -> None:
    lines = [
        "digraph runtime {",
        "  rankdir=LR;",
        '  graph [overlap=false, splines=true, sep="+30", esep="+20", pack=false, outputorder="edgesfirst", concentrate=false, nodesep="1.20", ranksep="1.60"];',
        '  node [shape=box, fontsize=8];',
        '  edge [color="#6b7280", penwidth=1.0, arrowsize=0.8, arrowhead=vee];',
    ]
    for src, dst in sorted(runtime_edges):
        lines.append(f'  "{src}" -> "{dst}";')
    lines.append("}")
    runtime_dot_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_merged_dot(
    static_edges: set[tuple[str, str]],
    runtime_edges: set[tuple[str, str]],
    merged_dot_path: Path,
) -> None:
    lines = [
        "digraph merged_callgraph {",
        "  rankdir=LR;",
        '  graph [overlap=prism, splines=true];',
        '  node [shape=box, fontsize=9, color="#374151", fontcolor="#111827"];',
        '  edge [color="#9ca3af", penwidth=1.1];',
    ]
    for src, dst in sorted(static_edges):
        lines.append(f'  "{src}" -> "{dst}";')

    lines.append("")
    lines.append('  edge [color="#dc2626", penwidth=1.9];')
    for src, dst in sorted(runtime_edges):
        lines.append(f'  "{src}" -> "{dst}";')
    lines.append("}")
    merged_dot_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _module_name_from_static_node(node: str) -> str:
    return node.split("__", 1)[0]


def _module_name_from_runtime_node(node: str, known_modules: set[str]) -> str:
    parts = node.split(".")
    if not parts:
        return node
    if len(parts) >= 3 and parts[0] == "workers" and parts[1] == "src":
        return parts[2]
    if parts[0] in known_modules:
        return parts[0]
    return parts[0]


def _write_single_graph_dot(
    dot_path: Path,
    static_edges: set[tuple[str, str]],
    runtime_edges: set[tuple[str, str]],
    node_purposes: dict[str, str] | None = None,
    *,
    node_font_size: int = 8,
    label_font_size: int = 6,
) -> None:
    node_purposes = node_purposes or {}

    def _display_name(node: str) -> str:
        parts = node.split("__")
        if len(parts) >= 3:
            return f"{parts[-2]}.{parts[-1]}"
        if len(parts) == 2:
            return parts[-1]
        return node

    def _single_line_html_text(text: str, max_chars: int = 120) -> str:
        single = " ".join(text.split())
        if len(single) > max_chars:
            single = single[: max_chars - 3].rstrip() + "..."
        return single

    def _node_line(node: str) -> str:
        purpose = node_purposes.get(node)
        if not purpose:
            return ""
        title = _display_name(node).replace("\\", "\\\\").replace('"', '\\"')
        body = _single_line_html_text(purpose)
        body = body.replace("\\", "\\\\").replace('"', '\\"')
        return f'  "{node}" [shape=box, margin=0, width=0, height=0, label="{title}\\n{body}"];'

    lines = [
        "digraph callgraph {",
        "  rankdir=LR;",
        '  graph [overlap=false, splines=true, sep="+30", esep="+20", pack=false, forcelabels=true, outputorder="edgesfirst", concentrate=false, nodesep="1.20", ranksep="1.60"];',
        f'  node [shape=box, margin=0, width=0, height=0, fontsize={node_font_size}, color="#374151", fontcolor="#111827"];',
        f'  edge [color="#94a3b8", penwidth=0.9, arrowsize=0.8, arrowhead=vee, fontsize={label_font_size}];',
    ]

    nodes: set[str] = set()
    for src, dst in static_edges | runtime_edges:
        nodes.add(src)
        nodes.add(dst)
    for node in sorted(nodes):
        line = _node_line(node)
        if line:
            lines.append(line)

    for src, dst in sorted(static_edges):
        lines.append(f'  "{src}" -> "{dst}";')
    lines.append("")
    lines.append('  edge [color="#dc2626", penwidth=1.6, arrowsize=0.95, arrowhead=vee];')
    for src, dst in sorted(runtime_edges):
        lines.append(f'  "{src}" -> "{dst}";')
    lines.append("}")
    dot_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _normalize_module_name(module_name: str) -> str:
    if module_name.startswith("workers.src."):
        return module_name.split(".", 2)[2]
    return module_name


def _function_comment_below(lines: list[str], def_lineno: int) -> str | None:
    idx = def_lineno
    while idx < len(lines):
        text = lines[idx].strip()
        if not text:
            idx += 1
            continue
        if "#" in text:
            body = text.split("#", 1)[1].strip()
            return body or None
        return None
    return None


def _first_docstring_line(text: str | None) -> str | None:
    if not text:
        return None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped:
            return stripped
    return None


def _collect_callee_purposes(source_dir: Path) -> dict[str, str]:
    purposes: dict[str, str] = {}
    for path in sorted(source_dir.glob("*.py")):
        module_name = path.stem
        text = path.read_text(encoding="utf-8", errors="ignore")
        lines = text.splitlines()
        try:
            tree = ast.parse(text, filename=str(path))
        except SyntaxError:
            continue

        class _Visitor(ast.NodeVisitor):
            def __init__(self) -> None:
                self.class_stack: list[str] = []

            def _node_id(self, fn_name: str) -> str:
                if self.class_stack:
                    return f"{module_name}__{self.class_stack[-1]}__{fn_name}"
                return f"{module_name}__{fn_name}"

            def _capture(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
                # Priority:
                # 1) Function docstring (""" ... """)
                # 2) Comment text after '#' on the line(s) directly below def
                purpose = _first_docstring_line(ast.get_docstring(node, clean=True))
                if not purpose:
                    purpose = _function_comment_below(lines, node.lineno)
                if purpose:
                    purposes[self._node_id(node.name)] = purpose

            def visit_ClassDef(self, node: ast.ClassDef) -> None:
                self.class_stack.append(node.name)
                self.generic_visit(node)
                self.class_stack.pop()

            def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
                self._capture(node)
                self.generic_visit(node)

            def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
                self._capture(node)
                self.generic_visit(node)

        _Visitor().visit(tree)
    return purposes


def _runtime_node_to_static_id(node: str) -> str:
    parts = node.split(".")
    if len(parts) >= 3 and parts[0] == "workers" and parts[1] == "src":
        parts = parts[2:]
    if not parts:
        return node
    return "__".join(parts)


def _build_edge_purposes_from_callee(
    static_edges: set[tuple[str, str]],
    runtime_edges: set[tuple[str, str]],
    callee_purposes: dict[str, str],
) -> dict[tuple[str, str], set[str]]:
    edge_purposes: dict[tuple[str, str], set[str]] = {}
    for edge in static_edges:
        dst = edge[1]
        purpose = callee_purposes.get(dst)
        if purpose:
            edge_purposes.setdefault(edge, set()).add(purpose)
    for edge in runtime_edges:
        dst_runtime = edge[1]
        dst = _runtime_node_to_static_id(dst_runtime)
        purpose = callee_purposes.get(dst)
        if purpose:
            edge_purposes.setdefault(edge, set()).add(purpose)
    return edge_purposes


def _build_node_purposes_from_callee(
    static_edges: set[tuple[str, str]],
    runtime_edges: set[tuple[str, str]],
    callee_purposes: dict[str, str],
) -> dict[str, str]:
    node_purposes: dict[str, str] = {}
    for _, dst in static_edges:
        purpose = callee_purposes.get(dst)
        if purpose:
            node_purposes[dst] = purpose
    for _, dst in runtime_edges:
        static_dst = _runtime_node_to_static_id(dst)
        purpose = callee_purposes.get(static_dst)
        if purpose:
            node_purposes[dst] = purpose
    return node_purposes


def _write_by_file_graphs(
    source_dir: Path,
    out_dir: Path,
    static_edges: set[tuple[str, str]],
    runtime_edges: set[tuple[str, str]],
    node_purposes: dict[str, str] | None = None,
    *,
    render_svg: bool,
    layout_engine: str,
    node_font_size: int,
    label_font_size: int,
) -> list[Path]:
    by_file_dir = out_dir / "by_file"
    by_file_dir.mkdir(parents=True, exist_ok=True)

    module_names = {p.stem for p in source_dir.glob("*.py")}
    produced: list[Path] = []

    for module in sorted(module_names):
        module_static: set[tuple[str, str]] = set()
        module_runtime: set[tuple[str, str]] = set()

        for src, dst in static_edges:
            src_m = _module_name_from_static_node(src)
            dst_m = _module_name_from_static_node(dst)
            if src_m == module or dst_m == module:
                module_static.add((src, dst))

        for src, dst in runtime_edges:
            src_m = _module_name_from_runtime_node(src, module_names)
            dst_m = _module_name_from_runtime_node(dst, module_names)
            if src_m == module or dst_m == module:
                module_runtime.add((src, dst))

        dot_path = by_file_dir / f"{module}.dot"
        _write_single_graph_dot(
            dot_path,
            module_static,
            module_runtime,
            node_purposes=node_purposes,
            node_font_size=node_font_size,
            label_font_size=label_font_size,
        )
        produced.append(dot_path)

        if render_svg:
            svg_path = by_file_dir / f"{module}.svg"
            _render_dot_to_svg(dot_path, svg_path, layout_engine)
            produced.append(svg_path)

    return produced


def _render_dot_to_svg(dot_path: Path, svg_path: Path, engine: str) -> bool:
    dot_cmd = shutil.which("dot")
    if not dot_cmd:
        return False
    cmd = [
        dot_cmd,
        f"-K{engine}",
        "-Goverlap=false",
        "-Gsplines=true",
        "-Gconcentrate=false",
        '-Goutputorder=edgesfirst',
        '-Gnodesep=1.20',
        '-Granksep=1.60',
        "-Gsep=+30",
        "-Gesep=+20",
        "-Gpack=false",
        "-Tsvg",
        str(dot_path),
        "-o",
        str(svg_path),
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"dot failed for {dot_path.name}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return True


def _run_runtime_trace_commands(
    script_path: Path,
    source_dir: Path,
    runtime_tsv: Path,
    trace_commands: list[str],
) -> None:
    if runtime_tsv.exists():
        runtime_tsv.unlink()
    runtime_tsv.parent.mkdir(parents=True, exist_ok=True)

    for command in trace_commands:
        tokens = shlex.split(command, posix=False)
        if not tokens:
            continue
        child_cmd = [
            sys.executable,
            str(script_path),
            "__trace",
            "--source-dir",
            str(source_dir),
            "--runtime-tsv",
            str(runtime_tsv),
            "--",
            *tokens,
        ]
        print(f"[trace] {command}")
        result = subprocess.run(child_cmd, check=False)
        if result.returncode != 0:
            print(
                f"[trace] warning: command exited with status {result.returncode}: {command}",
                file=sys.stderr,
            )


def _qualname(frame) -> str:
    module = frame.f_globals.get("__name__", "<unknown>")
    code = frame.f_code
    fn = getattr(code, "co_qualname", code.co_name)
    return f"{module}.{fn}"


def _trace_and_run(source_dir: Path, runtime_tsv: Path, command: list[str]) -> int:
    if not command:
        raise ValueError("missing command after --")

    source_prefix = str(source_dir.resolve()).lower()
    edges: set[tuple[str, str]] = set()

    def tracer(frame, event, arg):
        del arg
        if event != "call":
            return tracer
        callee_file = str(frame.f_code.co_filename or "").lower()
        caller = frame.f_back
        if caller is None:
            return tracer
        caller_file = str(caller.f_code.co_filename or "").lower()
        if not callee_file.startswith(source_prefix) or not caller_file.startswith(source_prefix):
            return tracer
        edges.add((_qualname(caller), _qualname(frame)))
        return tracer

    @atexit.register
    def _flush_runtime_edges() -> None:
        runtime_tsv.parent.mkdir(parents=True, exist_ok=True)
        with runtime_tsv.open("a", encoding="utf-8") as fp:
            for src, dst in sorted(edges):
                fp.write(f"{src}\t{dst}\n")

    sys.setprofile(tracer)
    try:
        if command[0] == "-m":
            if len(command) < 2:
                raise ValueError("trace command '-m' requires a module name")
            module = command[1]
            sys.argv = [module, *command[2:]]
            runpy.run_module(module, run_name="__main__", alter_sys=True)
            return 0

        script = command[0]
        sys.argv = [script, *command[1:]]
        runpy.run_path(script, run_name="__main__")
        return 0
    finally:
        sys.setprofile(None)


def _build_main_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate static/runtime/merged call graph artifacts."
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("workers/src"),
        help="Directory that contains source Python files.",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("artifacts/callgraph"),
        help="Output directory for DOT/SVG artifacts.",
    )
    parser.add_argument(
        "--runtime-tsv",
        type=Path,
        default=None,
        help="Runtime edge TSV path. Defaults to <out-dir>/runtime_edges.tsv",
    )
    parser.add_argument(
        "--trace",
        action="append",
        default=[],
        help='Runtime trace command. Example: --trace "-m pytest -q"',
    )
    parser.add_argument(
        "--skip-render",
        action="store_true",
        help="Skip SVG rendering and only emit DOT files.",
    )
    parser.add_argument(
        "--layout-engine",
        default="dot",
        help="Graphviz layout engine (default: dot).",
    )
    parser.add_argument(
        "--by-file",
        action="store_true",
        help="Also emit per-file neighborhood callgraphs under <out-dir>/by_file.",
    )
    parser.add_argument(
        "--annotate-purpose",
        action="store_true",
        help="Annotate edges using comments directly below callee function definitions.",
    )
    parser.add_argument(
        "--node-font-size",
        type=int,
        default=7,
        help="Node text size in DOT/SVG outputs (default: 7).",
    )
    parser.add_argument(
        "--label-font-size",
        type=int,
        default=4,
        help="Edge label text size in DOT/SVG outputs (default: 4).",
    )
    return parser


def _run_build(args: argparse.Namespace) -> int:
    source_dir = args.source_dir.resolve()
    out_dir = args.out_dir.resolve()
    runtime_tsv = (args.runtime_tsv or (out_dir / "runtime_edges.tsv")).resolve()

    out_dir.mkdir(parents=True, exist_ok=True)
    static_dot = out_dir / "static.dot"
    runtime_dot = out_dir / "runtime.dot"
    merged_dot = out_dir / "merged.dot"
    static_svg = out_dir / "static.svg"
    runtime_svg = out_dir / "runtime.svg"
    merged_svg = out_dir / "merged.svg"

    py_files = _collect_python_files(source_dir)
    if not py_files:
        raise RuntimeError(f"no Python files found under {source_dir}")

    _run_static_callgraph(py_files, static_dot)
    if args.trace:
        _run_runtime_trace_commands(Path(__file__).resolve(), source_dir, runtime_tsv, args.trace)

    static_edges = _read_dot_edges(static_dot)
    runtime_edges = _read_runtime_edges(runtime_tsv)
    callee_purposes = _collect_callee_purposes(source_dir) if args.annotate_purpose else {}
    node_purposes = _build_node_purposes_from_callee(
        static_edges,
        runtime_edges,
        callee_purposes,
    )
    _write_runtime_dot(runtime_edges, runtime_dot)
    _write_single_graph_dot(
        merged_dot,
        static_edges,
        runtime_edges,
        node_purposes=node_purposes,
        node_font_size=args.node_font_size,
        label_font_size=args.label_font_size,
    )

    rendered = False
    if not args.skip_render:
        if _render_dot_to_svg(static_dot, static_svg, args.layout_engine):
            _render_dot_to_svg(runtime_dot, runtime_svg, args.layout_engine)
            _render_dot_to_svg(merged_dot, merged_svg, args.layout_engine)
            rendered = True

    by_file_outputs: list[Path] = []
    if args.by_file:
        by_file_outputs = _write_by_file_graphs(
            source_dir,
            out_dir,
            static_edges,
            runtime_edges,
            node_purposes=node_purposes,
            render_svg=rendered,
            layout_engine=args.layout_engine,
            node_font_size=args.node_font_size,
            label_font_size=args.label_font_size,
        )

    print(f"source_dir: {source_dir}")
    print(f"python_files: {len(py_files)}")
    print(f"static_edges: {len(static_edges)}")
    print(f"runtime_edges: {len(runtime_edges)}")
    if args.annotate_purpose:
        print(f"callee_purposes: {len(callee_purposes)}")
        print(f"annotated_nodes: {len(node_purposes)}")
    print(f"static_dot: {static_dot}")
    print(f"runtime_dot: {runtime_dot}")
    print(f"merged_dot: {merged_dot}")
    if rendered:
        print(f"static_svg: {static_svg}")
        print(f"runtime_svg: {runtime_svg}")
        print(f"merged_svg: {merged_svg}")
    else:
        print("svg_rendered: false (Graphviz 'dot' not found or --skip-render enabled)")
    if by_file_outputs:
        print(f"by_file_outputs: {len(by_file_outputs)}")
        print(f"by_file_dir: {out_dir / 'by_file'}")
    return 0


def _build_trace_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--runtime-tsv", type=Path, required=True)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    return parser


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "__trace":
        parser = _build_trace_parser()
        args = parser.parse_args(argv[1:])
        command = list(args.command)
        if command and command[0] == "--":
            command = command[1:]
        return _trace_and_run(args.source_dir, args.runtime_tsv, command)

    parser = _build_main_parser()
    args = parser.parse_args(argv)
    return _run_build(args)


if __name__ == "__main__":
    raise SystemExit(main())

