"""Postgres view definitions rewritten as SQL over a version's Delta lakes.

A CAVE view lives only as SQL inside Postgres, and is built on other views: minnie's
connections_with_nuclei reads single_neurons, which reads nucleus_detection_lookup_v1,
which reads two `<table>__<pcg>_merge` views, each `<table> JOIN <table>__<pcg> USING (id)`.
lake_sql turns such a definition (pg_get_viewdef) into one statement that DuckDB or Daft
(daft.sql) runs over the lakes of the tables underneath, so a view can be built without
the frozen database:

- Views are expanded recursively, each reference becoming a subquery of its definition
  under the same name, until only tables remain.
- A `<table>__<pcg>_merge` view becomes the table's lake, keeping the rows that have a
  segmentation row: the lake is the frozen tables' LEFT JOIN (an annotation without one
  has null segmentation columns) and the view an inner join. A frozen version's tables
  hold only valid rows already (clean_table), as the lakes do.
- A point column (geometry(PointZ) in Postgres) is three integer columns in a lake,
  `<col>_x/_y/_z`: a point selected bare, aliased or through COALESCE becomes three
  columns, and ST_X/Y/Z(col) becomes col_x/_y/_z.
- Rewrites for Daft, which DuckDB doesn't need but runs the same: count is cast to BIGINT
  (Postgres's type; Daft's count is UInt64, which COALESCE with a BIGINT turns into a
  float); `a <> b` between two columns becomes `(a < b OR a > b)` (daft 0.7.26 pushes a
  column-vs-column != into the Parquet scan and returns no rows); a UNION's branches take
  the first branch's names, as in Postgres; every CTE gets a unique name (Daft refuses a
  CTE that shadows another of the same name); `agg(x) FILTER (WHERE c)` becomes
  `agg(CASE WHEN c THEN x ELSE NULL END)` (Daft has no FILTER, and wants the ELSE).

Pure functions of the definitions and the lakes' columns: no database, request or app.
"""

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set

import sqlglot
from sqlglot import exp

AXES = ("x", "y", "z")


class ViewNotSupported(ValueError):
    pass


@dataclass
class LakeView:
    sql: str
    lakes: Set[str]  # the lakes it reads
    columns: List[str]  # output columns, points as <col>_x/_y/_z


def _parse(sql: str) -> exp.Expression:
    return sqlglot.parse_one(sql.strip().rstrip(";"), read="postgres")


def point_names(view_columns: Dict[str, List[Dict]], lake_columns: Dict[str, Iterable[str]]) -> Set[str]:
    """Names that are points: geometry columns of the views, and lake columns stored as
    <col>_x/_y/_z. Matched by name, as CAVE's point columns are named consistently
    (pt_position, ctr_pt_position, ...)."""
    names = {c["name"] for cols in view_columns.values() for c in cols if c["type"].startswith("geometry")}
    for cols in lake_columns.values():
        cols = set(cols)
        names |= {c[:-2] for c in cols if c.endswith("_x") and f"{c[:-2]}_y" in cols and c[:-2] not in cols}
    return names


def _merge_view_table(tree: exp.Expression, lakes: Dict[str, Set[str]], pcg: str) -> Optional[tuple]:
    """(table, segmentation columns) if the view is `SELECT ... FROM t JOIN t__pcg USING (id)`."""
    if not isinstance(tree, exp.Select) or tree.args.get("where") or tree.args.get("group"):
        return None
    joins = tree.args.get("joins") or []
    source = tree.args.get("from") or tree.args.get("from_")
    if source is None or len(joins) != 1 or not isinstance(source.this, exp.Table):
        return None
    table, seg = source.this.name, joins[0].this
    if not isinstance(seg, exp.Table) or seg.name != f"{table}__{pcg}" or table not in lakes:
        return None
    using = [c.name for c in joins[0].args.get("using") or []]
    on = joins[0].args.get("on")
    if using != ["id"] and not (isinstance(on, exp.EQ) and {on.left.name, on.right.name} == {"id"}):
        return None
    seg_columns = [e.name for e in tree.expressions if isinstance(e, exp.Column) and e.table == seg.name]
    return table, [c for c in seg_columns if c in lakes[table]]


def _lake_of_merge(table: str, seg_columns: List[str]) -> exp.Expression:
    exists = " OR ".join(f'"{c}" IS NOT NULL' for c in seg_columns) or "TRUE"
    return _parse(f'SELECT * FROM "{table}" WHERE {exists}')


def _expand(name: str, definitions: Dict[str, str], lakes: Dict[str, Set[str]], pcg: str,
            used: Set[str], stack=()) -> exp.Expression:
    if name in stack:
        raise ViewNotSupported(f"views refer to each other in a cycle: {' -> '.join(stack + (name,))}")
    tree = _parse(definitions[name])
    merge = _merge_view_table(tree, lakes, pcg)
    if merge:
        used.add(merge[0])
        return _lake_of_merge(*merge)
    ctes = {cte.alias for cte in tree.find_all(exp.CTE)}
    for table in list(tree.find_all(exp.Table)):
        ref = table.name
        if ref in ctes or table.args.get("db"):
            continue
        if ref in definitions and ref not in lakes:
            sub = _expand(ref, definitions, lakes, pcg, used, stack + (name,))
            table.replace(exp.Subquery(this=sub, alias=exp.TableAlias(this=exp.to_identifier(table.alias or ref))))
        elif ref in lakes:
            used.add(ref)
        elif ref.endswith(f"__{pcg}") and ref[: -len(pcg) - 2] in lakes:
            raise ViewNotSupported(f"{name} joins {ref} other than as <table> JOIN <table>__{pcg} USING (id)")
        else:
            raise ViewNotSupported(f"{name} reads {ref}, which has no lake")
    return tree


def _unique_ctes(tree: exp.Expression) -> None:
    """Give every CTE its own name. Expanded views nest WITHs, and a CTE may shadow one of
    the same name (minnie's single_neurons has `merged` inside a CTE beside `merged`), which
    Postgres resolves by scope and Daft refuses. References are renamed scope by scope; a
    table keeps its old name as alias, so qualified columns still resolve."""
    counter = [0]

    def walk(node: exp.Expression, names: Dict[str, str]) -> None:
        with_ = node.args.get("with_")
        if with_ is not None:
            names = dict(names)
            for cte in with_.expressions:  # a CTE sees the ones before it
                walk(cte.this, names)
                counter[0] += 1
                names[cte.alias] = f"{cte.alias}__{counter[0]}"
                cte.set("alias", exp.TableAlias(this=exp.to_identifier(names[cte.alias])))
        if isinstance(node, exp.Table) and not node.args.get("db") and node.name in names:
            if not node.alias:
                node.set("alias", exp.TableAlias(this=exp.to_identifier(node.name)))
            node.set("this", exp.to_identifier(names[node.name]))
        for key, value in node.args.items():
            if key == "with_":
                continue
            for child in value if isinstance(value, list) else [value]:
                if isinstance(child, exp.Expression):
                    walk(child, names)

    walk(tree, {})


def _split_points(tree: exp.Expression, points: Set[str]) -> None:
    """Points in select lists become three columns; ST_X/Y/Z(point) a single one."""

    def axis(node):
        if isinstance(node, exp.Anonymous) and node.name.upper() in ("ST_X", "ST_Y", "ST_Z"):
            col = node.expressions[0]
            return exp.column(f"{col.name}_{node.name[-1].lower()}", table=col.table or None)
        return node

    tree.transform(axis, copy=False)

    def per_axis(e: exp.Expression, a: str) -> exp.Expression:
        e = e.copy()
        for c in e.find_all(exp.Column):
            if c.name in points:
                c.set("this", exp.to_identifier(f"{c.name}_{a}"))
        return e

    for select in tree.find_all(exp.Select):
        out = []
        for e in select.expressions:
            inner = e.this if isinstance(e, exp.Alias) else e
            refs = [c for c in inner.find_all(exp.Column) if c.name in points]
            if not refs:
                out.append(e)
                continue
            pointwise = isinstance(inner, exp.Column) or (
                isinstance(inner, exp.Coalesce) and all(isinstance(x, exp.Column) and x.name in points
                                                        for x in [inner.this, *inner.expressions]))
            if not pointwise:
                raise ViewNotSupported(f"a point is used in {inner.sql('postgres')!r}; only selected, "
                                       "aliased, COALESCEd or through ST_X/Y/Z")
            name = e.alias_or_name
            out += [exp.alias_(per_axis(inner, a), f"{name}_{a}") for a in AXES]
        select.set("expressions", out)
    for node in tree.find_all(exp.Column):
        if node.name in points and not isinstance(node.parent, exp.Select):
            raise ViewNotSupported(f"a point is compared or computed with: {node.parent.sql('postgres')!r}")


def _for_daft(tree: exp.Expression) -> exp.Expression:
    def unfilter(node):
        # agg(x) FILTER (WHERE c) -> agg(CASE WHEN c THEN x END), count(*) -> count(CASE WHEN c THEN 1 END):
        # the same, as aggregates skip NULLs; Daft has no FILTER
        if isinstance(node, exp.Filter) and isinstance(node.this, exp.AggFunc):
            agg, cond = node.this.copy(), node.expression.this
            arg = agg.this
            value = exp.Literal.number(1) if arg is None or isinstance(arg, exp.Star) else arg
            agg.set("this", exp.Case(ifs=[exp.If(this=cond.copy(), true=value)], default=exp.Null()))
            return agg
        return node

    def rewrite(node):
        if isinstance(node, exp.Count) and not isinstance(node.parent, exp.Cast):
            return exp.cast(node, "BIGINT")
        if isinstance(node, exp.NEQ) and isinstance(node.left, exp.Column) and isinstance(node.right, exp.Column):
            a, b = node.left, node.right
            return exp.paren(exp.or_(exp.LT(this=a.copy(), expression=b.copy()),
                                     exp.GT(this=a.copy(), expression=b.copy())))
        return node

    tree = tree.transform(unfilter).transform(rewrite)
    for union in tree.find_all(exp.Union):
        if isinstance(union.left, exp.Select) and isinstance(union.right, exp.Select):
            names = union.left.named_selects
            union.right.set("expressions", [
                e if e.alias_or_name == n else exp.alias_(e.this if isinstance(e, exp.Alias) else e, n)
                for e, n in zip(union.right.expressions, names)])
    return tree


def lake_sql(name: str, definitions: Dict[str, str], lake_columns: Dict[str, Iterable[str]], pcg: str,
             points: Set[str], dialect: str = "duckdb") -> LakeView:
    """The view `name` as one statement over the lakes (keyed by table name) in `dialect`.

    definitions: every view's pg_get_viewdef text, by name. lake_columns: each lake's
    columns. points: the names of point columns (point_names).
    """
    lakes = {t: set(c) for t, c in lake_columns.items()}
    used: Set[str] = set()
    tree = _expand(name, definitions, lakes, pcg, used)
    _unique_ctes(tree)
    _split_points(tree, points)
    tree = _for_daft(tree)
    sql = tree.sql(dialect=dialect)
    return LakeView(sql=sql, lakes=used, columns=list(tree.named_selects))


def view_order(views: List[Dict]) -> List[str]:
    """View names, each after the views it reads (for creating them, e.g. in tests)."""
    by_name = {v["name"]: v for v in views}
    out, seen = [], set()

    def visit(n):
        if n in seen or n not in by_name:
            return
        seen.add(n)
        for r in by_name[n].get("reads") or []:
            visit(re.sub(r"^public\.", "", r))
        out.append(n)

    for v in views:
        visit(v["name"])
    return out
