from __future__ import annotations

import html
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from kofedas_mcp import SERVER_VERSION, tool_definitions  # noqa: E402


GROUP_LABELS = {
    "sistema": "Sistema",
    "empresa": "Configuracion",
    "centro": "Configuracion",
    "usuario": "Configuracion",
    "grupo_usuario": "Configuracion",
    "parametro": "Configuracion",
    "auxiliar": "Auxiliares",
    "familia": "Auxiliares",
    "cliente": "Clientes",
    "proveedor": "Proveedores",
    "articulo": "Articulos",
    "inventario": "Articulos / stock",
    "stock": "Articulos / stock",
    "oferta": "Ofertas",
    "orden_compra": "Ordenes de Compra",
    "entrada_almacen": "Entradas de Almacen",
    "regularizacion": "Regularizaciones",
    "trasvase": "Regularizaciones",
    "recuento": "Regularizaciones",
    "venta": "Ventas",
    "ventas": "Ventas / dashboard",
    "rentabilidad": "Ventas / rentabilidad",
    "pedido": "Pedidos",
    "cartera": "Cartera",
    "dashboard": "Dashboard",
    "compras": "Dashboard",
}

GROUP_ORDER = [
    "Sistema",
    "Configuracion",
    "Auxiliares",
    "Clientes",
    "Proveedores",
    "Articulos",
    "Articulos / stock",
    "Ofertas",
    "Ordenes de Compra",
    "Entradas de Almacen",
    "Regularizaciones",
    "Ventas",
    "Ventas / rentabilidad",
    "Ventas / dashboard",
    "Pedidos",
    "Cartera",
    "Dashboard",
]

WRITE_HINTS = (
    "ESCRITURA",
    "Crea ",
    "Crea o actualiza",
    "Da de alta",
    "Importa",
    "Cierra",
    "Borra",
    "Ajusta el stock",
    "Genera trasvase",
)

WRITE_NAME_PARTS = (
    "_guardar",
    "_alta",
    "_importar",
    "_cerrar",
    "_borrar",
    "_regularizar",
    "trasvase_generar",
    "recuento_grabar",
)


def esc(value: Any) -> str:
    return html.escape(str(value if value is not None else ""), quote=True)


def prefix(name: str) -> str:
    for candidate in ("grupo_usuario", "orden_compra", "entrada_almacen"):
        if name.startswith(candidate + "_"):
            return candidate
    return name.split("_", 1)[0]


def group_label(name: str, description: str) -> str:
    first = description.split(".", 1)[0].strip()
    if first in {
        "Configuracion",
        "Auxiliares",
        "Clientes",
        "Proveedores",
        "Articulos",
        "Ofertas",
        "Ordenes de Compra",
        "Entradas de Almacen",
        "Regularizaciones",
        "Ventas",
        "Pedidos",
        "Pedidos/almacen",
        "Cartera",
        "Dashboard ERP",
        "Ventas/Rentabilidad",
        "Ventas/ANADOC",
        "Stock",
        "Recuentos",
    }:
        return {
            "Dashboard ERP": "Dashboard",
            "Ventas/Rentabilidad": "Ventas / rentabilidad",
            "Ventas/ANADOC": "Ventas / dashboard",
            "Stock": "Articulos / stock",
            "Recuentos": "Regularizaciones",
            "Pedidos/almacen": "Pedidos",
        }.get(first, first)
    return GROUP_LABELS.get(prefix(name), prefix(name).replace("_", " ").title())


def is_write_tool(name: str, description: str) -> bool:
    if name.endswith("_preparar") or "sin escribir" in description.lower() or "previsualizar" in name:
        return False
    return any(part in name for part in WRITE_NAME_PARTS) or any(hint in description for hint in WRITE_HINTS)


def param_summary(schema: dict[str, Any]) -> str:
    props = schema.get("properties") or {}
    required = set(schema.get("required") or [])
    if not props:
        return '<p class="empty">Sin parametros.</p>'
    rows = []
    for key, spec in props.items():
        ptype = spec.get("type", "")
        if isinstance(ptype, list):
            ptype = " | ".join(str(item) for item in ptype)
        if not ptype and "items" in spec:
            ptype = "array"
        req = "si" if key in required else "no"
        desc = spec.get("description", "")
        rows.append(
            "<tr>"
            f"<td class=\"param-name\">{esc(key)}</td>"
            f"<td>{esc(ptype)}</td>"
            f"<td>{req}</td>"
            f"<td>{esc(desc)}</td>"
            "</tr>"
        )
    return (
        "<details><summary>Parametros</summary>"
        "<table><thead><tr><th>Nombre</th><th>Tipo</th><th>Oblig.</th><th>Descripcion</th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody></table></details>"
    )


def tool_card(tool: dict[str, Any], kind: str) -> str:
    schema = tool.get("inputSchema") or {}
    return f"""
      <article class="tool-card {kind}">
        <div class="tool-title">
          <code>{esc(tool["name"])}</code>
          <span class="pill {kind}">{"Escritura" if kind == "write" else "Consulta"}</span>
        </div>
        <p>{esc(tool.get("description", ""))}</p>
        {param_summary(schema)}
      </article>
    """


def build_page() -> str:
    tools = sorted(tool_definitions(), key=lambda item: item["name"])
    grouped: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(lambda: {"read": [], "write": []})
    for tool in tools:
        description = tool.get("description", "")
        group = group_label(tool["name"], description)
        kind = "write" if is_write_tool(tool["name"], description) else "read"
        grouped[group][kind].append(tool)

    ordered_groups = sorted(grouped, key=lambda item: (GROUP_ORDER.index(item) if item in GROUP_ORDER else 99, item))
    read_count = sum(len(grouped[group]["read"]) for group in grouped)
    write_count = sum(len(grouped[group]["write"]) for group in grouped)
    toc = "".join(
        f'<a href="#{esc(anchor(group))}">{esc(group)} <span>{len(grouped[group]["read"]) + len(grouped[group]["write"])}</span></a>'
        for group in ordered_groups
    )
    sections = []
    for group in ordered_groups:
        read_tools = grouped[group]["read"]
        write_tools = grouped[group]["write"]
        sections.append(
            f"""
            <section id="{esc(anchor(group))}" class="group">
              <div class="group-head">
                <h2>{esc(group)}</h2>
                <div class="group-counts">
                  <span>{len(read_tools)} consultas</span>
                  <span>{len(write_tools)} escrituras</span>
                </div>
              </div>
              <div class="columns">
                <div>
                  <h3>Consultas</h3>
                  {''.join(tool_card(tool, "read") for tool in read_tools) or '<p class="empty">Sin consultas.</p>'}
                </div>
                <div>
                  <h3>Escritura</h3>
                  {''.join(tool_card(tool, "write") for tool in write_tools) or '<p class="empty">Sin funciones de escritura.</p>'}
                </div>
              </div>
            </section>
            """
        )

    return f"""<!doctype html>
<html lang="es">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Kofedas MCP - catalogo de funciones</title>
  <style>
    :root {{
      --bg:#f6f7f9; --panel:#ffffff; --ink:#17202a; --muted:#617084; --line:#dce3ea;
      --read:#2364aa; --read-bg:#e8f1fb; --write:#a15c16; --write-bg:#fff0d9;
    }}
    * {{ box-sizing:border-box; }}
    body {{ margin:0; background:var(--bg); color:var(--ink); font-family:Arial, Helvetica, sans-serif; letter-spacing:0; }}
    header {{ background:#fff; border-bottom:1px solid var(--line); position:sticky; top:0; z-index:2; }}
    .wrap {{ width:min(1380px, calc(100vw - 32px)); margin:0 auto; }}
    .top {{ display:flex; align-items:flex-start; justify-content:space-between; gap:18px; padding:18px 0; }}
    h1 {{ margin:0; font-size:23px; line-height:1.2; }}
    .sub {{ color:var(--muted); font-size:13px; margin-top:5px; }}
    .stats {{ display:grid; grid-template-columns:repeat(3, minmax(110px, 1fr)); gap:8px; min-width:360px; }}
    .stat {{ border:1px solid var(--line); border-radius:8px; padding:10px; background:#fbfcfd; }}
    .stat b {{ display:block; font-size:22px; }}
    .stat span {{ color:var(--muted); font-size:12px; }}
    main {{ padding:18px 0 34px; }}
    nav {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:18px; }}
    nav a {{ color:#184f86; background:#eaf2fb; border:1px solid #c9dced; border-radius:999px; padding:7px 11px; text-decoration:none; font-size:12px; font-weight:700; }}
    nav a span {{ color:#617084; font-weight:600; margin-left:4px; }}
    .group {{ background:var(--panel); border:1px solid var(--line); border-radius:8px; margin:14px 0; padding:14px; }}
    .group-head {{ display:flex; align-items:center; justify-content:space-between; gap:12px; border-bottom:1px solid var(--line); padding-bottom:10px; margin-bottom:12px; }}
    h2 {{ margin:0; font-size:18px; }}
    h3 {{ margin:0 0 10px; color:var(--muted); font-size:13px; text-transform:uppercase; }}
    .group-counts {{ display:flex; gap:8px; flex-wrap:wrap; color:var(--muted); font-size:12px; }}
    .columns {{ display:grid; grid-template-columns:1fr 1fr; gap:14px; align-items:start; }}
    .tool-card {{ border:1px solid var(--line); border-radius:8px; padding:11px; margin-bottom:10px; background:#fff; }}
    .tool-card.read {{ border-left:4px solid var(--read); }}
    .tool-card.write {{ border-left:4px solid var(--write); }}
    .tool-title {{ display:flex; align-items:center; justify-content:space-between; gap:8px; }}
    code {{ font-family:Consolas, ui-monospace, monospace; font-size:13px; font-weight:700; }}
    .pill {{ border-radius:999px; padding:3px 8px; font-size:11px; font-weight:700; white-space:nowrap; }}
    .pill.read {{ color:var(--read); background:var(--read-bg); }}
    .pill.write {{ color:var(--write); background:var(--write-bg); }}
    p {{ color:#314055; font-size:13px; line-height:1.45; margin:8px 0 0; }}
    details {{ margin-top:9px; }}
    summary {{ cursor:pointer; color:var(--muted); font-size:12px; font-weight:700; }}
    table {{ width:100%; border-collapse:collapse; margin-top:8px; font-size:12px; }}
    th, td {{ border-bottom:1px solid #edf1f5; padding:6px; text-align:left; vertical-align:top; }}
    th {{ background:#f7f9fb; color:var(--muted); }}
    .param-name {{ font-family:Consolas, ui-monospace, monospace; white-space:nowrap; }}
    .empty {{ color:var(--muted); font-style:italic; }}
    @media (max-width: 900px) {{
      .top {{ flex-direction:column; }}
      .stats {{ min-width:0; width:100%; }}
      .columns {{ grid-template-columns:1fr; }}
    }}
  </style>
</head>
<body>
  <header>
    <div class="wrap top">
      <div>
        <h1>Kofedas MCP - catalogo de funciones</h1>
        <div class="sub">Generado desde <code>kofedas_mcp.tool_definitions()</code> · servidor {esc(SERVER_VERSION)}</div>
      </div>
      <div class="stats">
        <div class="stat"><b>{len(tools)}</b><span>Total funciones</span></div>
        <div class="stat"><b>{read_count}</b><span>Consultas</span></div>
        <div class="stat"><b>{write_count}</b><span>Escritura</span></div>
      </div>
    </div>
  </header>
  <main class="wrap">
    <nav>{toc}</nav>
    {''.join(sections)}
  </main>
</body>
</html>
"""


def anchor(group: str) -> str:
    return "grupo-" + "".join(ch.lower() if ch.isalnum() else "-" for ch in group).strip("-")


def main() -> None:
    output = ROOT / "examples" / "catalogo_funciones_mcp.html"
    output.parent.mkdir(parents=True, exist_ok=True)
    page = "\n".join(line.rstrip() for line in build_page().splitlines()) + "\n"
    output.write_text(page, encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
