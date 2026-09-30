from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kofedas_mcp import KofedasToolRuntime, tool_definitions  # noqa: E402


PERIOD = {"desde": "2026-08-01", "hasta": "2026-08-31"}


def _minimal_xlsx() -> str:
    path = Path(tempfile.gettempdir()) / "kofedas_mcp_audit_tarifa.xlsx"
    files = {
        "[Content_Types].xml": """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>""",
        "_rels/.rels": """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>""",
        "xl/_rels/workbook.xml.rels": """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
</Relationships>""",
        "xl/workbook.xml": """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Tarifa" sheetId="1" r:id="rId1"/></sheets>
</workbook>""",
        "xl/worksheets/sheet1.xml": """<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">
  <sheetData>
    <row r="1">
      <c r="A1" t="inlineStr"><is><t>codigo</t></is></c>
      <c r="B1" t="inlineStr"><is><t>descripcion</t></is></c>
      <c r="C1" t="inlineStr"><is><t>coste</t></is></c>
      <c r="D1" t="inlineStr"><is><t>pvp</t></is></c>
    </row>
    <row r="2">
      <c r="A2" t="inlineStr"><is><t>AUDIT-001</t></is></c>
      <c r="B2" t="inlineStr"><is><t>Articulo auditoria MCP</t></is></c>
      <c r="C2"><v>1</v></c>
      <c r="D2"><v>2</v></c>
    </row>
  </sheetData>
</worksheet>""",
    }
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return str(path)


def _minimal_pdf_base64() -> str:
    # Small one-page PDF with a text stream. It is enough for pypdf extraction.
    data = b"""%PDF-1.4
1 0 obj << /Type /Catalog /Pages 2 0 R >> endobj
2 0 obj << /Type /Pages /Kids [3 0 R] /Count 1 >> endobj
3 0 obj << /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Resources << /Font << /F1 4 0 R >> >> /Contents 5 0 R >> endobj
4 0 obj << /Type /Font /Subtype /Type1 /BaseFont /Helvetica >> endobj
5 0 obj << /Length 93 >> stream
BT /F1 10 Tf 20 100 Td (FACTURA AUDIT-1) Tj 0 -14 Td (52333 REGLETA LED 1 1) Tj ET
endstream endobj
xref
0 6
0000000000 65535 f 
0000000009 00000 n 
0000000058 00000 n 
0000000115 00000 n 
0000000241 00000 n 
0000000311 00000 n 
trailer << /Root 1 0 R /Size 6 >>
startxref
454
%%EOF
"""
    return base64.b64encode(data).decode("ascii")


def _call(runtime: KofedasToolRuntime, name: str, args: dict[str, Any]) -> Any:
    result, is_error = runtime.invoke_tool(name, args, None)
    if is_error or not result.get("ok"):
        raise RuntimeError(result)
    return result.get("data")


def discover_samples(runtime: KofedasToolRuntime) -> dict[str, Any]:
    samples: dict[str, Any] = {"empresa": 1, "centro": 0}

    def optional(name: str, args: dict[str, Any]) -> Any:
        try:
            return _call(runtime, name, args)
        except Exception:
            return None

    clientes = optional("cliente_buscar", {"texto": "A", "limite": 1}) or []
    if clientes:
        samples["cliente"] = clientes[0]["cli_codcli"]
        samples["subcliente"] = clientes[0].get("cli_subcli", 0)

    stock_rows = runtime.db.query(
        """
        SELECT FIRST 1 E.ARTE_CODART
        FROM ARTICULE E
        JOIN ARTICUL A ON A.ART_NUMEMP = E.ARTE_NUMEMP AND A.ART_CODART = E.ARTE_CODART
        WHERE E.ARTE_NUMEMP = ? AND E.ARTE_CENTRO = ? AND COALESCE(E.ARTE_EXIST, 0) <> 0
        ORDER BY E.ARTE_FECMOV DESC
        """,
        (samples["empresa"], samples["centro"]),
        1,
    )
    if stock_rows:
        samples["articulo"] = stock_rows[0]["arte_codart"]
    else:
        articulos = optional("articulo_buscar", {"texto": "A", "limite": 1}) or []
        if articulos:
            samples["articulo"] = articulos[0]["art_codart"]

    proveedores = optional("proveedor_buscar", {"texto": "A", "limite": 1}) or []
    if proveedores:
        samples["proveedor"] = proveedores[0]["pro_codpro"]

    ventas = optional("venta_listar", {"limite": 1, **PERIOD}) or []
    if ventas:
        row = ventas[0]
        samples["venta"] = {
            "tipo_documento": row.get("cbv_tipdoc"),
            "ejercicio": row.get("cbv_ejerci"),
            "serie": row.get("cbv_serie"),
            "numero": row.get("cbv_numdoc"),
        }

    pedidos = optional("pedido_listar", {"limite": 1})
    pedido_rows = pedidos.get("items", []) if isinstance(pedidos, dict) else pedidos or []
    if pedido_rows:
        row = pedido_rows[0]
        samples["pedido"] = {
            "tipo_documento": row.get("cbv_tipdoc", "P"),
            "ejercicio": row.get("cbv_ejerci"),
            "serie": row.get("cbv_serie"),
            "numero": row.get("cbv_numdoc"),
        }

    for tool, key, fields in [
        ("orden_compra_listar", "orden", ("coc_ejerci", "coc_serie", "coc_numdoc")),
        ("entrada_almacen_listar", "entrada", ("cbm_ejerci", "cbm_serie", "cbm_numdoc")),
    ]:
        rows = optional(tool, {"limite": 1}) or []
        if rows:
            samples[key] = {"ejercicio": rows[0].get(fields[0]), "serie": rows[0].get(fields[1]), "numero": rows[0].get(fields[2])}

    ofertas = optional("oferta_listar", {"limite": 1}) or []
    if ofertas:
        samples["oferta"] = {"ejercicio": ofertas[0].get("ofe_ejerci"), "oferta": ofertas[0].get("ofe_numofe")}

    related_catalogs: dict[str, list[dict[str, Any]]] = {}
    for tool, key, fallback in [
        ("auxiliar_tablas", "aux_table", "FAMILI"),
        ("cliente_tablas", "cliente_table", "CLIENI"),
        ("articulo_tablas", "articulo_table", "ARTICULC"),
        ("proveedor_tablas", "proveedor_table", "PROVEEI"),
    ]:
        rows = optional(tool, {}) or []
        related_catalogs[key] = rows
        chosen = fallback
        for row in rows:
            table = row.get("tabla")
            if row.get("disponible", True) and table not in {"CLIEN", "ARTICUL", "PROVEE"}:
                chosen = table
                break
        samples[key] = chosen

    usuarios = optional("usuario_listar", {"limite": 1}) or []
    if usuarios:
        samples["usuario"] = usuarios[0].get("usu_nomusu")
    grupos = optional("grupo_usuario_listar", {"limite": 1}) or []
    if grupos:
        samples["grupo"] = grupos[0].get("gru_idgrupo")

    for table_key, sample_key, list_tool, list_args, excluded in [
        ("aux_table", "aux_keys", "auxiliar_listar", {"limite": 1}, set()),
        ("cliente_table", "cliente_keys", "cliente_relacion_listar", {"limite": 1}, {"CLIEN"}),
        ("articulo_table", "articulo_keys", "articulo_relacion_listar", {"articulo": samples.get("articulo"), "limite": 1}, {"ARTICUL"}),
        ("proveedor_table", "proveedor_keys", "proveedor_relacion_listar", {"proveedor": samples.get("proveedor"), "limite": 1}, {"PROVEE"}),
    ]:
        tables = [
            row.get("tabla")
            for row in related_catalogs.get(table_key, [])
            if row.get("disponible", True) and row.get("tabla") not in excluded
        ] or [samples.get(table_key)]
        for table in tables:
            rows = optional(list_tool, {"tabla": table, **list_args}) or []
            if not rows:
                continue
            keys = {}
            for pk in runtime._table_pk(str(table)):
                value = rows[0].get(pk.lower())
                if value is not None:
                    keys[pk] = value
            if keys:
                samples[table_key] = table
                samples[sample_key] = keys
                break

    samples["xlsx"] = _minimal_xlsx()
    samples["pdf_base64"] = _minimal_pdf_base64()

    return samples


def write_tools() -> set[str]:
    names: set[str] = set()
    for tool in tool_definitions():
        name = tool["name"]
        desc = tool["description"].upper()
        if (
            "ESCRITURA" in desc
            or "CRITICA" in desc
            or name.endswith("_guardar")
            or name.endswith("_alta")
            or name in {"remesa_crear", "efecto_cambiar_estado", "recuento_borrar"}
        ):
            names.add(name)
    return names


def args_for(name: str, samples: dict[str, Any]) -> dict[str, Any]:
    art = samples.get("articulo", "195750")
    cli = samples.get("cliente", 625)
    sub = samples.get("subcliente", 0)
    pro = samples.get("proveedor", 2815)
    empty = {
        "sistema_estado", "empresa_listar", "centro_listar", "usuario_listar", "grupo_usuario_listar",
        "parametro_listar", "auxiliar_tablas", "cliente_tablas", "proveedor_tablas", "articulo_tablas",
        "oferta_tablas", "orden_compra_tablas", "entrada_almacen_tablas", "venta_tablas",
        "cartera_tablas", "regularizacion_tablas",
    }
    if name in empty:
        return {}
    if name == "empresa_obtener":
        return {"empresa": 1}
    if name == "centro_obtener":
        return {"empresa": 1, "centro": 0}
    if name == "usuario_obtener":
        return {"usuario": samples.get("usuario", "admin")}
    if name == "grupo_usuario_obtener":
        return {"grupo": samples.get("grupo", "ADMIN")}
    if name == "parametro_obtener":
        return {"codigo": "RENTAB"}
    if name == "auxiliar_listar":
        return {"tabla": samples.get("aux_table", "FAMILI"), "limite": 2}
    if name == "auxiliar_obtener":
        return {"tabla": samples.get("aux_table", "FAMILI"), "claves": samples.get("aux_keys", {})}
    if name == "familia_listar":
        return {"nivel": "familia", "limite": 2}
    if name.endswith("_buscar"):
        return {"texto": "A", "limite": 2}
    if name in {"articulo_obtener", "articulo_completo", "stock_por_almacen", "stock_a_fecha", "rentabilidad_articulo_ventas"}:
        return {"articulo": art, "fecha": PERIOD["hasta"], **PERIOD}
    if name == "articulo_alta_preparar":
        return {"articulo": "TEST-CHECK"}
    if name == "stock_consultar":
        return {"articulo": art, "limite": 2}
    if name == "articulo_relacion_listar":
        return {"tabla": samples.get("articulo_table", "ARTICULC"), "articulo": art, "limite": 2}
    if name == "articulo_relacion_obtener":
        return {"tabla": samples.get("articulo_table", "ARTICULC"), "claves": samples.get("articulo_keys", {})}
    if name == "articulo_catalogo_listar":
        return {"catalogo": "familias", "limite": 2}
    if name == "articulo_compra_consultar":
        return {"articulo": art, "limite": 2}
    if name == "articulo_precio_coste":
        return {"articulo": art, "fecha": PERIOD["hasta"]}
    if name == "articulo_cambiar_tabla_precio":
        return {"articulo": art, "tabla_precio": 1, "simular": True}
    if name in {"articulo_familia_guardar", "articulo_familiancc_tabla_guardar", "articulo_tecnica_gestion", "articulo_imagen_gestion", "articulo_documento_gestion"}:
        return {"articulo": art, "simular": True}
    if name in {"cliente_obtener", "cliente_completo"}:
        return {"cliente": cli, "subcliente": sub}
    if name == "cliente_alta_preparar":
        return {"centro": 0}
    if name == "cliente_relacion_listar":
        return {"tabla": samples.get("cliente_table", "CLIENI"), "cliente": cli, "subcliente": sub, "limite": 2}
    if name == "cliente_relacion_obtener":
        return {"tabla": samples.get("cliente_table", "CLIENI"), "claves": samples.get("cliente_keys", {})}
    if name in {"proveedor_obtener", "proveedor_completo", "proveedor_articulos_listar"}:
        return {"proveedor": pro, "limite": 2}
    if name == "proveedor_alta_preparar":
        return {}
    if name == "proveedor_relacion_listar":
        return {"tabla": samples.get("proveedor_table", "PROVEEI"), "proveedor": pro, "limite": 2}
    if name == "proveedor_relacion_obtener":
        return {"tabla": samples.get("proveedor_table", "PROVEEI"), "claves": samples.get("proveedor_keys", {})}
    if name in {"oferta_listar", "oferta_articulos_listar"}:
        return {"limite": 2, "vigentes": False}
    if name == "oferta_obtener":
        return samples.get("oferta", {"ejercicio": 2026, "oferta": 1})
    if name == "oferta_alta_preparar":
        return {"proveedor": pro, "descripcion": "PRUEBA MCP", "articulos": [{"articulo": art, "precio": 1, "descuento": 0}]}
    if name in {"orden_compra_listar", "orden_compra_lineas_listar"}:
        return {"limite": 2}
    if name == "orden_compra_obtener":
        return samples.get("orden", {"ejercicio": 2026, "serie": "", "numero": 1})
    if name == "orden_compra_alta_preparar":
        return {"proveedor": pro, "articulos": [{"articulo": art, "cantidad": 1, "precio": 1}]}
    if name in {"entrada_almacen_listar", "entrada_almacen_lineas_listar", "entrada_almacen_pendientes_facturar", "entrada_almacen_pendientes_contabilizar"}:
        return {"limite": 2}
    if name == "entrada_almacen_obtener":
        return samples.get("entrada", {"ejercicio": 2026, "serie": "", "numero": 1})
    if name == "entrada_almacen_alta_preparar":
        return {"proveedor": pro, "lineas": [{"articulo": art, "cantidad": 1, "precio": 1}]}
    if name == "entrada_almacen_pdf_previsualizar":
        return {"content_base64": samples.get("pdf_base64"), "nombre_fichero": "audit.pdf", "proveedor": pro}
    if name in {"venta_listar", "venta_lineas_listar"}:
        return {"limite": 2, **PERIOD}
    if name == "venta_obtener":
        return samples.get("venta", {"tipo_documento": "P", "ejercicio": 2026, "serie": "", "numero": 1})
    if name == "venta_precio_articulo":
        return {"cliente": cli, "subcliente": sub, "articulo": art, "cantidad": 1, "fecha": PERIOD["hasta"]}
    if name in {"rentabilidad_articulos_resumen", "ventas_documentos_detalle", "ventas_documentos_resumen"}:
        return {"limite": 2, **PERIOD}
    if name == "venta_documento_alta_preparar":
        return {"cliente": cli, "subcliente": sub, "lineas": [{"articulo": art, "cantidad": 1}]}
    if name == "pedido_crear":
        return {"cliente": cli, "subcliente": sub, "lineas": [{"articulo": art, "cantidad": 1}]}
    if name == "pedido_listar":
        return {"limite": 2}
    if name in {"pedido_detalle", "pedido_pdf_gestion", "pedido_enviar"}:
        key = samples.get("pedido") or samples.get("venta", {"tipo_documento": "P", "ejercicio": 2026, "serie": "", "numero": 1})
        args = {"tipo_documento": key.get("tipo_documento", "P"), "ejercicio": key.get("ejercicio", 2026), "serie": key.get("serie", ""), "numero": key.get("numero", 1)}
        if name == "pedido_pdf_gestion":
            args["accion"] = "generar"
        return args
    if name in {"cartera_deuda_cliente", "cartera_riesgo_cliente"}:
        return {"cliente": cli, "subcliente": sub, "limite": 2}
    if name.startswith("cartera_") or name in {"vencimientos_listar", "remesa_detalle"}:
        if name == "remesa_detalle":
            return {"ejercicio": 1, "codigo": 1, "limite": 2}
        return {"limite": 2, "fecha_referencia": "2026-09-30"}
    if name.startswith("caja_") or name in {"descuadre", "tesoreria_resumen", "tesoreria_acciones_recomendadas"}:
        return {"limite": 2, **PERIOD}
    dashboard_like = (
        name.startswith("dashboard") or name.endswith("_resumen") or name.endswith("_acciones_recomendadas")
        or name.startswith("negocio_") or name.startswith("compras_") or name.startswith("clientes_")
        or name.startswith("stock_") or name.startswith("pedidos_") or name.startswith("proveedores_")
        or name.startswith("documentos_")
    )
    if dashboard_like:
        return {"limite": 2, **PERIOD}
    if name in {"regularizacion_listar", "recuento_listar"}:
        return {"limite": 2}
    if name == "inventario_valorar_articulos":
        return {"texto": "taladro", "solo_con_stock": True, "limite": 2, "fecha": PERIOD["hasta"]}
    if name == "articulo_tarifa_excel_previsualizar":
        return {"archivo": samples.get("xlsx"), "limite": 2}
    if name == "entrada_almacen_desde_pdf":
        return {"content_base64": samples.get("pdf_base64"), "nombre_fichero": "audit.pdf", "proveedor": pro, "simular": True}
    return {"limite": 2}


def run_one(name: str) -> int:
    os.environ.setdefault("KOFEDAS_MCP_ACCESS_LEVEL", "read")
    runtime = KofedasToolRuntime()
    raw_samples = os.getenv("KOFEDAS_AUDIT_SAMPLES")
    samples = json.loads(raw_samples) if raw_samples else discover_samples(runtime)
    args = args_for(name, samples)
    result, is_error = runtime.invoke_tool(name, args, None)
    payload = {"tool": name, "args": args, "is_error": is_error, "result": result}
    print(json.dumps(payload, ensure_ascii=False, default=str))
    return 1 if is_error or not result.get("ok") else 0


def run_all(timeout: int, pattern: str | None) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)
    env["KOFEDAS_MCP_ACCESS_LEVEL"] = "read"
    samples = discover_samples(KofedasToolRuntime())
    env["KOFEDAS_AUDIT_SAMPLES"] = json.dumps(samples, ensure_ascii=False, default=str)
    print("SAMPLES", env["KOFEDAS_AUDIT_SAMPLES"], flush=True)
    tools = [tool["name"] for tool in tool_definitions()]
    if pattern:
        tools = [name for name in tools if pattern.lower() in name.lower()]
    guarded = write_tools()
    results: list[dict[str, Any]] = []
    for name in tools:
        cmd = [sys.executable, "-u", str(Path(__file__).resolve()), "--one", name]
        try:
            completed = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=timeout)
            line = completed.stdout.strip().splitlines()[-1] if completed.stdout.strip() else "{}"
            data = json.loads(line)
            result = data.get("result", {})
            error_text = json.dumps(result, ensure_ascii=False, default=str)
            if name in guarded and data.get("is_error") and "requiere kofedas_mcp_access_level" in error_text.lower():
                status = "GUARDED"
            elif completed.returncode == 0 and not data.get("is_error") and result.get("ok"):
                status = "OK"
            else:
                status = "FAIL"
            row = {"status": status, "tool": name, "args": data.get("args"), "error": None if status in {"OK", "GUARDED"} else result}
        except subprocess.TimeoutExpired:
            row = {"status": "TIMEOUT", "tool": name, "args": None, "error": f">{timeout}s"}
        except Exception as exc:
            row = {"status": "HARNESS_ERROR", "tool": name, "args": None, "error": repr(exc)}
        results.append(row)
        print(f"{row['status']:13} {name}", flush=True)
        if row["status"] not in {"OK", "GUARDED"}:
            print(json.dumps(row, ensure_ascii=False, default=str)[:1200], flush=True)

    summary = {status: sum(1 for row in results if row["status"] == status) for status in sorted({row["status"] for row in results})}
    print("SUMMARY", json.dumps(summary, ensure_ascii=False), "TOTAL", len(results), flush=True)
    failures = [row for row in results if row["status"] not in {"OK", "GUARDED"}]
    if failures:
        print("FAILURES", json.dumps(failures, ensure_ascii=False, default=str, indent=2)[:12000], flush=True)
        return 1
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--one")
    parser.add_argument("--all", action="store_true")
    parser.add_argument("--timeout", type=int, default=45)
    parser.add_argument("--pattern")
    args = parser.parse_args()
    if args.one:
        return run_one(args.one)
    return run_all(args.timeout, args.pattern)


if __name__ == "__main__":
    raise SystemExit(main())
