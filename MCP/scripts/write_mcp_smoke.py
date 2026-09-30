from __future__ import annotations

import argparse
import base64
import io
import json
import os
import sys
import traceback
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = ROOT.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("KOFEDAS_MCP_ACCESS_LEVEL", "critical")
os.environ.setdefault("KOFEDAS_MCP_TOOL_PROFILE", "core")

from kofedas_mcp import (  # noqa: E402
    ARTICLE_WRITABLE_TABLES,
    AUXILIARY_TABLES,
    CLIENT_TABLES,
    KofedasToolRuntime,
    PROVIDER_WRITABLE_TABLES,
)


PNG_1X1 = (
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGA"
    "WjR9awAAAABJRU5ErkJggg=="
)

def pdf_base64(text: str = "PDF smoke MCP") -> str:
    from reportlab.pdfgen import canvas

    buffer = io.BytesIO()
    page = canvas.Canvas(buffer)
    y = 800
    for line in text.splitlines() or [text]:
        page.drawString(50, y, line)
        y -= 18
    page.save()
    return base64.b64encode(buffer.getvalue()).decode("ascii")


def jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


class WriteSmoke:
    def __init__(self, stamp: str) -> None:
        self.rt = KofedasToolRuntime()
        self.stamp = stamp
        self.short = stamp[-8:]
        self.today = date.today().isoformat()
        self.report: list[dict[str, Any]] = []
        self.generated: dict[str, Any] = {}

    def add(self, name: str, tables: list[str], args: dict[str, Any], fn: Callable[[], Any]) -> Any:
        item: dict[str, Any] = {
            "name": name,
            "tables": tables,
            "args": jsonable(args),
            "status": "pending",
        }
        try:
            result = fn()
            item["status"] = "ok"
            item["result"] = jsonable(result)
            return result
        except Exception as exc:  # noqa: BLE001 - smoke test must continue
            item["status"] = "error"
            item["error"] = str(exc)
            item["traceback"] = traceback.format_exc(limit=5)
            return None
        finally:
            self.report.append(item)

    def scalar(self, sql: str, params: tuple[Any, ...] = ()) -> Any:
        row = self.rt.db.one(sql, params) or {}
        return next(iter(row.values()), None) if row else None

    def row_copy(self, table: str, where: str = "", params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        sql = f"SELECT FIRST 1 * FROM {table}"
        if where:
            sql += " WHERE " + where
        return self.rt.db.one(sql, params)

    def row_for_columns(self, table: str, row: dict[str, Any]) -> dict[str, Any]:
        return {col: self.db_value(row.get(col.lower())) for col in self.rt._table_columns(table)}

    def db_value(self, value: Any) -> Any:
        if isinstance(value, str) and "T" in value and len(value) >= 19:
            return value.replace("T", " ")
        return value

    def next_int_key(self, table: str, column: str, base: int = 900000) -> int:
        row = self.rt.db.one(f"SELECT MAX({column}) AS MAXIMO FROM {table}", ())
        current = int((row or {}).get("maximo") or 0)
        return max(current + 1, base + int(self.short[-4:]))

    def mutate_key(self, table: str, data: dict[str, Any], pk: list[str]) -> dict[str, Any]:
        out = dict(data)
        if not pk:
            return out
        target = next((col for col in reversed(pk) if not col.endswith("_NUMEMP")), pk[-1])
        current = out.get(target)
        if isinstance(current, int):
            out[target] = self.next_int_key(table, target, 900000)
        elif isinstance(current, float):
            out[target] = float(self.next_int_key(table, target, 900000))
        elif current is None and any(token in target for token in ("COD", "NUM", "LIN", "ORD")):
            out[target] = self.next_int_key(table, target, 900000)
        else:
            text = f"MCP{self.short}"[: min(12, max(6, len(str(current or '')) or 12))]
            out[target] = text
        for col in out:
            if col not in pk and any(token in col for token in ("DESCRI", "NOMBRE", "NOM", "OBSERV")):
                out[col] = f"Prueba MCP {self.stamp}"[:60]
                break
        return out

    def verify_count(self, table: str, filters: dict[str, Any]) -> dict[str, Any]:
        where = " AND ".join(f"{col}=?" for col in filters)
        row = self.rt.db.one(f"SELECT COUNT(*) AS TOTAL FROM {table} WHERE {where}", tuple(filters.values()))
        return {"table": table, "filters": filters, "count": int((row or {}).get("total") or 0)}

    def setup_masters(self) -> None:
        empresa = self.rt.empresa
        centro = self.rt.centro
        mark = self.stamp

        self.add(
            "parametro_guardar",
            ["PARAMETROS"],
            {"codigo": f"MCP{self.short}", "valor": mark},
            lambda: self.rt.parametro_guardar({
                "empresa": empresa,
                "codigo": f"MCP{self.short}",
                "valor": mark,
                "descripcion": f"Prueba escritura MCP {mark}",
            }),
        )

        emp = self.row_copy("EMPRES", "EMP_NUMEMP = ?", (empresa,))
        if emp:
            data = {k: v for k, v in self.row_for_columns("EMPRES", emp).items() if k in self.rt._table_columns("EMPRES")}
            data["EMP_EMAIL"] = f"mcp-{self.short.lower()}@example.test"
            self.add("empresa_guardar", ["EMPRES"], {"empresa": empresa, "datos": data}, lambda: self.rt.empresa_guardar({"empresa": empresa, "datos": data}))

        cen = self.row_copy("CENTROS", "CEN_NUMEMP = ? AND CEN_CODCEN = ?", (empresa, centro))
        if cen:
            data = self.row_for_columns("CENTROS", cen)
            data["CEN_EMAIL"] = f"mcp-centro-{self.short.lower()}@example.test"
            self.add("centro_guardar", ["CENTROS"], {"empresa": empresa, "centro": centro, "datos": data}, lambda: self.rt.centro_guardar({"empresa": empresa, "centro": centro, "datos": data}))
            dest_center = 90 + int(self.short[-1])
            dest_data = dict(data)
            dest_data["CEN_CODCEN"] = dest_center
            dest_data["CEN_NOMCEN"] = f"Centro MCP {self.stamp}"[:30]
            dest_data["CEN_EMAIL"] = f"mcp-dest-{self.short.lower()}@example.test"
            self.add("centro_guardar:destino_trasvase", ["CENTROS"], {"empresa": empresa, "centro": dest_center, "datos": dest_data}, lambda: self.rt.centro_guardar({"empresa": empresa, "centro": dest_center, "datos": dest_data}))
            self.generated["centro_destino_trasvase"] = dest_center

        group = f"MCP{self.short[-5:]}"[:10]
        group_row = self.row_copy("GRUPUSU", "GRU_NUMEMP = ?", (empresa,))
        group_data = self.row_for_columns("GRUPUSU", group_row) if group_row else {}
        group_data.update({"GRU_IDGRUPO": group, "GRU_NIVEL": group_data.get("GRU_NIVEL") or "1", "GRU_NIVPRI": group_data.get("GRU_NIVPRI") or "1"})
        group_data.setdefault("GRU_FECMIN", "1900-01-01")
        group_data.setdefault("GRU_FECMAX", "2999-12-31")
        self.add(
            "grupo_usuario_guardar",
            ["GRUPUSU"],
            {"grupo": group},
            lambda: self.rt.grupo_usuario_guardar({
                "empresa": empresa,
                "grupo": group,
                "datos": group_data,
            }),
        )
        self.generated["grupo_usuario"] = group

        user = f"MCP{self.short[-5:]}"[:10]
        user_row = self.row_copy("USUAR", "USU_NUMEMP = ? AND USU_CODCEN = ?", (empresa, centro))
        user_data = self.row_for_columns("USUAR", user_row) if user_row else {}
        user_data.update({"USU_NOMUSU": user, "USU_PASSWORD": "MCP", "USU_NOMBRE": f"Usuario prueba {mark}"[:30], "USU_IDGRUPO": group})
        user_data.setdefault("USU_FECMIN", "1900-01-01")
        user_data.setdefault("USU_FECMAX", "2999-12-31")
        self.add(
            "usuario_guardar",
            ["USUAR"],
            {"usuario": user},
            lambda: self.rt.usuario_guardar({
                "empresa": empresa,
                "centro": centro,
                "usuario": user,
                "datos": user_data,
            }),
        )
        self.generated["usuario"] = user

    def setup_business_masters(self) -> None:
        empresa = self.rt.empresa
        centro = self.rt.centro
        mark = self.stamp

        provider = self.add(
            "proveedor_alta",
            ["PROVEE", "PROVEEI"],
            {"nombre": mark},
            lambda: self.rt.proveedor_alta({
                "empresa": empresa,
                "datos": {
                    "PRO_NOMCOR": f"PROV MCP {mark}"[:30],
                    "PRO_NOMFIS": f"Proveedor prueba MCP {mark}"[:40],
                    "PRO_CIF": f"PMCP{self.short}"[:15],
                    "PRO_CODMON": "E",
                },
                "informacion_adicional": {"OBS": f"Generado por write_mcp_smoke {mark}"},
            }),
        )
        proveedor = int((provider or {}).get("proveedor") or self.next_int_key("PROVEE", "PRO_CODPRO"))
        self.generated["proveedor"] = proveedor

        article_code = f"MCP{self.short}"[:12]
        self.add(
            "articulo_alta",
            ["ARTICUL", "ARTICULP", "ARTICULC", "ARTICULE", "ARTICULI"],
            {"articulo": article_code},
            lambda: self.rt.articulo_alta({
                "empresa": empresa,
                "centro": centro,
                "articulo": article_code,
                "datos": {
                    "ART_DESCRI": f"Articulo prueba MCP {mark}"[:50],
                    "ART_TIPIVA": 1,
                    "ART_PREBAS": 10,
                    "ART_DTOAUM1": 0,
                    "ART_TABPREC": 10,
                    "ART_CODPRO": proveedor,
                    "ART_CODMON": "E",
                    "ART_TIPPRE": "V",
                    "ART_UNIMED": "UD",
                    "ART_INDINV": "S",
                    "ART_CANPRE": 1,
                },
                "compra": {"ARTP_CODPRO": proveedor, "ARTP_REFPRO": f"REF{self.short}", "ARTP_PREBAS": 8},
                "codigos_barras": [{"codigo": f"84{self.short[-6:]}00001", "cantidad": 1}],
                "stock": {"centro": centro, "existencias": 3, "minimo": 1, "maximo": 10},
                "informacion_adicional": {"INFO": f"Alta smoke {mark}"},
            }),
        )
        self.generated["articulo"] = article_code

        client = self.add(
            "cliente_alta",
            ["CLIEN", "CLIENI"],
            {"nombre": mark},
            lambda: self.rt.cliente_alta({
                "empresa": empresa,
                "centro": centro,
                "datos": {
                    "CLI_NOMCLI": f"CLI MCP {mark}"[:40],
                    "CLI_RAZSOC": f"Cliente prueba MCP {mark}"[:40],
                    "CLI_CIF": f"CMCP{self.short}"[:15],
                    "CLI_CODMON": "E",
                },
                "informacion_adicional": {"OBS": f"Generado por write_mcp_smoke {mark}"},
            }),
        )
        self.generated["cliente"] = int((client or {}).get("cliente") or 0)
        self.generated["subcliente"] = int((client or {}).get("subcliente") or 0)

    def run_auxiliary_tables(self) -> None:
        for table in sorted(AUXILIARY_TABLES):
            columns = self.rt._table_columns(table)
            pk = self.rt._table_pk(table)
            row = self.row_copy(table)
            if row:
                data = self.row_for_columns(table, row)
                for col in columns:
                    if col not in pk and any(token in col for token in ("DESCRI", "NOMBRE", "NOM", "OBSERV")):
                        data[col] = f"Prueba MCP {self.stamp}"[:60]
                        break
            else:
                data = {}
                if table == "CAJAS":
                    data = {"CAJ_NUMEMP": self.rt.empresa, "CAJ_CENTRO": self.rt.centro, "CAJ_CODIGO": self.next_int_key(table, "CAJ_CODIGO"), "CAJ_DESCRI": f"Prueba MCP {self.stamp}"[:40]}
                elif table == "DETCAT":
                    catalog = self.rt.db.one("SELECT FIRST 1 CAT_NUMEMP, CAT_CODIGO FROM CATALO WHERE CAT_NUMEMP = ? ORDER BY CAT_CODIGO", (self.rt.empresa,))
                    article = self.rt.db.one("SELECT FIRST 1 ART_CODART FROM ARTICUL WHERE ART_NUMEMP = ? ORDER BY ART_CODART", (self.rt.empresa,))
                    data = {
                        "DCAT_NUMEMP": self.rt.empresa,
                        "DCAT_CODCAT": int((catalog or {}).get("cat_codigo") or 0),
                        "DCAT_NUMLIN": self.next_int_key(table, "DCAT_NUMLIN"),
                        "DCAT_ARTICU": str((article or {}).get("art_codart") or "005609"),
                    }
                elif table == "PUNPEN":
                    data = {"PUN_NUMEMP": self.rt.empresa, "PUN_CODIGO": self.next_int_key(table, "PUN_CODIGO"), "PUN_CODTAR": "", "PUN_FECHA": self.today, "PUN_PUNTOS": 0, "PUN_TOTAL": 0}
                elif table == "SSUBFAM":
                    sub = self.rt.db.one("SELECT FIRST 1 SUB_CODFAM, SUB_CODIGO FROM SUBFAM WHERE SUB_NUMEMP = ? ORDER BY SUB_CODFAM, SUB_CODIGO", (self.rt.empresa,))
                    data = {
                        "SSUB_NUMEMP": self.rt.empresa,
                        "SSUB_CODFAM": int((sub or {}).get("sub_codfam") or 0),
                        "SSUB_CODSUB": int((sub or {}).get("sub_codigo") or 0),
                        "SSUB_CODIGO": self.next_int_key(table, "SSUB_CODIGO"),
                        "SSUB_DESCRI": f"Prueba MCP {self.stamp}"[:40],
                        "SSUB_TABLA": 0,
                    }
                else:
                    for col in pk:
                        if col.endswith("_NUMEMP"):
                            data[col] = self.rt.empresa
                        elif any(token in col for token in ("COD", "NUM", "LIN", "ORD", "TAB", "TIP")):
                            data[col] = self.next_int_key(table, col)
                        else:
                            data[col] = f"MCP{self.short}"[:12]
                for col in columns:
                    if col not in data and any(token in col for token in ("DESCRI", "NOMBRE", "NOM")):
                        data[col] = f"Prueba MCP {self.stamp}"[:60]
                        break
            self.add(
                f"auxiliar_guardar:{table}",
                [table],
                {"tabla": table, "claves": {key: data.get(key) for key in pk}},
                lambda table=table, data=data: self.rt.auxiliar_guardar({"tabla": table, "datos": data}),
            )

    def run_relations(self) -> None:
        empresa = self.rt.empresa
        centro = self.rt.centro
        art = self.generated["articulo"]
        proveedor = self.generated["proveedor"]
        cliente = self.generated["cliente"]
        subcliente = self.generated["subcliente"]

        relation_payloads = {
            "ARTICULC": {"ARTC_NUMEMP": empresa, "ARTC_CODART": art, "ARTC_CODIGO": f"84{self.short[-6:]}00002", "ARTC_CANTID": 1},
            "ARTICULE": {"ARTE_NUMEMP": empresa, "ARTE_CODART": art, "ARTE_CENTRO": centro, "ARTE_EXIST": 4, "ARTE_MINIMO": 1, "ARTE_MAXIMO": 12, "ARTE_FECMOV": self.today},
            "ARTICULP": {"ARTP_NUMEMP": empresa, "ARTP_CODART": art, "ARTP_CODPRO": proveedor, "ARTP_REFPRO": f"REF2{self.short}", "ARTP_DESCRI": f"Compra MCP {self.stamp}"[:50], "ARTP_UNIMED": "UD", "ARTP_CANCON": 1, "ARTP_CANVEN": 1, "ARTP_UNIPAQ": 1, "ARTP_PREBAS": 8.5, "ARTP_DTOAUM1": 0, "ARTP_DTOAUM2": 0, "ARTP_DTOAUM3": 0, "ARTP_DTOAUM4": 0, "ARTP_DTOAUM5": 0, "ARTP_DTOAUM6": 0, "ARTP_CODMON": "E", "ARTP_CANPRE": 1},
            "ARTICULI": {"ARTI_NUMEMP": empresa, "ARTI_CODART": art, "ARTI_NUMLIN": 90, "ARTI_CODINF": "INFO", "ARTI_DESCRI": f"Relacion MCP {self.stamp}"},
        }
        for table in sorted(ARTICLE_WRITABLE_TABLES):
            payload = relation_payloads.get(table)
            if not payload:
                continue
            self.add(
                f"articulo_relacion_guardar:{table}",
                [table],
                {"tabla": table, "datos": payload},
                lambda table=table, payload=payload: self.rt.articulo_relacion_guardar({"tabla": table, "datos": payload}),
            )

        self.add("articulo_cambiar_tabla_precio", ["ARTICUL"], {"articulo": art, "tabla_precio": 10}, lambda: self.rt.articulo_cambiar_tabla_precio({"empresa": empresa, "articulo": art, "tabla_precio": 10, "simular": False}))
        self.add("articulo_familia_guardar", ["ARTICUL"], {"articulo": art}, lambda: self.rt.articulo_familia_guardar({"empresa": empresa, "articulo": art, "familia": 0, "subfamilia": 0, "simular": False}))
        self.add("articulo_familiancc_tabla_guardar", ["ARTICUL", "ARTICULI"], {"articulo": art}, lambda: self.rt.articulo_familiancc_tabla_guardar({"empresa": empresa, "articulo": art, "famncc": f"MCP {self.stamp}", "tabla_precio": 10, "simular": False}))
        self.add("articulo_tecnica_gestion", ["ARTCAR", "ARTICULI"], {"articulo": art}, lambda: self.rt.articulo_tecnica_gestion({"empresa": empresa, "articulo": art, "accion": "guardar", "texto": f"Caracteristicas tecnicas smoke {self.stamp}", "simular": False}))
        self.add("articulo_imagen_gestion", ["ARTICULI", "filesystem"], {"articulo": art}, lambda: self.rt.articulo_imagen_gestion({"empresa": empresa, "articulo": art, "accion": "guardar", "nombre_fichero": f"{art}.smoke.png", "content_base64": PNG_1X1, "simular": False}))
        self.add("articulo_documento_gestion", ["ARTICULI", "filesystem"], {"articulo": art}, lambda: self.rt.articulo_documento_gestion({"empresa": empresa, "articulo": art, "accion": "guardar", "nombre_fichero": f"{art}.smoke.pdf", "content_base64": pdf_base64(f"Documento articulo {art}\n{self.stamp}"), "simular": False}))

        client_payloads = {
            "CLIENI": {"CLII_NUMEMP": empresa, "CLII_CODCLI": cliente, "CLII_SUBCLI": subcliente, "CLII_NUMLIN": 90, "CLII_CODINF": "OBS", "CLII_DESCRI": f"Relacion MCP {self.stamp}"},
            "CLIFAM": {"CLIF_NUMEMP": empresa, "CLIF_CODCLI": cliente, "CLIF_CODFAM": 0, "CLIF_SUBFAM": 0, "CLIF_DTO": 1},
            "CLIART": {"CLIA_NUMEMP": empresa, "CLIA_CODCLI": cliente, "CLIA_CODART": art, "CLIA_PRECIO": 12, "CLIA_CANPRE": 1, "CLIA_BENEFI": 0, "CLIA_CODMON": "E", "CLIA_DESCUE": 0, "CLIA_CODARTC": art, "CLIA_DESCRI": f"Cliente articulo MCP {self.stamp}"[:50]},
        }
        for table in sorted(CLIENT_TABLES):
            payload = client_payloads.get(table)
            if not payload:
                continue
            self.add(
                f"cliente_relacion_guardar:{table}",
                [table],
                {"tabla": table, "datos": payload},
                lambda table=table, payload=payload: self.rt.cliente_relacion_guardar({"tabla": table, "datos": payload}),
            )

        provider_payloads = {
            "PROVEEI": {"PROI_NUMEMP": empresa, "PROI_CODPRO": proveedor, "PROI_NUMLIN": 90, "PROI_CODINF": "OBS", "PROI_TEXTO": f"Relacion MCP {self.stamp}"},
            "ARTICULP": {"ARTP_NUMEMP": empresa, "ARTP_CODART": art, "ARTP_CODPRO": proveedor, "ARTP_REFPRO": f"REF3{self.short}", "ARTP_DESCRI": f"Compra MCP {self.stamp}"[:50], "ARTP_UNIMED": "UD", "ARTP_CANCON": 1, "ARTP_CANVEN": 1, "ARTP_UNIPAQ": 1, "ARTP_PREBAS": 9, "ARTP_DTOAUM1": 0, "ARTP_DTOAUM2": 0, "ARTP_DTOAUM3": 0, "ARTP_DTOAUM4": 0, "ARTP_DTOAUM5": 0, "ARTP_DTOAUM6": 0, "ARTP_CODMON": "E", "ARTP_CANPRE": 1},
        }
        for table in sorted(PROVIDER_WRITABLE_TABLES):
            payload = provider_payloads.get(table)
            if not payload:
                continue
            self.add(
                f"proveedor_relacion_guardar:{table}",
                [table],
                {"tabla": table, "datos": payload},
                lambda table=table, payload=payload: self.rt.proveedor_relacion_guardar({"tabla": table, "datos": payload}),
            )

    def line(self, price: float = 12) -> dict[str, Any]:
        return {"articulo": self.generated["articulo"], "cantidad": 1, "precio": price, "descripcion": f"Linea MCP {self.stamp}"}

    def run_documents(self) -> None:
        empresa = self.rt.empresa
        centro = self.rt.centro
        cliente = self.generated["cliente"]
        subcliente = self.generated["subcliente"]
        proveedor = self.generated["proveedor"]
        art = self.generated["articulo"]
        tomorrow = (date.today() + timedelta(days=30)).isoformat()

        oferta = self.add("oferta_alta", ["OFERTAS", "DETOFER"], {"proveedor": proveedor}, lambda: self.rt.oferta_alta({"empresa": empresa, "centro": centro, "proveedor": proveedor, "descripcion": f"Oferta MCP {self.stamp}", "fecha_inicio": self.today, "fecha_fin": tomorrow, "articulos": [self.line(11)], "simular": False}))
        if oferta:
            self.generated["oferta"] = {"ejercicio": oferta["ejercicio"], "oferta": oferta["oferta"]}

        order = self.add("orden_compra_alta", ["CABORC", "DETORC"], {"proveedor": proveedor}, lambda: self.rt.orden_compra_alta({"empresa": empresa, "centro": centro, "proveedor": proveedor, "fecha": self.today, "articulos": [self.line(8)], "observaciones": f"MCP ABIERTA {self.stamp}", "simular": False}))
        if order:
            self.generated["orden_compra_abierta"] = {"ejercicio": order["ejercicio"], "serie": order["serie"], "numero": order["numero"]}

        order_to_close = self.add("orden_compra_alta:para_cierre", ["CABORC", "DETORC"], {"proveedor": proveedor}, lambda: self.rt.orden_compra_alta({"empresa": empresa, "centro": centro, "proveedor": proveedor, "fecha": self.today, "articulos": [self.line(8)], "observaciones": f"MCP CIERRE {self.stamp}", "simular": False}))
        if order_to_close:
            self.generated["orden_compra_cerrada"] = {"ejercicio": order_to_close["ejercicio"], "serie": order_to_close["serie"], "numero": order_to_close["numero"]}
            self.add("orden_compra_cerrar", ["CABORC", "DETORC"], self.generated["orden_compra_cerrada"], lambda: self.rt.orden_compra_cerrar({"empresa": empresa, "centro": centro, **self.generated["orden_compra_cerrada"], "simular": False}))

        entry_args = {"empresa": empresa, "centro": centro, "proveedor": proveedor, "fecha": self.today, "albaran": f"ALB{self.short}", "lineas": [self.line(8)], "observaciones": f"MCP {self.stamp}", "simular": False}
        entry = self.add("entrada_almacen_alta", ["CABDOCM", "DETMOVM", "ARTICULE"], entry_args, lambda: self.rt.entrada_almacen_alta(entry_args))
        if entry:
            self.generated["entrada_almacen"] = {"ejercicio": entry["ejercicio"], "serie": entry["serie"], "numero": entry["numero"]}

        pdf_text = f"ALBARAN PDF{self.short}\nFACTURA F{self.short}\n{self.today}\n{art} Linea MCP {self.stamp} 1 8 21"
        pdf_args = {"empresa": empresa, "centro": centro, "proveedor": proveedor, "content_base64": pdf_base64(pdf_text), "cabecera": {"fecha": self.today, "albaran": f"PDF{self.short}"}, "lineas": [self.line(8)], "simular": False}
        pdf_entry = self.add("entrada_almacen_desde_pdf", ["CABDOCM", "DETMOVM", "ARTICULE"], pdf_args, lambda: self.rt.entrada_almacen_desde_pdf(pdf_args))
        if pdf_entry:
            self.generated["entrada_pdf"] = {"ejercicio": pdf_entry["ejercicio"], "serie": pdf_entry["serie"], "numero": pdf_entry["numero"]}

        sale_args = {"empresa": empresa, "centro": centro, "cliente": cliente, "subcliente": subcliente, "tipo_documento": "P", "fecha": self.today, "referencia_cliente": f"MCP{self.short}", "lineas": [self.line(12)], "observaciones": f"MCP {self.stamp}", "simular": False}
        sale = self.add("venta_documento_alta", ["CABDOCV", "DETMOV", "NUMERA"], sale_args, lambda: self.rt.venta_documento_alta(sale_args))
        if sale:
            self.generated["venta_documento"] = {"ejercicio": sale["ejercicio"], "serie": sale["serie"], "numero": sale["numero"], "tipo_documento": sale["tipo_documento"], "tipo_accion": "0"}

        pedido_args = {"empresa": empresa, "centro": centro, "cliente": cliente, "subcliente": subcliente, "fecha": self.today, "lineas": [self.line(12)], "observaciones": f"Pedido MCP {self.stamp}", "simular": False}
        pedido = self.add("pedido_crear", ["CABDOCV", "DETMOV", "NUMERA"], pedido_args, lambda: self.rt.pedido_crear(pedido_args))
        if pedido:
            key = {"empresa": empresa, "centro": centro, "ejercicio": pedido["ejercicio"], "serie": pedido["serie"], "numero": pedido["numero"], "tipo_documento": "P", "tipo_accion": "0"}
            self.generated["pedido"] = key
            self.add("pedido_retirada_actualizar", ["CABDOCV"], key, lambda: self.rt.pedido_retirada_actualizar({**key, "retirado": f"RET {self.stamp}", "referencia": f"REF{self.short}", "simular": False}))
            self.add("pedido_situacion_actualizar", ["CABDOCV"], key, lambda: self.rt.pedido_situacion_actualizar({**key, "situacion": "P", "simular": False}))
            self.add("pedido_marcar_preparado", ["CABDOCV", "DETMOV"], key, lambda: self.rt.pedido_marcar_preparado({**key, "lineas": [{"linea": 10, "cantidad": 1}], "simular": False}))
            self.add("pedido_finalizar", ["CABDOCV"], key, lambda: self.rt.pedido_finalizar({**key, "situacion": "F", "simular": False}))
            self.add("pedido_albaranar", ["CABDOCV", "DETMOV", "NUMERA"], key, lambda: self.rt.pedido_albaranar({**key, "lineas": [{"linea": 10, "cantidad": 1}], "cerrar_pedido": False, "simular": False}))
            self.add("pedido_cerrar", ["CABDOCV", "DETMOV"], key, lambda: self.rt.pedido_cerrar({**key, "simular": False}))

    def run_stock(self) -> None:
        empresa = self.rt.empresa
        centro = self.rt.centro
        art = self.generated["articulo"]
        self.add("articulo_regularizar", ["CABDOCR", "DETMOVR", "ARTICULE"], {"articulo": art}, lambda: self.rt.articulo_regularizar({"empresa": empresa, "centro": centro, "articulo": art, "cantidad": 7, "observaciones": f"MCP {self.stamp}", "simular": False}))
        other = self.rt.db.one("SELECT FIRST 1 CEN_CODCEN FROM CENTROS WHERE CEN_NUMEMP = ? AND CEN_CODCEN <> ? ORDER BY CEN_CODCEN", (empresa, centro))
        if other:
            dest = int(other["cen_codcen"])
            self.add("trasvase_generar", ["CABDOCR", "DETMOVR", "ARTICULE"], {"articulo": art, "destino": dest}, lambda: self.rt.trasvase_generar({"empresa": empresa, "centro_origen": centro, "centro_destino": dest, "lineas": [{"articulo": art, "cantidad": 1}], "observaciones": f"MCP {self.stamp}", "simular": False}))
        else:
            self.report.append({"name": "trasvase_generar", "tables": ["CABDOCR", "DETMOVR", "ARTICULE"], "status": "skipped", "reason": "No hay un segundo centro para probar un trasvase real."})
        self.add("recuento_grabar", ["RECUENTO"], {"articulo": art}, lambda: self.rt.recuento_grabar({"empresa": empresa, "centro": centro, "articulo": art, "cantidad": 7, "simular": False}))
        self.add("recuento_borrar", ["RECUENTO"], {"articulo": art}, lambda: self.rt.recuento_borrar({"empresa": empresa, "centro": centro, "articulo": art, "simular": False}))

    def run_cartera(self) -> None:
        empresa = self.rt.empresa
        row = self.rt.db.one(
            """
            SELECT FIRST 1 CBVE_CENTRO, CBVE_TIPDOC, CBVE_TIPAC, CBVE_EJERCI, CBVE_SERIE, CBVE_NUMDOC, CBVE_NUMORD
            FROM CABDOCVE
            WHERE CBVE_NUMEMP = ?
            ORDER BY CBVE_EJERCI DESC, CBVE_NUMDOC DESC, CBVE_NUMORD
            """,
            (empresa,),
        )
        if not row:
            self.report.append({"name": "remesa_crear/efecto_cambiar_estado", "tables": ["REMESA", "CABDOCVE"], "status": "skipped", "reason": "No hay efectos en CABDOCVE para remesar/cambiar estado."})
            return
        effect = {
            "empresa": empresa,
            "centro": int(row["cbve_centro"]),
            "tipo_documento": row["cbve_tipdoc"],
            "tipo_accion": row["cbve_tipac"],
            "ejercicio": int(row["cbve_ejerci"]),
            "serie": row["cbve_serie"],
            "numero": int(row["cbve_numdoc"]),
            "orden": int(row["cbve_numord"]),
        }
        remesa = self.add("remesa_crear", ["REMESA", "CABDOCVE"], effect, lambda: self.rt.remesa_crear({"empresa": empresa, "fecha": self.today, "descripcion": f"Remesa MCP {self.stamp}", "efectos": [effect], "simular": False}))
        if remesa:
            self.generated["remesa"] = remesa["remesa"]
            self.add("efecto_cambiar_estado:asignar_remesa", ["CABDOCVE"], effect, lambda: self.rt.efecto_cambiar_estado({**effect, "accion": "asignar_remesa", "remesa_ejercicio": remesa["remesa"]["ejercicio"], "remesa_codigo": remesa["remesa"]["codigo"], "observaciones": f"MCP {self.stamp}", "simular": False}))
            self.add("efecto_cambiar_estado:quitar_remesa", ["CABDOCVE"], effect, lambda: self.rt.efecto_cambiar_estado({**effect, "accion": "quitar_remesa", "observaciones": f"MCP {self.stamp}", "simular": False}))

    def run(self) -> dict[str, Any]:
        self.setup_masters()
        self.run_auxiliary_tables()
        self.setup_business_masters()
        self.run_relations()
        self.run_documents()
        self.run_stock()
        self.run_cartera()

        art = self.generated.get("articulo")
        if art:
            self.generated["verificaciones"] = [
                self.verify_count("ARTICUL", {"ART_NUMEMP": self.rt.empresa, "ART_CODART": art}),
                self.verify_count("ARTICULI", {"ARTI_NUMEMP": self.rt.empresa, "ARTI_CODART": art}),
                self.verify_count("ARTICULE", {"ARTE_NUMEMP": self.rt.empresa, "ARTE_CODART": art}),
            ]
        return {
            "stamp": self.stamp,
            "empresa": self.rt.empresa,
            "centro": self.rt.centro,
            "generated": self.generated,
            "summary": {
                "ok": sum(1 for item in self.report if item["status"] == "ok"),
                "error": sum(1 for item in self.report if item["status"] == "error"),
                "skipped": sum(1 for item in self.report if item["status"] == "skipped"),
                "total": len(self.report),
            },
            "items": self.report,
        }


def write_markdown(path: Path, data: dict[str, Any]) -> None:
    lines = [
        f"# Kofedas MCP write smoke {data['stamp']}",
        "",
        f"- Empresa: {data['empresa']}",
        f"- Centro: {data['centro']}",
        f"- OK: {data['summary']['ok']}",
        f"- Errores: {data['summary']['error']}",
        f"- Omitidas: {data['summary']['skipped']}",
        "",
        "## Registros generados",
        "",
    ]
    for key, value in data["generated"].items():
        lines.append(f"- `{key}`: `{json.dumps(jsonable(value), ensure_ascii=False)}`")
    lines.extend(["", "## Detalle", ""])
    for item in data["items"]:
        lines.append(f"### {item['name']}")
        lines.append(f"- Estado: `{item['status']}`")
        lines.append(f"- Tablas: `{', '.join(item['tables'])}`")
        if item.get("error"):
            lines.append(f"- Error: `{item['error']}`")
        if item.get("result") is not None:
            lines.append(f"- Resultado: `{json.dumps(item['result'], ensure_ascii=False)[:1200]}`")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stamp", default="MCPW" + date.today().strftime("%Y%m%d") + "_" + __import__("datetime").datetime.now().strftime("%H%M%S"))
    args = parser.parse_args()
    smoke = WriteSmoke(args.stamp)
    data = smoke.run()
    reports = ROOT / "reports"
    reports.mkdir(exist_ok=True)
    json_path = reports / f"write_smoke_{args.stamp}.json"
    md_path = reports / f"write_smoke_{args.stamp}.md"
    json_path.write_text(json.dumps(jsonable(data), ensure_ascii=False, indent=2), encoding="utf-8")
    write_markdown(md_path, data)
    print(json.dumps({"json": str(json_path), "markdown": str(md_path), "summary": data["summary"], "generated": data["generated"]}, ensure_ascii=False, indent=2))
    return 0 if data["summary"]["error"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
