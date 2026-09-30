# MCP Kofedas ERP

Servidor MCP inicial para Kofedas, tomando como referencia la arquitectura del
MCP de Kronos y adaptando los nombres de tablas/campos detectados en los
fuentes Delphi de `C:\IA\Kofedas\Fuentes`.

## Estado inicial

Esta version expone lecturas operativas y un primer bloque de configuracion con
lectura/escritura controlada:

- `sistema_estado`
- `empresa_listar`
- `empresa_obtener`
- `empresa_guardar`
- `centro_listar`
- `centro_obtener`
- `centro_guardar`
- `usuario_listar`
- `usuario_obtener`
- `usuario_guardar`
- `grupo_usuario_listar`
- `grupo_usuario_obtener`
- `grupo_usuario_guardar`
- `parametro_listar`
- `parametro_obtener`
- `parametro_guardar`
- `auxiliar_tablas`
- `auxiliar_listar`
- `auxiliar_obtener`
- `auxiliar_guardar`
- `familia_listar`
- `articulo_buscar`
- `articulo_obtener`
- `stock_consultar`
- `articulo_tablas`
- `articulo_relacion_listar`
- `articulo_relacion_obtener`
- `articulo_relacion_guardar`
- `articulo_completo`
- `articulo_alta_preparar`
- `articulo_alta`
- `articulo_tarifa_excel_previsualizar`
- `articulo_tarifa_excel_importar`
- `cliente_buscar`
- `cliente_obtener`
- `cliente_tablas`
- `cliente_relacion_listar`
- `cliente_relacion_obtener`
- `cliente_relacion_guardar`
- `cliente_completo`
- `cliente_alta_preparar`
- `cliente_alta`
- `proveedor_buscar`
- `proveedor_obtener`
- `proveedor_articulos_listar`
- `proveedor_tablas`
- `proveedor_relacion_listar`
- `proveedor_relacion_obtener`
- `proveedor_relacion_guardar`
- `proveedor_completo`
- `proveedor_alta_preparar`
- `proveedor_alta`
- `oferta_tablas`
- `oferta_listar`
- `oferta_obtener`
- `oferta_articulos_listar`
- `oferta_alta_preparar`
- `oferta_alta`
- `orden_compra_tablas`
- `orden_compra_listar`
- `orden_compra_obtener`
- `orden_compra_lineas_listar`
- `orden_compra_alta_preparar`
- `orden_compra_alta`
- `orden_compra_cerrar`
- `entrada_almacen_tablas`
- `entrada_almacen_listar`
- `entrada_almacen_obtener`
- `entrada_almacen_lineas_listar`
- `entrada_almacen_pendientes_facturar`
- `entrada_almacen_pendientes_contabilizar`
- `entrada_almacen_alta_preparar`
- `entrada_almacen_alta`
- `entrada_almacen_pdf_previsualizar`
- `entrada_almacen_desde_pdf`
- `venta_tablas`
- `venta_listar`
- `venta_obtener`
- `venta_lineas_listar`
- `venta_precio_articulo`
- `rentabilidad_articulo_ventas`
- `rentabilidad_articulos_resumen`
- `venta_documento_alta_preparar`
- `venta_documento_alta`
- `venta_pedido_alta`
- `cartera_tablas`
- `cartera_efectos_detalle`
- `cartera_deuda_cliente`
- `cartera_deuda_por_cliente`
- `cartera_pendiente_remesar`
- `cartera_deuda_por_tipo`
- `dashboard_resumen`
- `ventas_resumen`
- `compras_resumen`
- `dashboard_evolucion_anual`
- `dashboard_series_temporales`
- `dashboard_rankings`
- `regularizacion_tablas`
- `regularizacion_listar`
- `stock_por_almacen`
- `stock_a_fecha`
- `inventario_valorar_articulos`
- `articulo_regularizar`
- `trasvase_generar`
- `recuento_listar`
- `recuento_grabar`
- `recuento_borrar`

Las herramientas `*_guardar` hacen upsert sobre campos tipados de cada tabla y
requieren `KOFEDAS_MCP_ACCESS_LEVEL=write` o `critical`. Las lecturas de
usuarios no devuelven contraseñas en claro.

El grupo Auxiliares expone un acceso generico con lista blanca para tablas como
`AGRUP1`, `AGRUP2`, `AGRUP3`, `BANCOS`, `CAJAS`, `CATALO`, `ENTIDA`,
`FAMILI`, `SUBFAM`, `SSUBFAM`, `FORENV`, `FORPAG`, `MEDIDAS`, `TABPREC`,
`TARJET`, `TIPIVA`, `TIPVEN`, `ZONAS`, entre otras. `auxiliar_guardar` usa la
clave primaria real de Firebird para decidir entre insertar o actualizar.

El grupo Articulos cubre `ARTICUL`, `ARTICULC`, `ARTICULE`, `ARTICULP`,
`ARTICULI`, `ARTICULA`, `ARTICULB` y `ELIMA`; `ELIMA` queda en lectura.
`articulo_alta` puede crear la ficha principal, ficha de compra, codigos de
barras, stock inicial e informacion adicional. `articulo_tarifa_excel_importar`
lee tarifas `.xlsx` con cabeceras flexibles; admite `mapeo`, `simular`,
generacion de codigos por `seccion + proveedor + contador`, altas de articulo,
ficha de compra `ARTICULP`, codigos de barras `ARTICULC` y stock `ARTICULE`.
Antes de escribir se puede usar `articulo_tarifa_excel_previsualizar`.

El grupo Clientes cubre `CLIEN`, `CLIENI`, `CLIFAM`, `CLIART`, `CLIACT`,
`CLIAGR`, `CLITAR` y `CLIDIR3`. `CLIPRO` existe en los fuentes Delphi, pero no
esta creada en la base Kofedas actual, por lo que se informa como no disponible
en `cliente_tablas`. `cliente_alta_preparar` calcula codigo y defaults sin
escribir; `cliente_alta` escribe `CLIEN`, informacion adicional opcional en
`CLIENI` y, si se solicita para subclientes, hereda `CLIACT` del subcliente 0.

El grupo Proveedores se basa en `PROVEE_UDM.pas` y `MNTPRO_U.pas`. Cubre
`PROVEE`, `PROVEEI`, `ARTICULP`, `CABDOCM`, `CABORC` y `ELIMP`; las tablas de
documentos y bajas se exponen como lectura. `proveedor_alta_preparar` calcula el
siguiente codigo y los defaults principales sin escribir; `proveedor_alta`
inserta `PROVEE` y la informacion adicional opcional en `PROVEEI`.

El grupo Ofertas cubre `OFERTAS` y `DETOFER`, segun `OFERTAS_UDM.pas` y
`MNTOFE_U.pas`. `oferta_articulos_listar` devuelve articulos en oferta con
filtros por vigencia, proveedor, oferta o texto. `oferta_alta` crea cabecera y
lineas a partir de una lista de articulos; cada linea puede indicar `articulo`,
`codigo_barras`/EAN o `referencia` de proveedor, y precios `precos`, `precio`,
`pvp`, `dto1` y `dto2`. Usa `simular=true` para revisar el plan antes de
escribir.

El grupo Ordenes de Compra cubre `CABORC`, `DETORC` y `ELIMOC`, segun
`CABORC_UDM.pas` y `MNTORC_U.pas`. `orden_compra_listar` permite consultar
pedidos por estado (`abierto`/`A`, `pendiente`/`P`, `cerrado`/`C`), proveedor,
fechas, serie, numero, texto o articulo. `orden_compra_alta` crea cabecera y
lineas a partir de una lista de articulos, resolviendo codigo por articulo,
EAN o referencia de proveedor y valorando importes con descuentos encadenados.
`orden_compra_cerrar` marca las lineas como cerradas, deja pendiente a cero y
actualiza la situacion de cabecera. Usa `simular=true` para revisar altas o
cierres antes de escribir.

El grupo Entradas de Almacen cubre `CABDOCM`, `DETMOVM`, `DETMOVMC`,
`ARTICULE` y `STOCKS`, segun `CABDOCM_UDM.pas` y `PEDPRO_U.pas`.
`entrada_almacen_listar` devuelve entradas con filtros por situacion, proveedor,
fechas, albaran, factura, serie o articulo. `entrada_almacen_pendientes_facturar`
usa `CBM_SITUAC='P'`; `entrada_almacen_pendientes_contabilizar` usa
`CBM_SITUAC='F'`. `entrada_almacen_alta` crea cabecera y lineas con valoracion
de descuentos, IVA, recargos, bases y totales, y actualiza stock existente en
`ARTICULE`. `entrada_almacen_pdf_previsualizar` extrae texto de un PDF mediante
`pypdf` y propone cabecera/lineas; `entrada_almacen_desde_pdf` permite grabar la
entrada a partir de esa propuesta y ajustes revisados. Usa siempre
`simular=true` para validar el PDF antes de escribir.

El grupo Regularizaciones cubre `CABDOCR`, `DETMOVR`, `RECUENTO`, `ARTICULE`
y `STOCKS`, segun `CABDOCR_UDM.pas`, `TRASVART_U.pas`, `RECINV_U.pas` y
`LIBESP_U.UTL_STOCK`. `articulo_regularizar` ajusta el stock actual de un
articulo a una cantidad objetivo y genera movimiento `DETMOVR`; `recuento_*`
solo trabaja sobre `RECUENTO` y no modifica existencias hasta que se procese el
inventario. `trasvase_generar` crea salida en tienda origen y entrada espejo en
destino. `stock_por_almacen` lee existencias actuales por centro y
`stock_a_fecha` reconstruye el stock historico desde el stock actual
deshaciendo entradas, ventas y regularizaciones posteriores a la fecha indicada.
`inventario_valorar_articulos` valora existencias a coste leyendo
`PARAMETROS.RENTAB` (`PBASE`, `PMEDIO` o `ULTIMO`) como
`ARTICUL_UB.PRECIO_COSTE_ARTICUL`; tambien permite forzar el modo para
diagnostico y devuelve detalle por articulo, origen del coste y totales.

El grupo Ventas cubre `CABDOCV` y `DETMOV`, con apoyo de `NUMERA`, `CLIEN`,
`CLIART`, `CLIFAM`, `CLIACT`, `OFERTAS` y `DETOFER`, segun `CABDOCV_UDM.pas`
y el motor de precios de Kronos adaptado a las tablas Kofedas disponibles.
`venta_listar`, `venta_obtener` y `venta_lineas_listar` consultan cabeceras y
lineas por tipo, cliente, estado, fecha, articulo o texto. `venta_precio_articulo`
calcula el precio por cliente, fecha y cantidad resolviendo articulo por codigo,
EAN o codigo propio del cliente y aplicando, por este orden, precio especial
`CLIART`, oferta vigente, descuento de familia y tarifa del cliente. Para altas,
`venta_documento_alta_preparar` permite revisar cabecera, lineas, IVA y totales
sin escribir; `venta_pedido_alta` crea pedidos de cliente (`CBV_TIPDOC='P'`)
con `CABDOCV`/`DETMOV` y numeracion `NUMERA`. Los pedidos/presupuestos no
modifican existencias, igual que el flujo Delphi de pedido de cliente.
Las herramientas `rentabilidad_articulo_ventas` y
`rentabilidad_articulos_resumen` replican el enfoque de `ANAVEN`: leen
`DETMOV`/`CABDOCV`, calculan venta neta (`DMV_VALLINS-DMV_IMPDTO`) y coste con
`PRECIO_COSTE_ARTICUL`/`PARAMETROS.RENTAB`; para lineas fantasma usan
`DMV_CANPRE` o `PARAMETROS.RENTAF`, igual que `RENTABILIDAD_LINEA`.

El grupo Cartera cubre los vencimientos/efectos de clientes en `CABDOCVE`,
enlazando con `CLIEN` y `CABDOCV` cuando hay cabecera origen. Permite listar
efectos por cliente, documento, tipo de efecto, situacion (`pendiente`,
`vencido`, `impagado`, `cobrado`) y estado de remesa (`CBVE_EJEREM` /
`CBVE_CODREM`). `cartera_deuda_cliente` devuelve la deuda de un cliente;
`cartera_deuda_por_cliente` resume deuda por cliente; `cartera_pendiente_remesar`
lista efectos pendientes no remesados; y `cartera_deuda_por_tipo` agrupa por
tipo de documento, tipo de efecto, situacion y remesado. En la base Kofedas
actual no existen tablas `REMESAS/DETREM/COBROS` con esos nombres, por lo que
este grupo se centra en la informacion de cartera disponible en `CABDOCVE`.

El grupo Dashboard recoge consultas de cuadro de mando similares a Kronos,
adaptadas a las tablas Kofedas disponibles. `dashboard_resumen` consolida
ventas (`CABDOCV`), compras/entradas (`CABDOCM`), cartera y pendientes.
`ventas_resumen` y `compras_resumen` agrupan por mes, año, cliente/proveedor,
articulo, familia, centro o tipo/situacion. `dashboard_evolucion_anual`
compara ventas, compras y margen bruto aproximado por año; `dashboard_series_temporales`
combina ventas y compras en una serie por mes/año; `dashboard_rankings` devuelve
los principales clientes, proveedores y articulos de venta/compra del periodo.
Todas son consultas de solo lectura.

El DSN ODBC por defecto es `Kronos`, tal como se usa para la base de datos ERP
Kofedas.

## Ejecucion

```powershell
cd C:\IA\Kofedas\MCP
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install --upgrade pip
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:KOFEDAS_ODBC_DSN = "Kronos"
$env:KOFEDAS_MCP_ACCESS_LEVEL = "critical"
$env:KOFEDAS_MCP_TRANSPORT = "stdio"
.\.venv\Scripts\python.exe server.py
```

Para HTTP local:

```powershell
$env:KOFEDAS_MCP_TRANSPORT = "streamable-http"
$env:KOFEDAS_MCP_HTTP_PORT = "8010"
.\.venv\Scripts\python.exe server.py
```
