import io
import pandas as pd
import streamlit as st
import zipfile

# Configuración de la página
st.set_page_config(page_title="Datastage de archivos de SIANAM", page_icon="🔗", layout="wide")

# Interfaz de usuario
col1, col2 = st.columns([1, 5])
with col2:
    st.title("Generador de Datastage (SIANAM)")
    st.caption("Integración a nivel PARTIDA (551) basada en cabeceras reales de archivos .asc")
    st.info("Soporta múltiples archivos del mismo tipo y archivos comprimidos en formato .ZIP")

with col1:
    try:
        st.image("VERTICAL_LOGO_MARCHA.png", width="stretch")
    except:
        st.write("LOGO")

# --- Funciones de Utilidad ---

def _read_any(file_obj, filename):
    """Lee archivos detectando el delimitador pipe (|) y encoding."""
    name = filename.lower()
    
    # Si es Excel
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(file_obj)
    
    # Si es ASC o CSV
    if name.endswith(".asc") or name.endswith(".csv"):
        try:
            if hasattr(file_obj, 'seek'): file_obj.seek(0)
            return pd.read_csv(file_obj, delimiter="|", header=0, skipinitialspace=True, encoding="utf-8", index_col=False)
        except Exception:
            if hasattr(file_obj, 'seek'): file_obj.seek(0)
            return pd.read_csv(file_obj, delimiter="|", header=0, skipinitialspace=True, encoding="latin-1", index_col=False)
    
    return None

def _normalize_keys(df, keys):
    """Limpia las llaves de unión para evitar errores por espacios o tipos."""
    if not df.empty:
        for k in keys:
            if k in df.columns:
                df[k] = df[k].astype(str).str.strip()
    return df

# --- Carga de Archivos ---
st.subheader("1) Carga los archivos (.asc / .csv / .xlsx / .zip)")
uploaded_files = st.file_uploader(
    "Arrastra aquí tus archivos sueltos o en un archivo .ZIP",
    type=["asc", "csv", "xlsx", "xls", "zip"],
    accept_multiple_files=True
)

st.markdown("---")

# --- Definición de Llaves Reales ---
join_keys_pedimento = ["Patente", "Pedimento", "SeccionAduanera"]
join_keys_partida = join_keys_pedimento + ["SecuenciaFraccion"]

if st.button("Generar Datastage Integrado", type="primary", disabled=not uploaded_files):
    try:
        with st.spinner("Extrayendo, leyendo y agrupando archivos..."):
            
            # Diccionario para agrupar las listas de DataFrames por tipo
            dfs_dict = {"501": [], "510": [], "505": [], "551": [], "557": []}

            # Función local para clasificar y agregar DataFrames
            def procesar_y_clasificar(file_obj, filename):
                df = _read_any(file_obj, filename)
                if df is not None and not df.empty:
                    for key in dfs_dict.keys():
                        if key in filename:
                            dfs_dict[key].append(df)
                            break # Solo lo asigna al primer tipo que coincida

            # 1. Leer y clasificar todos los archivos (sueltos o en ZIP)
            for f in uploaded_files:
                if f.name.lower().endswith(".zip"):
                    with zipfile.ZipFile(f) as z:
                        for zname in z.namelist():
                            # Ignorar carpetas y archivos ocultos de macOS
                            if not zname.endswith("/") and "__MACOSX" not in zname:
                                with z.open(zname) as zf:
                                    # Convertimos a BytesIO para que pandas lo lea sin problema
                                    file_bytes = io.BytesIO(zf.read())
                                    procesar_y_clasificar(file_bytes, zname)
                else:
                    procesar_y_clasificar(f, f.name)

            # 2. Concatenar DataFrames del mismo tipo en uno solo
            df_501 = pd.concat(dfs_dict["501"], ignore_index=True) if dfs_dict["501"] else pd.DataFrame()
            df_510 = pd.concat(dfs_dict["510"], ignore_index=True) if dfs_dict["510"] else pd.DataFrame()
            df_505 = pd.concat(dfs_dict["505"], ignore_index=True) if dfs_dict["505"] else pd.DataFrame()
            df_551 = pd.concat(dfs_dict["551"], ignore_index=True) if dfs_dict["551"] else pd.DataFrame()
            df_557 = pd.concat(dfs_dict["557"], ignore_index=True) if dfs_dict["557"] else pd.DataFrame()

            # Validar que al menos exista el archivo base 551
            if df_551.empty:
                st.error("No se encontró información para el archivo base 551. Verifica los archivos cargados.")
                st.stop()

            st.success(f"Archivos agrupados correctamente. Base de partidas (551): {len(df_551)} filas.")

            # Normalización de llaves en los DataFrames concatenados
            df_501 = _normalize_keys(df_501, join_keys_pedimento)
            df_510 = _normalize_keys(df_510, join_keys_pedimento)
            df_505 = _normalize_keys(df_505, join_keys_pedimento)
            df_551 = _normalize_keys(df_551, join_keys_partida)
            df_557 = _normalize_keys(df_557, join_keys_partida)

            # 3. BASE: Empezamos con las Partidas (551)
            df_final = df_551.copy()

            # 4. INTEGRAR 501 (Datos Generales)
            if not df_501.empty:
                # Aggregate all required columns by concatenating unique values
                columns_501 = [
                    "TipoOperacion", "ClaveDocumento", "SeccionAduaneraEntrada", "CurpContribuyente", "Rfc", "CurpAgenteA", "TipoCambio", "TotalFletes", "TotalSeguros", "TotalEmbalajes", "TotalIncrementables", "TotalDeducibles", "PesoBrutoMercancia", "MedioTransporteSalida", "MedioTransporteArribo", "MedioTransporteEntrada_Salida", "DestinoMercancia", "NombreContribuyente", "CalleContribuyente", "NumInteriorContribuyente", "NumExteriorContribuyente", "CPContribuyente", "MunicipioContribuyente", "EntidadFedContribuyente", "PaisContribuyente", "TipoPedimento", "FechaRecepcionPedimento", "FechaPagoReal", "RfcTransportista", "CurpTransportista", "NombreTransportista", "PaisTransporte", "IdentificadorTransporte", "NumContenedor", "TipoContenedor", "FechaFacturacion", "NumeroFactura", "TerminoFacturacion", "MonedaFacturacion", "ValorDolares", "ValorMonedaExtranjera", "PaisFacturacion", "EntidadFedFacturacion", "IndentFiscalProveedor", "ProveedorMercancia", "CalleProveedor", "NumInteriorProveedor", "NumExteriorProveedor", "CpProveedor", "MunicipioProveedor", "TipoFecha", "FechaOperacion", "FechaValidacionPagoR", "ClaveCaso", "IdentificadorCaso", "ComplementoCaso", "ClaveContribucion", "TasaContribucion", "TipoTasa", "FormaPago", "ImportePago", "SecuenciaObservacion", "Observaciones", "Fraccion", "SecuenciaFraccion", "SubdivisionFraccion", "DescripcionMercancia", "PrecioUnitario", "ValorAduana", "ValorComercial", "CantidadUMComercial", "UnidadMedidaComercial", "CantidadUMTarifa", "UnidadMedidaTarifa", "ValorAgregado", "ClaveVinculacion", "MetodoValorizacion", "CodigoMercanciaProducto", "MarcaMercanciaProducto", "ModeloMercanciaProducto", "PaisOrigenDestino", "PaisCompradorVendedor", "EntidadFedOrigen", "EntidadFedDestino", "EntidadFedComprador", "EntidadFedVendedor", "Folio", "RFCoPatenteAduanal", "Fecha_Inicial", "Fecha_Final", "Fecha_Ejecucion", "Total_Fracciones", "Total_Contribuciones", "ConsecutivoRemesa", "NumeroSeleccion", "FechaSeleccion", "HoraSeleccion", "SemaforoFisca"
                ]
                # Only keep columns that exist in df_501
                columns_501 = [c for c in columns_501 if c in df_501.columns]
                agg_dict_501 = {col: (lambda x: ', '.join(x.dropna().astype(str).unique())) for col in columns_501}
                df_501_agg = df_501.groupby(join_keys_pedimento).agg(agg_dict_501).reset_index()
                df_final = pd.merge(df_final, df_501_agg, on=join_keys_pedimento, how="left", suffixes=("", "_501"))

            # 5. PIVOTAR 557 (Contribuciones de la Partida)
            if not df_557.empty:
                df_557["col_name"] = "557_" + df_557["ClaveContribucion"].astype(str) + "_" + df_557["FormaPago"].astype(str)
                df_557_pivot = df_557.pivot_table(
                    index=join_keys_partida,
                    columns="col_name",
                    values="ImportePago",
                    aggfunc="sum"
                ).reset_index()
                df_final = pd.merge(df_final, df_557_pivot, on=join_keys_partida, how="left")

            # 6. PIVOTAR 510 (Contribuciones del Pedimento)
            if not df_510.empty:
                # Pivot as before for ImportePago
                df_510["col_name"] = "510_" + df_510["ClaveContribucion"].astype(str) + "_" + df_510["FormaPago"].astype(str)
                df_510_pivot = df_510.pivot_table(
                    index=join_keys_pedimento,
                    columns="col_name",
                    values="ImportePago",
                    aggfunc="sum"
                ).reset_index()
                # Also aggregate all required columns by concatenating unique values
                columns_510 = [
                    "TipoOperacion", "ClaveDocumento", "SeccionAduaneraEntrada", "CurpContribuyente", "Rfc", "CurpAgenteA", "TipoCambio", "TotalFletes", "TotalSeguros", "TotalEmbalajes", "TotalIncrementables", "TotalDeducibles", "PesoBrutoMercancia", "MedioTransporteSalida", "MedioTransporteArribo", "MedioTransporteEntrada_Salida", "DestinoMercancia", "NombreContribuyente", "CalleContribuyente", "NumInteriorContribuyente", "NumExteriorContribuyente", "CPContribuyente", "MunicipioContribuyente", "EntidadFedContribuyente", "PaisContribuyente", "TipoPedimento", "FechaRecepcionPedimento", "FechaPagoReal", "RfcTransportista", "CurpTransportista", "NombreTransportista", "PaisTransporte", "IdentificadorTransporte", "NumContenedor", "TipoContenedor", "FechaFacturacion", "NumeroFactura", "TerminoFacturacion", "MonedaFacturacion", "ValorDolares", "ValorMonedaExtranjera", "PaisFacturacion", "EntidadFedFacturacion", "IndentFiscalProveedor", "ProveedorMercancia", "CalleProveedor", "NumInteriorProveedor", "NumExteriorProveedor", "CpProveedor", "MunicipioProveedor", "TipoFecha", "FechaOperacion", "FechaValidacionPagoR", "ClaveCaso", "IdentificadorCaso", "ComplementoCaso", "ClaveContribucion", "TasaContribucion", "TipoTasa", "FormaPago", "ImportePago", "SecuenciaObservacion", "Observaciones", "Fraccion", "SecuenciaFraccion", "SubdivisionFraccion", "DescripcionMercancia", "PrecioUnitario", "ValorAduana", "ValorComercial", "CantidadUMComercial", "UnidadMedidaComercial", "CantidadUMTarifa", "UnidadMedidaTarifa", "ValorAgregado", "ClaveVinculacion", "MetodoValorizacion", "CodigoMercanciaProducto", "MarcaMercanciaProducto", "ModeloMercanciaProducto", "PaisOrigenDestino", "PaisCompradorVendedor", "EntidadFedOrigen", "EntidadFedDestino", "EntidadFedComprador", "EntidadFedVendedor", "Folio", "RFCoPatenteAduanal", "Fecha_Inicial", "Fecha_Final", "Fecha_Ejecucion", "Total_Fracciones", "Total_Contribuciones", "ConsecutivoRemesa", "NumeroSeleccion", "FechaSeleccion", "HoraSeleccion", "SemaforoFisca"
                ]
                columns_510 = [c for c in columns_510 if c in df_510.columns]
                agg_dict_510 = {col: (lambda x: ', '.join(x.dropna().astype(str).unique())) for col in columns_510}
                df_510_agg = df_510.groupby(join_keys_pedimento).agg(agg_dict_510).reset_index()
                # Merge both pivot and agg
                df_final = pd.merge(df_final, df_510_pivot, on=join_keys_pedimento, how="left")
                df_final = pd.merge(df_final, df_510_agg, on=join_keys_pedimento, how="left", suffixes=("", "_510"))

            # 7. INTEGRAR 505 (Facturas)
            if not df_505.empty:
                columns_505 = [
                    "TipoOperacion", "ClaveDocumento", "SeccionAduaneraEntrada", "CurpContribuyente", "Rfc", "CurpAgenteA", "TipoCambio", "TotalFletes", "TotalSeguros", "TotalEmbalajes", "TotalIncrementables", "TotalDeducibles", "PesoBrutoMercancia", "MedioTransporteSalida", "MedioTransporteArribo", "MedioTransporteEntrada_Salida", "DestinoMercancia", "NombreContribuyente", "CalleContribuyente", "NumInteriorContribuyente", "NumExteriorContribuyente", "CPContribuyente", "MunicipioContribuyente", "EntidadFedContribuyente", "PaisContribuyente", "TipoPedimento", "FechaRecepcionPedimento", "FechaPagoReal", "RfcTransportista", "CurpTransportista", "NombreTransportista", "PaisTransporte", "IdentificadorTransporte", "NumContenedor", "TipoContenedor", "FechaFacturacion", "NumeroFactura", "TerminoFacturacion", "MonedaFacturacion", "ValorDolares", "ValorMonedaExtranjera", "PaisFacturacion", "EntidadFedFacturacion", "IndentFiscalProveedor", "ProveedorMercancia", "CalleProveedor", "NumInteriorProveedor", "NumExteriorProveedor", "CpProveedor", "MunicipioProveedor", "TipoFecha", "FechaOperacion", "FechaValidacionPagoR", "ClaveCaso", "IdentificadorCaso", "ComplementoCaso", "ClaveContribucion", "TasaContribucion", "TipoTasa", "FormaPago", "ImportePago", "SecuenciaObservacion", "Observaciones", "Fraccion", "SecuenciaFraccion", "SubdivisionFraccion", "DescripcionMercancia", "PrecioUnitario", "ValorAduana", "ValorComercial", "CantidadUMComercial", "UnidadMedidaComercial", "CantidadUMTarifa", "UnidadMedidaTarifa", "ValorAgregado", "ClaveVinculacion", "MetodoValorizacion", "CodigoMercanciaProducto", "MarcaMercanciaProducto", "ModeloMercanciaProducto", "PaisOrigenDestino", "PaisCompradorVendedor", "EntidadFedOrigen", "EntidadFedDestino", "EntidadFedComprador", "EntidadFedVendedor", "Folio", "RFCoPatenteAduanal", "Fecha_Inicial", "Fecha_Final", "Fecha_Ejecucion", "Total_Fracciones", "Total_Contribuciones", "ConsecutivoRemesa", "NumeroSeleccion", "FechaSeleccion", "HoraSeleccion", "SemaforoFisca"
                ]
                columns_505 = [c for c in columns_505 if c in df_505.columns]
                agg_dict_505 = {col: (lambda x: ', '.join(x.dropna().astype(str).unique())) for col in columns_505}
                df_505_agg = df_505.groupby(join_keys_pedimento).agg(agg_dict_505).reset_index()
                df_final = pd.merge(df_final, df_505_agg, on=join_keys_pedimento, how="left", suffixes=("", "_505"))

            # --- Resultados ---
            st.subheader("2) Resultado")
            st.write(f"Partidas totales generadas: **{len(df_final)}**")
            st.dataframe(df_final.head(50), width="stretch")

            csv = df_final.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Descargar Datastage CSV",
                data=csv,
                file_name="datastage_integrado.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"Error durante el procesamiento: {str(e)}")
        st.exception(e)

st.markdown("---")
st.caption("Generado para Marcha Internacional")