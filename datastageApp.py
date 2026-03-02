import io
import pandas as pd
import streamlit as st

# Configuración de la página
st.set_page_config(page_title="Datastage de archivos de SIANAM", page_icon="🔗", layout="wide")

        
st.sidebar.subheader("¿Quiéres alguna app que simplifique tus procesos de comercio exterior?")
st.sidebar.markdown(
        """
        <p style="font-size: 16px;">
            <strong>Genera PDF para VUCEM:</strong> <a href="https://apps.marchainternacional.com/" target="_blank"> VUCEM PDF APP.com</a><br>
           
            
        </p>
        """,
        unsafe_allow_html=True
        )
st.sidebar.markdown(
        """
        <p style="font-size: 16px;">
            <strong>Sitio web:</strong> <a href="https://www.marchainternacional.com" target="_blank">marchainternacional.com</a><br>
            <strong>Celular:</strong> <a href="https://wa.me/528443500729" target="_blank">+52844-350-0729</a><br>
            <strong>Email:</strong> <a href="mailto:operaciones@marchainternacional.com">operaciones@marchainternacional.com</a>
        </p>
        """,
        unsafe_allow_html=True
        )
        
st.sidebar.markdown(
        """
    <a href="https://www.patreon.com/jhchavezr">
    <img src="https://c5.patreon.com/external/logo/become_a_patron_button.png" alt="Become a Patron" />
    </a>
        """,
        unsafe_allow_html=True
        )


# Interfaz de usuario
col1, col2 = st.columns([1, 5])
with col2:
    st.title("Generador de Datastage (SIANAM)")
    st.caption("Integración a nivel PARTIDA (551) basada en cabeceras reales de archivos .asc")
    st.info("Nota: Este proceso utiliza los nombres cortos detectados en tus archivos (Patente, Pedimento, etc.)")

with st.expander("¿Cómo funciona?", expanded=False):
    st.markdown("""
    - Carga los archivos descomprimidos del datastage(.asc).
    - Se hacen uniones completas (Full joins en R) usando las siguientes llaves: **Patente, Pedimento, SeccionAduanera**, y **SecuenciaFraccion**.
    - Solo usa los archivos con terminación: `_501`, `_510`, `_505`, `_551`, `_557`.
    - Descarga el archivo integrado (Al cerrar la página se borran todos los datos, no se guarda ninguna información).
    """)
with col1:
    try:
        st.image("VERTICAL_LOGO_MARCHA.png", use_container_width=True)
    except:
        st.write("LOGO")

# --- Funciones de Utilidad ---

def _read_any(file):
    """Lee archivos detectando el delimitador pipe (|) y encoding."""
    file.seek(0)
    try:
        # Se asume delimitador '|' según los archivos .asc compartidos
        return pd.read_csv(file, delimiter="|", header=0, skipinitialspace=True, encoding="utf-8", index_col=False)
    except Exception:
        file.seek(0)
        return pd.read_csv(file, delimiter="|", header=0, skipinitialspace=True, encoding="latin-1", index_col=False)

def _normalize_keys(df, keys):
    """Limpia las llaves de unión para evitar errores por espacios o tipos."""
    for k in keys:
        if k in df.columns:
            df[k] = df[k].astype(str).str.strip()
    return df

# --- Carga de Archivos ---
st.subheader("1) Carga los archivos (.asc)")
uploaded_files = st.file_uploader(
    "Arrastra aquí tus archivos 501, 510, 505, 551 y 557",
    type=["asc", "csv"],
    accept_multiple_files=True
)

file_map = {}
if uploaded_files:
    for f in uploaded_files:
        if "501" in f.name: file_map["501"] = f
        if "510" in f.name: file_map["510"] = f
        if "505" in f.name: file_map["505"] = f
        if "551" in f.name: file_map["551"] = f
        if "557" in f.name: file_map["557"] = f

    missing = [k for k in ["501", "510", "505", "551", "557"] if k not in file_map]
    if missing:
        st.warning(f"Faltan los siguientes archivos: {', '.join(missing)}")

st.markdown("---")

# --- Definición de Llaves Reales (basadas en tus archivos .asc) ---
# Nombres detectados en los archivos: Patente, Pedimento, SeccionAduanera
join_keys_pedimento = ["Patente", "Pedimento", "SeccionAduanera"]
join_keys_partida = join_keys_pedimento + ["SecuenciaFraccion"]

if st.button("Generar Datastage Integrado", type="primary", disabled=len(file_map) < 5):
    try:
        with st.spinner("Procesando archivos..."):
            # Carga de datos
            df_501 = _read_any(file_map["501"])
            df_510 = _read_any(file_map["510"])
            df_505 = _read_any(file_map["505"])
            df_551 = _read_any(file_map["551"])
            df_557 = _read_any(file_map["557"])

            # Normalización de llaves
            df_501 = _normalize_keys(df_501, join_keys_pedimento)
            df_510 = _normalize_keys(df_510, join_keys_pedimento)
            df_505 = _normalize_keys(df_505, join_keys_pedimento)
            df_551 = _normalize_keys(df_551, join_keys_partida)
            df_557 = _normalize_keys(df_557, join_keys_partida)

            # 1. BASE: Empezamos con las Partidas (551)
            df_final = df_551.copy()

            # 2. INTEGRAR 501 (Datos Generales)
            df_501_clean = df_501.drop_duplicates(subset=join_keys_pedimento)
            df_final = pd.merge(df_final, df_501_clean, on=join_keys_pedimento, how="left", suffixes=("", "_501"))

            # 3. PIVOTAR 557 (Contribuciones de la Partida)
            if not df_557.empty:
                # Nombres de campos comunes en SIANAM: ClaveContribucion, FormaPago, ImportePago
                df_557["col_name"] = "557_" + df_557["ClaveContribucion"].astype(str) + "_" + df_557["FormaPago"].astype(str)
                df_557_pivot = df_557.pivot_table(
                    index=join_keys_partida,
                    columns="col_name",
                    values="ImportePago",
                    aggfunc="sum"
                ).reset_index()
                df_final = pd.merge(df_final, df_557_pivot, on=join_keys_partida, how="left")

            # 4. PIVOTAR 510 (Contribuciones del Pedimento)
            if not df_510.empty:
                df_510["col_name"] = "510_" + df_510["ClaveContribucion"].astype(str) + "_" + df_510["FormaPago"].astype(str)
                df_510_pivot = df_510.pivot_table(
                    index=join_keys_pedimento,
                    columns="col_name",
                    values="ImportePago",
                    aggfunc="sum"
                ).reset_index()
                df_final = pd.merge(df_final, df_510_pivot, on=join_keys_pedimento, how="left")

            # 5. INTEGRAR 505 (Facturas)
            if not df_505.empty:
                df_505_grouped = df_505.groupby(join_keys_pedimento).agg({
                    "NumeroFactura": lambda x: ", ".join(x.astype(str).unique()),
                    "ValorDolares": "sum"
                }).reset_index()
                df_final = pd.merge(df_final, df_505_grouped, on=join_keys_pedimento, how="left", suffixes=("", "_505"))

            # --- Resultados ---
            st.subheader("2) Resultado")
            st.write(f"Partidas totales: **{len(df_final)}**")
            st.dataframe(df_final.head(50), use_container_width=True)

            csv = df_final.to_csv(index=False).encode("utf-8")
            st.download_button(
                label="⬇️ Descargar Datastage CSV",
                data=csv,
                file_name="datastage_integrado.csv",
                mime="text/csv"
            )

    except Exception as e:
        st.error(f"Error durante el procesamiento: {str(e)}")
        st.info("Verifica que los archivos cargados tengan las columnas: Patente, Pedimento, SeccionAduanera.")

st.markdown("---")
st.caption("Generado para Marcha Internacional")