# app.py
# Requirements (install before running):
#   streamlit>=1.36
#   pandas>=2.2
#   openpyxl>=3.1
#   XlsxWriter>=3.2
#
# Run:
#   streamlit run app.py

import io
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Datastage de archivos de SIANAM", page_icon="🔗", layout="wide")

col1, col2 = st.columns([1, 5])
with col2:
    st.title("Construye el datastage para el correcto monitoreo de tus operaciones de comercio exterior")
    st.caption("Genera un datastage a partir de los archivos de la Matriz de Seguridad SIANAM 501/510/505/551/557. \n Este es un proyecto de código abierto y no guarda ningún tipo de información de tus archivos. \n\n **Nota**: Este es un prototipo, no un producto final. Si requieres alguna funcionalidad extra enviar a webmaster@marchainternacional.com \n\n")
    st.logo("ESCUDO_LOGO_MARCHA.png", link="https://www.marchainternacional.com")
with col1:
    st.image("VERTICAL_LOGO_MARCHA.png", use_container_width=True )
        
        
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

with st.expander("¿Cómo funciona?", expanded=False):
    st.markdown("""
    - Carga los archivos descomprimidos del datastage(.asc).
    - Se hacen uniones completas (Full joins en R) usando las siguientes llaves: **Patente, Pedimento, SeccionAduanera**, y **SecuenciaFraccion**.
    - Solo usa los archivos con terminación: `_501`, `_510`, `_505`, `_551`, `_557`.
    - Descarga el archivo integrado (Al cerrar la página se borran todos los datos, no se guarda ninguna información).
    """)

def _read_any(file):
    name = file.name.lower()
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return pd.read_excel(file)
    if name.endswith(".asc") or name.endswith(".csv"):
        file.seek(0)
        try:
            return pd.read_csv(file, delimiter="|", header=0, skipinitialspace=True, encoding="utf-8",  index_col=False)
        except Exception:
            file.seek(0)
            return pd.read_csv(file, delimiter="|", header=0, skipinitialspace=True, encoding="latin-1",  index_col=False)
    raise ValueError("Unsupported file type: " + name)

def _safe_merge(left, right, keys, suffixes):
    missing_left = [k for k in keys if k not in left.columns]
    missing_right = [k for k in keys if k not in right.columns]
    if missing_left or missing_right:
        msg = []
        if missing_left:
            msg.append(f"Left missing keys: {missing_left}")
        if missing_right:
            msg.append(f"Right missing keys: {missing_right}")
        raise KeyError(" | ".join(msg))
    for k in keys:
        left[k] = left[k].astype(str)
        right[k] = right[k].astype(str)
    return pd.merge(left, right, how="outer", on=keys, suffixes=suffixes)

st.subheader("1) Carga los datos")
uploaded_files = st.file_uploader(
    "Arrastra y suelta tus archivos aquí (.asc/.csv/.xlsx/.xls)",
    type=["asc", "csv", "xlsx", "xls"],
    accept_multiple_files=True,
    key="allfiles"
)

def _find_file(files, code):
    for f in files:
        if code in f.name:
            return f
    return None

file_map = {}
if uploaded_files:
    file_map["f501"] = _find_file(uploaded_files, "501")
    file_map["f510"] = _find_file(uploaded_files, "510")
    file_map["f505"] = _find_file(uploaded_files, "505")
    file_map["f551"] = _find_file(uploaded_files, "551")
    file_map["f557"] = _find_file(uploaded_files, "557")

missing = [k for k, v in file_map.items() if v is None]
if missing:
    st.warning(f"Missing files: {', '.join(m[1:] for m in missing)}")

st.markdown("---")
st.subheader("2) Generar la unión de archivos")
join_keys = ["Patente", "Pedimento", "SeccionAduanera"]
last_keys = join_keys + ["SecuenciaFraccion"]

if st.button("Crear archivo CSV", type="primary", disabled=not all(file_map.values())):
    try:
        with st.spinner("Reading files..."):
            df_501 = _read_any(file_map["f501"])
            df_510 = _read_any(file_map["f510"])
            df_505 = _read_any(file_map["f505"])
            df_551 = _read_any(file_map["f551"])
            df_557 = _read_any(file_map["f557"])

        st.success("Files loaded! Showing previews (first 10 rows).")
        with st.expander("Previsualización df_501", expanded=False): st.dataframe(df_501.head(10))
        with st.expander("Previsualización df_510", expanded=False): st.dataframe(df_510.head(10))
        with st.expander("Previsualización df_505", expanded=False): st.dataframe(df_505.head(10))
        with st.expander("Previsualización df_551", expanded=False): st.dataframe(df_551.head(10))
        with st.expander("Previsualización df_557", expanded=False): st.dataframe(df_557.head(10))

        st.markdown("---")
        st.subheader("3) Merging…")

        df_all = _safe_merge(df_501, df_510, keys=join_keys, suffixes=("_501", "_510"))
        st.write("✅ Unión exitosa 501 ↔ 510")
        df_all = _safe_merge(df_all, df_505, keys=join_keys, suffixes=("", "_505"))
        st.write("✅ Unión exitosa archivo previo ↔ 505")
        df_all = _safe_merge(df_all, df_551, keys=join_keys, suffixes=("", "_551"))
        st.write("✅ Unión exitosa archivo previo  ↔ 551")
        df_all = _safe_merge(df_all, df_557, keys=last_keys, suffixes=("", "_557"))
        st.write("✅ Unión exitosa archivo previo ↔ 557")

        sort_cols = [c for c in last_keys if c in df_all.columns]
        if sort_cols:
            df_all = df_all.sort_values(by=sort_cols)

        st.markdown("---")
        st.subheader("4) Resultado")
        st.write(f"Filas: **{len(df_all):,}**  |  Columnas: **{df_all.shape[1]:,}**")
        st.dataframe(df_all.reset_index(), use_container_width=True)

        csv = df_all.reset_index().to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Descargar CSV (UTF-8)",
            data=csv,
            file_name="df_all.csv",
            mime="text/csv"
        )

    except KeyError as e:
        st.error(f"Key columns missing: {e}")
    except Exception as e:
        st.exception(e)

st.markdown("---")
st.markdown(
    "Hecho con Streamlit y pandas por [Marcha Internacional](https://marchainternacional.com)",
    unsafe_allow_html=True
)
