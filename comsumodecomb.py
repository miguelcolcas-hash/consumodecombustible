import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import urllib.parse
import requests
import io
import plotly.express as px

# --- 1. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="Supervisión Térmica - Osinergmin", layout="wide", initial_sidebar_state="expanded")
st.title("🏭 Dashboard de Supervisión - Consumo de Combustibles (SEIN)")
st.markdown("Fiscalización Dinámica del IEOD del COES - Sector Termoeléctrico")

MESES = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
    5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
    9: "Setiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
}

# --- 2. FUNCIONES DE EXTRACCIÓN Y LIMPIEZA (ETL) ---
def generar_urls_coes(fecha):
    año = fecha.strftime("%Y")
    mes_num = fecha.strftime("%m")
    dia = fecha.strftime("%d")
    mes_titulo = MESES[fecha.month]
    fecha_str = fecha.strftime("%d%m")
    
    path_nuevo = f"Post Operación/Reportes/IEOD/{año}/{mes_num}_{mes_titulo}/{dia}/AnexoA_{fecha_str}.xlsx"
    path_legacy = f"Post Operación/Reportes/IEOD/{año}/{mes_num}_{mes_titulo}/{dia}/Anexo1_Resumen_{fecha_str}.xlsx"
    
    return [
        (f"https://www.coes.org.pe/portal/browser/download?url={urllib.parse.quote(path_nuevo)}", "AnexoA"),
        (f"https://www.coes.org.pe/portal/browser/download?url={urllib.parse.quote(path_legacy)}", "Anexo1")
    ]

@st.cache_data(show_spinner=False)
def extraer_datos_combustible(fecha):
    urls = generar_urls_coes(fecha)
    headers = {'User-Agent': 'Mozilla/5.0'}
    df_raw = None
    
    for url, tipo_anexo in urls:
        try:
            res = requests.get(url, headers=headers, timeout=20)
            if res.status_code == 200:
                archivo_excel = io.BytesIO(res.content)
                xls = pd.ExcelFile(archivo_excel, engine='openpyxl')
                hojas_limpias = {h.strip().upper(): h for h in xls.sheet_names}
                
                if "CONSUMO_COMB" in hojas_limpias:
                    nombre_real_hoja = hojas_limpias["CONSUMO_COMB"]
                    
                    # LECTURA EXACTA SEGÚN IMAGEN: Fila 7 (header=6), Columnas B a G (6 columnas)
                    df_raw = pd.read_excel(xls, sheet_name=nombre_real_hoja, header=6, usecols="B:G")
                    break
        except Exception:
            continue
            
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(), f"[{fecha.strftime('%d/%m/%Y')}] No se halló el archivo o la hoja 'CONSUMO_COMB'."

    try:
        # ASIGNACIÓN DE COLUMNAS BASADA EN LA ESTRUCTURA REAL
        df_raw.columns = ['EMPRESA', 'CENTRAL', 'MEDIDOR', 'TIPO_COMBUSTIBLE', 'UNIDAD_MEDIDA', 'CONSUMO']
    except ValueError as e:
        return pd.DataFrame(), f"[{fecha.strftime('%d/%m/%Y')}] Error de estructura: El COES modificó las columnas. Detalles: {str(e)}"

    # --- LIMPIEZA Y TRANSFORMACIÓN ---
    df_raw = df_raw.dropna(subset=['EMPRESA', 'CENTRAL'])
    
    df_raw['CONSUMO'] = df_raw['CONSUMO'].astype(str).str.replace(',', '', regex=False)
    df_raw['CONSUMO'] = pd.to_numeric(df_raw['CONSUMO'], errors='coerce').fillna(0)
    
    if df_raw.empty:
        return pd.DataFrame(), f"[{fecha.strftime('%d/%m/%Y')}] Tabla vacía tras la limpieza."

    for col in ['EMPRESA', 'CENTRAL', 'MEDIDOR', 'TIPO_COMBUSTIBLE', 'UNIDAD_MEDIDA']:
        df_raw[col] = df_raw[col].astype(str).str.strip().str.upper()

    # --- REGLA NORMATIVA: Conversión de gas a Mm3 ---
    mask_gas = df_raw['TIPO_COMBUSTIBLE'].str.contains('GAS', na=False)
    mask_m3 = df_raw['UNIDAD_MEDIDA'].str.contains('M3', na=False)
    
    df_raw.loc[mask_gas & mask_m3, 'CONSUMO'] = df_raw.loc[mask_gas & mask_m3, 'CONSUMO'] / 1000000.0
    # Modificación exacta de nomenclatura (Mm3)
    df_raw.loc[mask_gas & mask_m3, 'UNIDAD_MEDIDA'] = 'Mm3'

    df_raw['FECHA_OPERATIVA'] = pd.to_datetime(fecha)
    
    return df_raw, None

def procesar_rango_fechas(start_date, end_date, progress_bar, status_text):
    fechas = pd.date_range(start_date, end_date)
    total_dias = len(fechas)
    lista_dfs = []
    alertas = []
    
    for i, f in enumerate(fechas):
        status_text.markdown(f"**⏳ Procesando Datos COES:** {f.strftime('%d/%m/%Y')} *(Día {i+1} de {total_dias})*")
        df_dia, error = extraer_datos_combustible(f)
        
        if not df_dia.empty:
            lista_dfs.append(df_dia)
        if error:
            alertas.append(error)
            
        progress_bar.progress((i + 1) / total_dias)
            
    if lista_dfs:
        return pd.concat(lista_dfs, ignore_index=True), alertas
    return pd.DataFrame(), alertas

# --- 3. INTERFAZ DE USUARIO ---
st.sidebar.header("Parámetros de Fiscalización")
rango_fechas = st.sidebar.date_input("Intervalo de Fechas (IEOD)", value=(datetime(2026, 1, 1), datetime(2026, 1, 3)))

if st.sidebar.button("Extraer Consumo", type="primary"):
    if isinstance(rango_fechas, tuple) and len(rango_fechas) == 2:
        start_date, end_date = rango_fechas
        status_text = st.empty()
        progress_bar = st.progress(0)
        
        df_consolidado, alertas = procesar_rango_fechas(start_date, end_date, progress_bar, status_text)
        
        st.session_state['df_combustible'] = df_consolidado
        st.session_state['alertas_comb'] = alertas
            
        status_text.empty()
        progress_bar.empty()

# --- 4. VISUALIZACIÓN DE DATOS ---
if 'df_combustible' in st.session_state:
    df_datos = st.session_state['df_combustible']
    alertas = st.session_state['alertas_comb']
    
    if df_datos.empty:
        st.error("🚨 Extracción fallida o sin datos: Revise la bitácora para más detalles.")
        if alertas:
            with st.expander("Ver bitácora de errores del COES"):
                for a in alertas: st.write(a)
    else:
        if alertas:
            with st.expander("⚠️ Alertas de Extracción de Datos (Días con problemas)"):
                for alerta in alertas:
                    st.warning(alerta)
                    
        st.success("✅ Extracción y transformación completada.")
        st.markdown("---")
        
        # ==========================================
        # FILTROS DESPLEGABLES (CASCADA)
        # ==========================================
        st.markdown("### 🔍 Filtros Operativos")
        
        lista_empresas = sorted(df_datos['EMPRESA'].unique())
        
        col_f1, col_f2, col_f3 = st.columns(3)
        
        with col_f1:
            filtro_emp = st.multiselect("🏢 Empresa Concesionaria:", options=lista_empresas, placeholder="Choose options...")
            
        if filtro_emp:
            df_temp_cen = df_datos[df_datos['EMPRESA'].isin(filtro_emp)]
        else:
            df_temp_cen = df_datos
            
        lista_centrales = sorted(df_temp_cen['CENTRAL'].unique())
        
        with col_f2:
            filtro_cen = st.multiselect("⚡ Central Termoeléctrica:", options=lista_centrales, placeholder="Choose options...")
            
        lista_comb = sorted(df_datos['TIPO_COMBUSTIBLE'].unique())
        
        with col_f3:
            filtro_comb = st.multiselect("🛢️ Tipo de Combustible:", options=lista_comb, placeholder="Choose options...")

        df_filtrado = df_datos.copy()
        if filtro_emp:
            df_filtrado = df_filtrado[df_filtrado['EMPRESA'].isin(filtro_emp)]
        if filtro_cen:
            df_filtrado = df_filtrado[df_filtrado['CENTRAL'].isin(filtro_cen)]
        if filtro_comb:
            df_filtrado = df_filtrado[df_filtrado['TIPO_COMBUSTIBLE'].isin(filtro_comb)]

        if df_filtrado.empty:
            st.warning("⚠️ No hay datos para la combinación de filtros seleccionada.")
        else:
            st.markdown("---")
            # ==========================================
            # GRÁFICAS: BARRAS APILADAS CON ANOTACIONES TOTALES/MAX
            # ==========================================
            st.markdown("### 📊 Consumo Diario Detallado por Tipo de Combustible")
            
            # --- NUEVOS CONTROLES DE VISUALIZACIÓN ---
            col_opt1, col_opt2 = st.columns(2)
            with col_opt1:
                mostrar_total = st.toggle("Mostrar Etiqueta de Total Diario", value=True)
            with col_opt2:
                mostrar_max = st.toggle("Mostrar Etiqueta de Central Máxima", value=True)
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            combustibles_presentes = sorted(df_filtrado['TIPO_COMBUSTIBLE'].unique())
            
            for comb in combustibles_presentes:
                df_plot = df_filtrado[df_filtrado['TIPO_COMBUSTIBLE'] == comb]
                if df_plot.empty: continue
                
                unidad = df_plot['UNIDAD_MEDIDA'].iloc[0]
                
                # Agrupamos por Fecha y Central
                df_grp = df_plot.groupby(['FECHA_OPERATIVA', 'CENTRAL'])['CONSUMO'].sum().reset_index()
                
                if df_grp['CONSUMO'].sum() == 0:
                    st.info(f"ℹ️ No hay consumo de **{comb}** en el periodo y centrales seleccionadas.")
                    st.markdown("<br>", unsafe_allow_html=True)
                    continue
                
                df_grp_plot = df_grp[df_grp['CONSUMO'] > 0].copy()
                
                # 1. Totalizamos el consumo por día
                df_totales = df_grp.groupby('FECHA_OPERATIVA', as_index=False).agg(TOTAL=('CONSUMO', 'sum'))
                
                # 2. Encontramos la Central con mayor consumo por día
                idx_max = df_grp_plot.groupby('FECHA_OPERATIVA')['CONSUMO'].idxmax()
                df_max_cen = df_grp_plot.loc[idx_max, ['FECHA_OPERATIVA', 'CENTRAL']].rename(columns={'CENTRAL': 'MAX_CENTRAL'})
                
                # Fusionamos ambos cálculos
                df_anotaciones = pd.merge(df_totales, df_max_cen, on='FECHA_OPERATIVA', how='left')
                
                fig = px.bar(
                    df_grp_plot,
                    x="FECHA_OPERATIVA", 
                    y="CONSUMO", 
                    color="CENTRAL",
                    title=f"Despacho Operativo de {comb} ({unidad})",
                    labels={
                        "CONSUMO": f"Volumen Consumido ({unidad})", 
                        "FECHA_OPERATIVA": "Día Operativo", 
                        "CENTRAL": "Central Térmica"
                    },
                    barmode="stack",
                    text_auto='.2s' 
                )
                
                max_y = df_totales['TOTAL'].max()
                margen_y = max_y * 1.2 if max_y > 0 else 1 

                fig.update_layout(
                    xaxis_tickangle=-45, 
                    hovermode="x unified", 
                    height=550, 
                    xaxis_title="Fecha",
                    yaxis_title=f"Consumo Total ({unidad})",
                    yaxis=dict(range=[0, margen_y])
                )
                
                # Inyectamos anotaciones condicionales basadas en los botones Toggle
                for _, row in df_anotaciones.iterrows():
                    if row['TOTAL'] > 0 and pd.notna(row['MAX_CENTRAL']):
                        lineas_texto = []
                        
                        if mostrar_total:
                            lineas_texto.append(f"<b>Total: {row['TOTAL']:,.1f}</b>")
                        if mostrar_max:
                            lineas_texto.append(f"⚡ Max: {row['MAX_CENTRAL']}")
                            
                        # Solo agregamos la anotación si al menos un botón está encendido
                        if lineas_texto:
                            texto_anotacion = "<br>".join(lineas_texto)
                            fig.add_annotation(
                                x=row['FECHA_OPERATIVA'],
                                y=row['TOTAL'],
                                text=texto_anotacion,
                                showarrow=False,
                                yshift=20,
                                font=dict(size=11, color="black"),
                                align="center"
                            )
                
                fig.update_xaxes(dtick="86400000", tickformat="%d/%m/%Y")
                
                st.plotly_chart(fig, use_container_width=True)
                st.markdown("<br>", unsafe_allow_html=True)

            st.markdown("---")
            
            # ==========================================
            # TABLA DE TRAZABILIDAD
            # ==========================================
            st.markdown("### 🗄️ Trazabilidad de Registros Crudos")
            df_mostrar = df_filtrado[['FECHA_OPERATIVA', 'EMPRESA', 'CENTRAL', 'MEDIDOR', 'TIPO_COMBUSTIBLE', 'CONSUMO', 'UNIDAD_MEDIDA']].copy()
            df_mostrar['FECHA_OPERATIVA'] = df_mostrar['FECHA_OPERATIVA'].dt.strftime('%d/%m/%Y')
            st.dataframe(df_mostrar, use_container_width=True, hide_index=True)

else:
    st.info("👈 Configura el rango de fechas en el panel lateral y haz clic en 'Extraer Consumo' para iniciar la fiscalización.")