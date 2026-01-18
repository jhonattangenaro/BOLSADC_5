from flask import Flask, render_template, request, jsonify, send_from_directory
from extractor import descargar_y_guardar, obtener_historico_rapido, precargar_datos_comunes
from datetime import datetime, timedelta
import os
import logging
import math
from datos_manuales import (
    agregar_datos_manuales, 
    obtener_datos_manuales,
    listar_fechas_con_datos_manuales,
    verificar_fecha_con_datos,
    eliminar_datos_manuales
)
from sqlite_manager import sqlite_manager
from query_cache import query_cache

# Configuración de Flask y Logging
app = Flask(__name__, static_folder='static')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Variable para controlar si ya se precargó el caché
_cache_precargado = False

# ========== CONSTANTES PARA LA REEXPRESIÓN MONETARIA ==========
# Fecha de la reexpresión monetaria (27 de julio de 2025)
FECHA_REEXPRESION = datetime(2025, 7, 27).date()
# Factor de conversión (dividir entre 1000)
FACTOR_CONVERSION_REEXPRESION = 1000

# ========== FUNCIONES PARA DÓLAR BCV ==========
def crear_tabla_dolar_bcv():
    """Crea la tabla para datos del dólar BCV si no existe."""
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Crear tabla dolar_bcv si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS dolar_bcv (
            fecha TEXT PRIMARY KEY,
            tasa REAL NOT NULL,
            variacion REAL,
            fuente TEXT DEFAULT 'excel',
            creado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Crear índice para búsquedas rápidas
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_dolar_fecha ON dolar_bcv(fecha)')
    
    conn.commit()
    conn.close()
    logger.info("✅ Tabla dolar_bcv creada/verificada")

def cargar_datos_dolar_bcv_desde_excel():
    """Carga datos del dólar BCV desde el archivo Excel a SQLite."""
    try:
        import pandas as pd
        import sqlite3
        
        # Ruta del archivo Excel
        excel_path = "dolar_bcv.xlsx"
        
        if not os.path.exists(excel_path):
            logger.error(f"❌ Archivo Excel no encontrado: {excel_path}")
            return False, "Archivo Excel no encontrado"
        
        # Leer el archivo Excel
        df = pd.read_excel(excel_path)
        logger.info(f"📊 Leyendo archivo Excel con {len(df)} registros")
        
        # Verificar columnas necesarias
        columnas_necesarias = ['Fecha', 'Tasa', 'Variación']
        if not all(col in df.columns for col in columnas_necesarias):
            logger.error(f"❌ Columnas faltantes en Excel. Esperadas: {columnas_necesarias}")
            return False, "Columnas faltantes en el Excel"
        
        # Crear tabla si no existe
        crear_tabla_dolar_bcv()
        
        # Conectar a SQLite
        db_path = "database/bolsa_datos.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Insertar datos
        registros_insertados = 0
        registros_actualizados = 0
        
        for _, row in df.iterrows():
            fecha = str(int(row['Fecha']))  # Convertir a string
            tasa = float(row['Tasa'])
            variacion = float(row['Variación'])
            
            # Verificar si ya existe
            cursor.execute('SELECT COUNT(*) FROM dolar_bcv WHERE fecha = ?', (fecha,))
            existe = cursor.fetchone()[0] > 0
            
            if existe:
                # Actualizar
                cursor.execute('''
                    UPDATE dolar_bcv 
                    SET tasa = ?, variacion = ?
                    WHERE fecha = ?
                ''', (tasa, variacion, fecha))
                registros_actualizados += 1
            else:
                # Insertar nuevo
                cursor.execute('''
                    INSERT INTO dolar_bcv (fecha, tasa, variacion, fuente)
                    VALUES (?, ?, ?, ?)
                ''', (fecha, tasa, variacion, 'excel'))
                registros_insertados += 1
        
        conn.commit()
        conn.close()
        
        mensaje = f"✅ Datos dólar BCV cargados: {registros_insertados} nuevos, {registros_actualizados} actualizados"
        logger.info(mensaje)
        return True, mensaje
        
    except Exception as e:
        logger.error(f"❌ Error cargando datos dólar BCV: {e}")
        return False, f"Error: {str(e)}"

def obtener_tasa_dolar_bcv(fecha_str):
    """
    Obtiene la tasa del dólar BCV para una fecha específica.
    Si no hay datos para esa fecha, busca la más cercana anterior.
    """
    try:
        import sqlite3
        
        # Convertir fecha al formato YYYYMMDD
        if '-' in fecha_str:
            fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d')
            fecha_formato = fecha_dt.strftime('%Y%m%d')
        elif len(fecha_str) == 8:
            fecha_formato = fecha_str
        else:
            # Asumir que ya está en formato correcto
            fecha_formato = fecha_str
        
        db_path = "database/bolsa_datos.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Buscar la tasa exacta para la fecha
        cursor.execute('''
            SELECT fecha, tasa, variacion 
            FROM dolar_bcv 
            WHERE fecha = ?
        ''', (fecha_formato,))
        
        resultado = cursor.fetchone()
        
        if resultado:
            conn.close()
            return {
                'fecha': resultado[0],
                'tasa': resultado[1],
                'variacion': resultado[2],
                'encontrado_exacto': True
            }
        
        # Si no hay exacto, buscar la tasa más cercana anterior
        cursor.execute('''
            SELECT fecha, tasa, variacion 
            FROM dolar_bcv 
            WHERE fecha <= ?
            ORDER BY fecha DESC
            LIMIT 1
        ''', (fecha_formato,))
        
        resultado = cursor.fetchone()
        
        conn.close()
        
        if resultado:
            return {
                'fecha': resultado[0],
                'tasa': resultado[1],
                'variacion': resultado[2],
                'encontrado_exacto': False
            }
        else:
            # Si no hay datos en absoluto
            return {
                'fecha': fecha_formato,
                'tasa': 0,
                'variacion': 0,
                'encontrado_exacto': False,
                'error': 'No hay datos del dólar BCV'
            }
            
    except Exception as e:
        logger.error(f"Error obteniendo tasa dólar BCV para {fecha_str}: {e}")
        return {
            'fecha': fecha_str,
            'tasa': 0,
            'variacion': 0,
            'error': str(e)
        }

def obtener_datos_dolar_bcv_historico(fecha_desde, fecha_hasta):
    """Obtiene los datos del dólar BCV en un rango de fechas."""
    try:
        import sqlite3
        
        # Convertir fechas a formato YYYYMMDD para SQLite
        if '-' in fecha_desde:
            fecha_desde_sql = fecha_desde.replace('-', '')
        else:
            fecha_desde_sql = fecha_desde
            
        if '-' in fecha_hasta:
            fecha_hasta_sql = fecha_hasta.replace('-', '')
        else:
            fecha_hasta_sql = fecha_hasta
        
        db_path = "database/bolsa_datos.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Obtener datos del dólar BCV en el rango
        cursor.execute('''
            SELECT fecha, tasa, variacion 
            FROM dolar_bcv 
            WHERE fecha BETWEEN ? AND ?
            ORDER BY fecha
        ''', (fecha_desde_sql, fecha_hasta_sql))
        
        datos = cursor.fetchall()
        conn.close()
        
        resultados = []
        for fecha, tasa, variacion in datos:
            resultados.append({
                'fecha': str(fecha),
                'tasa': float(tasa) if tasa else 0,
                'variacion': float(variacion) if variacion else 0,
                'fecha_formateada': f"{fecha[:4]}-{fecha[4:6]}-{fecha[6:]}"
            })
        
        return resultados
        
    except Exception as e:
        logger.error(f"Error obteniendo datos históricos dólar BCV: {e}")
        return []

def calcular_comparacion_dolar_accion(datos_historicos):
    """
    Calcula la comparación entre una acción y el dólar BCV.
    Retorna datos con valores en dólares y diferencias porcentuales.
    """
    if not datos_historicos:
        return []
    
    resultados = []
    
    for dato in datos_historicos:
        fecha_str = dato['fecha']
        precio_bs = dato['precio']
        
        # Obtener tasa del dólar para esta fecha
        tasa_dolar = obtener_tasa_dolar_bcv(fecha_str)
        tasa_valor = tasa_dolar['tasa'] if tasa_dolar['tasa'] > 0 else 1
        
        # Calcular valor en dólares
        precio_usd = precio_bs / tasa_valor if tasa_valor > 0 else 0
        
        # Si hay precio anterior, calcular variación en dólares
        if 'anterior' in dato and dato['anterior'] > 0:
            anterior_bs = dato['anterior']
            
            # Obtener tasa para el día anterior (simplificado: misma tasa)
            # Para mayor precisión, buscaríamos la tasa del día anterior real
            anterior_usd = anterior_bs / tasa_valor if tasa_valor > 0 else 0
            
            # Calcular variación en dólares
            variacion_usd_bs = precio_bs - anterior_bs
            variacion_usd_usd = precio_usd - anterior_usd
            variacion_usd_porcentaje = (variacion_usd_usd / anterior_usd * 100) if anterior_usd > 0 else 0
        else:
            anterior_usd = 0
            variacion_usd_bs = 0
            variacion_usd_usd = 0
            variacion_usd_porcentaje = 0
        
        # Calcular diferencia con variación del dólar BCV
        variacion_dolar_bcv = tasa_dolar.get('variacion', 0) * 100  # Convertir a porcentaje
        variacion_accion = dato.get('variacion', 0)
        
        diferencia_vs_dolar = variacion_accion - variacion_dolar_bcv
        
        resultados.append({
            **dato,
            'tasa_dolar': tasa_valor,
            'precio_usd': precio_usd,
            'anterior_usd': anterior_usd,
            'variacion_usd_porcentaje': round(variacion_usd_porcentaje, 2),
            'variacion_dolar_bcv': round(variacion_dolar_bcv, 2),
            'diferencia_vs_dolar': round(diferencia_vs_dolar, 2),
            'dolar_encontrado_exacto': tasa_dolar.get('encontrado_exacto', False),
            'fecha_dolar': tasa_dolar.get('fecha', fecha_str)
        })
    
    return resultados

def calcular_estadisticas_comparacion_dolar(datos_comparacion):
    """Calcula estadísticas de comparación con el dólar BCV."""
    if not datos_comparacion:
        return {}
    
    # Filtrar días con datos de dólar válidos
    datos_validos = [d for d in datos_comparacion if d['tasa_dolar'] > 0]
    
    if not datos_validos:
        return {}
    
    # Calcular estadísticas
    dias_con_dolar = len(datos_validos)
    dias_sin_dolar = len(datos_comparacion) - dias_con_dolar
    
    # Calcular diferencias promedio
    diferencias = [d['diferencia_vs_dolar'] for d in datos_validos]
    diferencia_promedio = sum(diferencias) / len(diferencias) if diferencias else 0
    
    # Contar días que superaron al dólar
    dias_supera_dolar = sum(1 for d in datos_validos if d['diferencia_vs_dolar'] > 0)
    dias_inferior_dolar = sum(1 for d in datos_validos if d['diferencia_vs_dolar'] < 0)
    dias_igual_dolar = sum(1 for d in datos_validos if d['diferencia_vs_dolar'] == 0)
    
    # Calcular correlación (simplificada)
    variaciones_accion = [d.get('variacion', 0) for d in datos_validos]
    variaciones_dolar = [d.get('variacion_dolar_bcv', 0) for d in datos_validos]
    
    return {
        'dias_con_dolar': dias_con_dolar,
        'dias_sin_dolar': dias_sin_dolar,
        'diferencia_promedio_vs_dolar': round(diferencia_promedio, 2),
        'dias_supera_dolar': dias_supera_dolar,
        'dias_inferior_dolar': dias_inferior_dolar,
        'dias_igual_dolar': dias_igual_dolar,
        'porcentaje_supera_dolar': round((dias_supera_dolar / dias_con_dolar * 100), 1) if dias_con_dolar > 0 else 0,
        'mejor_diferencia': max(diferencias) if diferencias else 0,
        'peor_diferencia': min(diferencias) if diferencias else 0,
        'precio_usd_actual': datos_validos[-1]['precio_usd'] if datos_validos else 0,
        'tasa_dolar_actual': datos_validos[-1]['tasa_dolar'] if datos_validos else 0
    }

# Inicializar tabla dolar_bcv al iniciar
crear_tabla_dolar_bcv()

# ========== RUTA PARA SERVIR LOGOS ==========
@app.route('/logos/<path:filename>')
def serve_logo(filename):
    """Sirve logos desde la carpeta static/img/logos"""
    return send_from_directory('static/img/logos', filename)

def es_fin_de_semana(fecha_dt):
    """Determina si una fecha es fin de semana (sábado o domingo)"""
    return fecha_dt.weekday() >= 5  # Sábado=5, Domingo=6

def obtener_ultimo_dia_habil(fecha_dt):
    """Obtiene el último día hábil (no fin de semana)"""
    fecha_actual = fecha_dt
    
    # Retroceder hasta encontrar un día hábil (máximo 10 días)
    for i in range(10):
        if not es_fin_de_semana(fecha_actual):
            return fecha_actual
        fecha_actual = fecha_actual - timedelta(days=1)
    
    return fecha_dt  # Si no encuentra, devolver la fecha original

def buscar_datos_habiles(fecha_dt):
    """
    Busca hacia atrás (máximo 10 días) hasta encontrar un día con 
    datos de mercado. EVITA FINES DE SEMANA INTELIGENTEMENTE.
    """
    # Si la fecha solicitada es fin de semana, empezar desde el viernes
    if es_fin_de_semana(fecha_dt):
        # Calcular cuántos días retroceder
        if fecha_dt.weekday() == 5:  # Sábado
            fecha_inicio = fecha_dt - timedelta(days=1)
        else:  # Domingo
            fecha_inicio = fecha_dt - timedelta(days=2)
        logger.info(f"🔁 Fecha solicitada es fin de semana. Buscando desde {fecha_inicio.strftime('%Y%m%d')}")
    else:
        fecha_inicio = fecha_dt
    
    # Buscar hasta 10 días hacia atrás
    for i in range(10):
        fecha_actual = fecha_inicio - timedelta(days=i)
        fecha_str = fecha_actual.strftime('%Y%m%d')
        
        # Verificar si es fin de semana (no buscar en sábados/domingos)
        if es_fin_de_semana(fecha_actual):
            continue  # Saltar fines de semana
        
        logger.info(f"Intentando cargar datos para la fecha hábil: {fecha_str}")
        
        acciones, indice = descargar_y_guardar(fecha_str)
        
        if acciones:
            logger.info(f"Datos encontrados para el día hábil: {fecha_str} ({len(acciones)} registros)")
            
            # Determinar si la fecha encontrada es fin de semana
            es_fin_semana_encontrado = es_fin_de_semana(fecha_actual)
            
            return fecha_str, acciones, indice, es_fin_semana_encontrado
    
    # Si no encuentra datos en días hábiles, intentar con cualquier día
    logger.warning("No se encontraron datos en días hábiles, buscando en cualquier día...")
    for i in range(10):
        fecha_str = (fecha_dt - timedelta(days=i)).strftime('%Y%m%d')
        logger.info(f"Intentando cualquier día: {fecha_str}")
        
        acciones, indice = descargar_y_guardar(fecha_str)
        
        if acciones:
            logger.info(f"Datos encontrados para: {fecha_str} ({len(acciones)} registros)")
            es_fin_semana_encontrado = es_fin_de_semana(datetime.strptime(fecha_str, '%Y%m%d'))
            return fecha_str, acciones, indice, es_fin_semana_encontrado
            
    return None, [], None, False

def calcular_tops(acciones):
    """
    Calcula las top 5 ganadoras y top 5 perdedoras del día.
    """
    if not acciones:
        return [], [], [], []
    
    # Filtrar acciones con variación válida
    acciones_con_variacion = [
        acc for acc in acciones 
        if isinstance(acc.get('variacion'), (int, float))
    ]
    
    if not acciones_con_variacion:
        return [], [], [], []
    
    # 1. TOP GANADORAS (por variación)
    ganadoras = sorted(
        [acc for acc in acciones_con_variacion if acc.get('variacion', 0) > 0],
        key=lambda x: x.get('variacion', 0),
        reverse=True
    )[:5]
    
    # 2. TOP PERDEDORAS (por variación)
    perdedoras = sorted(
        [acc for acc in acciones_con_variacion if acc.get('variacion', 0) < 0],
        key=lambda x: x.get('variacion', 0)
    )[:5]
    
    # 3. MÁS NEGOCIADAS (por monto transado)
    mas_negociadas = sorted(
        acciones,
        key=lambda x: x.get('monto', 0),
        reverse=True
    )[:5]
    
    # 4. MENOS NEGOCIADAS (por monto transado)
    menos_negociadas = sorted(
        acciones,
        key=lambda x: x.get('monto', 0)
    )[:5]
    
    # Log para debugging
    if ganadoras:
        logger.info(f"Top ganadora: {ganadoras[0].get('simbolo', 'N/A')} ({ganadoras[0].get('variacion', 0):.2f}%)")
    if perdedoras:
        logger.info(f"Top perdedora: {perdedoras[0].get('simbolo', 'N/A')} ({perdedoras[0].get('variacion', 0):.2f}%)")
    if mas_negociadas:
        logger.info(f"Top negociada: {mas_negociadas[0].get('simbolo', 'N/A')} (Bs. {mas_negociadas[0].get('monto', 0):,.2f})")
    if menos_negociadas:
        logger.info(f"Menos negociada: {menos_negociadas[0].get('simbolo', 'N/A')} (Bs. {menos_negociadas[0].get('monto', 0):,.2f})")
    
    return ganadoras, perdedoras, mas_negociadas, menos_negociadas

def calcular_estadisticas(acciones):
    """
    Calcula estadísticas detalladas de las acciones.
    Retorna: total, en_alza, en_baja, estables, monto_total
    """
    if not acciones:
        return 0, 0, 0, 0, 0
    
    en_alza = sum(1 for acc in acciones if acc.get('variacion', 0) > 0)
    en_baja = sum(1 for acc in acciones if acc.get('variacion', 0) < 0)
    estables = sum(1 for acc in acciones if acc.get('variacion', 0) == 0)
    total = len(acciones)
    monto_total = sum(acc.get('monto', 0) for acc in acciones)
    
    # Verificar que la suma sea correcta
    if (en_alza + en_baja + estables) != total:
        logger.warning(f"Suma de categorías ({en_alza}+{en_baja}+{estables}) no coincide con total ({total})")
    
    return total, en_alza, en_baja, estables, monto_total

def calcular_estadisticas_historicas(datos_historicos):
    """
    Calcula estadísticas para los datos históricos.
    """
    if not datos_historicos:
        return {}
    
    precios = [d['precio'] for d in datos_historicos]
    variaciones = [d['variacion'] for d in datos_historicos]
    
    # Estadísticas básicas
    precio_inicial = datos_historicos[0]['precio'] if datos_historicos else 0
    precio_final = datos_historicos[-1]['precio'] if datos_historicos else 0
    cambio_bs = precio_final - precio_inicial
    rendimiento_porcentaje = (cambio_bs / precio_inicial * 100) if precio_inicial > 0 else 0
    
    # Estadísticas adicionales
    precio_maximo = max(precios) if precios else 0
    precio_minimo = min(precios) if precios else 0
    precio_promedio = sum(precios) / len(precios) if precios else 0
    
    # Calcular volatilidad (desviación estándar de los rendimientos)
    if len(variaciones) > 1:
        mean = sum(variaciones) / len(variaciones)
        variance = sum((x - mean) ** 2 for x in variaciones) / len(variaciones)
        volatilidad = math.sqrt(variance)
    else:
        volatilidad = 0
    
    # Contar días en alza, baja y estables
    dias_alza = sum(1 for v in variaciones if v > 0)
    dias_baja = sum(1 for v in variaciones if v < 0)
    dias_estables = sum(1 for v in variaciones if v == 0)
    total_dias = len(datos_historicos)
    
    # Encontrar mayor ganancia y pérdida diaria
    max_ganancia_diaria = max(variaciones) if variaciones else 0
    max_perdida_diaria = min(variaciones) if variaciones else 0
    
    return {
        'precio_inicial': precio_inicial,
        'precio_final': precio_final,
        'cambio_bs': cambio_bs,
        'rendimiento_porcentaje': rendimiento_porcentaje,
        'precio_maximo': precio_maximo,
        'precio_minimo': precio_minimo,
        'precio_promedio': precio_promedio,
        'volatilidad': volatilidad,
        'dias_alza': dias_alza,
        'dias_baja': dias_baja,
        'dias_estables': dias_estables,
        'total_dias': total_dias,
        'max_ganancia_diaria': max_ganancia_diaria,
        'max_perdida_diaria': abs(max_perdida_diaria),
        'primera_fecha': datos_historicos[0]['fecha_formateada'] if datos_historicos else '',
        'ultima_fecha': datos_historicos[-1]['fecha_formateada'] if datos_historicos else ''
    }

def obtener_nombre_accion(simbolo):
    """Obtiene el nombre real de una acción desde SQLite"""
    if not simbolo:
        return simbolo.upper()
    
    try:
        import sqlite3
        db_path = "database/bolsa_datos.db"
        if not os.path.exists(db_path):
            return simbolo.upper()
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Buscar en acciones automáticas primero
        cursor.execute('''
            SELECT nombre FROM acciones 
            WHERE simbolo = ? 
            LIMIT 1
        ''', (simbolo.upper(),))
        
        resultado = cursor.fetchone()
        if resultado and resultado[0]:
            nombre = resultado[0]
            # Limpiar sufijos
            for sufijo in [' (Manual)', ' (archivo_dat)', ' (automatico)']:
                if sufijo in nombre:
                    nombre = nombre.replace(sufijo, '')
            
            # Verificar que el nombre no sea solo el símbolo
            if nombre.upper() != simbolo.upper() and len(nombre) > 3:
                conn.close()
                return nombre
        
        # Si no se encuentra en acciones automáticas, buscar en datos manuales
        cursor.execute('''
            SELECT nombre FROM datos_manuales 
            WHERE simbolo = ? 
            LIMIT 1
        ''', (simbolo.upper(),))
        
        resultado = cursor.fetchone()
        if resultado and resultado[0]:
            nombre = resultado[0]
            # Limpiar sufijos
            for sufijo in [' (Manual)', ' (archivo_dat)', ' (automatico)']:
                if sufijo in nombre:
                    nombre = nombre.replace(sufijo, '')
            
            if nombre.upper() != simbolo.upper() and len(nombre) > 3:
                conn.close()
                return nombre
        
        conn.close()
        return simbolo.upper()
        
    except Exception as e:
        logger.error(f"Error obteniendo nombre de acción {simbolo}: {e}")
        return simbolo.upper()

# ========== FUNCIÓN PARA AJUSTAR VALORES POR REEXPRESIÓN ==========
def ajustar_valor_por_reexpresion(fecha_str, valor):
    """
    Ajusta el valor del índice según la reexpresión monetaria.
    Para fechas anteriores al 27/07/2025, divide el valor entre 1000.
    
    Args:
        fecha_str: str en formato YYYYMMDD
        valor: float, valor del índice
    
    Returns:
        float: valor ajustado
        bool: True si se aplicó ajuste, False si no
    """
    try:
        # Convertir fecha string a objeto date
        if len(fecha_str) == 8:
            fecha_dt = datetime.strptime(fecha_str, '%Y%m%d').date()
        else:
            # Intentar otros formatos
            try:
                fecha_dt = datetime.strptime(fecha_str, '%Y-%m-%d').date()
            except:
                # Si no se puede parsear, no aplicar ajuste
                return valor, False
        
        # Aplicar ajuste si la fecha es anterior a la reexpresión
        if fecha_dt < FECHA_REEXPRESION:
            valor_ajustado = valor / FACTOR_CONVERSION_REEXPRESION
            logger.info(f"✅ Ajustando valor de {fecha_str}: {valor:,.2f} → {valor_ajustado:,.2f} (÷{FACTOR_CONVERSION_REEXPRESION})")
            return valor_ajustado, True
        else:
            return valor, False
            
    except Exception as e:
        logger.error(f"Error ajustando valor por reexpresión para {fecha_str}: {e}")
        return valor, False

# ========== FUNCIÓN CORREGIDA PARA OBTENER DATOS DEL ÍNDICE ==========
def obtener_datos_indice_historico(fecha_desde, fecha_hasta):
    """Obtiene los datos del índice IBC desde SQLite en un rango de fechas."""
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    if not os.path.exists(db_path):
        logger.warning(f"Base de datos no encontrada: {db_path}")
        return [], {}
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # PRIMERO: Verificar que las fechas estén en el formato correcto
        logger.info(f"Buscando índices del {fecha_desde} al {fecha_hasta}")
        
        # Verificar si la tabla indices_manuales existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='indices_manuales'")
        tabla_manuales_existe = cursor.fetchone() is not None
        
        # CONSULTA MEJORADA: Combinar datos de ambas tablas
        if tabla_manuales_existe:
            query = '''
                SELECT fecha, valor, variacion, 'automatico' as fuente 
                FROM indices 
                WHERE fecha BETWEEN ? AND ?
                UNION ALL
                SELECT fecha, valor, variacion, 'manual' as fuente 
                FROM indices_manuales 
                WHERE fecha BETWEEN ? AND ?
                ORDER BY fecha
            '''
            cursor.execute(query, (fecha_desde, fecha_hasta, fecha_desde, fecha_hasta))
        else:
            query = '''
                SELECT fecha, valor, variacion, 'automatico' as fuente 
                FROM indices 
                WHERE fecha BETWEEN ? AND ?
                ORDER BY fecha
            '''
            cursor.execute(query, (fecha_desde, fecha_hasta))
        
        todos_datos = cursor.fetchall()
        
        if not todos_datos:
            logger.warning("No se encontraron datos del índice en el rango especificado")
            return [], {}
        
        # Procesar datos con ajuste por reexpresión
        resultados = []
        valores_ajustados = 0
        valores_no_ajustados = 0
        
        for fecha, valor, variacion, fuente in todos_datos:
            if fecha:
                try:
                    # Asegurar que el valor sea float
                    valor_float = float(valor) if valor is not None else 0
                    variacion_float = float(variacion) if variacion is not None else 0
                    
                    # Aplicar ajuste por reexpresión monetaria
                    valor_ajustado, se_ajusto = ajustar_valor_por_reexpresion(fecha, valor_float)
                    
                    if se_ajusto:
                        valores_ajustados += 1
                    else:
                        valores_no_ajustados += 1
                    
                    resultados.append({
                        'fecha': str(fecha),
                        'valor': valor_ajustado,
                        'valor_original': valor_float,  # Guardar valor original para referencia
                        'variacion': variacion_float,
                        'fuente': fuente,
                        'ajustado': se_ajusto  # Indicar si se aplicó ajuste
                    })
                except (ValueError, TypeError) as e:
                    logger.warning(f"Error procesando registro {fecha}: {e}")
                    continue
        
        # Eliminar duplicados por fecha (mantener manual sobre automático)
        fechas_vistas = {}
        for dato in resultados:
            fecha = dato['fecha']
            if fecha not in fechas_vistas:
                fechas_vistas[fecha] = dato
            else:
                # Si ya existe, priorizar manual sobre automático
                if dato['fuente'] == 'manual' and fechas_vistas[fecha]['fuente'] == 'automatico':
                    fechas_vistas[fecha] = dato
        
        resultados_unicos = list(fechas_vistas.values())
        
        # Ordenar por fecha (ascendente)
        resultados_unicos.sort(key=lambda x: x['fecha'])
        
        logger.info(f"Total de registros únicos del índice: {len(resultados_unicos)}")
        logger.info(f"Valores ajustados: {valores_ajustados}, No ajustados: {valores_no_ajustados}")
        
        # Calcular estadísticas básicas
        estadisticas = {}
        if resultados_unicos:
            valores_list = [d['valor'] for d in resultados_unicos if d['valor'] > 0]
            
            if valores_list:
                # Convertir fecha de YYYYMMDD a DD/MM/YYYY para mostrar
                fecha_inicio_fmt = ""
                fecha_fin_fmt = ""
                
                try:
                    fecha_inicio = resultados_unicos[0]['fecha']
                    if len(fecha_inicio) == 8:
                        fecha_dt = datetime.strptime(fecha_inicio, '%Y%m%d')
                        fecha_inicio_fmt = fecha_dt.strftime('%d/%m/%Y')
                    else:
                        fecha_inicio_fmt = fecha_inicio
                except:
                    fecha_inicio_fmt = resultados_unicos[0]['fecha']
                
                try:
                    fecha_fin = resultados_unicos[-1]['fecha']
                    if len(fecha_fin) == 8:
                        fecha_dt = datetime.strptime(fecha_fin, '%Y%m%d')
                        fecha_fin_fmt = fecha_dt.strftime('%d/%m/%Y')
                    else:
                        fecha_fin_fmt = fecha_fin
                except:
                    fecha_fin_fmt = resultados_unicos[-1]['fecha']
                
                # Calcular rendimiento histórico
                precio_inicial = valores_list[0]
                precio_final = valores_list[-1]
                variacion_historica = ((precio_final - precio_inicial) / precio_inicial * 100) if precio_inicial > 0 else 0
                
                # Contar fuentes y ajustes
                automaticos = sum(1 for d in resultados_unicos if d['fuente'] == 'automatico')
                manuales = sum(1 for d in resultados_unicos if d['fuente'] == 'manual')
                ajustados = sum(1 for d in resultados_unicos if d.get('ajustado', False))
                
                estadisticas = {
                    'maximo': max(valores_list),
                    'minimo': min(valores_list),
                    'promedio': sum(valores_list) / len(valores_list),
                    'total_datos': len(resultados_unicos),
                    'total_valores_validos': len(valores_list),
                    'fecha_inicio': resultados_unicos[0]['fecha'],
                    'fecha_fin': resultados_unicos[-1]['fecha'],
                    'fecha_inicio_formateada': fecha_inicio_fmt,
                    'fecha_fin_formateada': fecha_fin_fmt,
                    'var_rel_historica': round(variacion_historica, 2),
                    'fuentes': {
                        'automaticos': automaticos,
                        'manuales': manuales
                    },
                    'ajustes': {
                        'ajustados': ajustados,
                        'no_ajustados': len(resultados_unicos) - ajustados,
                        'factor_conversion': FACTOR_CONVERSION_REEXPRESION,
                        'fecha_reexpresion': FECHA_REEXPRESION.strftime('%d/%m/%Y')
                    }
                }
                
                logger.info(f"Estadísticas del índice: {estadisticas}")
        
        return resultados_unicos, estadisticas
        
    except Exception as e:
        logger.error(f"❌ Error en obtener_datos_indice_historico: {e}")
        import traceback
        traceback.print_exc()
        return [], {}
    finally:
        conn.close()

# ========== RUTAS PRINCIPALES CON MANEJO DE FINES DE SEMANA ==========
@app.route('/')
def index():
    """
    Dashboard principal - EVITA AUTOMÁTICAMENTE FINES DE SEMANA
    """
    # 1. Obtener fecha del calendario (si el usuario la seleccionó)
    fecha_seleccionada = request.args.get('fecha')
    
    try:
        if fecha_seleccionada:
            # Convertir formato del input HTML (YYYY-MM-DD) a objeto datetime
            target_date = datetime.strptime(fecha_seleccionada, '%Y-%m-%d')
            
            # Verificar si la fecha seleccionada es fin de semana
            if es_fin_de_semana(target_date):
                logger.info(f"⚠️ Fecha seleccionada es fin de semana: {fecha_seleccionada}")
                # No ajustamos automáticamente, dejamos que el usuario vea si quiere
        else:
            # Si no hay selección, usar el día de hoy
            target_date = datetime.now()
            
            # 🔥 NUEVO: Si es fin de semana, ajustar automáticamente al último día hábil
            if es_fin_de_semana(target_date):
                target_date = obtener_ultimo_dia_habil(target_date)
                logger.info(f"🔁 Es fin de semana. Ajustando automáticamente al último día hábil: {target_date.strftime('%Y-%m-%d')}")
                
    except Exception as e:
        logger.error(f"Error procesando fecha: {e}")
        target_date = datetime.now()

    # 2. Buscar datos (con manejo inteligente de fines de semana)
    resultado = buscar_datos_habiles(target_date)
    fecha_real, acciones, indice, es_fin_semana_encontrado = resultado
    
    # 3. Calcular todos los tops
    top_ganadoras, top_perdedoras, mas_negociadas, menos_negociadas = calcular_tops(acciones)
    
    # 4. Calcular estadísticas completas
    total_acciones, en_alza, en_baja, estables, monto_total = calcular_estadisticas(acciones)
    
    # 5. Preparar formato para el input de fecha en el HTML (YYYY-MM-DD)
    if fecha_real:
        fecha_input_format = datetime.strptime(fecha_real, '%Y%m%d').strftime('%Y-%m-%d')
        fecha_mostrar = datetime.strptime(fecha_real, '%Y%m%d').strftime('%d/%m/%Y')
        
        # Determinar si la fecha mostrada es fin de semana
        fecha_dt = datetime.strptime(fecha_real, '%Y%m%d')
        es_fin_semana = es_fin_de_semana(fecha_dt)
        
        # Log de datos obtenidos
        tipo_dia = "FIN DE SEMANA" if es_fin_semana else "DÍA HÁBIL"
        logger.info(f"Mostrando datos para {fecha_real} ({tipo_dia}): {total_acciones} acciones")
        logger.info(f"Estadísticas: ↑ {en_alza} | ↓ {en_baja} | → {estables}")
        logger.info(f"Top ganadoras: {len(top_ganadoras)}, Top perdedoras: {len(top_perdedoras)}")
        logger.info(f"Más negociadas: {len(mas_negociadas)}, Menos negociadas: {len(menos_negociadas)}")
        logger.info(f"Monto total transado: Bs. {monto_total:,.2f}")
        
        # Si tenemos índices, mostrar información del índice
        if indice and isinstance(indice, dict):
            logger.info(f"Índice general: {indice.get('valor', 0):.2f} ({indice.get('variacion', 0):.2f}%)")
    else:
        fecha_input_format = ""
        fecha_mostrar = "No disponible"
        es_fin_semana = False
        logger.warning("No se encontraron datos para mostrar")
        total_acciones, en_alza, en_baja, estables, monto_total = 0, 0, 0, 0, 0

    return render_template(
        'index.html',
        acciones=acciones,
        indice=indice,
        fecha_mostrar=fecha_mostrar,
        fecha_input=fecha_input_format,
        top_ganadoras=top_ganadoras,
        top_perdedoras=top_perdedoras,
        mas_negociadas=mas_negociadas,
        menos_negociadas=menos_negociadas,
        total_acciones=total_acciones,
        en_alza=en_alza,
        en_baja=en_baja,
        estables=estables,
        monto_total=monto_total,
        es_fin_semana=es_fin_semana
    )


@app.route('/consulta')
def consulta():
    """
    Página de consulta histórica con filtros por fecha.
    USANDO SQLITE PARA MÁXIMA VELOCIDAD
    """
    import time
    inicio = time.time()
    
    # Obtener parámetros del formulario
    simbolo = request.args.get('simbolo', '').upper()
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    pagina = int(request.args.get('pagina', 1))
    
    # Si no hay fechas, usar valores por defecto (último mes)
    hoy = datetime.now()
    if not fecha_desde:
        fecha_desde = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
    if not fecha_hasta:
        fecha_hasta = hoy.strftime('%Y-%m-%d')
    
    # CORREGIDO: NO limitar automáticamente a 365 días
    # Solo validar que las fechas tengan formato correcto
    try:
        fecha_inicio_dt = datetime.strptime(fecha_desde, '%Y-%m-%d')
        fecha_fin_dt = datetime.strptime(fecha_hasta, '%Y-%m-%d')
        dias = (fecha_fin_dt - fecha_inicio_dt).days
        
        # Log para debugging
        logger.info(f"Consulta para {simbolo}: {fecha_desde} a {fecha_hasta} ({dias} días)")
        
        # Solo advertir si el rango es muy grande, pero permitirlo
        if dias > 365:
            logger.warning(f"Rango grande ({dias} días) para {simbolo}, puede tardar en cargar")
        
        # Verificar que la fecha desde no sea mayor que fecha hasta
        if fecha_inicio_dt > fecha_fin_dt:
            # Intercambiar si están invertidas
            fecha_desde, fecha_hasta = fecha_hasta, fecha_desde
            fecha_inicio_dt, fecha_fin_dt = fecha_fin_dt, fecha_inicio_dt
            logger.info(f"Fechas invertidas, corrigiendo: {fecha_desde} a {fecha_hasta}")
            
    except Exception as e:
        logger.error(f"Error procesando fechas: {e}")
        # Usar valores por defecto si hay error
        fecha_desde = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
        fecha_hasta = hoy.strftime('%Y-%m-%d')
    
    # Calcular fechas para los botones de período rápido
    fecha_7d = (hoy - timedelta(days=7)).strftime('%Y-%m-%d')
    fecha_30d = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
    fecha_90d = (hoy - timedelta(days=90)).strftime('%Y-%m-%d')
    fecha_365d = (hoy - timedelta(days=365)).strftime('%Y-%m-%d')
    fecha_hoy = hoy.strftime('%Y-%m-%d')
    
    datos_historicos = []
    estadisticas = {}
    
    # Obtener nombre real de la acción
    accion_nombre = obtener_nombre_accion(simbolo)
    
    # VERIFICAR CACHÉ DE CONSULTAS PRIMERO (para consultas repetidas)
    datos_cacheados = query_cache.get_cached_query(simbolo, fecha_desde, fecha_hasta)
    
    if datos_cacheados and simbolo and fecha_desde and fecha_hasta:
        print(f"⚡ ¡CONSULTA DESDE CACHÉ DE CONSULTAS! {simbolo} ({fecha_desde} a {fecha_hasta})")
        datos_historicos = datos_cacheados
        
        if datos_historicos:
            # Ordenar los datos para estadísticas (más antiguo a más reciente)
            datos_para_estadisticas = sorted(datos_historicos, key=lambda x: x['fecha'])
            estadisticas = calcular_estadisticas_historicas(datos_para_estadisticas)
            
            # NUEVO: Calcular comparación con dólar BCV
            datos_con_dolar = calcular_comparacion_dolar_accion(datos_para_estadisticas)
            estadisticas_dolar = calcular_estadisticas_comparacion_dolar(datos_con_dolar)
            
            # Paginación (50 registros por página)
            registros_por_pagina = 50
            total_paginas = (len(datos_historicos) + registros_por_pagina - 1) // registros_por_pagina
            
            # Limitar a la página actual (los datos ya están en orden descendente)
            inicio_idx = (pagina - 1) * registros_por_pagina
            fin_idx = inicio_idx + registros_por_pagina
            datos_paginados = datos_historicos[inicio_idx:fin_idx]
            
            # NUEVO: Preparar datos con dólar para la página actual
            fechas_paginadas = [d['fecha'] for d in datos_paginados]
            datos_con_dolar_paginados = [d for d in datos_con_dolar if d['fecha'] in fechas_paginadas]
        else:
            datos_paginados = []
            datos_con_dolar_paginados = []
            total_paginas = 1
            estadisticas_dolar = {}
            
    # Si hay un símbolo y fechas, buscar datos históricos (USANDO SQLITE - MUY RÁPIDO)
    elif simbolo and fecha_desde and fecha_hasta:
        logger.info(f"Consultando histórico SQLite para {simbolo} desde {fecha_desde} hasta {fecha_hasta}")
        
        # USAR FUNCIÓN OPTIMIZADA CON SQLITE
        datos_historicos = obtener_historico_rapido(simbolo, fecha_desde, fecha_hasta)
        
        # GUARDAR EN CACHÉ DE CONSULTAS (para consultas repetidas exactamente iguales)
        query_cache.cache_query(simbolo, fecha_desde, fecha_hasta, datos_historicos)
        
        if datos_historicos:
            # Ordenar los datos para estadísticas (más antiguo a más reciente)
            datos_para_estadisticas = sorted(datos_historicos, key=lambda x: x['fecha'])
            estadisticas = calcular_estadisticas_historicas(datos_para_estadisticas)
            
            # NUEVO: Calcular comparación con dólar BCV
            datos_con_dolar = calcular_comparacion_dolar_accion(datos_para_estadisticas)
            estadisticas_dolar = calcular_estadisticas_comparacion_dolar(datos_con_dolar)
            
            # Paginación (50 registros por página)
            registros_por_pagina = 50
            total_paginas = (len(datos_historicos) + registros_por_pagina - 1) // registros_por_pagina
            
            # Limitar a la página actual (los datos ya están en orden descendente)
            inicio_idx = (pagina - 1) * registros_por_pagina
            fin_idx = inicio_idx + registros_por_pagina
            datos_paginados = datos_historicos[inicio_idx:fin_idx]
            
            # NUEVO: Preparar datos con dólar para la página actual
            fechas_paginadas = [d['fecha'] for d in datos_paginados]
            datos_con_dolar_paginados = [d for d in datos_con_dolar if d['fecha'] in fechas_paginadas]
        else:
            datos_paginados = []
            datos_con_dolar_paginados = []
            total_paginas = 1
            estadisticas_dolar = {}
    else:
        datos_paginados = []
        datos_con_dolar_paginados = []
        total_paginas = 1
        estadisticas_dolar = {}
    
    # Preparar datos para el gráfico (usar todos los datos, pero en orden cronológico para el gráfico)
    labels = []
    valores = []
    cambios = []
    
    # **NUEVO: Calcular fechas_con_datos**
    fechas_con_datos = len(datos_historicos) if datos_historicos else 0
    
    if datos_historicos:
        # Ordenar los datos para el gráfico (más antiguo a más reciente)
        datos_para_grafico = sorted(datos_historicos, key=lambda x: x['fecha'])
        
        # Ajustar el paso según la cantidad de datos para el gráfico
        # Más datos = más espaciado para no sobrecargar el gráfico
        if len(datos_para_grafico) > 500:
            paso = len(datos_para_grafico) // 200  # Máximo 200 puntos
        elif len(datos_para_grafico) > 200:
            paso = len(datos_para_grafico) // 100  # Máximo 100 puntos
        elif len(datos_para_grafico) > 100:
            paso = len(datos_para_grafico) // 50   # Máximo 50 puntos
        else:
            paso = 1
        
        paso = max(1, paso)  # Asegurar que paso sea al menos 1
        
        datos_grafico = datos_para_grafico[::paso]
        
        labels = [d['fecha_formateada'] for d in datos_grafico]
        valores = [d['precio'] for d in datos_grafico]
        cambios = [d['variacion'] for d in datos_grafico]
    
    # Formatear fechas para mostrar
    try:
        fecha_desde_dt = datetime.strptime(fecha_desde, '%Y-%m-%d')
        fecha_hasta_dt = datetime.strptime(fecha_hasta, '%Y-%m-%d')
        fecha_desde_formato = fecha_desde_dt.strftime('%d/%m/%Y')
        fecha_hasta_formato = fecha_hasta_dt.strftime('%d/%m/%Y')
    except:
        fecha_desde_formato = fecha_desde
        fecha_hasta_formato = fecha_hasta
    
    # Medir tiempo de procesamiento
    tiempo_procesamiento = time.time() - inicio
    
    # Log de velocidad
    fuente = "CACHÉ_QUERY" if datos_cacheados else "SQLITE"
    logger.info(f"⏱️  Tiempo {fuente}: {tiempo_procesamiento:.3f}s para {simbolo} ({fechas_con_datos} registros)")
    
    return render_template(
        'consulta.html',
        simbolo=simbolo,
        accion_nombre=accion_nombre,
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        fecha_desde_formato=fecha_desde_formato,
        fecha_hasta_formato=fecha_hasta_formato,
        fecha_7d=fecha_7d,
        fecha_30d=fecha_30d,
        fecha_90d=fecha_90d,
        fecha_365d=fecha_365d,
        fecha_hoy=fecha_hoy,
        datos_historicos=datos_paginados,
        datos_con_dolar=datos_con_dolar_paginados,
        estadisticas=estadisticas,
        estadisticas_dolar=estadisticas_dolar,
        labels=labels,
        valores=valores,
        cambios=cambios,
        pagina=pagina,
        total_paginas=total_paginas,
        fechas_con_datos=fechas_con_datos,
        # Pasar estadísticas individualmente para fácil acceso en la plantilla
        precio_inicial=estadisticas.get('precio_inicial', 0),
        precio_final=estadisticas.get('precio_final', 0),
        cambio_bs=estadisticas.get('cambio_bs', 0),
        rendimiento_porcentaje=estadisticas.get('rendimiento_porcentaje', 0),
        precio_maximo=estadisticas.get('precio_maximo', 0),
        precio_minimo=estadisticas.get('precio_minimo', 0),
        precio_promedio=estadisticas.get('precio_promedio', 0),
        volatilidad=estadisticas.get('volatilidad', 0),
        dias_alza=estadisticas.get('dias_alza', 0),
        dias_baja=estadisticas.get('dias_baja', 0),
        dias_estables=estadisticas.get('dias_estables', 0),
        total_dias=estadisticas.get('total_dias', 0),
        max_ganancia_diaria=estadisticas.get('max_ganancia_diaria', 0),
        max_perdida_diaria=estadisticas.get('max_perdida_diaria', 0),
        primera_fecha=estadisticas.get('primera_fecha', ''),
        ultima_fecha=estadisticas.get('ultima_fecha', '')
    )

# ========== RUTAS DE ADMINISTRACIÓN ==========
@app.route('/admin/ingreso-manual')
def ingreso_manual():
    """Página para ingreso manual de datos"""
    fecha_seleccionada = request.args.get('fecha')
    estado_datos = None
    datos_existentes = None
    mostrar_formulario = False
    
    if fecha_seleccionada:
        try:
            # Convertir fecha de YYYY-MM-DD a YYYYMMDD
            fecha_dt = datetime.strptime(fecha_seleccionada, '%Y-%m-%d')
            fecha_vvc = fecha_dt.strftime('%Y%m%d')
            
            # Verificar estado de datos
            estado_datos = verificar_fecha_con_datos(fecha_vvc)
            
            # Obtener datos existentes si hay manuales
            datos_manual, _ = obtener_datos_manuales(fecha_vvc)
            if datos_manual:
                datos_existentes = datos_manual
                mostrar_formulario = True
            else:
                mostrar_formulario = True
                
        except Exception as e:
            logger.error(f"Error procesando fecha en ingreso manual: {e}")
    
    fechas_manuales = listar_fechas_con_datos_manuales()
    
    return render_template(
        'ingreso_manual.html',
        fecha_seleccionada=fecha_seleccionada,
        fecha_formateada=fecha_seleccionada,
        estado_datos=estado_datos,
        datos_existentes=datos_existentes,
        mostrar_formulario=mostrar_formulario,
        fechas_manuales=fechas_manuales
    )

@app.route('/admin/guardar-manual', methods=['POST'])
def guardar_datos_manuales():
    """Guardar datos ingresados manualmente - CON NOMBRES REALES AUTOMÁTICOS Y SQLITE"""
    try:
        fecha = request.form.get('fecha')
        fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
        fecha_vvc = fecha_dt.strftime('%Y%m%d')
        
        # Procesar datos del índice
        indice_valor = float(request.form.get('indice_valor', 0))
        indice_variacion = float(request.form.get('indice_variacion', 0))
        
        indice_data = {
            'valor': indice_valor,
            'variacion': indice_variacion
        }
        
        # Procesar datos de acciones
        simbolos = request.form.getlist('simbolo[]')
        anteriores = request.form.getlist('anterior[]')
        hoy_list = request.form.getlist('hoy[]')
        cantidades = request.form.getlist('cantidad[]')
        montos = request.form.getlist('monto[]')
        
        acciones_data = []
        
        for i in range(len(simbolos)):
            if simbolos[i].strip():  # Solo si hay símbolo
                simbolo = simbolos[i].strip().upper()
                anterior = float(anteriores[i])
                hoy = float(hoy_list[i])
                diferencia = hoy - anterior
                variacion = (diferencia / anterior * 100) if anterior > 0 else 0
                
                # Usar el símbolo como nombre temporal - se corregirá automáticamente
                acciones_data.append({
                    'simbolo': simbolo,
                    'nombre': simbolo,  # Nombre temporal, se corregirá automáticamente
                    'anterior': anterior,
                    'hoy': hoy,
                    'diferencia_bs': round(diferencia, 4),
                    'variacion': round(variacion, 2),
                    'cantidad': int(cantidades[i]),
                    'monto': float(montos[i])
                })
        
        # Guardar en SQLite - LOS NOMBRES SE CORREGIRÁN AUTOMÁTICAMENTE
        success = agregar_datos_manuales(fecha_vvc, acciones_data, indice_data)
        
        if success:
            logger.info(f"Datos manuales guardados en SQLite para {fecha_vvc}: {len(acciones_data)} acciones")
            
            # Mostrar ejemplos de nombres corregidos
            if acciones_data:
                from datos_manuales import obtener_nombre_real_accion
                ejemplo = acciones_data[0]['simbolo']
                nombre_real = obtener_nombre_real_accion(ejemplo)
                logger.info(f"Ejemplo corregido: {ejemplo} → {nombre_real}")
            
            return render_template('exito.html', 
                                 mensaje=f"Datos manuales guardados exitosamente para {fecha}",
                                 detalles=f"{len(acciones_data)} acciones registradas en SQLite (nombres corregidos automáticamente)",
                                 volver_url="/admin/ingreso-manual")
        else:
            return render_template('error.html',
                                 mensaje="Error al guardar datos manuales",
                                 detalles="Inténtalo de nuevo",
                                 volver_url="/admin/ingreso-manual")
                                 
    except Exception as e:
        logger.error(f"Error guardando datos manuales: {e}")
        return render_template('error.html',
                             mensaje="Error del servidor",
                             detalles=str(e),
                             volver_url="/admin/ingreso-manual")

@app.route('/admin/eliminar-manual/<fecha>')
def eliminar_manual(fecha):
    """Eliminar datos manuales para una fecha"""
    try:
        # Convertir fecha si es necesario
        if '-' in fecha:
            fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
            fecha_vvc = fecha_dt.strftime('%Y%m%d')
        else:
            fecha_vvc = fecha
        
        success = eliminar_datos_manuales(fecha_vvc)
        
        if success:
            return render_template('exito.html',
                                 mensaje=f"Datos manuales eliminados de SQLite para {fecha_vvc}",
                                 detalles="Los datos automáticos se usarán si están disponibles",
                                 volver_url="/admin/ingreso-manual")
        else:
            return render_template('error.html',
                                 mensaje="No se encontraron datos manuales para eliminar",
                                 detalles=f"Fecha: {fecha_vvc}",
                                 volver_url="/admin/ingreso-manual")
                                 
    except Exception as e:
        logger.error(f"Error eliminando datos manuales: {e}")
        return render_template('error.html',
                             mensaje="Error al eliminar datos",
                             detalles=str(e),
                             volver_url="/admin/ingreso-manual")

# ========== RUTAS PARA DÓLAR BCV ==========
@app.route('/admin/dolar-bcv')
def admin_dolar_bcv():
    """Página de administración del dólar BCV."""
    return render_template('admin_dolar_bcv.html')

@app.route('/admin/cargar-dolar-bcv', methods=['POST'])
def cargar_dolar_bcv():
    """Endpoint para cargar datos del dólar BCV."""
    try:
        success, mensaje = cargar_datos_dolar_bcv_desde_excel()
        
        if success:
            return jsonify({
                'success': True,
                'message': mensaje
            })
        else:
            return jsonify({
                'success': False,
                'message': mensaje
            }), 400
            
    except Exception as e:
        logger.error(f"Error cargando dólar BCV: {e}")
        return jsonify({
            'success': False,
            'message': f'Error del servidor: {str(e)}'
        }), 500

@app.route('/api/dolar-bcv/estado')
def api_dolar_bcv_estado():
    """API para verificar estado de datos del dólar BCV."""
    try:
        import sqlite3
        
        db_path = "database/bolsa_datos.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Verificar si la tabla existe
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dolar_bcv'")
        tabla_existe = cursor.fetchone() is not None
        
        if not tabla_existe:
            conn.close()
            return jsonify({
                'success': False,
                'message': 'Tabla dolar_bcv no existe',
                'data': None
            })
        
        # Obtener estadísticas
        cursor.execute('SELECT COUNT(*) FROM dolar_bcv')
        total_registros = cursor.fetchone()[0]
        
        cursor.execute('SELECT MIN(fecha), MAX(fecha) FROM dolar_bcv')
        min_max = cursor.fetchone()
        fecha_min = min_max[0] if min_max else None
        fecha_max = min_max[1] if min_max else None
        
        # Obtener última tasa
        cursor.execute('SELECT tasa, fecha FROM dolar_bcv ORDER BY fecha DESC LIMIT 1')
        ultimo = cursor.fetchone()
        ultima_tasa = ultimo[0] if ultimo else 0
        ultima_fecha = ultimo[1] if ultimo else None
        
        conn.close()
        
        return jsonify({
            'success': True,
            'message': 'Datos dólar BCV disponibles',
            'data': {
                'tabla_existe': True,
                'total_registros': total_registros,
                'fecha_min': fecha_min,
                'fecha_max': fecha_max,
                'ultima_tasa': ultima_tasa,
                'ultima_fecha': ultima_fecha,
                'rango_dias': (datetime.strptime(fecha_max, '%Y%m%d') - datetime.strptime(fecha_min, '%Y%m%d')).days if fecha_min and fecha_max else 0
            }
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo estado dólar BCV: {e}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'data': None
        }), 500

@app.route('/api/dolar-bcv/<fecha>')
def api_dolar_bcv_fecha(fecha):
    """API para obtener tasa del dólar BCV por fecha."""
    try:
        datos = obtener_tasa_dolar_bcv(fecha)
        
        return jsonify({
            'success': True,
            'message': 'Tasa dólar BCV obtenida',
            'data': datos
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo dólar BCV para {fecha}: {e}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'data': None
        }), 500

# ========== RUTAS DE CACHÉ Y SQLITE ==========
@app.route('/admin/cache-status')
def cache_status():
    """Muestra el estado del caché y SQLite."""
    sqlite_stats = sqlite_manager.estadisticas()
    query_stats = query_cache.get_cache_stats()
    
    return jsonify({
        'success': True,
        'sqlite_status': {
            'total_acciones': sqlite_stats['total_acciones'],
            'total_indices': sqlite_stats['total_indices'],
            'total_manuales': sqlite_stats['total_manuales'],
            'fechas_unicas': sqlite_stats['fechas_unicas'],
            'fechas_en_cache': sqlite_stats['fechas_en_cache'],
            'db_size_mb': f"{sqlite_stats['db_size_mb']:.2f}",
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        },
        'query_cache': {
            'consultas_cacheadas': query_stats['total_queries_cached'],
            'cache_hits': query_stats['cache_hits'],
            'cache_misses': query_stats['cache_misses'],
            'hit_rate': f"{query_stats['hit_rate']*100:.1f}%",
            'most_frequent': query_stats['most_frequent'],
            'recent_queries': query_stats['recent_queries']
        }
    })

@app.route('/admin/clear-cache')
def clear_cache():
    """Limpia el caché en memoria."""
    sqlite_manager.limpiar_cache()
    query_cache.clear_query_cache()
    
    return render_template('exito.html',
                         mensaje="Caché limpiado exitosamente",
                         detalles="Se limpiaron ambos cachés: SQLite y consultas",
                         volver_url="/")

@app.route('/admin/clear-query-cache')
def clear_query_cache():
    """Limpia solo el caché de consultas."""
    query_cache.clear_query_cache()
    
    return render_template('exito.html',
                         mensaje="Caché de consultas limpiado",
                         detalles="Las consultas se procesarán nuevamente la próxima vez",
                         volver_url="/admin/cache-status")

# ========== RUTAS ESPECIALES PARA CORREGIR NOMBRES ==========
@app.route('/admin/corregir-nombres')
def corregir_nombres():
    """Corrige automáticamente los nombres de acciones manuales en SQLite"""
    try:
        from datos_manuales import corregir_nombres_manuales
        corregidas = corregir_nombres_manuales()
        
        return render_template('exito.html',
                             mensaje=f"Nombres de acciones corregidos en SQLite",
                             detalles=f"{corregidas} registros actualizados automáticamente",
                             volver_url="/admin/ingreso-manual")
    except Exception as e:
        logger.error(f"Error corrigiendo nombres: {e}")
        return render_template('error.html',
                             mensaje="Error corrigiendo nombres",
                             detalles=str(e),
                             volver_url="/admin/ingreso-manual")

@app.route('/admin/optimizar-sqlite')
def optimizar_sqlite():
    """Optimiza la base de datos SQLite"""
    try:
        import sqlite3
        db_path = "database/bolsa_datos.db"
        
        if not os.path.exists(db_path):
            return render_template('error.html',
                                 mensaje="Base de datos SQLite no encontrada",
                                 detalles=f"Archivo no existe: {db_path}",
                                 volver_url="/admin/cache-status")
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Ejecutar optimizaciones
        cursor.execute("VACUUM")
        cursor.execute("PRAGMA optimize")
        cursor.execute("PRAGMA analysis_limit=400")
        cursor.execute("ANALYZE")
        
        conn.commit()
        conn.close()
        
        return render_template('exito.html',
                             mensaje="Base de datos SQLite optimizada",
                             detalles="Se ejecutaron VACUUM, PRAGMA optimize y ANALYZE",
                             volver_url="/admin/cache-status")
    except Exception as e:
        logger.error(f"Error optimizando SQLite: {e}")
        return render_template('error.html',
                             mensaje="Error optimizando SQLite",
                             detalles=str(e),
                             volver_url="/admin/cache-status")

# ========== API ENDPOINTS ==========
@app.route('/api/datos/<fecha>')
def api_datos(fecha):
    """
    Endpoint API para obtener datos en formato JSON.
    Fecha formato: YYYYMMDD
    """
    try:
        acciones, indice = descargar_y_guardar(fecha)
        
        if not acciones:
            return jsonify({
                'success': False,
                'message': f'No se encontraron datos para la fecha {fecha}',
                'data': None
            }), 404
        
        # Calcular todos los tops y estadísticas
        top_ganadoras, top_perdedoras, mas_negociadas, menos_negociadas = calcular_tops(acciones)
        total_acciones, en_alza, en_baja, estables, monto_total = calcular_estadisticas(acciones)
        
        return jsonify({
            'success': True,
            'message': 'Datos obtenidos correctamente desde SQLite',
            'fecha': fecha,
            'estadisticas': {
                'total_acciones': total_acciones,
                'en_alza': en_alza,
                'en_baja': en_baja,
                'estables': estables,
                'monto_total': monto_total
            },
            'top_ganadoras': [
                {
                    'simbolo': acc.get('simbolo'),
                    'nombre': acc.get('nombre'),
                    'variacion': acc.get('variacion'),
                    'precio': acc.get('hoy'),
                    'diferencia': acc.get('diferencia_bs'),
                    'monto': acc.get('monto')
                }
                for acc in top_ganadoras
            ],
            'top_perdedoras': [
                {
                    'simbolo': acc.get('simbolo'),
                    'nombre': acc.get('nombre'),
                    'variacion': acc.get('variacion'),
                    'precio': acc.get('hoy'),
                    'diferencia': acc.get('diferencia_bs'),
                    'monto': acc.get('monto')
                }
                for acc in top_perdedoras
            ],
            'mas_negociadas': [
                {
                    'simbolo': acc.get('simbolo'),
                    'nombre': acc.get('nombre'),
                    'monto': acc.get('monto'),
                    'precio': acc.get('hoy'),
                    'cantidad': acc.get('cantidad')
                }
                for acc in mas_negociadas
            ],
            'menos_negociadas': [
                {
                    'simbolo': acc.get('simbolo'),
                    'nombre': acc.get('nombre'),
                    'monto': acc.get('monto'),
                    'precio': acc.get('hoy'),
                    'cantidad': acc.get('cantidad')
                }
                for acc in menos_negociadas
            ],
            'indice': indice,
            'acciones': acciones
        })
        
    except Exception as e:
        logger.error(f"Error en API para fecha {fecha}: {e}")
        return jsonify({
            'success': False,
            'message': f'Error del servidor: {str(e)}',
            'data': None
        }), 500

@app.route('/api/historico')
def api_historico():
    """
    API para obtener datos históricos de una acción.
    Parámetros: simbolo, fecha_desde, fecha_hasta
    """
    simbolo = request.args.get('simbolo', '').upper()
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    
    if not simbolo or not fecha_desde or not fecha_hasta:
        return jsonify({
            'success': False,
            'message': 'Parámetros requeridos: simbolo, fecha_desde, fecha_hasta',
            'data': None
        }), 400
    
    try:
        # Verificar caché de consultas primero
        datos_cacheados = query_cache.get_cached_query(simbolo, fecha_desde, fecha_hasta)
        
        if datos_cacheados:
            print(f"⚡ API desde caché de consultas: {simbolo}")
            datos_historicos = datos_cacheados
        else:
            # Usar SQLite para consulta ultra rápida
            datos_historicos = obtener_historico_rapido(simbolo, fecha_desde, fecha_hasta)
            # Guardar en caché para próximas consultas
            query_cache.cache_query(simbolo, fecha_desde, fecha_hasta, datos_historicos)
        
        estadisticas = calcular_estadisticas_historicas(datos_historicos)
        
        return jsonify({
            'success': True,
            'message': f'Datos históricos obtenidos para {simbolo} desde SQLite',
            'simbolo': simbolo,
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta,
            'from_cache': datos_cacheados is not None,
            'estadisticas': estadisticas,
            'registros': len(datos_historicos),
            'datos': datos_historicos
        })
        
    except Exception as e:
        logger.error(f"Error en API histórica: {e}")
        return jsonify({
            'success': False,
            'message': f'Error del servidor: {str(e)}',
            'data': None
        }), 500

# ========== NUEVA API PARA HISTORICO COMPLETO ==========
@app.route('/api/historico-completo')
def api_historico_completo():
    """API para obtener el primer y último registro de una acción"""
    simbolo = request.args.get('simbolo', '').upper()
    
    if not simbolo:
        return jsonify({'success': False, 'message': 'Parámetro simbolo requerido'})
    
    try:
        import sqlite3
        
        db_path = "database/bolsa_datos.db"
        if not os.path.exists(db_path):
            return jsonify({'success': False, 'message': 'Base de datos no encontrada'})
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Obtener la fecha más antigua para esta acción
        cursor.execute('''
            SELECT MIN(fecha) as primera_fecha 
            FROM ( 
                SELECT fecha FROM acciones WHERE simbolo = ?
                UNION
                SELECT fecha FROM datos_manuales WHERE simbolo = ?
            )
        ''', (simbolo, simbolo))
        
        primera_fecha = cursor.fetchone()[0]
        
        # Obtener la fecha más reciente (hoy o la última disponible)
        cursor.execute('''
            SELECT MAX(fecha) as ultima_fecha 
            FROM ( 
                SELECT fecha FROM acciones WHERE simbolo = ?
                UNION
                SELECT fecha FROM datos_manuales WHERE simbolo = ?
            )
        ''', (simbolo, simbolo))
        
        ultima_fecha = cursor.fetchone()[0]
        
        conn.close()
        
        if primera_fecha and ultima_fecha:
            # Convertir fechas de YYYYMMDD a YYYY-MM-DD
            primera_fecha_fmt = f"{primera_fecha[:4]}-{primera_fecha[4:6]}-{primera_fecha[6:]}"
            ultima_fecha_fmt = f"{ultima_fecha[:4]}-{ultima_fecha[4:6]}-{ultima_fecha[6:]}"
            
            # Obtener nombre de la acción
            nombre_accion = obtener_nombre_accion(simbolo)
            
            return jsonify({
                'success': True,
                'message': f'Histórico completo para {simbolo}',
                'simbolo': simbolo,
                'nombre': nombre_accion,
                'fecha_desde': primera_fecha_fmt,
                'fecha_hasta': ultima_fecha_fmt,
                'primera_fecha': primera_fecha,
                'ultima_fecha': ultima_fecha
            })
        else:
            return jsonify({'success': False, 'message': f'No se encontraron datos para {simbolo}'})
            
    except Exception as e:
        logger.error(f"Error obteniendo histórico completo: {e}")
        return jsonify({'success': False, 'message': f'Error del servidor: {str(e)}'}), 500

# ========== NUEVA API PARA OBTENER ACCIONES ACTIVAS ==========
@app.route('/api/acciones-activas')
def api_acciones_activas():
    """
    API para obtener acciones que han tenido actividad en los últimos 30 días.
    """
    try:
        import sqlite3
        
        db_path = "database/bolsa_datos.db"
        if not os.path.exists(db_path):
            return jsonify({'success': False, 'message': 'Base de datos no encontrada', 'acciones': []})
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Calcular fecha hace 30 días
        hoy = datetime.now()
        fecha_limite = (hoy - timedelta(days=30)).strftime('%Y%m%d')
        
        # Obtener acciones únicas que han tenido actividad en los últimos 30 días
        cursor.execute('''
            SELECT DISTINCT a.simbolo, a.nombre
            FROM acciones a
            WHERE a.fecha >= ?
            UNION
            SELECT DISTINCT dm.simbolo, dm.nombre
            FROM datos_manuales dm
            WHERE dm.fecha >= ?
            ORDER BY simbolo
        ''', (fecha_limite, fecha_limite))
        
        acciones = []
        simbolos_unicos = set()
        
        for simbolo, nombre in cursor.fetchall():
            if simbolo and simbolo not in simbolos_unicos:
                simbolos_unicos.add(simbolo)
                
                # Limpiar el nombre
                nombre_limpio = str(nombre)
                for sufijo in [' (Manual)', ' (archivo_dat)', ' (automatico)']:
                    if sufijo in nombre_limpio:
                        nombre_limpio = nombre_limpio.replace(sufijo, '')
                
                # Obtener estadísticas recientes de esta acción
                cursor.execute('''
                    SELECT COUNT(*) as dias, 
                           AVG(variacion) as variacion_promedio,
                           MAX(hoy) as precio_max,
                           MIN(hoy) as precio_min
                    FROM (
                        SELECT hoy, variacion FROM acciones 
                        WHERE simbolo = ? AND fecha >= ?
                        UNION ALL
                        SELECT hoy, variacion FROM datos_manuales 
                        WHERE simbolo = ? AND fecha >= ?
                    )
                ''', (simbolo, fecha_limite, simbolo, fecha_limite))
                
                stats = cursor.fetchone()
                dias_activos = stats[0] if stats else 0
                variacion_prom = stats[1] if stats and stats[1] else 0
                precio_max = stats[2] if stats and stats[2] else 0
                precio_min = stats[3] if stats and stats[3] else 0
                
                # Determinar tendencia
                if variacion_prom > 1:
                    tendencia = 'alza'
                    icono = '📈'
                elif variacion_prom < -1:
                    tendencia = 'baja'
                    icono = '📉'
                else:
                    tendencia = 'estable'
                    icono = '⚖️'
                
                acciones.append({
                    'simbolo': simbolo.upper(),
                    'nombre': nombre_limpio if nombre_limpio and nombre_limpio != simbolo else simbolo.upper(),
                    'dias_activos': dias_activos,
                    'variacion_promedio': round(variacion_prom, 2),
                    'precio_max': precio_max,
                    'precio_min': precio_min,
                    'tendencia': tendencia,
                    'icono': icono
                })
        
        conn.close()
        
        # Ordenar por días activos (más activas primero)
        acciones.sort(key=lambda x: x['dias_activos'], reverse=True)
        
        return jsonify({
            'success': True,
            'message': f'Encontradas {len(acciones)} acciones activas en los últimos 30 días',
            'fecha_limite': fecha_limite,
            'acciones': acciones
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo acciones activas: {e}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'acciones': []
        }), 500

# También mantén la API anterior para compatibilidad
@app.route('/api/acciones-disponibles')
def api_acciones_disponibles():
    """
    API para obtener todas las acciones disponibles en la base de datos.
    """
    try:
        import sqlite3
        
        db_path = "database/bolsa_datos.db"
        if not os.path.exists(db_path):
            return jsonify({'success': False, 'message': 'Base de datos no encontrada', 'acciones': []})
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Obtener acciones únicas con sus nombres
        cursor.execute('''
            SELECT DISTINCT simbolo, nombre 
            FROM (
                SELECT simbolo, nombre FROM acciones 
                UNION 
                SELECT simbolo, nombre FROM datos_manuales
            ) 
            WHERE simbolo IS NOT NULL AND simbolo != ''
            ORDER BY simbolo
        ''')
        
        acciones = []
        for simbolo, nombre in cursor.fetchall():
            # Limpiar el nombre
            nombre_limpio = str(nombre)
            for sufijo in [' (Manual)', ' (archivo_dat)', ' (automatico)']:
                if sufijo in nombre_limpio:
                    nombre_limpio = nombre_limpio.replace(sufijo, '')
            
            acciones.append({
                'simbolo': simbolo.upper(),
                'nombre': nombre_limpio if nombre_limpio and nombre_limpio != simbolo else simbolo.upper()
            })
        
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'Encontradas {len(acciones)} acciones únicas',
            'acciones': acciones
        })
        
    except Exception as e:
        logger.error(f"Error obteniendo acciones disponibles: {e}")
        return jsonify({
            'success': False,
            'message': f'Error: {str(e)}',
            'acciones': []
        }), 500

@app.route('/api/status')
def api_status():
    """
    Endpoint para verificar el estado del servidor.
    """
    sqlite_stats = sqlite_manager.estadisticas()
    query_stats = query_cache.get_cache_stats()
    
    return jsonify({
        'success': True,
        'message': 'Servidor BVC funcionando correctamente con SQLite',
        'status': {
            'servidor': 'online',
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'version': '3.6.0',  # Nueva versión con comparativa IBC vs $BCV
            'database': {
                'engine': 'SQLite',
                'total_acciones': sqlite_stats['total_acciones'],
                'fechas_unicas': sqlite_stats['fechas_unicas'],
                'size_mb': f"{sqlite_stats['db_size_mb']:.2f}",
                'optimized': True
            },
            'cache': {
                'sqlite_cache_hits': sqlite_stats['fechas_en_cache'],
                'query_cache_hits': query_stats['cache_hits'],
                'query_cache_hit_rate': f"{query_stats['hit_rate']*100:.1f}%"
            },
            'caracteristicas': [
                'Dashboard con Índice IBC',
                'Top Ganadoras/Perdedoras',
                'Top Más/Menos Negociadas',
                'Estadísticas completas',
                'Consulta histórica ULTRA RÁPIDO con SQLite',
                'Caché de consultas en memoria',
                'Carga desde archivos .dat locales',
                'Sistema de datos manuales',
                'API REST para integraciones',
                'Nombres reales automáticos para datos manuales',
                'Base de datos SQLite optimizada',
                'Manejo automático de fines de semana',
                'Autocomplete dinámico de acciones',
                'Listado de acciones activas (últimos 30 días)',
                'Histórico completo de acciones con un solo clic',
                '✅ AJUSTE POR REEXPRESIÓN MONETARIA (antes del 27/07/2025: ÷1000)',
                '✅ SISTEMA DÓLAR BCV INTEGRADO',
                '✅ COMPARATIVA IBC vs DÓLAR BCV EN GRÁFICO DUAL'
            ]
        }
    })

# ========== RUTA PARA DIAGNÓSTICO DE TABLAS ==========
@app.route('/admin/diagnostico-tablas')
def diagnostico_tablas():
    """Diagnóstico de tablas en la base de datos"""
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    if not os.path.exists(db_path):
        return jsonify({'error': 'Base de datos no encontrada'})
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Obtener todas las tablas
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tablas = cursor.fetchall()
    
    resultado = {}
    for tabla in tablas:
        nombre_tabla = tabla[0]
        # Obtener estructura
        cursor.execute(f"PRAGMA table_info({nombre_tabla})")
        columnas = cursor.fetchall()
        # Obtener conteo de registros
        cursor.execute(f"SELECT COUNT(*) FROM {nombre_tabla}")
        count = cursor.fetchone()[0]
        
        resultado[nombre_tabla] = {
            'columnas': [col[1] for col in columnas],  # Nombre de columnas
            'tipos': [col[2] for col in columnas],     # Tipo de datos
            'registros': count
        }
    
    conn.close()
    
    return jsonify({
        'base_datos': db_path,
        'tablas': resultado,
        'total_tablas': len(tablas)
    })

# ========== ENDPOINT DE DEPURACIÓN PARA ÍNDICES ==========
@app.route('/debug/indices/<fecha_desde>/<fecha_hasta>')
def debug_indices(fecha_desde, fecha_hasta):
    """Endpoint para depurar datos del índice"""
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Convertir fechas al formato SQLite (YYYYMMDD)
    fecha_desde_sql = fecha_desde.replace('-', '')
    fecha_hasta_sql = fecha_hasta.replace('-', '')
    
    logger.info(f"Depurando índices del {fecha_desde_sql} al {fecha_hasta_sql}")
    
    # Obtener datos de la tabla indices
    cursor.execute('''
        SELECT fecha, valor, variacion, fuente 
        FROM indices 
        WHERE fecha BETWEEN ? AND ? 
        ORDER BY fecha
    ''', (fecha_desde_sql, fecha_hasta_sql))
    
    datos = cursor.fetchall()
    
    # También obtener de índices manuales
    cursor.execute('''
        SELECT fecha, valor, variacion, fuente 
        FROM indices_manuales 
        WHERE fecha BETWEEN ? AND ? 
        ORDER BY fecha
    ''', (fecha_desde_sql, fecha_hasta_sql))
    
    datos_manuales = cursor.fetchall()
    
    conn.close()
    
    # Formatear datos para respuesta
    indices_formateados = []
    for f in datos:
        valor = float(f[1]) if f[1] is not None else 0
        valor_ajustado, se_ajusto = ajustar_valor_por_reexpresion(f[0], valor)
        
        indices_formateados.append({
            'fecha': f[0],
            'valor_original': valor,
            'valor_ajustado': valor_ajustado,
            'variacion': float(f[2]) if f[2] is not None else 0,
            'fuente': f[3] if f[3] else 'desconocida',
            'ajustado': se_ajusto
        })
    
    manuales_formateados = []
    for f in datos_manuales:
        valor = float(f[1]) if f[1] is not None else 0
        valor_ajustado, se_ajusto = ajustar_valor_por_reexpresion(f[0], valor)
        
        manuales_formateados.append({
            'fecha': f[0],
            'valor_original': valor,
            'valor_ajustado': valor_ajustado,
            'variacion': float(f[2]) if f[2] is not None else 0,
            'fuente': f[3] if f[3] else 'manual',
            'ajustado': se_ajusto
        })
    
    # Combinar todos los datos (eliminar duplicados)
    todos_datos = {}
    for dato in indices_formateados + manuales_formateados:
        fecha = dato['fecha']
        if fecha not in todos_datos:
            todos_datos[fecha] = dato
    
    # Convertir a lista y ordenar
    todos_datos_lista = sorted(todos_datos.values(), key=lambda x: x['fecha'])
    
    return jsonify({
        'fecha_desde': fecha_desde,
        'fecha_hasta': fecha_hasta,
        'fecha_desde_sql': fecha_desde_sql,
        'fecha_hasta_sql': fecha_hasta_sql,
        'total_indices': len(datos),
        'total_manuales': len(datos_manuales),
        'total_todos': len(todos_datos_lista),
        'datos_indices': indices_formateados,
        'datos_manuales': manuales_formateados,
        'todos_datos': todos_datos_lista,
        'reexpresion': {
            'fecha_reexpresion': FECHA_REEXPRESION.strftime('%Y-%m-%d'),
            'factor_conversion': FACTOR_CONVERSION_REEXPRESION,
            'ajustados': sum(1 for d in todos_datos_lista if d['ajustado'])
        }
    })

# ========== RUTAS PARA RANKINGS Y ÍNDICES ==========
@app.route('/rankings')
def rankings():
    """
    Rankings por rango de fechas: Top ganadoras, perdedoras, más y menos negociadas.
    """
    # Obtener parámetros del formulario
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    
    # Si no hay fechas, usar valores por defecto (último mes)
    hoy = datetime.now()
    if not fecha_desde:
        fecha_desde = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
    if not fecha_hasta:
        fecha_hasta = hoy.strftime('%Y-%m-%d')
    
    # Calcular fechas para los botones de período rápido
    fecha_7d = (hoy - timedelta(days=7)).strftime('%Y-%m-%d')
    fecha_30d = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
    fecha_90d = (hoy - timedelta(days=90)).strftime('%Y-%m-%d')
    fecha_365d = (hoy - timedelta(days=365)).strftime('%Y-%m-%d')
    fecha_hoy = hoy.strftime('%Y-%m-%d')
    
    # Inicializar resultados
    top_ganadoras = []
    top_perdedoras = []
    mas_negociadas = []
    menos_negociadas = []
    estadisticas_periodo = {}
    
    # Si hay fechas seleccionadas, calcular rankings
    if fecha_desde and fecha_hasta:
        # Convertir fechas para SQLite
        fecha_desde_sql = datetime.strptime(fecha_desde, '%Y-%m-%d').strftime('%Y%m%d')
        fecha_hasta_sql = datetime.strptime(fecha_hasta, '%Y-%m-%d').strftime('%Y%m%d')
        
        logger.info(f"Calculando rankings del {fecha_desde} al {fecha_hasta}")
        
        # Obtener rankings desde SQLite
        rankings_data = obtener_rankings_por_rango(fecha_desde_sql, fecha_hasta_sql)
        
        top_ganadoras = rankings_data.get('top_ganadoras', [])
        top_perdedoras = rankings_data.get('top_perdedoras', [])
        mas_negociadas = rankings_data.get('mas_negociadas', [])
        menos_negociadas = rankings_data.get('menos_negociadas', [])
        estadisticas_periodo = rankings_data.get('estadisticas', {})
    
    # Formatear fechas para mostrar
    try:
        fecha_desde_dt = datetime.strptime(fecha_desde, '%Y-%m-%d')
        fecha_hasta_dt = datetime.strptime(fecha_hasta, '%Y-%m-%d')
        fecha_desde_formato = fecha_desde_dt.strftime('%d/%m/%Y')
        fecha_hasta_formato = fecha_hasta_dt.strftime('%d/%m/%Y')
    except:
        fecha_desde_formato = fecha_desde
        fecha_hasta_formato = fecha_hasta
    
    return render_template(
        'rankings.html',
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        fecha_desde_formato=fecha_desde_formato,
        fecha_hasta_formato=fecha_hasta_formato,
        fecha_7d=fecha_7d,
        fecha_30d=fecha_30d,
        fecha_90d=fecha_90d,
        fecha_365d=fecha_365d,
        fecha_hoy=fecha_hoy,
        top_ganadoras=top_ganadoras,
        top_perdedoras=top_perdedoras,
        mas_negociadas=mas_negociadas,
        menos_negociadas=menos_negociadas,
        estadisticas_periodo=estadisticas_periodo
    )

# ========== RUTA CORREGIDA PARA ÍNDICES IBC ==========
@app.route('/indices')
def indices():
    """
    Página de análisis del Índice Bursátil de Caracas (IBC) con comparativa del dólar BCV.
    """
    # Obtener parámetros del formulario
    fecha_desde = request.args.get('fecha_desde', '')
    fecha_hasta = request.args.get('fecha_hasta', '')
    
    # Si no hay fechas, usar valores por defecto (último mes)
    hoy = datetime.now()
    if not fecha_desde:
        fecha_desde = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
    if not fecha_hasta:
        fecha_hasta = hoy.strftime('%Y-%m-%d')
    
    # Calcular fechas para los botones de período rápido
    fecha_7d = (hoy - timedelta(days=7)).strftime('%Y-%m-%d')
    fecha_30d = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
    fecha_90d = (hoy - timedelta(days=90)).strftime('%Y-%m-%d')
    fecha_365d = (hoy - timedelta(days=365)).strftime('%Y-%m-%d')
    fecha_hoy = hoy.strftime('%Y-%m-%d')
    
    try:
        # Convertir fechas a formato SQLite
        fecha_desde_sql = datetime.strptime(fecha_desde, '%Y-%m-%d').strftime('%Y%m%d')
        fecha_hasta_sql = datetime.strptime(fecha_hasta, '%Y-%m-%d').strftime('%Y%m%d')
        
        logger.info(f"Consultando índices del {fecha_desde_sql} al {fecha_hasta_sql}")
        
    except Exception as e:
        logger.error(f"Error formateando fechas: {e}")
        # Usar valores por defecto si hay error
        fecha_desde = (hoy - timedelta(days=30)).strftime('%Y-%m-%d')
        fecha_hasta = hoy.strftime('%Y-%m-%d')
        fecha_desde_sql = (hoy - timedelta(days=30)).strftime('%Y%m%d')
        fecha_hasta_sql = hoy.strftime('%Y%m%d')
    
    # Obtener datos del índice desde SQLite usando la función corregida
    datos_indice, estadisticas_indice = obtener_datos_indice_historico(fecha_desde_sql, fecha_hasta_sql)
    
    # NUEVO: Obtener datos del dólar BCV para el mismo período
    datos_dolar_bcv = obtener_datos_dolar_bcv_historico(fecha_desde, fecha_hasta)
    
    # Preparar datos para el gráfico - CORREGIDO PARA ALINEAR FECHAS Y MANEJAR DATOS FALTANTES
    labels = []
    valores = []
    dolar_tasas = []  # Para datos del dólar
    
    # Alinear datos del IBC y dólar por fecha
    if datos_indice:
        logger.info(f"Datos del índice obtenidos: {len(datos_indice)} registros")
        
        # Ordenar por fecha ascendente para el gráfico
        datos_ordenados = sorted(datos_indice, key=lambda x: x['fecha'])
        
        # Crear diccionario de tasas de dólar por fecha
        dolar_por_fecha = {}
        if datos_dolar_bcv:
            for d in datos_dolar_bcv:
                fecha_dolar = d['fecha']
                dolar_por_fecha[fecha_dolar] = d['tasa']
        
        # **CORRECCIÓN: Usar interpolación para datos faltantes del dólar**
        for i, dato in enumerate(datos_ordenados):
            fecha_ibc = dato['fecha']
            
            # Formatear fecha para el eje X
            try:
                if len(fecha_ibc) == 8:
                    fecha_dt = datetime.strptime(fecha_ibc, '%Y%m%d')
                    labels.append(fecha_dt.strftime('%d/%m'))
                else:
                    labels.append(fecha_ibc)
            except:
                labels.append(fecha_ibc)
            
            # Usar el valor ajustado del índice
            valores.append(dato['valor'])
            
            # Obtener tasa del dólar para esta fecha
            if fecha_ibc in dolar_por_fecha:
                dolar_tasas.append(dolar_por_fecha[fecha_ibc])
            else:
                # **CORRECCIÓN: En lugar de usar 0, usar interpolación o el último valor conocido**
                if dolar_tasas and i > 0:
                    # Usar el último valor conocido del dólar
                    dolar_tasas.append(dolar_tasas[-1])
                else:
                    # Si es el primer dato y no hay valor, buscar la tasa más cercana
                    # Buscar la tasa del dólar más cercana anterior a esta fecha
                    tasa_estimada = 0
                    if datos_dolar_bcv:
                        # Ordenar tasas de dólar por fecha y buscar la más cercana anterior
                        tasas_ordenadas = sorted(datos_dolar_bcv, key=lambda x: x['fecha'])
                        for tasa in tasas_ordenadas:
                            if tasa['fecha'] <= fecha_ibc:
                                tasa_estimada = tasa['tasa']
                            else:
                                break
                        if tasa_estimada == 0 and tasas_ordenadas:
                            # Si no hay anterior, usar la primera disponible
                            tasa_estimada = tasas_ordenadas[0]['tasa']
                    
                    dolar_tasas.append(tasa_estimada)
    
    # **AÑADIR: Log para depuración**
    logger.info(f"Datos preparados para gráfico:")
    logger.info(f"  - Fechas (labels): {len(labels)} registros")
    logger.info(f"  - Valores IBC: {len(valores)} registros")
    logger.info(f"  - Tasas Dólar: {len(dolar_tasas)} registros")
    
    # Calcular estadísticas del período
    try:
        fecha_inicio_dt = datetime.strptime(fecha_desde, '%Y-%m-%d')
        fecha_fin_dt = datetime.strptime(fecha_hasta, '%Y-%m-%d')
        total_dias = (fecha_fin_dt - fecha_inicio_dt).days + 1
    except:
        total_dias = 30
    
    fechas_con_datos = len(datos_indice) if datos_indice else 0
    
    # Obtener el último dato para el resumen
    indice_actual = None
    if datos_indice and len(datos_indice) > 0:
        # Ordenar por fecha descendente para obtener el último
        datos_ordenados_desc = sorted(datos_indice, key=lambda x: x['fecha'], reverse=True)
        ultimo = datos_ordenados_desc[0]
        
        # Calcular variación diaria si hay más de un dato
        if len(datos_ordenados_desc) > 1:
            penultimo = datos_ordenados_desc[1]
            var_abs = ultimo['valor'] - penultimo['valor']
            var_rel = (var_abs / penultimo['valor'] * 100) if penultimo['valor'] > 0 else 0
        else:
            var_abs = 0
            var_rel = 0
        
        # Usar variación histórica de las estadísticas
        var_rel_historica = estadisticas_indice.get('var_rel_historica', 0) if estadisticas_indice else 0
        
        indice_actual = {
            'valor': ultimo['valor'],
            'valor_original': ultimo.get('valor_original', ultimo['valor']),
            'variacion': ultimo.get('variacion', 0),
            'var_rel': round(var_rel, 2),
            'var_abs': round(var_abs, 2),
            'var_rel_historica': round(var_rel_historica, 2),
            'var_abs_historica': (ultimo['valor'] - datos_indice[0]['valor']) if datos_indice else 0,
            'fecha': ultimo['fecha'],
            'fuente': ultimo.get('fuente', 'desconocida'),
            'ajustado': ultimo.get('ajustado', False)
        }
    
        # **CORREGIR LOG PARA MOSTRAR INFORMACIÓN DETALLADA**
    logger.info(f"Enviando a plantilla: {len(labels)} labels, {len(valores)} valores IBC, {len(dolar_tasas)} tasas dólar")
    
    # Mostrar muestras de datos para verificar
    if labels and len(labels) > 5:
        logger.info(f"Muestras de datos gráfico (primeros 5):")
        for j in range(min(5, len(labels))):
            logger.info(f"  {labels[j]}: IBC={valores[j]:.2f}, $={dolar_tasas[j]:.2f}")
    
    return render_template(
        'indices.html',
        fecha_desde=fecha_desde,
        fecha_hasta=fecha_hasta,
        fecha_7d=fecha_7d,
        fecha_30d=fecha_30d,
        fecha_90d=fecha_90d,
        fecha_365d=fecha_365d,
        fecha_hoy=fecha_hoy,
        labels=labels,
        valores=valores,
        dolar_tasas=dolar_tasas,
        total_dias=total_dias,
        fechas_con_datos=fechas_con_datos,
        indice=indice_actual,
        datos_indice=datos_indice,
        datos_dolar_bcv=datos_dolar_bcv,
        estadisticas_indice=estadisticas_indice,
        fecha_reexpresion=FECHA_REEXPRESION.strftime('%Y-%m-%d'),
        factor_conversion=FACTOR_CONVERSION_REEXPRESION
    )

# ========== FUNCIONES AUXILIARES PARA ÍNDICES Y RANKINGS ==========
def obtener_rankings_por_rango(fecha_desde, fecha_hasta):
    """
    Obtiene rankings por rango de fechas desde SQLite.
    """
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    if not os.path.exists(db_path):
        return {}
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 1. Obtener todas las acciones en el rango
        cursor.execute('''
            SELECT simbolo, nombre, variacion, monto, fecha
            FROM (
                SELECT simbolo, nombre, variacion, monto, fecha 
                FROM acciones 
                WHERE fecha BETWEEN ? AND ?
                UNION ALL
                SELECT simbolo, nombre, variacion, monto, fecha 
                FROM datos_manuales 
                WHERE fecha BETWEEN ? AND ?
            ) 
            ORDER BY fecha, simbolo
        ''', (fecha_desde, fecha_hasta, fecha_desde, fecha_hasta))
        
        datos = cursor.fetchall()
        
        if not datos:
            return {}
        
        # 2. Procesar datos: agrupar por símbolo
        acciones_dict = {}
        
        for simbolo, nombre, variacion, monto, fecha in datos:
            if simbolo not in acciones_dict:
                acciones_dict[simbolo] = {
                    'simbolo': simbolo,
                    'nombre': nombre,
                    'variaciones': [],
                    'montos': [],
                    'fechas': [],
                    'dias_presente': 0
                }
            
            acciones_dict[simbolo]['variaciones'].append(variacion)
            acciones_dict[simbolo]['montos'].append(monto)
            acciones_dict[simbolo]['fechas'].append(fecha)
            acciones_dict[simbolo]['dias_presente'] += 1
        
        # 3. Calcular promedios y totales
        acciones_procesadas = []
        for simbolo, data in acciones_dict.items():
            # Calcular variación promedio
            variaciones = data['variaciones']
            variacion_promedio = sum(variaciones) / len(variaciones) if variaciones else 0
            
            # Calcular monto total
            monto_total = sum(data['montos'])
            
            # Encontrar máxima variación positiva y negativa
            max_variacion_positiva = max([v for v in variaciones if v > 0], default=0)
            max_variacion_negativa = min([v for v in variaciones if v < 0], default=0)
            
            # Calcular consistencia (% días en alza)
            dias_alza = sum(1 for v in variaciones if v > 0)
            consistencia_alza = (dias_alza / len(variaciones) * 100) if variaciones else 0
            
            acciones_procesadas.append({
                'simbolo': simbolo,
                'nombre': data['nombre'],
                'variacion_promedio': round(variacion_promedio, 2),
                'monto_total': round(monto_total, 2),
                'dias_presente': data['dias_presente'],
                'max_variacion_positiva': round(max_variacion_positiva, 2),
                'max_variacion_negativa': round(max_variacion_negativa, 2),
                'consistencia_alza': round(consistencia_alza, 1),
                'primera_fecha': min(data['fechas']),
                'ultima_fecha': max(data['fechas'])
            })
        
        # 4. Calcular rankings
        # Top Ganadoras (por variación promedio)
        top_ganadoras = sorted(
            [a for a in acciones_procesadas if a['variacion_promedio'] > 0],
            key=lambda x: x['variacion_promedio'],
            reverse=True
        )[:10]  # Top 10
        
        # Top Perdedoras (por variación promedio)
        top_perdedoras = sorted(
            [a for a in acciones_procesadas if a['variacion_promedio'] < 0],
            key=lambda x: x['variacion_promedio']
        )[:10]  # Top 10
        
        # Más Negociadas (por monto total)
        mas_negociadas = sorted(
            acciones_procesadas,
            key=lambda x: x['monto_total'],
            reverse=True
        )[:10]  # Top 10
        
        # Menos Negociadas (por monto total, excluyendo ceros)
        menos_negociadas = sorted(
            [a for a in acciones_procesadas if a['monto_total'] > 0],
            key=lambda x: x['monto_total']
        )[:10]  # Top 10
        
        # 5. Calcular estadísticas del período
        total_acciones = len(acciones_procesadas)
        total_monto = sum(a['monto_total'] for a in acciones_procesadas)
        acciones_alza = sum(1 for a in acciones_procesadas if a['variacion_promedio'] > 0)
        acciones_baja = sum(1 for a in acciones_procesadas if a['variacion_promedio'] < 0)
        acciones_estables = sum(1 for a in acciones_procesadas if a['variacion_promedio'] == 0)
        
        # Encontrar acciones más consistentes
        acciones_consistentes = sorted(
            [a for a in acciones_procesadas if a['dias_presente'] >= 5],
            key=lambda x: x['consistencia_alza'],
            reverse=True
        )[:5]
        
        estadisticas = {
            'total_acciones': total_acciones,
            'total_monto': total_monto,
            'acciones_alza': acciones_alza,
            'acciones_baja': acciones_baja,
            'acciones_estables': acciones_estables,
            'porcentaje_alza': round((acciones_alza / total_acciones * 100) if total_acciones > 0 else 0, 1),
            'porcentaje_baja': round((acciones_baja / total_acciones * 100) if total_acciones > 0 else 0, 1),
            'monto_promedio': round(total_monto / total_acciones, 2) if total_acciones > 0 else 0,
            'dias_rango': (datetime.strptime(fecha_hasta, '%Y%m%d') - datetime.strptime(fecha_desde, '%Y%m%d')).days + 1,
            'acciones_consistentes': acciones_consistentes
        }
        
        return {
            'top_ganadoras': top_ganadoras,
            'top_perdedoras': top_perdedoras,
            'mas_negociadas': mas_negociadas,
            'menos_negociadas': menos_negociadas,
            'estadisticas': estadisticas
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo rankings por rango: {e}")
        return {}
    finally:
        conn.close()

# Precargar datos comunes al recibir la primera solicitud
@app.before_request
def iniciar_cache():
    """Precarga datos comunes al recibir la primera solicitud."""
    global _cache_precargado
    if not _cache_precargado:
        try:
            precargar_datos_comunes()
            logger.info("✅ Caché SQLite precargado exitosamente")
            _cache_precargado = True
        except Exception as e:
            logger.error(f"Error precargando caché SQLite: {e}")

# Inyectar la función now() y constantes para que funcionen en el HTML
@app.context_processor
def inject_now():
    return {
        'now': datetime.now,
        'fecha_reexpresion': FECHA_REEXPRESION.strftime('%Y-%m-%d'),
        'factor_conversion': FACTOR_CONVERSION_REEXPRESION
    }

# ========== EJECUCIÓN DEL SERVIDOR ==========
if __name__ == '__main__':
    # Precargar caché SQLite al iniciar
    try:
        print("=" * 60)
        print("=== SERVIDOR BVC - DASHBOARD DE MERCADO ===")
        print("=" * 60)
        print("🚀 CARACTERÍSTICAS PRINCIPALES:")
        print("  • ✅ Base de datos SQLite (100x más rápido)")
        print("  • ✅ Manejo automático de fines de semana")
        print("  • ✅ Índices optimizados para consultas")
        print("  • ✅ Caché de consultas en memoria")
        print("  • ✅ Autocomplete dinámico de acciones")
        print("  • ✅ Listado de acciones activas (últimos 30 días)")
        print("  • ✅ Botón 'Histórico Completo' en consultas")
        print("  • ✅ Nueva página de Índices IBC (CON COMPARATIVA $BCV)")
        print(f"  • ✅ AJUSTE POR REEXPRESIÓN MONETARIA (antes del {FECHA_REEXPRESION.strftime('%d/%m/%Y')}: ÷{FACTOR_CONVERSION_REEXPRESION})")
        print("  • ✅ LOGO GENAROCOIN en imágenes para compartir")
        print("  • ✅ SISTEMA DÓLAR BCV INTEGRADO")
        print("  • ✅ GRÁFICO COMPARATIVO IBC vs DÓLAR BCV")
        print("=" * 60)
        
        print("🔄 Precargando caché SQLite al iniciar...")
        precargar_datos_comunes()
        sqlite_stats = sqlite_manager.estadisticas()
        query_stats = query_cache.get_cache_stats()
        print(f"✅ SQLite precargado: {sqlite_stats['fechas_unicas']} fechas, {sqlite_stats['total_acciones']} acciones")
        print(f"📊 Tamaño BD: {sqlite_stats['db_size_mb']:.2f} MB")
        _cache_precargado = True
        
        # Verificar tabla dólar BCV
        crear_tabla_dolar_bcv()
        print(f"💰 Tabla dolar_bcv verificada/creada")
        
        # Información sobre manejo de fines de semana
        hoy = datetime.now()
        if es_fin_de_semana(hoy):
            ultimo_habil = obtener_ultimo_dia_habil(hoy)
            print(f"📅 Hoy es fin de semana ({hoy.strftime('%A')})")
            print(f"🔁 Se mostrarán datos del último día hábil: {ultimo_habil.strftime('%d/%m/%Y')}")
        else:
            print(f"📅 Hoy es día hábil: {hoy.strftime('%A %d/%m/%Y')}")
        
    except Exception as e:
        print(f"⚠️  Error precargando caché SQLite: {e}")
    
    print("=" * 60)
    print(f"Iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Accede a: http://127.0.0.1:5000")
    print("=" * 60)
    print("🔧 ENDPOINTS PRINCIPALES:")
    print("  📊 Dashboard: /")
    print("  📈 Histórico (ULTRA RÁPIDO): /consulta")
    print("  📝 Datos manuales: /admin/ingreso-manual")
    print("  🏆 Rankings: /rankings")
    print("  📊 Índices IBC vs $BCV: /indices")
    print("  💰 Dólar BCV: /admin/dolar-bcv")
    print("  🔍 Acciones activas: /api/acciones-activas")
    print("  🔍 Todas las acciones: /api/acciones-disponibles")
    print("  📅 Histórico completo: /api/historico-completo?simbolo=ARCA")
    print("  📤 Compartir análisis con logo GenaroCoin")
    print("  🔧 Corregir nombres: /admin/corregir-nombres")
    print("  ⚡ Optimizar SQLite: /admin/optimizar-sqlite")
    print("  🧹 Limpiar caché: /admin/clear-cache")
    print("  📡 Estado SQLite: /admin/cache-status")
    print("  🔍 Diagnóstico tablas: /admin/diagnostico-tablas")
    print("  🐛 Depuración índices: /debug/indices/2025-01-01/2026-01-12")
    print("=" * 60)
    
    # Configurar para múltiples hilos
    app.run(debug=True, port=5000, threaded=True)
