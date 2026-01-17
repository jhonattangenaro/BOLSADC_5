
import requests
import os
from datetime import datetime, timedelta
from sqlite_manager import sqlite_manager  # NUEVO - Usamos SQLite en lugar de TinyDB

# Importar funciones de dat_parser si existe
try:
    from dat_parser import buscar_en_data_cache, parsear_archivo_dat
    DAT_PARSER_DISPONIBLE = True
except ImportError:
    print("⚠️  Módulo dat_parser no encontrado, creando funciones básicas...")
    DAT_PARSER_DISPONIBLE = False
    
    def buscar_en_data_cache(fecha_str, carpeta_cache="data_cache"):
        return [], None
    
    def parsear_archivo_dat(ruta_archivo):
        return [], None

def limpiar_numero(texto):
    """Convierte '6.230,00' o '6230' a float 6230.0 correctamente."""
    if not texto or not isinstance(texto, str): 
        return 0.0
    
    t = texto.strip()
    if t in ["-", "", "N/D", "0", "0,00", "0.00"]: 
        return 0.0
    
    try:
        # Eliminar símbolos de moneda y espacios
        t = t.replace('Bs.', '').replace('bs.', '').replace('Bs', '').replace('$', '').strip()
        
        # Si tiene formato con puntos y comas (1.250,50)
        if "." in t and "," in t:
            partes = t.split(",")
            if len(partes[-1]) == 2:
                t = t.replace(".", "").replace(",", ".")
            else:
                t = t.replace(",", "")
        elif "," in t:
            partes = t.split(",")
            if len(partes[-1]) == 2:
                t = t.replace(",", ".")
            else:
                t = t.replace(",", "")
        
        t = ''.join(c for c in t if c.isdigit() or c in '.-')
        
        if not t:
            return 0.0
            
        result = float(t)
        
        if result < 0.0001 and result > -0.0001:
            return 0.0
            
        return result
    except Exception as e:
        print(f"Error limpiando número '{texto}': {e}")
        return 0.0

def descargar_y_guardar(fecha_vvc):
    """Función principal para obtener datos. PRIORIZA SQLITE Y ARCHIVOS .DAT"""
    
    # 1. PRIMERO BUSCAR EN SQLITE (MUY RÁPIDO)
    datos_sqlite = sqlite_manager.obtener_acciones_por_fecha(fecha_vvc)
    if datos_sqlite:
        indice_sqlite = sqlite_manager.obtener_indice_por_fecha(fecha_vvc)
        print(f"📊 SQLite {fecha_vvc}: {len(datos_sqlite)} registros")
        return datos_sqlite, indice_sqlite
    
    # 2. BUSCAR EN ARCHIVOS .DAT (NUEVO - PRIORIDAD ALTA)
    if DAT_PARSER_DISPONIBLE and os.path.exists("data_cache"):
        print(f"🔍 Buscando en data_cache para {fecha_vvc}...")
        acciones_dat, indice_dat = buscar_en_data_cache(fecha_vvc)
        
        if acciones_dat:
            print(f"📁 Datos encontrados en archivo .dat: {len(acciones_dat)} registros")
            
            # Guardar en SQLite para futuras consultas
            if acciones_dat:
                print(f"💾 Guardando {len(acciones_dat)} registros de archivo .dat en SQLite...")
                sqlite_manager.insertar_acciones(fecha_vvc, acciones_dat)
                if indice_dat: 
                    sqlite_manager.insertar_indice(fecha_vvc, indice_dat)
                
                return acciones_dat, indice_dat
    
    # 3. Si no está en SQLite ni en archivos .dat, intentar descargar de BVC
    url = f"https://www.bolsadecaracas.com/descargar-diario-bolsa/?type=dat&fecha={fecha_vvc}"
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=10)
        if response.status_code == 200 and "R|" in response.text:
            acciones_dia = []
            indice_dia = None
            lineas = response.text.split('\n')
            
            print(f"🌐 Descargando datos automáticos para {fecha_vvc}...")
            
            for linea in lineas:
                d = linea.split('|')
                if linea.startswith('R|') and len(d) >= 13:
                    simbolo = d[2].strip()
                    p_anterior = limpiar_numero(d[3])
                    p_hoy = limpiar_numero(d[4])
                    
                    if p_anterior > 0 and p_hoy > 0:
                        diferencia_bs = p_hoy - p_anterior
                        variacion_real = (diferencia_bs / p_anterior) * 100
                    else:
                        diferencia_bs = 0
                        variacion_real = 0.0
                    
                    acciones_dia.append({
                        'fecha': fecha_vvc,
                        'nombre': d[1].strip(),
                        'simbolo': simbolo,
                        'anterior': p_anterior,
                        'hoy': p_hoy,
                        'diferencia_bs': round(diferencia_bs, 4),
                        'variacion': round(variacion_real, 2),
                        'cantidad': int(limpiar_numero(d[11])),
                        'monto': limpiar_numero(d[12]),
                        'fuente': 'automatico'
                    })
                elif linea.startswith('IG|') and len(d) >= 5:
                    indice_dia = {
                        'fecha': fecha_vvc,
                        'valor': limpiar_numero(d[2]),
                        'variacion': limpiar_numero(d[4]),
                        'fuente': 'automatico'
                    }
            
            if acciones_dia:
                print(f"💾 Guardando {len(acciones_dia)} registros automáticos en SQLite...")
                sqlite_manager.insertar_acciones(fecha_vvc, acciones_dia)
                if indice_dia: 
                    sqlite_manager.insertar_indice(fecha_vvc, indice_dia)
                
                return acciones_dia, indice_dia
    except Exception as e:
        print(f"❌ Error descargando {fecha_vvc}: {e}")
    
    # 4. Si no hay datos automáticos, buscar datos manuales (ya están en SQLite)
    print(f"🔍 Datos automáticos no disponibles para {fecha_vvc}, buscando datos manuales en SQLite...")
    from datos_manuales import obtener_datos_manuales
    datos_manual, indice_manual = obtener_datos_manuales(fecha_vvc)
    
    if datos_manual:
        return datos_manual, indice_manual
    
    return [], None

# FUNCIÓN OPTIMIZADA PARA HISTÓRICO (USANDO SQLITE)
def obtener_historico_rapido(simbolo, fecha_desde, fecha_hasta):
    """
    Obtiene histórico de un símbolo de manera optimizada usando SQLite.
    MUCHO MÁS RÁPIDO que la versión anterior con TinyDB.
    """
    try:
        # Convertir fechas de formato YYYY-MM-DD a YYYYMMDD para SQLite
        fecha_inicio = datetime.strptime(fecha_desde, '%Y-%m-%d')
        fecha_fin = datetime.strptime(fecha_hasta, '%Y-%m-%d')
        
        if fecha_fin < fecha_inicio:
            fecha_fin, fecha_inicio = fecha_inicio, fecha_fin
        
        # Formatear fechas para SQLite
        fecha_desde_sql = fecha_inicio.strftime('%Y%m%d')
        fecha_hasta_sql = fecha_fin.strftime('%Y%m%d')
        
        print(f"🔍 Buscando histórico SQLite para {simbolo} del {fecha_desde_sql} al {fecha_hasta_sql}...")
        
        # USAR SQLITE PARA CONSULTA ULTRA RÁPIDA
        datos_historicos = sqlite_manager.obtener_historico_simbolo(
            simbolo, 
            fecha_desde_sql, 
            fecha_hasta_sql
        )
        
        if datos_historicos:
            print(f"✅ Encontrados {len(datos_historicos)} registros para {simbolo} en SQLite")
            
            # Formatear datos para salida
            datos_formateados = []
            for dato in datos_historicos:
                fecha_str = dato['fecha']
                try:
                    if len(fecha_str) == 8:
                        fecha_dt = datetime.strptime(fecha_str, '%Y%m%d')
                        fecha_formateada = fecha_dt.strftime('%d/%m/%Y')
                    else:
                        fecha_formateada = fecha_str
                except:
                    fecha_formateada = fecha_str
                
                datos_formateados.append({
                    'fecha': fecha_str,
                    'fecha_formateada': fecha_formateada,
                    'precio': dato.get('precio', dato.get('hoy', 0)),
                    'variacion': dato.get('variacion', 0),
                    'cambio_bs': dato.get('cambio_bs', dato.get('diferencia_bs', 0)),
                    'cantidad': dato.get('cantidad', 0),
                    'monto': dato.get('monto', 0),
                    'anterior': dato.get('anterior', 0),
                    'fuente': dato.get('fuente', 'desconocida')
                })
            
            # MODIFICADO: Ordenar de más reciente a más antiguo
            datos_formateados.sort(key=lambda x: x['fecha'], reverse=True)
            
            return datos_formateados
        else:
            print(f"⚠️  No se encontraron datos en SQLite para {simbolo}")
            return []
        
    except Exception as e:
        print(f"❌ Error obteniendo histórico rápido desde SQLite: {e}")
        return []

# Función para precargar datos comunes
def precargar_datos_comunes():
    """Precarga datos de los últimos 30 días en caché SQLite."""
    hoy = datetime.now()
    fecha_inicio = hoy - timedelta(days=30)
    
    print("🔄 Precargando datos comunes en caché SQLite...")
    
    # PRIMERO CARGAR DESDE ARCHIVOS .DAT SI EXISTEN
    if DAT_PARSER_DISPONIBLE and os.path.exists("data_cache"):
        print("📂 Detectada carpeta data_cache, cargando datos recientes en SQLite...")
        try:
            from dat_parser import cargar_solo_recientes_sqlite
            # Necesitamos crear esta función en dat_parser.py
            cargar_solo_recientes_sqlite(30)
        except Exception as e:
            print(f"⚠️  Error cargando desde data_cache a SQLite: {e}")
    
    # Luego precargar en caché de SQLite
    sqlite_manager.precargar_cache(30)
    print("✅ Precarga SQLite completada")

# Función auxiliar para buscar en archivos .dat si no hay datos
def buscar_datos_externos(fecha_str):
    """
    Busca datos en todas las fuentes posibles.
    Útil cuando SQLite no tiene datos.
    """
    # 1. Buscar en archivos .dat
    if DAT_PARSER_DISPONIBLE and os.path.exists("data_cache"):
        acciones, indice = buscar_en_data_cache(fecha_str)
        if acciones:
            # Guardar en SQLite para futuras consultas
            sqlite_manager.insertar_acciones(fecha_str, acciones)
            if indice:
                sqlite_manager.insertar_indice(fecha_str, indice)
            return acciones, indice
    
    # 2. Intentar descargar
    return descargar_y_guardar(fecha_str)
# Función para obtener datos históricos del dólar BCV
def obtener_dolar_bcv_historico(fecha_desde, fecha_hasta):
    """
    Obtiene datos históricos del dólar BCV desde SQLite.
    """
    try:
        import sqlite3
        
        # Convertir fechas a formato YYYYMMDD
        fecha_desde_sql = fecha_desde.replace('-', '') if '-' in fecha_desde else fecha_desde
        fecha_hasta_sql = fecha_hasta.replace('-', '') if '-' in fecha_hasta else fecha_hasta
        
        db_path = "database/bolsa_datos.db"
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
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
        print(f"❌ Error obteniendo histórico dólar BCV: {e}")
        return []