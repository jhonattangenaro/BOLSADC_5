# dat_parser.py - Parser para archivos .dat de BVC con soporte SQLite
import os
import re
from datetime import datetime
from sqlite_manager import sqlite_manager

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

def parsear_archivo_dat(ruta_archivo):
    """
    Parsea un archivo .dat de BVC y extrae los datos.
    Retorna: (acciones, indice)
    """
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            contenido = f.read()
        
        acciones = []
        indice = None
        
        # Extraer fecha del nombre del archivo (ej: "20240115.dat")
        nombre_archivo = os.path.basename(ruta_archivo)
        match = re.search(r'(\d{8})\.dat$', nombre_archivo)
        if not match:
            return [], None
        
        fecha_str = match.group(1)
        
        # Procesar línea por línea
        lineas = contenido.split('\n')
        
        for linea in lineas:
            linea = linea.strip()
            if not linea:
                continue
            
            # Datos de acciones (líneas que empiezan con R|)
            if linea.startswith('R|') and '|' in linea:
                partes = linea.split('|')
                if len(partes) >= 13:
                    simbolo = partes[2].strip()
                    p_anterior = limpiar_numero(partes[3])
                    p_hoy = limpiar_numero(partes[4])
                    
                    # Calcular diferencia y variación
                    if p_anterior > 0 and p_hoy > 0:
                        diferencia_bs = p_hoy - p_anterior
                        variacion_real = (diferencia_bs / p_anterior) * 100
                    else:
                        diferencia_bs = 0
                        variacion_real = 0.0
                    
                    acciones.append({
                        'fecha': fecha_str,
                        'nombre': partes[1].strip(),
                        'simbolo': simbolo,
                        'anterior': p_anterior,
                        'hoy': p_hoy,
                        'diferencia_bs': round(diferencia_bs, 4),
                        'variacion': round(variacion_real, 2),
                        'cantidad': int(limpiar_numero(partes[11])),
                        'monto': limpiar_numero(partes[12]),
                        'fuente': 'archivo_dat'
                    })
            
            # Datos del índice (líneas que empiezan con IG|)
            elif linea.startswith('IG|') and '|' in linea:
                partes = linea.split('|')
                if len(partes) >= 5:
                    indice = {
                        'fecha': fecha_str,
                        'valor': limpiar_numero(partes[2]),
                        'variacion': limpiar_numero(partes[4]),
                        'fuente': 'archivo_dat'
                    }
        
        print(f"📁 Archivo {nombre_archivo}: {len(acciones)} acciones")
        return acciones, indice
        
    except Exception as e:
        print(f"❌ Error parseando archivo {ruta_archivo}: {e}")
        return [], None

def cargar_desde_data_cache_sqlite(carpeta_cache="data_cache"):
    """
    Carga todos los archivos .dat de la carpeta data_cache en SQLite.
    """
    if not os.path.exists(carpeta_cache):
        print(f"❌ Carpeta {carpeta_cache} no encontrada")
        return False
    
    archivos_dat = [f for f in os.listdir(carpeta_cache) if f.endswith('.dat')]
    
    if not archivos_dat:
        print(f"⚠️  No se encontraron archivos .dat en {carpeta_cache}")
        return False
    
    print(f"📂 Encontrados {len(archivos_dat)} archivos .dat en {carpeta_cache}")
    
    total_acciones = 0
    total_archivos_procesados = 0
    archivos_procesados = 0
    
    for archivo in sorted(archivos_dat):
        ruta_completa = os.path.join(carpeta_cache, archivo)
        
        # Extraer fecha del nombre del archivo
        match = re.search(r'(\d{8})\.dat$', archivo)
        if not match:
            print(f"⚠️  Nombre de archivo inválido: {archivo}")
            continue
        
        fecha_str = match.group(1)
        total_archivos_procesados += 1
        
        # Mostrar progreso cada 100 archivos
        if total_archivos_procesados % 100 == 0:
            print(f"📊 Progreso: {total_archivos_procesados}/{len(archivos_dat)} archivos procesados")
        
        # Verificar si ya existen datos para esta fecha en SQLite
        datos_existentes = sqlite_manager.obtener_acciones_por_fecha(fecha_str)
        
        if datos_existentes:
            # Ya existen datos, omitir
            continue
        
        # Parsear y cargar datos
        acciones, indice = parsear_archivo_dat(ruta_completa)
        
        if acciones:
            # Guardar en SQLite
            insertados = sqlite_manager.insertar_acciones(fecha_str, acciones)
            
            if indice:
                sqlite_manager.insertar_indice(fecha_str, indice)
            
            total_acciones += insertados
            archivos_procesados += 1
            
            # Mostrar progreso cada 50 archivos cargados
            if archivos_procesados % 50 == 0:
                print(f"✅ {archivos_procesados} archivos cargados, {total_acciones} acciones")
    
    print(f"🎉 Carga completada: {archivos_procesados} archivos nuevos procesados, {total_acciones} acciones totales")
    
    # Mostrar estadísticas
    stats = sqlite_manager.estadisticas()
    print(f"📊 SQLite actual: {stats['fechas_unicas']} fechas, {stats['total_acciones']} acciones")
    
    return True

def buscar_en_data_cache(fecha_str, carpeta_cache="data_cache"):
    """
    Busca datos para una fecha específica en la carpeta data_cache.
    """
    # Primero buscar archivo exacto
    ruta_archivo = os.path.join(carpeta_cache, f"{fecha_str}.dat")
    
    if os.path.exists(ruta_archivo):
        return parsear_archivo_dat(ruta_archivo)
    
    # Si no existe, buscar en archivos existentes
    if os.path.exists(carpeta_cache):
        archivos = os.listdir(carpeta_cache)
        for archivo in archivos:
            if fecha_str in archivo and archivo.endswith('.dat'):
                return parsear_archivo_dat(os.path.join(carpeta_cache, archivo))
    
    return [], None

def cargar_solo_recientes_sqlite(dias=30, carpeta_cache="data_cache"):
    """
    Carga solo los archivos más recientes en SQLite para optimizar la inicialización.
    """
    if not os.path.exists(carpeta_cache):
        return False
    
    archivos_dat = [f for f in os.listdir(carpeta_cache) if f.endswith('.dat')]
    
    if not archivos_dat:
        return False
    
    # Ordenar por fecha (más recientes primero)
    archivos_dat.sort(reverse=True)
    
    # Tomar solo los últimos N días
    archivos_a_cargar = archivos_dat[:min(dias, len(archivos_dat))]
    
    print(f"🔄 Cargando {len(archivos_a_cargar)} archivos recientes en SQLite...")
    
    total_acciones = 0
    
    for archivo in archivos_a_cargar:
        try:
            match = re.search(r'(\d{8})\.dat$', archivo)
            if not match:
                continue
                
            fecha_str = match.group(1)
            
            # Verificar si ya existen datos en SQLite
            datos_existentes = sqlite_manager.obtener_acciones_por_fecha(fecha_str)
            if datos_existentes:
                continue
            
            # Parsear archivo
            acciones, indice = parsear_archivo_dat(os.path.join(carpeta_cache, archivo))
            
            if acciones:
                insertados = sqlite_manager.insertar_acciones(fecha_str, acciones)
                if indice:
                    sqlite_manager.insertar_indice(fecha_str, indice)
                
                total_acciones += insertados
                
        except Exception as e:
            print(f"⚠️  Error con {archivo}: {e}")
    
    print(f"✅ Carga rápida SQLite completada: {total_acciones} acciones de {len(archivos_a_cargar)} días recientes")
    return True

# Función de compatibilidad para código existente
def cargar_desde_data_cache(carpeta_cache="data_cache"):
    """Versión de compatibilidad que usa SQLite"""
    return cargar_desde_data_cache_sqlite(carpeta_cache)

def cargar_solo_recientes(dias=30, carpeta_cache="data_cache"):
    """Versión de compatibilidad que usa SQLite"""
    return cargar_solo_recientes_sqlite(dias, carpeta_cache)

if __name__ == "__main__":
    print("=" * 60)
    print("=== CARGA DE DATOS DESDE ARCHIVOS .DAT A SQLite ===")
    print("=" * 60)
    
    # Primero cargar solo los recientes
    cargar_solo_recientes_sqlite(30)
    
    print("\n¿Deseas cargar TODOS los archivos a SQLite? (s/n)")
    respuesta = input().lower()
    
    if respuesta == 's':
        print("\n🔄 Iniciando carga completa a SQLite...")
        result = cargar_desde_data_cache_sqlite()
        
        if result:
            print("✅ Carga completa a SQLite exitosa")
        else:
            print("❌ Error en la carga de datos a SQLite")
    else:
        print("✅ Carga básica a SQLite completada")
    
    print("=" * 60)