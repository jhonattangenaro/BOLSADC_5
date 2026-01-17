
#!/usr/bin/env python3
# cargar_todos_dat.py - Carga TODOS los archivos .dat a SQLite

import os
import re
import sqlite3
from datetime import datetime
import time

def limpiar_numero(texto):
    """Convierte '6.230,00' o '6230' a float 6230.0 correctamente."""
    if not texto or not isinstance(texto, str): 
        return 0.0
    
    t = texto.strip()
    if t in ["-", "", "N/D", "0", "0,00", "0.00"]: 
        return 0.0
    
    try:
        t = t.replace('Bs.', '').replace('bs.', '').replace('Bs', '').replace('$', '').strip()
        
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
            
        return float(t)
    except:
        return 0.0

def parsear_archivo_dat(ruta_archivo):
    """Parsea un archivo .dat de BVC"""
    try:
        with open(ruta_archivo, 'r', encoding='utf-8') as f:
            contenido = f.read()
        
        acciones = []
        indice = None
        
        nombre_archivo = os.path.basename(ruta_archivo)
        match = re.search(r'(\d{8})\.dat$', nombre_archivo)
        if not match:
            return [], None
        
        fecha_str = match.group(1)
        
        lineas = contenido.split('\n')
        
        for linea in lineas:
            linea = linea.strip()
            if not linea:
                continue
            
            if linea.startswith('R|') and '|' in linea:
                partes = linea.split('|')
                if len(partes) >= 13:
                    simbolo = partes[2].strip()
                    p_anterior = limpiar_numero(partes[3])
                    p_hoy = limpiar_numero(partes[4])
                    
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
            
            elif linea.startswith('IG|') and '|' in linea:
                partes = linea.split('|')
                if len(partes) >= 5:
                    indice = {
                        'fecha': fecha_str,
                        'valor': limpiar_numero(partes[2]),
                        'variacion': limpiar_numero(partes[4]),
                        'fuente': 'archivo_dat'
                    }
        
        return acciones, indice
        
    except Exception as e:
        print(f"❌ Error parseando {ruta_archivo}: {e}")
        return [], None

def cargar_a_sqlite():
    """Carga todos los archivos .dat a SQLite"""
    carpeta_cache = "data_cache"
    if not os.path.exists(carpeta_cache):
        print(f"❌ Carpeta {carpeta_cache} no encontrada")
        return False
    
    archivos_dat = [f for f in os.listdir(carpeta_cache) if f.endswith('.dat')]
    
    if not archivos_dat:
        print(f"⚠️  No hay archivos .dat en {carpeta_cache}")
        return False
    
    print(f"📂 Encontrados {len(archivos_dat)} archivos .dat")
    
    # Crear directorio database si no existe
    os.makedirs("database", exist_ok=True)
    
    # Conectar a SQLite
    db_path = "database/bolsa_datos.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Crear tablas si no existen
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS acciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT NOT NULL,
        simbolo TEXT NOT NULL,
        nombre TEXT NOT NULL,
        anterior REAL,
        hoy REAL,
        diferencia_bs REAL,
        variacion REAL,
        cantidad INTEGER,
        monto REAL,
        fuente TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(fecha, simbolo)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS indices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT NOT NULL,
        valor REAL,
        variacion REAL,
        fuente TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(fecha)
    )
    ''')
    
    # Crear índices para máxima velocidad
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_fecha ON acciones(fecha)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_simbolo ON acciones(simbolo)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_fecha_simbolo ON acciones(fecha, simbolo)')
    
    conn.commit()
    
    total_acciones = 0
    total_archivos = 0
    archivos_procesados = 0
    start_time = time.time()
    
    for archivo in sorted(archivos_dat):
        ruta_completa = os.path.join(carpeta_cache, archivo)
        
        match = re.search(r'(\d{8})\.dat$', archivo)
        if not match:
            continue
        
        fecha_str = match.group(1)
        total_archivos += 1
        
        # Mostrar progreso cada 100 archivos
        if total_archivos % 100 == 0:
            elapsed = time.time() - start_time
            print(f"📊 Progreso: {total_archivos}/{len(archivos_dat)} archivos, {total_acciones} acciones, {elapsed:.1f}s")
        
        # Verificar si ya existen datos para esta fecha
        cursor.execute("SELECT COUNT(*) FROM acciones WHERE fecha = ?", (fecha_str,))
        if cursor.fetchone()[0] > 0:
            continue  # Ya existen datos
        
        # Parsear archivo
        acciones, indice = parsear_archivo_dat(ruta_completa)
        
        if acciones:
            # Insertar acciones
            for accion in acciones:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO acciones 
                        (fecha, simbolo, nombre, anterior, hoy, diferencia_bs, 
                         variacion, cantidad, monto, fuente)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        fecha_str,
                        accion.get('simbolo', ''),
                        accion.get('nombre', ''),
                        accion.get('anterior', 0),
                        accion.get('hoy', 0),
                        accion.get('diferencia_bs', 0),
                        accion.get('variacion', 0),
                        accion.get('cantidad', 0),
                        accion.get('monto', 0),
                        'archivo_dat'
                    ))
                    total_acciones += 1
                except Exception as e:
                    if "UNIQUE constraint failed" not in str(e):
                        print(f"⚠️  Error insertando {accion.get('simbolo', '')} para {fecha_str}: {e}")
            
            # Insertar índice
            if indice:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO indices 
                        (fecha, valor, variacion, fuente)
                        VALUES (?, ?, ?, ?)
                    ''', (
                        fecha_str,
                        indice.get('valor', 0),
                        indice.get('variacion', 0),
                        'archivo_dat'
                    ))
                except Exception as e:
                    if "UNIQUE constraint failed" not in str(e):
                        print(f"⚠️  Error insertando índice para {fecha_str}: {e}")
            
            conn.commit()
            archivos_procesados += 1
    
    conn.close()
    
    elapsed_total = time.time() - start_time
    print(f"\n🎉 CARGA COMPLETADA")
    print(f"   • Archivos procesados: {archivos_procesados}/{len(archivos_dat)}")
    print(f"   • Acciones insertadas: {total_acciones}")
    print(f"   • Tiempo total: {elapsed_total:.1f} segundos")
    print(f"   • Velocidad: {total_acciones/elapsed_total:.0f} acciones/segundo")
    
    # Mostrar estadísticas
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT COUNT(*) FROM acciones")
    total_bd = cursor.fetchone()[0]
    
    cursor.execute("SELECT MIN(fecha), MAX(fecha) FROM acciones")
    min_max = cursor.fetchone()
    
    cursor.execute("SELECT COUNT(DISTINCT fecha) FROM acciones")
    fechas_unicas = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(DISTINCT simbolo) FROM acciones")
    simbolos_unicos = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"\n📊 ESTADÍSTICAS SQLite:")
    print(f"   • Total acciones en BD: {total_bd}")
    print(f"   • Fechas únicas: {fechas_unicas}")
    print(f"   • Símbolos únicos: {simbolos_unicos}")
    print(f"   • Rango temporal: {min_max[0]} a {min_max[1]}")
    
    return True

if __name__ == "__main__":
    print("=" * 60)
    print("=== CARGA COMPLETA DE ARCHIVOS .DAT A SQLite ===")
    print("=" * 60)
    print("Este proceso cargará TODOS los archivos .dat de la carpeta")
    print("data_cache a la base de datos SQLite para máxima velocidad.")
    print("=" * 60)
    
    respuesta = input("¿Continuar con la carga completa? (s/n): ")
    
    if respuesta.lower() == 's':
        print("\n🔄 Iniciando carga...")
        cargar_a_sqlite()
        
        print("\n" + "=" * 60)
        print("✅ PROCESO COMPLETADO")
        print("=" * 60)
        print("Ahora tu aplicación tendrá acceso instantáneo a todos los")
        print("datos históricos desde archivos .dat locales.")
        print("\n👉 Para usar la aplicación:")
        print("   python app.py")
        print("=" * 60)
    else:
        print("\n❌ Carga cancelada")