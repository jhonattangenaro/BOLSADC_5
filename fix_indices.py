# fix_indices.py - Script para corregir los índices IBC
import sqlite3
import os
from datetime import datetime, timedelta

def crear_tabla_indices():
    """Crea la tabla indices si no existe"""
    db_path = "database/bolsa_datos.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Crear tabla indices
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS indices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            valor REAL NOT NULL,
            variacion REAL DEFAULT 0,
            fuente TEXT DEFAULT 'automatico',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fecha)
        )
    ''')
    
    # Crear tabla indices_manuales
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS indices_manuales (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            fecha TEXT NOT NULL,
            valor REAL NOT NULL,
            variacion REAL DEFAULT 0,
            fuente TEXT DEFAULT 'manual',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(fecha)
        )
    ''')
    
    # Crear índice para búsquedas rápidas
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_indices_fecha ON indices(fecha)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_indices_manuales_fecha ON indices_manuales(fecha)')
    
    conn.commit()
    conn.close()
    
    print("✅ Tablas de índices creadas")

def verificar_datos_indices():
    """Verifica si hay datos en la tabla indices"""
    db_path = "database/bolsa_datos.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Verificar si existe la tabla
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='indices'")
    tabla_existe = cursor.fetchone()
    
    if not tabla_existe:
        print("❌ La tabla 'indices' no existe")
        return False
    
    # Contar registros
    cursor.execute("SELECT COUNT(*) FROM indices")
    count = cursor.fetchone()[0]
    
    # Verificar algunos datos
    cursor.execute("SELECT fecha, valor FROM indices ORDER BY fecha DESC LIMIT 5")
    datos = cursor.fetchall()
    
    conn.close()
    
    print(f"📊 Tabla indices tiene {count} registros")
    
    if datos:
        print("📅 Últimos 5 registros:")
        for fecha, valor in datos:
            print(f"  {fecha}: {valor}")
    
    return count > 0

def poblar_datos_indices_desde_acciones():
    """Intenta crear datos del índice a partir de las acciones"""
    db_path = "database/bolsa_datos.db"
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Obtener todas las fechas únicas donde hay acciones
    cursor.execute('''
        SELECT DISTINCT fecha 
        FROM acciones 
        WHERE fecha IS NOT NULL AND fecha != ''
        ORDER BY fecha
    ''')
    
    fechas = [f[0] for f in cursor.fetchall()]
    
    print(f"📅 Encontradas {len(fechas)} fechas con acciones")
    
    # Para cada fecha, calcular un "índice simulado" basado en el promedio de precios
    for fecha in fechas:
        # Obtener precios del día
        cursor.execute('''
            SELECT AVG(hoy) as promedio_precio, COUNT(*) as cantidad
            FROM acciones 
            WHERE fecha = ? AND hoy > 0
        ''', (fecha,))
        
        resultado = cursor.fetchone()
        promedio = resultado[0] if resultado and resultado[0] else 0
        cantidad = resultado[1] if resultado else 0
        
        if promedio > 0 and cantidad > 5:  # Solo si hay suficientes datos
            # Obtener el promedio del día anterior para calcular variación
            try:
                fecha_dt = datetime.strptime(fecha, '%Y%m%d')
                fecha_anterior_dt = fecha_dt - timedelta(days=1)
                fecha_anterior = fecha_anterior_dt.strftime('%Y%m%d')
                
                cursor.execute('''
                    SELECT AVG(hoy) as promedio_anterior
                    FROM acciones 
                    WHERE fecha = ? AND hoy > 0
                ''', (fecha_anterior,))
                
                resultado_anterior = cursor.fetchone()
                promedio_anterior = resultado_anterior[0] if resultado_anterior and resultado_anterior[0] else promedio
                
                # Calcular variación
                variacion = ((promedio - promedio_anterior) / promedio_anterior * 100) if promedio_anterior > 0 else 0
                
                # Insertar en la tabla indices
                try:
                    cursor.execute('''
                        INSERT OR IGNORE INTO indices (fecha, valor, variacion, fuente)
                        VALUES (?, ?, ?, 'calculado')
                    ''', (fecha, round(promedio, 2), round(variacion, 2)))
                except:
                    pass
                    
            except:
                pass
    
    conn.commit()
    
    # Contar cuántos se insertaron
    cursor.execute("SELECT COUNT(*) FROM indices WHERE fuente = 'calculado'")
    insertados = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"✅ Insertados {insertados} registros de índice calculado")
    return insertados

def agregar_datos_indice_manual():
    """Agrega algunos datos de índice manuales para pruebas"""
    db_path = "database/bolsa_datos.db"
    
    datos_prueba = [
        ("20250105", 1250.50, 1.25),
        ("20250106", 1265.75, 1.22),
        ("20250107", 1278.90, 1.04),
        ("20250108", 1290.25, 0.89),
        ("20250109", 1305.60, 1.19),
    ]
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    for fecha, valor, variacion in datos_prueba:
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO indices (fecha, valor, variacion, fuente)
                VALUES (?, ?, ?, 'prueba')
            ''', (fecha, valor, variacion))
        except Exception as e:
            print(f"Error insertando {fecha}: {e}")
    
    conn.commit()
    conn.close()
    
    print(f"✅ Agregados {len(datos_prueba)} datos de prueba al índice")

def main():
    print("=" * 60)
    print("FIX INDICES IBC - CORRECCIÓN DE ÍNDICES")
    print("=" * 60)
    
    # 1. Crear tablas si no existen
    crear_tabla_indices()
    
    # 2. Verificar si hay datos
    if verificar_datos_indices():
        print("✅ Ya hay datos del índice, no es necesario poblar")
        return
    
    # 3. Poblar datos calculados desde acciones
    print("\n📈 Calculando índices desde datos de acciones...")
    poblar_datos_indices_desde_acciones()
    
    # 4. Agregar datos de prueba
    print("\n➕ Agregando datos de prueba...")
    agregar_datos_indice_manual()
    
    # 5. Verificar resultado final
    print("\n✅ Verificación final:")
    verificar_datos_indices()
    
    print("\n" + "=" * 60)
    print("✅ Proceso completado")
    print("👉 Ahora puedes ir a http://127.0.0.1:5000/indices")
    print("=" * 60)

if __name__ == "__main__":
    main()