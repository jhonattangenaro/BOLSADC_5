#!/usr/bin/env python3
# migrate_to_sqlite.py - Migra datos de TinyDB (JSON) a SQLite

import sqlite3
import json
import os
from datetime import datetime

def migrate_to_sqlite():
    print("=" * 60)
    print("=== MIGRACIÓN DE TINYDB (JSON) A SQLITE ===")
    print("=" * 60)
    
    # Rutas de archivos
    json_db_path = "database/bolsa_datos.json"
    sqlite_db_path = "database/bolsa_datos.db"
    
    # Verificar si existe el archivo JSON
    if not os.path.exists(json_db_path):
        print(f"❌ No se encuentra el archivo JSON: {json_db_path}")
        return False
    
    # Crear directorio si no existe
    os.makedirs("database", exist_ok=True)
    
    # Conectar a SQLite
    print(f"📦 Creando base de datos SQLite: {sqlite_db_path}")
    conn = sqlite3.connect(sqlite_db_path)
    cursor = conn.cursor()
    
    # Crear tablas optimizadas
    print("🔧 Creando tablas optimizadas...")
    
    # Tabla de acciones
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
    
    # Tabla de índices
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
    
    # Tabla de datos manuales
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS datos_manuales (
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
    
    # Tabla de índices manuales
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS indices_manuales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha TEXT NOT NULL,
        valor REAL,
        variacion REAL,
        fuente TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(fecha)
    )
    ''')
    
    # Crear índices para consultas rápidas
    print("⚡ Creando índices para optimización...")
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_fecha ON acciones(fecha)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_simbolo ON acciones(simbolo)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_fecha_simbolo ON acciones(fecha, simbolo)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_datos_manuales_fecha ON datos_manuales(fecha)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_datos_manuales_simbolo ON datos_manuales(simbolo)')
    
    conn.commit()
    
    # Cargar datos del JSON
    print("📊 Cargando datos desde JSON...")
    import tinydb
    from tinydb import TinyDB, Query
    
    json_db = TinyDB(json_db_path)
    
    # Función para insertar datos con manejo de errores
    def insertar_datos(tabla_json, tabla_sql, cursor):
        if tabla_json in json_db.tables():
            datos = json_db.table(tabla_json).all()
            print(f"  📁 {tabla_json}: {len(datos)} registros")
            
            insertados = 0
            for doc in datos:
                try:
                    # Convertir a tupla según la tabla
                    if tabla_json == 'acciones' or tabla_json == 'datos_manuales':
                        cursor.execute(f'''
                            INSERT OR REPLACE INTO {tabla_sql} 
                            (fecha, simbolo, nombre, anterior, hoy, diferencia_bs, variacion, cantidad, monto, fuente)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            doc.get('fecha', ''),
                            doc.get('simbolo', ''),
                            doc.get('nombre', ''),
                            doc.get('anterior', 0),
                            doc.get('hoy', 0),
                            doc.get('diferencia_bs', 0),
                            doc.get('variacion', 0),
                            doc.get('cantidad', 0),
                            doc.get('monto', 0),
                            doc.get('fuente', '')
                        ))
                    elif tabla_json == 'indices' or tabla_json == 'indices_manuales':
                        cursor.execute(f'''
                            INSERT OR REPLACE INTO {tabla_sql} 
                            (fecha, valor, variacion, fuente)
                            VALUES (?, ?, ?, ?)
                        ''', (
                            doc.get('fecha', ''),
                            doc.get('valor', 0),
                            doc.get('variacion', 0),
                            doc.get('fuente', '')
                        ))
                    insertados += 1
                    
                    if insertados % 1000 == 0:
                        print(f"    ✅ {insertados}/{len(datos)}")
                        
                except Exception as e:
                    print(f"    ⚠️  Error con registro {doc.get('simbolo', 'N/A')}: {e}")
            
            conn.commit()
            print(f"    ✅ {insertados} registros insertados en {tabla_sql}")
            return insertados
        return 0
    
    # Migrar todas las tablas
    total_registros = 0
    
    print("\n📋 Migrando tablas:")
    print("-" * 40)
    
    total_registros += insertar_datos('acciones', 'acciones', cursor)
    total_registros += insertar_datos('indices', 'indices', cursor)
    total_registros += insertar_datos('datos_manuales', 'datos_manuales', cursor)
    total_registros += insertar_datos('indices_manuales', 'indices_manuales', cursor)
    
    # Estadísticas finales
    print("\n" + "=" * 60)
    print("📈 ESTADÍSTICAS FINALES")
    print("-" * 60)
    
    cursor.execute("SELECT COUNT(*) FROM acciones")
    acc_count = cursor.fetchone()[0]
    print(f"📊 Acciones: {acc_count} registros")
    
    cursor.execute("SELECT COUNT(*) FROM indices")
    idx_count = cursor.fetchone()[0]
    print(f"📈 Índices: {idx_count} registros")
    
    cursor.execute("SELECT COUNT(*) FROM datos_manuales")
    man_count = cursor.fetchone()[0]
    print(f"📝 Datos manuales: {man_count} registros")
    
    # Optimizar base de datos
    print("\n⚡ Optimizando base de datos SQLite...")
    cursor.execute("VACUUM")
    cursor.execute("PRAGMA optimize")
    
    # Cerrar conexión
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ MIGRACIÓN COMPLETADA EXITOSAMENTE")
    print("=" * 60)
    print(f"📦 Archivo SQLite creado: {sqlite_db_path}")
    print(f"💾 Tamaño aproximado: {os.path.getsize(sqlite_db_path) / 1024 / 1024:.2f} MB")
    print("\n🎯 BENEFICIOS DE SQLite:")
    print("   • 10-100x más rápido en consultas")
    print("   • Menor uso de memoria")
    print("   • Transacciones ACID")
    print("   • Índices para búsquedas instantáneas")
    print("   • Escalabilidad mejorada")
    print("=" * 60)
    
    return True

if __name__ == "__main__":
    migrate_to_sqlite()