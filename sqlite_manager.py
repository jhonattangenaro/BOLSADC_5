
# sqlite_manager.py - Gestor de base de datos SQLite optimizado

import sqlite3
import os
from datetime import datetime, timedelta
import threading
import time

class SQLiteManager:
    def __init__(self, db_path="database/bolsa_datos.db"):
        self.db_path = db_path
        self.cache_dir = "cache"
        
        # Crear directorios si no existen
        os.makedirs("database", exist_ok=True)
        os.makedirs(self.cache_dir, exist_ok=True)
        
        # Caché en memoria
        self.memory_cache = {}
        self.query_cache = {}  # Caché específico para consultas históricas
        self.cache_lock = threading.Lock()
        
        # Inicializar base de datos
        self.init_database()
        
    def init_database(self):
        """Inicializa la base de datos SQLite con tablas optimizadas"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Crear tablas si no existen (solo estructura básica)
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
        
        # Crear índices para máxima velocidad
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_fecha ON acciones(fecha)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_simbolo ON acciones(simbolo)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_acciones_fecha_simbolo ON acciones(fecha, simbolo)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_datos_manuales_fecha ON datos_manuales(fecha)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_datos_manuales_simbolo ON datos_manuales(simbolo)')
        
        conn.commit()
        conn.close()
        
    def get_connection(self):
        """Obtiene una conexión a la base de datos"""
        return sqlite3.connect(self.db_path, check_same_thread=False)
    
    # ========== MÉTODOS PARA ACCIONES ==========
    
    def obtener_acciones_por_fecha(self, fecha_str):
        """Obtiene acciones para una fecha específica (MUY RÁPIDO)"""
        # Verificar caché primero
        cache_key = f"acciones_{fecha_str}"
        with self.cache_lock:
            if cache_key in self.memory_cache:
                return self.memory_cache[cache_key]
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Consulta optimizada con SQLite
            cursor.execute('''
                SELECT fecha, simbolo, nombre, anterior, hoy, diferencia_bs, 
                       variacion, cantidad, monto, fuente
                FROM acciones 
                WHERE fecha = ?
                UNION
                SELECT fecha, simbolo, nombre, anterior, hoy, diferencia_bs, 
                       variacion, cantidad, monto, fuente
                FROM datos_manuales 
                WHERE fecha = ?
                ORDER BY simbolo
            ''', (fecha_str, fecha_str))
            
            columnas = [desc[0] for desc in cursor.description]
            resultados = []
            
            for fila in cursor.fetchall():
                resultado = dict(zip(columnas, fila))
                resultados.append(resultado)
            
            # Guardar en caché
            with self.cache_lock:
                self.memory_cache[cache_key] = resultados
                # Limitar tamaño del caché
                if len(self.memory_cache) > 100:
                    # Eliminar el más antiguo
                    self.memory_cache.pop(next(iter(self.memory_cache)))
            
            return resultados
            
        finally:
            conn.close()
    
    def obtener_historico_simbolo(self, simbolo, fecha_desde, fecha_hasta):
        """Obtiene histórico de un símbolo (EXTREMADAMENTE RÁPIDO)"""
        cache_key = f"historico_{simbolo}_{fecha_desde}_{fecha_hasta}"
        
        with self.cache_lock:
            if cache_key in self.query_cache:
                return self.query_cache[cache_key]
        
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # CONSULTA OPTIMIZADA CON SQLite - MODIFICADA: ORDER BY fecha DESC
            cursor.execute('''
                SELECT fecha, simbolo, nombre, hoy as precio, variacion, 
                       diferencia_bs as cambio_bs, cantidad, monto, anterior, fuente
                FROM (
                    SELECT fecha, simbolo, nombre, hoy, variacion, diferencia_bs, 
                           cantidad, monto, anterior, fuente, 1 as orden
                    FROM acciones 
                    WHERE simbolo = ? AND fecha BETWEEN ? AND ?
                    UNION ALL
                    SELECT fecha, simbolo, nombre, hoy, variacion, diferencia_bs, 
                           cantidad, monto, anterior, fuente, 2 as orden
                    FROM datos_manuales 
                    WHERE simbolo = ? AND fecha BETWEEN ? AND ?
                )
                ORDER BY fecha DESC, orden  -- MODIFICADO: DESC para fechas más recientes primero
            ''', (simbolo.upper(), fecha_desde, fecha_hasta, 
                  simbolo.upper(), fecha_desde, fecha_hasta))
            
            columnas = [desc[0] for desc in cursor.description]
            resultados = []
            
            for fila in cursor.fetchall():
                resultado = dict(zip(columnas, fila))
                # Formatear fecha
                if resultado['fecha'] and len(resultado['fecha']) == 8:
                    fecha_dt = datetime.strptime(resultado['fecha'], '%Y%m%d')
                    resultado['fecha_formateada'] = fecha_dt.strftime('%d/%m/%Y')
                resultados.append(resultado)
            
            # Guardar en caché de consultas
            with self.cache_lock:
                self.query_cache[cache_key] = resultados
                # Limitar tamaño
                if len(self.query_cache) > 50:
                    self.query_cache.pop(next(iter(self.query_cache)))
            
            return resultados
            
        finally:
            conn.close()
    
    def insertar_acciones(self, fecha_str, acciones_data):
        """Inserta múltiples acciones en la base de datos"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            insertados = 0
            for accion in acciones_data:
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
                        accion.get('fuente', 'automatico')
                    ))
                    insertados += 1
                except Exception as e:
                    print(f"Error insertando acción {accion.get('simbolo', '')}: {e}")
            
            conn.commit()
            
            # Limpiar caché para esta fecha
            cache_key = f"acciones_{fecha_str}"
            with self.cache_lock:
                if cache_key in self.memory_cache:
                    del self.memory_cache[cache_key]
            
            return insertados
            
        finally:
            conn.close()
    
    # ========== MÉTODOS PARA ÍNDICES ==========
    
    def obtener_indice_por_fecha(self, fecha_str):
        """Obtiene el índice para una fecha específica"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Intentar primero con datos manuales
            cursor.execute('''
                SELECT fecha, valor, variacion, fuente
                FROM indices_manuales 
                WHERE fecha = ?
                UNION
                SELECT fecha, valor, variacion, fuente
                FROM indices 
                WHERE fecha = ?
                LIMIT 1
            ''', (fecha_str, fecha_str))
            
            fila = cursor.fetchone()
            if fila:
                columnas = [desc[0] for desc in cursor.description]
                return dict(zip(columnas, fila))
            
            return None
            
        finally:
            conn.close()
    
    def insertar_indice(self, fecha_str, indice_data):
        """Inserta un índice en la base de datos"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('''
                INSERT OR REPLACE INTO indices 
                (fecha, valor, variacion, fuente)
                VALUES (?, ?, ?, ?)
            ''', (
                fecha_str,
                indice_data.get('valor', 0),
                indice_data.get('variacion', 0),
                indice_data.get('fuente', 'automatico')
            ))
            
            conn.commit()
            return True
            
        finally:
            conn.close()
    
    # ========== MÉTODOS PARA DATOS MANUALES ==========
    
    def insertar_datos_manuales(self, fecha_str, acciones_data, indice_data=None):
        """Inserta datos manuales en la base de datos"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Insertar acciones manuales
            insertados = 0
            for accion in acciones_data:
                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO datos_manuales 
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
                        'manual'
                    ))
                    insertados += 1
                except Exception as e:
                    print(f"Error insertando acción manual {accion.get('simbolo', '')}: {e}")
            
            # Insertar índice manual
            if indice_data:
                cursor.execute('''
                    INSERT OR REPLACE INTO indices_manuales 
                    (fecha, valor, variacion, fuente)
                    VALUES (?, ?, ?, ?)
                ''', (
                    fecha_str,
                    indice_data.get('valor', 0),
                    indice_data.get('variacion', 0),
                    'manual'
                ))
            
            conn.commit()
            
            # Limpiar cachés
            with self.cache_lock:
                cache_key = f"acciones_{fecha_str}"
                if cache_key in self.memory_cache:
                    del self.memory_cache[cache_key]
                # Limpiar caché de consultas que puedan incluir esta fecha
                keys_to_remove = [k for k in self.query_cache.keys() if fecha_str in k]
                for k in keys_to_remove:
                    del self.query_cache[k]
            
            return insertados
            
        finally:
            conn.close()
    
    def obtener_datos_manuales(self, fecha_str):
        """Obtiene datos manuales para una fecha"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Acciones manuales
            cursor.execute('''
                SELECT fecha, simbolo, nombre, anterior, hoy, diferencia_bs, 
                       variacion, cantidad, monto, fuente
                FROM datos_manuales 
                WHERE fecha = ?
                ORDER BY simbolo
            ''', (fecha_str,))
            
            columnas = [desc[0] for desc in cursor.description]
            acciones = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
            
            # Índice manual
            cursor.execute('''
                SELECT fecha, valor, variacion, fuente
                FROM indices_manuales 
                WHERE fecha = ?
            ''', (fecha_str,))
            
            fila_indice = cursor.fetchone()
            indice = dict(zip([desc[0] for desc in cursor.description], fila_indice)) if fila_indice else None
            
            return acciones, indice
            
        finally:
            conn.close()
    
    def eliminar_datos_manuales(self, fecha_str):
        """Elimina datos manuales para una fecha"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute('DELETE FROM datos_manuales WHERE fecha = ?', (fecha_str,))
            cursor.execute('DELETE FROM indices_manuales WHERE fecha = ?', (fecha_str,))
            
            conn.commit()
            
            # Limpiar cachés
            with self.cache_lock:
                cache_key = f"acciones_{fecha_str}"
                if cache_key in self.memory_cache:
                    del self.memory_cache[cache_key]
            
            return True
            
        finally:
            conn.close()
    
    # ========== MÉTODOS DE CACHÉ Y OPTIMIZACIÓN ==========
    
    def precargar_cache(self, dias=30):
        """Precarga datos recientes en caché"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            # Obtener las fechas más recientes
            cursor.execute('''
                SELECT DISTINCT fecha 
                FROM acciones 
                ORDER BY fecha DESC 
                LIMIT ?
            ''', (dias,))
            
            fechas = [fila[0] for fila in cursor.fetchall()]
            
            with self.cache_lock:
                for fecha in fechas:
                    cache_key = f"acciones_{fecha}"
                    if cache_key not in self.memory_cache:
                        # Cargar datos
                        cursor.execute('''
                            SELECT fecha, simbolo, nombre, anterior, hoy, diferencia_bs, 
                                   variacion, cantidad, monto, fuente
                            FROM acciones 
                            WHERE fecha = ?
                            ORDER BY simbolo
                        ''', (fecha,))
                        
                        columnas = [desc[0] for desc in cursor.description]
                        resultados = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
                        
                        self.memory_cache[cache_key] = resultados
            
            print(f"✅ Precargadas {len(fechas)} fechas en caché")
            
        finally:
            conn.close()
    
    def limpiar_cache(self):
        """Limpia el caché en memoria"""
        with self.cache_lock:
            self.memory_cache.clear()
            self.query_cache.clear()
        print("🧹 Caché limpiado")
    
    def estadisticas(self):
        """Muestra estadísticas de la base de datos"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM acciones")
            total_acciones = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM indices")
            total_indices = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM datos_manuales")
            total_manuales = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT fecha) FROM acciones")
            fechas_unicas = cursor.fetchone()[0]
            
            with self.cache_lock:
                return {
                    'total_acciones': total_acciones,
                    'total_indices': total_indices,
                    'total_manuales': total_manuales,
                    'fechas_unicas': fechas_unicas,
                    'fechas_en_cache': len(self.memory_cache),
                    'consultas_en_cache': len(self.query_cache),
                    'db_size_mb': os.path.getsize(self.db_path) / 1024 / 1024 if os.path.exists(self.db_path) else 0
                }
                
        finally:
            conn.close()

# Instancia global
sqlite_manager = SQLiteManager()