# cache_manager.py - Sistema de caché optimizado para consultas históricas

from tinydb import TinyDB, Query
from datetime import datetime, timedelta
import os
import threading

class CacheManager:
    def __init__(self):
        self.db_dir = "database"
        if not os.path.exists(self.db_dir):
            os.makedirs(self.db_dir)
        
        self.db = TinyDB(os.path.join(self.db_dir, 'bolsa_datos.json'))
        self.table_acciones = self.db.table('acciones')
        self.table_indices = self.db.table('indices')
        self.query = Query()
        
        # Cache en memoria para acelerar consultas frecuentes
        self.memory_cache = {}
        self.cache_lock = threading.Lock()
        
    def obtener_datos_fecha(self, fecha_str):
        """Obtiene datos de una fecha específica con caché en memoria."""
        # Verificar caché en memoria primero
        if fecha_str in self.memory_cache:
            return self.memory_cache[fecha_str]
        
        # Buscar en base de datos
        acciones = self.table_acciones.search(self.query.fecha == fecha_str)
        
        if acciones:
            # Convertir a diccionarios
            resultado = [dict(doc) for doc in acciones]
            
            # Guardar en caché de memoria (máximo 100 fechas)
            with self.cache_lock:
                if len(self.memory_cache) > 100:
                    # Eliminar el más antiguo (FIFO)
                    self.memory_cache.pop(next(iter(self.memory_cache)))
                self.memory_cache[fecha_str] = resultado
            
            return resultado
        
        return []
    
    def obtener_datos_rango_fechas(self, fechas):
        """Obtiene datos para un rango de fechas en una sola consulta."""
        if not fechas:
            return {}
        
        # Optimización: obtener todos los datos en una sola consulta
        acciones = self.table_acciones.search(self.query.fecha.one_of(fechas))
        
        # Organizar por fecha para acceso rápido
        datos_por_fecha = {}
        for accion in acciones:
            fecha = accion['fecha']
            if fecha not in datos_por_fecha:
                datos_por_fecha[fecha] = []
            datos_por_fecha[fecha].append(dict(accion))
        
        return datos_por_fecha
    
    def obtener_historico_simbolo(self, simbolo, fechas):
        """Obtiene histórico de un símbolo específico optimizado."""
        if not fechas:
            return []
        
        # Consulta optimizada: buscar todas las fechas del símbolo en una sola operación
        acciones = self.table_acciones.search(
            (self.query.simbolo == simbolo.upper()) & 
            (self.query.fecha.one_of(fechas))
        )
        
        # Ordenar por fecha
        acciones_ordenadas = sorted(acciones, key=lambda x: x['fecha'])
        return [dict(doc) for doc in acciones_ordenadas]
    
    def precargar_cache_rango(self, fecha_inicio, fecha_fin):
        """Precarga en caché un rango de fechas completo."""
        # Generar todas las fechas del rango
        fecha_actual = fecha_inicio
        fechas = []
        
        while fecha_actual <= fecha_fin:
            fechas.append(fecha_actual.strftime('%Y%m%d'))
            fecha_actual += timedelta(days=1)
        
        # Precargar en caché de memoria
        with self.cache_lock:
            for fecha_str in fechas:
                if fecha_str not in self.memory_cache:
                    acciones = self.table_acciones.search(self.query.fecha == fecha_str)
                    if acciones:
                        self.memory_cache[fecha_str] = [dict(doc) for doc in acciones]
        
        print(f"✅ Precargadas {len(fechas)} fechas en caché")
    
    def limpiar_cache(self):
        """Limpia el caché en memoria."""
        with self.cache_lock:
            self.memory_cache.clear()
        print("🧹 Caché limpiado")
    
    def estadisticas_cache(self):
        """Muestra estadísticas del caché."""
        return {
            'fechas_en_cache': len(self.memory_cache),
            'total_registros': sum(len(v) for v in self.memory_cache.values())
        }

# Instancia global del caché
cache_manager = CacheManager()