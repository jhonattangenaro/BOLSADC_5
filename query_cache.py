# query_cache.py - Caché específico para consultas frecuentes
import hashlib
import json
from datetime import datetime, timedelta
import time

class QueryCache:
    def __init__(self):
        self.query_cache = {}  # {hash: resultado}
        self.max_cache_size = 100  # Máximo 100 consultas en caché
        self.cache_hits = 0
        self.cache_misses = 0
        
    def _generate_hash(self, simbolo, fecha_desde, fecha_hasta):
        """Genera un hash único para la consulta."""
        key = f"{simbolo.upper()}_{fecha_desde}_{fecha_hasta}"
        return hashlib.md5(key.encode()).hexdigest()
    
    def get_cached_query(self, simbolo, fecha_desde, fecha_hasta):
        """Obtiene consulta del caché si existe."""
        query_hash = self._generate_hash(simbolo, fecha_desde, fecha_hasta)
        
        if query_hash in self.query_cache:
            # Verificar si el caché no ha expirado (1 hora)
            cache_entry = self.query_cache[query_hash]
            cache_time = cache_entry['timestamp']
            current_time = datetime.now()
            
            if (current_time - cache_time).total_seconds() < 3600:  # 1 hora
                self.cache_hits += 1
                cache_entry['hits'] = cache_entry.get('hits', 0) + 1
                cache_entry['last_accessed'] = datetime.now()
                
                print(f"⚡ CONSULTA CACHEADA: {simbolo} ({fecha_desde} a {fecha_hasta})")
                print(f"   📊 {len(cache_entry['data'])} registros, Hits: {cache_entry['hits']}")
                
                return cache_entry['data']
        
        self.cache_misses += 1
        return None
    
    def cache_query(self, simbolo, fecha_desde, fecha_hasta, data):
        """Guarda una consulta en el caché."""
        if not data:
            return
        
        query_hash = self._generate_hash(simbolo, fecha_desde, fecha_hasta)
        
        # Limpiar caché si es muy grande
        if len(self.query_cache) >= self.max_cache_size:
            # Eliminar el menos usado o más antiguo
            oldest_key = min(self.query_cache.keys(), 
                           key=lambda k: self.query_cache[k].get('last_accessed', 
                                                                self.query_cache[k]['timestamp']))
            removed = self.query_cache.pop(oldest_key)
            print(f"🗑️  Caché eliminado: {removed.get('simbolo')} ({removed.get('hits', 0)} hits)")
        
        # Guardar en caché
        self.query_cache[query_hash] = {
            'timestamp': datetime.now(),
            'last_accessed': datetime.now(),
            'data': data,
            'simbolo': simbolo.upper(),
            'fecha_desde': fecha_desde,
            'fecha_hasta': fecha_hasta,
            'count': len(data),
            'hits': 0
        }
        
        print(f"💾 Consulta guardada en caché: {simbolo} ({len(data)} registros)")
    
    def clear_query_cache(self):
        """Limpia el caché de consultas."""
        cleared = len(self.query_cache)
        self.query_cache.clear()
        print(f"🧹 Caché de consultas limpiado ({cleared} consultas eliminadas)")
    
    def get_cache_stats(self):
        """Obtiene estadísticas del caché de consultas."""
        total_hits = sum(q.get('hits', 0) for q in self.query_cache.values())
        
        return {
            'total_queries_cached': len(self.query_cache),
            'cache_hits': self.cache_hits,
            'cache_misses': self.cache_misses,
            'total_hits_all_queries': total_hits,
            'hit_rate': self.cache_hits / (self.cache_hits + self.cache_misses) 
                        if (self.cache_hits + self.cache_misses) > 0 else 0,
            'most_frequent': self._get_most_frequent(),
            'recent_queries': self._get_recent_queries()
        }
    
    def _get_most_frequent(self):
        """Obtiene las consultas más frecuentes."""
        if not self.query_cache:
            return []
        
        sorted_queries = sorted(self.query_cache.items(), 
                               key=lambda x: x[1].get('hits', 0), 
                               reverse=True)[:5]
        
        return [{
            'simbolo': q[1]['simbolo'],
            'periodo': f"{q[1]['fecha_desde']} a {q[1]['fecha_hasta']}",
            'hits': q[1].get('hits', 0),
            'count': q[1]['count'],
            'last_accessed': q[1]['last_accessed'].strftime('%H:%M:%S')
        } for q in sorted_queries]
    
    def _get_recent_queries(self):
        """Obtiene las consultas recientes."""
        if not self.query_cache:
            return []
        
        sorted_queries = sorted(self.query_cache.items(), 
                               key=lambda x: x[1]['last_accessed'], 
                               reverse=True)[:5]
        
        return [{
            'simbolo': q[1]['simbolo'],
            'periodo': f"{q[1]['fecha_desde']} a {q[1]['fecha_hasta']}",
            'last_accessed': q[1]['last_accessed'].strftime('%H:%M:%S'),
            'hits': q[1].get('hits', 0)
        } for q in sorted_queries]

# Instancia global
query_cache = QueryCache()