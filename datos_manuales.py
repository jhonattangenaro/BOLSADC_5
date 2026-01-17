# datos_manuales.py - Módulo para gestionar datos manuales usando SQLite
import os
from datetime import datetime
from sqlite_manager import sqlite_manager  # NUEVO - Usamos SQLite

def crear_tablas_manuales():
    """Crea las tablas para datos manuales si no existen"""
    # Las tablas ya se crean automáticamente en sqlite_manager
    print("✅ Tablas para datos manuales verificadas en SQLite")

def obtener_nombre_real_accion(simbolo):
    """Busca el nombre real de una acción en la base de datos automática"""
    # Necesitamos acceder a la base de datos SQLite
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    if not os.path.exists(db_path):
        return simbolo.upper()
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Buscar en datos automáticos primero
        cursor.execute('''
            SELECT nombre 
            FROM acciones 
            WHERE simbolo = ? AND (fuente = 'automatico' OR fuente = 'archivo_dat')
            LIMIT 1
        ''', (simbolo.upper(),))
        
        resultado = cursor.fetchone()
        if resultado and resultado[0]:
            nombre = resultado[0]
            # Limpiar el nombre
            for sufijo in [' (Manual)', ' (archivo_dat)', ' (automatico)']:
                if sufijo in nombre:
                    nombre = nombre.replace(sufijo, '')
            
            # Si el nombre no es solo el símbolo, usarlo
            if nombre != simbolo.upper() and len(nombre) > 3:
                return nombre
        
        # Si no se encuentra, buscar en datos manuales
        cursor.execute('''
            SELECT nombre 
            FROM datos_manuales 
            WHERE simbolo = ? AND nombre != simbolo
            LIMIT 1
        ''', (simbolo.upper(),))
        
        resultado = cursor.fetchone()
        if resultado and resultado[0]:
            nombre = resultado[0]
            for sufijo in [' (Manual)', ' (archivo_dat)', ' (automatico)']:
                if sufijo in nombre:
                    nombre = nombre.replace(sufijo, '')
            
            if nombre != simbolo.upper() and len(nombre) > 3:
                return nombre
    
    finally:
        conn.close()
    
    # Si no se encuentra, devolver el símbolo
    return simbolo.upper()

def agregar_datos_manuales(fecha, datos_acciones, datos_indice=None):
    """
    Agrega datos manuales para una fecha específica usando SQLite
    
    Args:
        fecha: str en formato YYYYMMDD
        datos_acciones: lista de diccionarios con datos de acciones
        datos_indice: diccionario con datos del índice (opcional)
    """
    # Buscar nombres reales para cada acción
    for accion in datos_acciones:
        simbolo = accion.get('simbolo', '').upper()
        
        # Buscar nombre real de la acción
        nombre_real = obtener_nombre_real_accion(simbolo)
        
        accion['nombre'] = nombre_real
        accion['simbolo'] = simbolo
    
    # Guardar en SQLite
    insertados = sqlite_manager.insertar_datos_manuales(fecha, datos_acciones, datos_indice)
    
    if insertados > 0:
        print(f"✅ {insertados} acciones manuales agregadas para {fecha}")
        if datos_acciones:
            print(f"   Ejemplo: {datos_acciones[0].get('simbolo')} → {datos_acciones[0].get('nombre')}")
        return True
    else:
        print(f"❌ Error agregando datos manuales para {fecha}")
        return False

def obtener_datos_manuales(fecha):
    """Obtiene datos manuales para una fecha específica desde SQLite"""
    return sqlite_manager.obtener_datos_manuales(fecha)

def listar_fechas_con_datos_manuales():
    """Lista todas las fechas que tienen datos manuales desde SQLite"""
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    if not os.path.exists(db_path):
        return []
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT DISTINCT fecha 
            FROM datos_manuales 
            ORDER BY fecha DESC
        ''')
        
        fechas = [fila[0] for fila in cursor.fetchall()]
        return fechas
        
    finally:
        conn.close()

def eliminar_datos_manuales(fecha):
    """Elimina datos manuales para una fecha específica desde SQLite"""
    return sqlite_manager.eliminar_datos_manuales(fecha)

def verificar_fecha_con_datos(fecha):
    """Verifica qué tipo de datos existen para una fecha"""
    from extractor import descargar_y_guardar
    
    # Primero verificar datos automáticos (SQLite)
    datos_auto, indice_auto = descargar_y_guardar(fecha)
    
    # Luego verificar datos manuales (SQLite)
    datos_manual, indice_manual = obtener_datos_manuales(fecha)
    
    resultado = {
        'fecha': fecha,
        'automatico': len(datos_auto) > 0 if datos_auto else False,
        'manual': datos_manual is not None and len(datos_manual) > 0,
        'total_automatico': len(datos_auto) if datos_auto else 0,
        'total_manual': len(datos_manual) if datos_manual else 0,
        'fuente_automatico': datos_auto[0].get('fuente', 'desconocida') if datos_auto and len(datos_auto) > 0 else None,
        'fuente_manual': 'manual' if datos_manual and len(datos_manual) > 0 else None
    }
    
    return resultado

def obtener_acciones_manuales_por_simbolo(simbolo):
    """Obtiene todas las acciones manuales para un símbolo específico desde SQLite"""
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    if not os.path.exists(db_path):
        return []
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT fecha, simbolo, nombre, anterior, hoy, diferencia_bs, 
                   variacion, cantidad, monto, fuente
            FROM datos_manuales 
            WHERE simbolo = ?
            ORDER BY fecha
        ''', (simbolo.upper(),))
        
        columnas = [desc[0] for desc in cursor.description]
        acciones = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
        
        return acciones
        
    finally:
        conn.close()

def obtener_todas_acciones_manuales():
    """Obtiene todas las acciones ingresadas manualmente desde SQLite"""
    import sqlite3
    
    db_path = "database/bolsa_datos.db"
    if not os.path.exists(db_path):
        return []
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        cursor.execute('''
            SELECT fecha, simbolo, nombre, anterior, hoy, diferencia_bs, 
                   variacion, cantidad, monto, fuente
            FROM datos_manuales 
            ORDER BY fecha DESC, simbolo
        ''')
        
        columnas = [desc[0] for desc in cursor.description]
        acciones = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
        
        return acciones
        
    finally:
        conn.close()

def corregir_nombres_manuales():
    """Corrige todos los nombres de acciones manuales usando nombres reales"""
    print("🔧 Corrigiendo nombres de acciones manuales en SQLite...")
    
    # Obtener todas las acciones manuales
    acciones_manuales = obtener_todas_acciones_manuales()
    
    if not acciones_manuales:
        print("⚠️  No hay acciones manuales para corregir")
        return 0
    
    print(f"📊 Encontradas {len(acciones_manuales)} acciones manuales")
    
    import sqlite3
    db_path = "database/bolsa_datos.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    corregidas = 0
    simbolos_procesados = set()
    
    try:
        for accion in acciones_manuales:
            simbolo = accion['simbolo'].upper()
            
            if simbolo in simbolos_procesados:
                continue
                
            simbolos_procesados.add(simbolo)
            
            # Buscar nombre real
            nombre_real = obtener_nombre_real_accion(simbolo)
            nombre_actual = accion.get('nombre', simbolo)
            
            if nombre_actual != nombre_real:
                # Actualizar en base de datos
                cursor.execute('''
                    UPDATE datos_manuales 
                    SET nombre = ?
                    WHERE simbolo = ?
                ''', (nombre_real, simbolo))
                
                # Contar cuántas se actualizaron
                cursor.execute('SELECT changes()')
                actualizadas = cursor.fetchone()[0]
                
                if actualizadas > 0:
                    print(f"✅ {simbolo}: '{nombre_actual}' → '{nombre_real}' ({actualizadas} registros)")
                    corregidas += actualizadas
        
        conn.commit()
        
    finally:
        conn.close()
    
    print(f"\n✅ Corrección completada: {corregidas} registros actualizados")
    return corregidas

if __name__ == "__main__":
    # Ejemplo de uso
    crear_tablas_manuales()
    
    # Corrección automática de nombres
    print("\n=== CORRECCIÓN DE NOMBRES MANUALES (SQLite) ===")
    corregidos = corregir_nombres_manuales()
    
    print("\nMódulo de datos manuales cargado correctamente")
    print("Funciones disponibles:")
    print("  - agregar_datos_manuales(fecha, datos_acciones, datos_indice)")
    print("  - obtener_datos_manuales(fecha)")
    print("  - listar_fechas_con_datos_manuales()")
    print("  - eliminar_datos_manuales(fecha)")
    print("  - verificar_fecha_con_datos(fecha)")
    print("  - obtener_acciones_manuales_por_simbolo(simbolo)")
    print("  - obtener_todas_acciones_manuales()")
    print("  - corregir_nombres_manuales()")