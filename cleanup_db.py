#!/usr/bin/env python3
# cleanup_db.py - Script para limpiar datos problemáticos de la base de datos

from tinydb import TinyDB, Query
import os

print("=== LIMPIADOR DE BASE DE DATOS BVC ===")

DB_DIR = "database"
if not os.path.exists(DB_DIR):
    print(f"ERROR: No existe el directorio {DB_DIR}")
    exit(1)

db = TinyDB(os.path.join(DB_DIR, 'bolsa_datos.json'))
table_acciones = db.table('acciones')
table_indices = db.table('indices')
Registro = Query()

# Fechas problemáticas identificadas
fechas_problematicas = ['20251229', '20251230']

print(f"Buscando datos de las fechas: {fechas_problematicas}")

for fecha in fechas_problematicas:
    # Contar registros antes de eliminar
    acciones_count = len(table_acciones.search(Registro.fecha == fecha))
    indices_count = len(table_indices.search(Registro.fecha == fecha))
    
    print(f"\nFecha: {fecha}")
    print(f"  Acciones encontradas: {acciones_count}")
    print(f"  Índices encontrados: {indices_count}")
    
    if acciones_count > 0:
        # Mostrar algunos datos antes de eliminar
        registros = table_acciones.search(Registro.fecha == fecha)
        print(f"  Ejemplo de datos a eliminar:")
        for i, reg in enumerate(registros[:3]):  # Mostrar primeros 3
            print(f"    {reg.get('simbolo', 'N/A')}: anterior={reg.get('anterior', 0)}, hoy={reg.get('hoy', 0)}, variacion={reg.get('variacion', 0)}%")
        
        # Preguntar confirmación
        respuesta = input(f"  ¿Eliminar {acciones_count} registros de acciones? (s/n): ")
        if respuesta.lower() == 's':
            table_acciones.remove(Registro.fecha == fecha)
            print(f"  ✓ {acciones_count} registros de acciones eliminados")
        else:
            print(f"  ✗ Eliminación cancelada")
    
    if indices_count > 0:
        respuesta = input(f"  ¿Eliminar {indices_count} registros de índices? (s/n): ")
        if respuesta.lower() == 's':
            table_indices.remove(Registro.fecha == fecha)
            print(f"  ✓ {indices_count} registros de índices eliminados")
        else:
            print(f"  ✗ Eliminación cancelada")

# Opción para eliminar otras fechas manualmente
print("\n=== ELIMINACIÓN MANUAL ===")
fecha_manual = input("Ingrese una fecha específica para eliminar (formato YYYYMMDD) o Enter para salir: ")

if fecha_manual:
    acciones_count = len(table_acciones.search(Registro.fecha == fecha_manual))
    indices_count = len(table_indices.search(Registro.fecha == fecha_manual))
    
    print(f"\nFecha: {fecha_manual}")
    print(f"  Acciones encontradas: {acciones_count}")
    print(f"  Índices encontrados: {indices_count}")
    
    if acciones_count > 0:
        respuesta = input(f"  ¿Eliminar {acciones_count} registros de acciones? (s/n): ")
        if respuesta.lower() == 's':
            table_acciones.remove(Registro.fecha == fecha_manual)
            print(f"  ✓ {acciones_count} registros de acciones eliminados")
    
    if indices_count > 0:
        respuesta = input(f"  ¿Eliminar {indices_count} registros de índices? (s/n): ")
        if respuesta.lower() == 's':
            table_indices.remove(Registro.fecha == fecha_manual)
            print(f"  ✓ {indices_count} registros de índices eliminados")

print("\n=== RESUMEN FINAL ===")
print(f"Total de acciones en BD: {len(table_acciones)}")
print(f"Total de índices en BD: {len(table_indices)}")
print("\n✓ Proceso completado. La próxima vez que ejecutes la app, se descargarán los datos frescos.")