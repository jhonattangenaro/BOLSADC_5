#!/usr/bin/env python3
# corregir_nombres.py - Corrige los nombres de acciones manuales

from tinydb import TinyDB, Query
import os
import sys

# Agregar el directorio actual al path para importar módulos
sys.path.append('.')

from datos_manuales import corregir_nombres_manuales, crear_tablas_manuales

def main():
    print("=" * 60)
    print("=== CORRECCIÓN DE NOMBRES DE ACCIONES MANUALES ===")
    print("=" * 60)
    
    # Verificar que exista la base de datos
    DB_DIR = "database"
    if not os.path.exists(DB_DIR):
        print(f"❌ No existe el directorio {DB_DIR}")
        exit(1)
    
    # Crear tablas si no existen
    crear_tablas_manuales()
    
    # Corregir nombres
    print("\n🔍 Buscando acciones manuales para corregir...")
    corregidas = corregir_nombres_manuales()
    
    if corregidas == 0:
        print("\n✅ No se encontraron nombres para corregir")
    else:
        print(f"\n🎉 ¡Corrección completada! {corregidas} nombres actualizados")
    
    # Mostrar ejemplo de correcciones
    print("\n" + "=" * 60)
    print("📋 EJEMPLOS DE CORRECCIONES:")
    print("=" * 60)
    
    db = TinyDB(os.path.join(DB_DIR, 'bolsa_datos.json'))
    if 'datos_manuales' not in db.tables():
        print("No hay datos manuales para mostrar")
        return
    
    table_manual = db.table('datos_manuales')
    Registro = Query()
    
    # Mostrar algunos ejemplos
    acciones = table_manual.all()
    if acciones:
        print("Últimas 5 acciones manuales:")
        for i, accion in enumerate(acciones[-5:], 1):
            print(f"{i}. {accion.get('simbolo')}: {accion.get('nombre')}")
    
    print("\n" + "=" * 60)
    print("🎯 Ahora las acciones manuales mostrarán sus nombres reales")
    print("   Ejemplo: 'BVCC' → 'BOLSA V. CCAS.'")
    print("=" * 60)

if __name__ == "__main__":
    main()