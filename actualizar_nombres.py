#!/usr/bin/env python3
# actualizar_nombres.py - Actualiza nombres en datos existentes

import os
import sys
from datetime import datetime

# Agregar el directorio actual al path
sys.path.append('.')

from datos_manuales import (
    crear_tablas_manuales, 
    obtener_todas_acciones_manuales,
    obtener_nombre_real_accion,
    eliminar_datos_manuales,
    agregar_datos_manuales
)

def actualizar_todos_datos_manuales():
    """Actualiza todos los datos manuales existentes con nombres correctos"""
    print("🔄 Actualizando todos los datos manuales...")
    
    crear_tablas_manuales()
    
    # Obtener todas las fechas con datos manuales
    from datos_manuales import listar_fechas_con_datos_manuales
    fechas = listar_fechas_con_datos_manuales()
    
    if not fechas:
        print("⚠️  No hay fechas con datos manuales")
        return 0
    
    print(f"📅 Encontradas {len(fechas)} fechas con datos manuales")
    
    total_actualizadas = 0
    
    for fecha in fechas:
        print(f"\n📊 Procesando fecha: {fecha}")
        
        # Obtener datos manuales actuales
        from datos_manuales import obtener_datos_manuales
        datos_acciones, datos_indice = obtener_datos_manuales(fecha)
        
        if not datos_acciones:
            continue
        
        # Corregir nombres en los datos
        datos_corregidos = []
        for accion in datos_acciones:
            simbolo = accion.get('simbolo', '')
            nombre_real = obtener_nombre_real_accion(simbolo)
            
            # Crear copia con nombre corregido
            accion_corregida = accion.copy()
            accion_corregida['nombre'] = nombre_real
            
            datos_corregidos.append(accion_corregida)
            
            # Mostrar cambios si el nombre cambió
            if accion.get('nombre') != nombre_real:
                print(f"   ✅ {simbolo}: '{accion.get('nombre')}' → '{nombre_real}'")
        
        # Eliminar datos antiguos y guardar corregidos
        from datos_manuales import eliminar_datos_manuales
        eliminar_datos_manuales(fecha)
        
        # Guardar datos corregidos
        from datos_manuales import agregar_datos_manuales
        agregar_datos_manuales(fecha, datos_corregidos, datos_indice)
        
        total_actualizadas += len(datos_corregidos)
        print(f"   📝 {len(datos_corregidos)} acciones actualizadas")
    
    return total_actualizadas

def main():
    print("=" * 60)
    print("=== ACTUALIZACIÓN COMPLETA DE NOMBRES MANUALES ===")
    print("=" * 60)
    
    print("\nEste script hará lo siguiente:")
    print("1. 🔍 Buscará todas las acciones manuales existentes")
    print("2. 📚 Buscará los nombres REALES en datos automáticos")
    print("3. ✏️  Actualizará los nombres incorrectos")
    print("4. 💾 Guardará los datos corregidos")
    print("\nEjemplo: 'BVCC (Manual)' → 'BOLSA V. CCAS.'")
    
    print("\n" + "=" * 60)
    print("¿Continuar con la actualización? (s/n)")
    respuesta = input().strip().lower()
    
    if respuesta != 's':
        print("❌ Actualización cancelada")
        return
    
    print("\n🔄 Iniciando actualización...")
    
    total = actualizar_todos_datos_manuales()
    
    print("\n" + "=" * 60)
    if total > 0:
        print(f"✅ ACTUALIZACIÓN COMPLETADA: {total} acciones corregidas")
    else:
        print("✅ No se encontraron datos para actualizar")
    
    print("\n🎯 A partir de ahora:")
    print("   • Los datos manuales nuevos usarán nombres reales automáticamente")
    print("   • Los datos existentes han sido corregidos")
    print("   • La aplicación mostrará nombres correctos en todas partes")
    print("=" * 60)

if __name__ == "__main__":
    main()