#!/usr/bin/env python3
# cargar_cache.py - Script para cargar datos desde archivos .dat

from dat_parser import cargar_solo_recientes, cargar_desde_data_cache
import time

def main():
    print("=" * 60)
    print("=== CARGA DE DATOS DESDE ARCHIVOS .DAT ===")
    print("=" * 60)
    
    # Medir tiempo de ejecución
    inicio = time.time()
    
    # Opción 1: Carga rápida (solo últimos 30 días)
    print("\n1️⃣  CARGANDO DATOS RECIENTES (Últimos 30 días)")
    print("   Esta carga es rápida y suficiente para empezar.")
    print("-" * 60)
    
    if cargar_solo_recientes(30):
        print("✅ Carga rápida completada exitosamente!")
    else:
        print("⚠️  No se pudo realizar la carga rápida")
        print("📁 Verifica que la carpeta 'data_cache' exista y tenga archivos .dat")
        return
    
    tiempo_rapido = time.time() - inicio
    print(f"⏱️  Tiempo carga rápida: {tiempo_rapido:.1f} segundos")
    
    # Preguntar si cargar todos los datos
    print("\n" + "=" * 60)
    print("2️⃣  ¿DESEAS CARGAR TODOS LOS DATOS HISTÓRICOS?")
    print("-" * 60)
    print("⚠️  ADVERTENCIA: Tienes 1366 archivos .dat")
    print("   La carga completa puede tomar varios minutos.")
    print("   ¿Continuar con la carga completa? (s/n)")
    
    respuesta = input().strip().lower()
    
    if respuesta == 's':
        print("\n🔄 INICIANDO CARGA COMPLETA...")
        print("   Esto puede tomar un tiempo, por favor espera.")
        print("-" * 60)
        
        inicio_completa = time.time()
        
        if cargar_desde_data_cache():
            tiempo_completa = time.time() - inicio_completa
            print(f"✅ ¡CARGA COMPLETA EXITOSA!")
            print(f"⏱️  Tiempo total: {tiempo_completa:.1f} segundos")
        else:
            print("❌ Error en la carga completa")
    else:
        print("\n✅ Carga básica completada")
        print("   Puedes ejecutar la aplicación ahora.")
    
    # Tiempo total
    tiempo_total = time.time() - inicio
    print("\n" + "=" * 60)
    print("📊 RESUMEN FINAL")
    print("-" * 60)
    print(f"⏱️  Tiempo total ejecución: {tiempo_total:.1f} segundos")
    print("🎯 Ahora tu aplicación cargará instantáneamente desde archivos locales")
    print("\n👉 EJECUTA LA APLICACIÓN:")
    print("   python app.py")
    print("=" * 60)

if __name__ == "__main__":
    main()