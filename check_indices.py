# check_indices.py - Verifica el estado de los datos del índice IBC
import sqlite3
import os

print("=" * 60)
print("CHECK INDICES - DIAGNÓSTICO DE DATOS DEL ÍNDICE IBC")
print("=" * 60)

# Verificar base de datos
db_path = "database/bolsa_datos.db"
print(f"📁 Base de datos: {db_path}")
print(f"   Existe: {os.path.exists(db_path)}")

if not os.path.exists(db_path):
    print("❌ ERROR: No existe la base de datos")
    print("💡 Solución: Ejecuta primero la aplicación para crear la base de datos")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 1. Verificar tablas
print("\n" + "=" * 60)
print("1. TABLAS DISPONIBLES:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tablas = cursor.fetchall()

for tabla in tablas:
    # Contar registros en cada tabla
    cursor.execute(f"SELECT COUNT(*) FROM {tabla[0]}")
    count = cursor.fetchone()[0]
    print(f"   📋 {tabla[0]:20} → {count:5} registros")

# 2. Verificar datos en 'indices'
print("\n" + "=" * 60)
print("2. DATOS EN TABLA 'indices':")

cursor.execute("SELECT COUNT(*) FROM indices")
total_indices = cursor.fetchone()[0]
print(f"   📊 Total registros: {total_indices}")

if total_indices > 0:
    # Últimos registros
    cursor.execute("SELECT fecha, valor, variacion FROM indices ORDER BY fecha DESC LIMIT 5")
    registros = cursor.fetchall()
    
    print("   📅 Últimos 5 registros:")
    for fecha, valor, variacion in registros:
        # Formatear fecha
        if len(fecha) == 8:
            fecha_fmt = f"{fecha[6:]}/{fecha[4:6]}/{fecha[:4]}"
        else:
            fecha_fmt = fecha
        
        variacion_str = f"+{variacion:.2f}%" if variacion > 0 else f"{variacion:.2f}%"
        print(f"      • {fecha_fmt}: {valor:10.2f} ({variacion_str})")
    
    # Rango de fechas
    cursor.execute("SELECT MIN(fecha), MAX(fecha) FROM indices")
    min_fecha, max_fecha = cursor.fetchone()
    print(f"   📅 Rango de fechas: {min_fecha} a {max_fecha}")
    
    # Estadísticas
    cursor.execute("SELECT AVG(valor), MIN(valor), MAX(valor) FROM indices")
    avg_val, min_val, max_val = cursor.fetchone()
    print(f"   📈 Estadísticas:")
    print(f"      • Promedio: {avg_val:.2f}")
    print(f"      • Mínimo:   {min_val:.2f}")
    print(f"      • Máximo:   {max_val:.2f}")
else:
    print("   ⚠️  La tabla 'indices' está vacía")
    print("   💡 Ejecuta: python fix_indices.py")

# 3. Verificar datos en 'indices_manuales'
print("\n" + "=" * 60)
print("3. DATOS EN TABLA 'indices_manuales':")

cursor.execute("SELECT COUNT(*) FROM indices_manuales")
total_manuales = cursor.fetchone()[0]
print(f"   📊 Total registros: {total_manuales}")

if total_manuales > 0:
    cursor.execute("SELECT fecha, valor FROM indices_manuales ORDER BY fecha DESC LIMIT 3")
    print("   📅 Últimos registros manuales:")
    for fecha, valor in cursor.fetchall():
        print(f"      • {fecha}: {valor:.2f}")

# 4. Verificar estructura de tablas
print("\n" + "=" * 60)
print("4. ESTRUCTURA DE TABLAS:")

for tabla in ['indices', 'indices_manuales']:
    try:
        cursor.execute(f"PRAGMA table_info({tabla})")
        columnas = cursor.fetchall()
        print(f"   🏗️  {tabla}:")
        for col in columnas:
            print(f"      • {col[1]:15} ({col[2]:10}) {'PK' if col[5] else ''}")
    except:
        print(f"   ⚠️  Tabla '{tabla}' no existe")

conn.close()

# 5. Resumen y recomendaciones
print("\n" + "=" * 60)
print("5. RESUMEN Y RECOMENDACIONES:")

if total_indices == 0 and total_manuales == 0:
    print("   ❌ PROBLEMA: No hay datos del índice IBC")
    print("   ✅ SOLUCIÓN: Ejecuta estos comandos:")
    print("       1. python fix_indices.py")
    print("       2. python app.py")
    print("       3. Visita: http://127.0.0.1:5000/indices")
elif total_indices < 10:
    print("   ⚠️  ADVERTENCIA: Pocos datos del índice")
    print("   💡 SUGERENCIA: Ejecuta 'python fix_indices.py' para poblar más datos")
else:
    print("   ✅ OK: Hay datos del índice disponibles")
    print("   💡 TIP: Si el gráfico no se muestra, verifica la consola del navegador")

print("\n" + "=" * 60)
print("Comandos útiles:")
print("   • python fix_indices.py          # Corrige/puebla datos del índice")
print("   • python app.py                  # Inicia la aplicación")
print("   • python -c \"from app import obtener_datos_indice_historico; print(obtener_datos_indice_historico('20250101', '20250110'))\"")
print("=" * 60)