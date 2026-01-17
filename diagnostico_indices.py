# diagnostico_indices.py - Diagnóstico completo del sistema de índices IBC
import sqlite3
import os
from datetime import datetime, timedelta

def main():
    print("🔍 DIAGNÓSTICO DEL SISTEMA DE ÍNDICES IBC")
    print("=" * 70)
    
    # 1. Verificar archivos y directorios
    print("\n1. 📁 VERIFICACIÓN DE ARCHIVOS:")
    
    db_path = "database/bolsa_datos.db"
    print(f"   • Base de datos: {db_path}")
    
    if os.path.exists(db_path):
        size_mb = os.path.getsize(db_path) / (1024 * 1024)
        print(f"     ✅ Existe ({size_mb:.2f} MB)")
    else:
        print("     ❌ NO EXISTE - La aplicación no ha creado la base de datos")
        print("     💡 Ejecuta primero: python app.py")
        return
    
    # 2. Conectar y verificar tablas
    print("\n2. 🗃️  VERIFICACIÓN DE TABLAS SQLite:")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Listar todas las tablas
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
        tablas = [t[0] for t in cursor.fetchall()]
        
        print(f"   • Tablas encontradas: {len(tablas)}")
        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cursor.fetchone()[0]
            print(f"     • {tabla:20} → {count:6} registros")
    
    except Exception as e:
        print(f"     ❌ Error conectando a SQLite: {e}")
        return
    
    # 3. Diagnóstico específico de índices
    print("\n3. 📊 DIAGNÓSTICO DE ÍNDICES IBC:")
    
    for tabla_indice in ['indices', 'indices_manuales']:
        if tabla_indice in tablas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla_indice}")
            total = cursor.fetchone()[0]
            
            print(f"   • {tabla_indice}: {total} registros")
            
            if total > 0:
                # Obtener rango de fechas
                cursor.execute(f"SELECT MIN(fecha), MAX(fecha) FROM {tabla_indice}")
                min_fecha, max_fecha = cursor.fetchone()
                
                # Formatear fechas
                if min_fecha and max_fecha:
                    min_fmt = f"{min_fecha[6:]}/{min_fecha[4:6]}/{min_fecha[:4]}" if len(min_fecha) == 8 else min_fecha
                    max_fmt = f"{max_fecha[6:]}/{max_fecha[4:6]}/{max_fecha[:4]}" if len(max_fecha) == 8 else max_fecha
                    
                    print(f"     📅 Rango: {min_fmt} a {max_fmt}")
                
                # Mostrar algunos datos de ejemplo
                cursor.execute(f"SELECT fecha, valor, variacion FROM {tabla_indice} ORDER BY fecha DESC LIMIT 3")
                ejemplos = cursor.fetchall()
                
                print(f"     📈 Ejemplos (más recientes):")
                for fecha, valor, variacion in ejemplos:
                    fecha_fmt = f"{fecha[6:]}/{fecha[4:6]}/{fecha[:4]}" if len(fecha) == 8 else fecha
                    variacion_str = f"+{variacion:.2f}%" if variacion and variacion > 0 else f"{variacion:.2f}%" if variacion else "0.00%"
                    print(f"       - {fecha_fmt}: {valor:.2f} ({variacion_str})")
            else:
                print(f"     ⚠️  Tabla vacía")
        else:
            print(f"   • {tabla_indice}: ❌ NO EXISTE")
    
    # 4. Probar consulta simulando lo que hace Flask
    print("\n4. 🔄 SIMULANDO CONSULTA FLASK:")
    
    # Fechas de ejemplo (últimos 30 días)
    fecha_hasta = datetime.now().strftime('%Y%m%d')
    fecha_desde = (datetime.now() - timedelta(days=30)).strftime('%Y%m%d')
    
    print(f"   • Consultando: {fecha_desde} a {fecha_hasta}")
    
    # Intentar obtener datos como lo hace Flask
    try:
        # Esta es la consulta que usa obtener_datos_indice_historico
        cursor.execute('''
            SELECT fecha, valor, variacion 
            FROM indices 
            WHERE fecha BETWEEN ? AND ? 
            ORDER BY fecha
        ''', (fecha_desde, fecha_hasta))
        
        datos_indices = cursor.fetchall()
        
        cursor.execute('''
            SELECT fecha, valor, variacion 
            FROM indices_manuales 
            WHERE fecha BETWEEN ? AND ? 
            ORDER BY fecha
        ''', (fecha_desde, fecha_hasta))
        
        datos_manuales = cursor.fetchall()
        
        total_datos = len(datos_indices) + len(datos_manuales)
        
        print(f"   • Resultados:")
        print(f"     - Datos automáticos: {len(datos_indices)}")
        print(f"     - Datos manuales:    {len(datos_manuales)}")
        print(f"     - Total:             {total_datos}")
        
        if total_datos > 0:
            print(f"   ✅ HAY DATOS para mostrar en el gráfico")
            
            # Preparar datos como lo hace Flask
            todos_datos = datos_indices + datos_manuales
            labels = []
            valores = []
            
            for fecha, valor, variacion in todos_datos:
                if fecha and valor:
                    try:
                        # Formatear fecha como lo hace Flask
                        if len(fecha) == 8:
                            fecha_dt = datetime.strptime(fecha, '%Y%m%d')
                            labels.append(fecha_dt.strftime('%d/%m/%Y'))
                        else:
                            labels.append(fecha)
                        
                        valores.append(float(valor) if valor else 0)
                    except:
                        pass
            
            print(f"   • Datos procesados:")
            print(f"     - Labels generados: {len(labels)}")
            print(f"     - Valores generados: {len(valores)}")
            
            if len(valores) > 0:
                print(f"     - Primer valor: {valores[0]}")
                print(f"     - Último valor: {valores[-1]}")
                print(f"     - Rango valores: {min(valores):.2f} a {max(valores):.2f}")
        
        else:
            print(f"   ❌ NO HAY DATOS en el rango seleccionado")
            print(f"   💡 Ejecuta: python fix_indices.py")
    
    except Exception as e:
        print(f"   ❌ Error en consulta: {e}")
    
    # 5. Verificar problemas comunes
    print("\n5. 🔧 VERIFICANDO PROBLEMAS COMUNES:")
    
    # Verificar si valores son NULL o 0
    cursor.execute("SELECT COUNT(*) FROM indices WHERE valor IS NULL OR valor = 0")
    nulos = cursor.fetchone()[0]
    if nulos > 0:
        print(f"   • ⚠️  {nulos} registros con valor NULL o 0 en 'indices'")
    
    # Verificar fechas inválidas
    cursor.execute("SELECT COUNT(*) FROM indices WHERE LENGTH(fecha) != 8")
    fechas_invalidas = cursor.fetchone()[0]
    if fechas_invalidas > 0:
        print(f"   • ⚠️  {fechas_invalidas} fechas con formato inválido")
    
    # 6. Recomendaciones finales
    print("\n6. ✅ RECOMENDACIONES FINALES:")
    
    total_indices = 0
    if 'indices' in tablas:
        cursor.execute("SELECT COUNT(*) FROM indices")
        total_indices = cursor.fetchone()[0]
    
    total_manuales = 0
    if 'indices_manuales' in tablas:
        cursor.execute("SELECT COUNT(*) FROM indices_manuales")
        total_manuales = cursor.fetchone()[0]
    
    if total_indices + total_manuales == 0:
        print("   ❌ PROBLEMA CRÍTICO: No hay datos del índice")
        print("   🚀 SOLUCIÓN: Ejecuta estos pasos:")
        print("     1. Detén Flask (Ctrl+C)")
        print("     2. python fix_indices.py")
        print("     3. python app.py")
        print("     4. Visita http://127.0.0.1:5000/indices")
    elif total_indices + total_manuales < 5:
        print("   ⚠️  PROBLEMA: Muy pocos datos del índice")
        print("   💡 SUGERENCIA: Ejecuta 'python fix_indices.py' para poblar datos")
    else:
        print("   ✅ SISTEMA APARENTEMENTE OK")
        print("   🔍 Si el gráfico no se muestra:")
        print("     1. Abre consola del navegador (F12)")
        print("     2. Verifica errores JavaScript")
        print("     3. Revisa que Chart.js se cargue")
    
    conn.close()
    
    print("\n" + "=" * 70)
    print("📝 RESUMEN EJECUTIVO:")
    print(f"   • Base de datos: {'✅ OK' if os.path.exists(db_path) else '❌ FALTA'}")
    print(f"   • Tabla 'indices': {total_indices} registros")
    print(f"   • Tabla 'indices_manuales': {total_manuales} registros")
    print(f"   • Total datos índice: {total_indices + total_manuales}")
    print("=" * 70)

if __name__ == "__main__":
    main()