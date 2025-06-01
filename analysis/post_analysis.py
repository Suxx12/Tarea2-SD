#!/usr/bin/env python3
"""
Script de análisis post-procesamiento para los datos procesados por Apache Pig
Tarea 2 - Sistemas Distribuidos
"""

import pandas as pd
import os
import json
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

def load_pig_results(base_path="/data/processed"):
    """Cargar los resultados del procesamiento de Pig"""
    results = {}
    
    try:
        # Datos limpios principales
        if os.path.exists(f"{base_path}/clean_incidents/part-m-00000"):
            results['clean_incidents'] = pd.read_csv(
                f"{base_path}/clean_incidents/part-m-00000",
                names=['id', 'incident_type', 'subtype', 'uuid', 'pubMillis', 'dateTime',
                       'country', 'state', 'comuna', 'street', 'magvar', 'reliability',
                       'reportDescription', 'reportRating', 'confidence', 'nComments',
                       'latitude', 'longitude', 'x', 'y']
            )
            print(f"✓ Datos limpios cargados: {len(results['clean_incidents'])} registros")
        
        # Análisis por comuna
        if os.path.exists(f"{base_path}/comuna_analysis/part-r-00000"):
            results['comuna_stats'] = pd.read_csv(
                f"{base_path}/comuna_analysis/part-r-00000",
                names=['comuna', 'total_incidents', 'avg_reliability', 'avg_confidence']
            )
            print(f"✓ Estadísticas por comuna: {len(results['comuna_stats'])} comunas")
        
        # Análisis por tipo
        if os.path.exists(f"{base_path}/type_analysis/part-r-00000"):
            results['type_stats'] = pd.read_csv(
                f"{base_path}/type_analysis/part-r-00000",
                names=['incident_type', 'total_count', 'avg_reliability']
            )
            print(f"✓ Estadísticas por tipo: {len(results['type_stats'])} tipos")
        
        # Análisis temporal
        if os.path.exists(f"{base_path}/temporal_analysis/part-r-00000"):
            results['hourly_stats'] = pd.read_csv(
                f"{base_path}/temporal_analysis/part-r-00000",
                names=['hour', 'incidents_count']
            )
            print(f"✓ Análisis temporal cargado")
        
        # Top comunas críticas
        if os.path.exists(f"{base_path}/top_critical_comunas/part-r-00000"):
            results['top_comunas'] = pd.read_csv(
                f"{base_path}/top_critical_comunas/part-r-00000",
                names=['comuna', 'total_incidents', 'avg_reliability', 'avg_confidence']
            )
            print(f"✓ Top comunas críticas cargadas")
        
        # Métricas de procesamiento
        metrics = {}
        for metric_type in ['raw_count', 'clean_count', 'final_count']:
            metric_file = f"{base_path}/metrics_{metric_type}/part-r-00000"
            if os.path.exists(metric_file):
                with open(metric_file, 'r') as f:
                    metrics[metric_type] = int(f.read().strip())
        
        if metrics:
            results['processing_metrics'] = metrics
            print(f"✓ Métricas de procesamiento cargadas")
    
    except Exception as e:
        print(f"Error cargando resultados: {e}")
    
    return results

def generate_summary_report(results):
    """Generar reporte resumen"""
    print("\n" + "="*60)
    print("REPORTE RESUMEN - ANÁLISIS DE INCIDENTES DE TRÁFICO WAZE")
    print("="*60)
    
    # Métricas de procesamiento
    if 'processing_metrics' in results:
        metrics = results['processing_metrics']
        print(f"\n📊 MÉTRICAS DE PROCESAMIENTO:")
        print(f"   • Registros originales: {metrics.get('raw_count', 'N/A'):,}")
        print(f"   • Registros después de limpieza: {metrics.get('clean_count', 'N/A'):,}")
        print(f"   • Registros finales (sin duplicados): {metrics.get('final_count', 'N/A'):,}")
        
        if 'clean_count' in metrics and 'raw_count' in metrics:
            clean_rate = (metrics['clean_count'] / metrics['raw_count']) * 100
            print(f"   • Tasa de datos válidos: {clean_rate:.1f}%")
    
    # Estadísticas por tipo de incidente
    if 'type_stats' in results:
        print(f"\n🚦 TIPOS DE INCIDENTES MÁS FRECUENTES:")
        top_types = results['type_stats'].head(5)
        for _, row in top_types.iterrows():
            print(f"   • {row['incident_type']}: {row['total_count']:,} incidentes (Confiabilidad: {row['avg_reliability']:.1f})")
    
    # Top comunas con más incidentes
    if 'top_comunas' in results:
        print(f"\n🏙️ COMUNAS CON MÁS INCIDENTES:")
        for _, row in results['top_comunas'].head(5).iterrows():
            print(f"   • {row['comuna']}: {row['total_incidents']:,} incidentes")
    
    # Análisis temporal
    if 'hourly_stats' in results:
        hourly_data = results['hourly_stats']
        peak_hour = hourly_data.loc[hourly_data['incidents_count'].idxmax()]
        print(f"\n⏰ ANÁLISIS TEMPORAL:")
        print(f"   • Hora pico: {int(peak_hour['hour'])}:00 hrs ({peak_hour['incidents_count']:,} incidentes)")
        
        # Horas de mayor tráfico (top 3)
        top_hours = hourly_data.nlargest(3, 'incidents_count')
        print(f"   • Top 3 horas críticas:")
        for _, row in top_hours.iterrows():
            print(f"     - {int(row['hour'])}:00 hrs: {row['incidents_count']:,} incidentes")

def generate_detailed_analysis(results, output_dir="./analysis_results"):
    """Generar análisis detallado con gráficos"""
    os.makedirs(output_dir, exist_ok=True)
    
    # Configurar estilo de gráficos
    plt.style.use('default')
    sns.set_palette("husl")
    
    # 1. Distribución de tipos de incidentes
    if 'type_stats' in results:
        plt.figure(figsize=(12, 6))
        type_data = results['type_stats'].sort_values('total_count', ascending=True)
        
        plt.subplot(1, 2, 1)
        plt.barh(type_data['incident_type'], type_data['total_count'])
        plt.title('Distribución de Tipos de Incidentes')
        plt.xlabel('Número de Incidentes')
        
        plt.subplot(1, 2, 2)
        plt.pie(type_data['total_count'], labels=type_data['incident_type'], autopct='%1.1f%%')
        plt.title('Proporción de Tipos de Incidentes')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/incident_types_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Gráfico de distribución de tipos guardado")
    
    # 2. Top comunas críticas
    if 'top_comunas' in results:
        plt.figure(figsize=(14, 8))
        top_15 = results['top_comunas'].head(15)
        
        plt.barh(range(len(top_15)), top_15['total_incidents'])
        plt.yticks(range(len(top_15)), top_15['comuna'])
        plt.xlabel('Número de Incidentes')
        plt.title('Top 15 Comunas con Mayor Número de Incidentes')
        plt.gca().invert_yaxis()
        
        # Agregar valores en las barras
        for i, v in enumerate(top_15['total_incidents']):
            plt.text(v + max(top_15['total_incidents']) * 0.01, i, str(v), 
                    va='center', fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/top_comunas_incidents.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Gráfico de top comunas guardado")
    
    # 3. Análisis temporal por horas
    if 'hourly_stats' in results:
        plt.figure(figsize=(12, 6))
        hourly_data = results['hourly_stats'].sort_values('hour')
        
        plt.plot(hourly_data['hour'], hourly_data['incidents_count'], 
                marker='o', linewidth=2, markersize=6)
        plt.title('Distribución Temporal de Incidentes por Hora del Día')
        plt.xlabel('Hora del Día')
        plt.ylabel('Número de Incidentes')
        plt.xticks(range(0, 24, 2))
        plt.grid(True, alpha=0.3)
        
        # Destacar hora pico
        peak_hour = hourly_data.loc[hourly_data['incidents_count'].idxmax()]
        plt.axvline(x=peak_hour['hour'], color='red', linestyle='--', alpha=0.7, 
                   label=f'Hora Pico: {int(peak_hour["hour"])}:00')
        plt.legend()
        
        plt.tight_layout()
        plt.savefig(f"{output_dir}/hourly_incidents_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()
        print(f"✓ Gráfico de distribución horaria guardado")

def export_summary_json(results, output_file="./analysis_results/summary.json"):
    """Exportar resumen en formato JSON"""
    summary = {
        "timestamp": datetime.now().isoformat(),
        "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "metrics": {},
        "top_incident_types": [],
        "top_comunas": [],
        "temporal_analysis": {}
    }
    
    # Métricas de procesamiento
    if 'processing_metrics' in results:
        summary["metrics"] = results['processing_metrics']
    
    # Top tipos de incidentes
    if 'type_stats' in results:
        summary["top_incident_types"] = results['type_stats'].head(10).to_dict('records')
    
    # Top comunas
    if 'top_comunas' in results:
        summary["top_comunas"] = results['top_comunas'].head(10).to_dict('records')
    
    # Análisis temporal
    if 'hourly_stats' in results:
        hourly_data = results['hourly_stats']
        peak_hour = hourly_data.loc[hourly_data['incidents_count'].idxmax()]
        summary["temporal_analysis"] = {
            "peak_hour": int(peak_hour['hour']),
            "peak_incidents": int(peak_hour['incidents_count']),
            "hourly_distribution": hourly_data.to_dict('records')
        }
    
    # Guardar JSON
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Resumen exportado a: {output_file}")

def main():
    """Función principal"""
    print("Iniciando análisis post-procesamiento...")
    
    # Cargar resultados de Pig
    results = load_pig_results()
    
    if not results:
        print("❌ No se encontraron resultados del procesamiento de Pig")
        print("Asegúrate de que el script de Pig se haya ejecutado correctamente")
        return
    
    # Generar reporte resumen
    generate_summary_report(results)
    
    # Generar análisis detallado
    try:
        generate_detailed_analysis(results)
        print(f"\n✓ Análisis detallado generado en ./analysis_results/")
    except ImportError:
        print("\n⚠️ matplotlib/seaborn no disponible. Saltando generación de gráficos.")
    except Exception as e:
        print(f"\n⚠️ Error generando gráficos: {e}")
    
    # Exportar resumen JSON
    export_summary_json(results)
    
    print(f"\n🎉 Análisis completado exitosamente!")
    print(f"📁 Resultados disponibles en: ./analysis_results/")

if __name__ == "__main__":
    main()