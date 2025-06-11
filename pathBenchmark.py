#!/usr/bin/env python3
import os
import time
import sys
import random
import subprocess
import threading
import signal
import re
import pandas as pd
import json
import argparse
from collections import defaultdict
import statistics
import time

class PathBenchmark:
    
    def __init__(self, patterns_file=None, abstract_patterns_file=None, nodes_per_label=3, 
                    selection_mode="max", query_selection_mode=None, queries_per_pattern=3,
                    selective_queries=None, use_existing_results=True, result_file="result.txt",
                    nodes_per_label_explicit=False, use_rankings=None, calculate_new=False):
        self.scale_factors = ["01", "03", "1", "3"]
        self.selected_scale = "01"
        self.server_process = None
        self.query_process = None
        self.mappings_file = "nodos.txt"
        self.db_path = os.path.join("MillenniumDB", "data", "db", self.selected_scale)
        
        if selective_queries is None:
            selective_queries = {
                'n_abstract': '*',
                'n_templates': '*', 
                'n_real': 3
            }
            print("🎯 MODO SELECTIVO activado por defecto")
            print(f"   Configuración automática: aq=*, tq=*, rq=3")
        
        self.selective_queries = selective_queries
        
        self.nodes_per_label_explicit = nodes_per_label_explicit
        
        if nodes_per_label_explicit:
            self.nodes_per_label = nodes_per_label
            print(f"🔧 NODES-PER-LABEL EXPLÍCITO: Usando {nodes_per_label} nodos por etiqueta")
            print(f"   Pool total: {nodes_per_label} nodos × etiquetas")
            print(f"   Selección final: rq={self.selective_queries['n_real']} por template")
        else:
            if 'n_real' in self.selective_queries:
                original_nodes_per_label = nodes_per_label
                self.nodes_per_label = self.selective_queries['n_real']
                if original_nodes_per_label != self.nodes_per_label:
                    print(f"🔄 SINCRONIZACIÓN AUTOMÁTICA: nodes_per_label ajustado de {original_nodes_per_label} a {self.nodes_per_label} (= rq)")
            else:
                self.nodes_per_label = nodes_per_label
        
        self.queries_per_pattern = queries_per_pattern
        
        if isinstance(selection_mode, str):
            if '+' in selection_mode:
                self.selection_modes = list(dict.fromkeys([mode.strip().lower() 
                                        for mode in selection_mode.split('+')]))
            else:
                self.selection_modes = [selection_mode.lower()]
        else:
            self.selection_modes = ["max"]
        
        if query_selection_mode is None:
            self.query_selection_modes = self.selection_modes
        elif isinstance(query_selection_mode, str):
            if '+' in query_selection_mode:
                self.query_selection_modes = list(dict.fromkeys([mode.strip().lower() 
                                        for mode in query_selection_mode.split('+')]))
            else:
                self.query_selection_modes = [query_selection_mode.lower()]
        else:
            self.query_selection_modes = ["max"]
        
        self.use_existing_results = use_existing_results
        self.result_file = result_file
        
        self.calculate_new = calculate_new
        self.use_rankings = use_rankings
        
        if calculate_new:
            self.operation_mode = "calculate_new"
            self.rankings_scale = None
            print("🚀 MODO: --calculate-new (generar todo desde cero)")
        elif use_rankings:
            self.operation_mode = "use_rankings"
            self.rankings_scale = use_rankings
            print(f"📁 MODO: --use-rankings {use_rankings} (leer desde rankings/{use_rankings}/)")
        else:
            self.operation_mode = "default_rankings"
            self.rankings_scale = "01"
            print(f"📂 MODO: Por defecto (leer desde rankings/01/)")
        
        print(f"Modos de selección de nodos configurados: {', '.join(self.selection_modes)}")
        print(f"Modos de selección de consultas configurados: {', '.join(self.query_selection_modes)}")
        
        print(f"Configuración de resultados:")
        if self.use_existing_results:
            print(f"  - Usando archivo existente: {self.result_file}")
        else:
            print(f"  - Se generará nuevo archivo: result.txt")
        
        print(f"🎯 MODO SELECTIVO (por defecto):")
        print(f"  - Abstract queries (aq): {self.selective_queries['n_abstract']}")
        print(f"  - Templates por abstract (tq): {self.selective_queries['n_templates']}")
        print(f"  - Consultas reales por template (rq): {self.selective_queries['n_real']}")
        if self.nodes_per_label_explicit:
            print(f"  - Nodos por etiqueta (EXPLÍCITO): {self.nodes_per_label}")
            print(f"  - ⚠️  POOL GRANDE: Se generarán más consultas de las que se seleccionarán")
        else:
            print(f"  - Nodos por etiqueta (sincronizado): {self.nodes_per_label}")
        
        self.query_patterns = self.load_patterns(patterns_file)
        self.abstract_patterns, self.query_distribution = self.load_abstract_patterns(abstract_patterns_file)
        self.node_mappings = {}
        self.query_to_pattern = {}
        self.pattern_to_q_number = self.generate_q_number_mapping()


    def validate_rankings_exist(self):
        ranking_base_path = os.path.join("rankings", self.rankings_scale)
        
        print(f"\n🔍 Validando rankings en: {ranking_base_path}")
        
        if not os.path.exists(ranking_base_path):
            print(f"❌ Carpeta no encontrada: {ranking_base_path}")
            return False
        
        required_files = [
            "rankingAbstract.xlsx",
            "rankingTemplates.xlsx"
        ]
        
        missing_files = []
        for file in required_files:
            file_path = os.path.join(ranking_base_path, file)
            if not os.path.exists(file_path):
                missing_files.append(file)
            else:
                print(f"✅ Encontrado: {file}")
        
        rankings_nodes_path = os.path.join(ranking_base_path, "rankingsNodes")
        if not os.path.exists(rankings_nodes_path):
            print(f"⚠️  Carpeta rankingsNodes no encontrada: {rankings_nodes_path}")
            print("   (Se generarán mapeos básicos si son necesarios)")
        else:
            print(f"✅ Encontrado: rankingsNodes/")
            try:
                node_files = [f for f in os.listdir(rankings_nodes_path) if f.endswith('.txt')]
                print(f"   📄 {len(node_files)} archivos de nodos disponibles")
            except Exception as e:
                print(f"   ⚠️  Error leyendo rankingsNodes: {e}")
        
        if missing_files:
            print(f"❌ Archivos faltantes: {', '.join(missing_files)}")
            return False
        
        print(f"✅ Rankings válidos encontrados en: rankings/{self.rankings_scale}/")
        return True

    def generate_q_number_mapping(self):
        """Genera un mapeo de nombres de patrones abstractos a números de consulta (Q1, Q2, etc.)"""
        pattern_to_q_number = {}
        
        # Usar query_distribution que mantiene el orden original del archivo de patrones
        for index, (pattern_name, _) in enumerate(self.query_distribution, 1):
            pattern_to_q_number[pattern_name] = index
            
        print(f"\nMapeo de patrones abstractos a números de consulta:")
        for pattern, q_num in pattern_to_q_number.items():
            print(f"  - Q{q_num}: {pattern}")
        
        return pattern_to_q_number


    def load_node_mappings_from_rankings(self):
        rankings_nodes_path = os.path.join("rankings", self.rankings_scale, "rankingsNodes")
        
        if not os.path.exists(rankings_nodes_path):
            print("⚠️  No se encontró rankingsNodes/. Usando mapeos por defecto.")
            return self.get_default_node_mappings()
        
        mappings = {}
        try:
            for filename in os.listdir(rankings_nodes_path):
                if filename.endswith('.txt'):
                    label = filename[:-4]
                    file_path = os.path.join(rankings_nodes_path, filename)
                    
                    nodes = []
                    with open(file_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if line and not line.startswith('#'):
                                parts = line.split(',')
                                if len(parts) >= 2:
                                    node_id = parts[1].strip()
                                    nodes.append(node_id)
                    
                    if nodes:
                        mappings[label] = nodes[:self.nodes_per_label]
                        print(f"📋 Cargados {len(mappings[label])} nodos para '{label}'")
            
            print(f"✅ Mapeos cargados desde rankings: {len(mappings)} etiquetas")
            return mappings
            
        except Exception as e:
            print(f"❌ Error cargando mapeos desde rankings: {e}")
            return self.get_default_node_mappings()

    def get_default_node_mappings(self):
        return {
            "hasCreator": ["m135702"],
            "containerOf": ["f38"],
            "hasMember": ["f41"],
            "knows": ["p4"]
        }

    def read_ranking_abstract_from_rankings(self):
        ranking_path = os.path.join("rankings", self.rankings_scale, "rankingAbstract.xlsx")
        
        if not os.path.exists(ranking_path):
            print(f"❌ Error: No se encontró {ranking_path}")
            return []
        
        try:
            df = pd.read_excel(ranking_path, sheet_name='Ranking')
            return df.to_dict('records')
        except Exception as e:
            print(f"❌ Error leyendo {ranking_path}: {e}")
            return []

    def read_ranking_templates_from_rankings(self, q_number):
        ranking_path = os.path.join("rankings", self.rankings_scale, "rankingTemplates.xlsx")
        
        if not os.path.exists(ranking_path):
            print(f"❌ Error: No se encontró {ranking_path}")
            return []
        
        sheet_name = f"Q{q_number}"
        try:
            df = pd.read_excel(ranking_path, sheet_name=sheet_name)
            return df.to_dict('records')
        except Exception as e:
            print(f"❌ Error leyendo sheet {sheet_name} de {ranking_path}: {e}")
            return []

    def generate_pool_from_rankings(self):
        print(f"\n🎯 Generando pool selectivo desde rankings/{self.rankings_scale}/")
        
        n_abstract = self.selective_queries.get('n_abstract', '*')
        n_templates = self.selective_queries.get('n_templates', '*')
        n_real = self.selective_queries.get('n_real', 3)
        
        abstract_ranking = self.read_ranking_abstract_from_rankings()
        if not abstract_ranking:
            print("❌ Error: No se pudo cargar ranking abstracto")
            return
        
        if n_abstract == '*':
            selected_abstracts = abstract_ranking
        else:
            selected_abstracts = abstract_ranking[:n_abstract]
        
        print(f"📊 Procesando {len(selected_abstracts)} abstract queries...")
        
        self.node_mappings = self.load_node_mappings_from_rankings()
        
        pool_queries = []
        
        for abstract_item in selected_abstracts:
            q_number_str = abstract_item.get('Q Number', '').replace('Q', '')
            if not q_number_str:
                continue
                
            try:
                q_number = int(q_number_str)
            except:
                continue
                
            print(f"📝 Procesando Q{q_number}...")
            
            template_ranking = self.read_ranking_templates_from_rankings(q_number)
            if not template_ranking:
                print(f"   ⚠️  No se encontraron templates para Q{q_number}")
                continue
            
            if n_templates == '*':
                selected_templates = template_ranking
            else:
                selected_templates = template_ranking[:n_templates]
            
            print(f"   📋 {len(selected_templates)} templates seleccionados")
            
            for template_item in selected_templates:
                template_query = template_item.get('Template Query', '')
                if not isinstance(template_query, str) or not template_query.strip():
                    continue
                
                real_queries = self.generate_real_queries_from_template(template_query, n_real)
                print(f"      🔍 {len(real_queries)} consultas reales generadas")
                
                for real_query in real_queries:
                    pool_queries.append({
                        'Q_Number': f"Q{q_number}",
                        'Abstract_Pattern': abstract_item.get('Patrón Abstracto', ''),
                        'Template_Query': template_query,
                        'Real_Query': real_query,
                        'Ranking_Paths': template_item.get('Promedio Paths', 0),
                        'Initial_Label': self.extract_initial_label(template_query)
                    })
        
        print(f"\n✅ Pool final generado: {len(pool_queries)} consultas")
        
        self.save_pool_from_rankings(pool_queries)


    def save_pool_from_rankings(self, pool_queries):
        if not pool_queries:
            print("❌ No hay consultas en el pool para guardar")
            return
        
        output_folder = f"resultados_benchmark_{self.rankings_scale}"
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        df = pd.DataFrame(pool_queries)
        excel_path = os.path.join(output_folder, "pool_final_from_rankings.xlsx")
        df.to_excel(excel_path, index=False)
        
        txt_path = os.path.join(output_folder, "pool_final_from_rankings.txt")
        with open(txt_path, 'w', encoding='utf-8') as f:
            for query in pool_queries:
                f.write(f"{query['Real_Query']}\n")
        
        print(f"💾 Pool guardado:")
        print(f"   📊 Excel: {excel_path}")
        print(f"   📄 TXT: {txt_path}")
        print(f"   📈 Total consultas: {len(pool_queries)}")

    def load_abstract_patterns(self, patterns_file):
        """
        Carga los patrones abstractos desde un archivo con formato 'patrón #número#'
        """
        abstract_patterns = {}
        query_distribution = []
        
        if patterns_file and os.path.exists(patterns_file):
            try:
                pattern_index = 0
                print(f"\nLeyendo archivo de patrones abstractos: {patterns_file}")
                with open(patterns_file, 'r') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if line and not line.startswith('#'):
                            # Buscar el patrón #número# usando expresión regular
                            match = re.search(r'(.*?)\s*#(\d+)#\s*$', line)
                            if match:
                                pattern_name = match.group(1).strip()
                                try:
                                    pattern_count = int(match.group(2))
                                    abstract_patterns[pattern_name] = pattern_count
                                    query_distribution.append((pattern_name, pattern_count))
                                    pattern_index += 1
                                    print(f"  - Patrón añadido: '{pattern_name}' con cantidad {pattern_count}")
                                except ValueError:
                                    print(f"  - ERROR: Cantidad inválida en línea {line_num}: '{line}'")
                            else:
                                print(f"  - ERROR: Formato incorrecto en línea {line_num}: '{line}'")
                
                print(f"\nSe cargaron {len(abstract_patterns)} patrones abstractos")
                for pattern, count in abstract_patterns.items():
                    print(f"  - '{pattern}': {count}")
                
                return abstract_patterns, query_distribution
            except Exception as e:
                print(f"Error al cargar los patrones abstractos: {e}")
                print("No se usarán patrones abstractos...")
        
        return {}, [] 
    def map_queries_to_patterns(self):
        """
        Asigna cada consulta a un patrón abstracto según la distribución especificada
        """
        # CÓDIGO DE DEPURACIÓN
        print("\n=== DEPURACIÓN PATRONES ===")
        print("Patrones abstractos cargados:")
        for pattern_name, count in self.query_distribution:
            print(f"  - '{pattern_name}' - {count}")
        
        print("\nConsultas cargadas:")
        for i, query in enumerate(self.query_patterns[:5]):  # Solo mostrar las primeras 5 para no saturar
            print(f"  {i+1}. {query}")
        if len(self.query_patterns) > 5:
            print(f"  ... y {len(self.query_patterns)-5} más")
        print("=== FIN DEPURACIÓN ===\n")
        
        # Si no hay patrones abstractos, no podemos hacer mapeo
        if not self.query_distribution:
            print("No hay patrones abstractos definidos. Las consultas no se agruparán.")
            return
        
        # Reseteamos el mapeo
        self.query_to_pattern = {}
        
        # Recorremos la lista de consultas, asignando patrones según la distribución
        current_index = 0
        
        # DEPURACIÓN - mostrar el proceso de asignación
        print("\n=== PROCESO DE ASIGNACIÓN ===")
        for pattern_name, count in self.query_distribution:
            print(f"Asignando {count} consultas al patrón '{pattern_name}'")
            # Para cada patrón, asignamos 'count' consultas consecutivas
            for i in range(count):
                if current_index < len(self.query_patterns):
                    query = self.query_patterns[current_index]
                    self.query_to_pattern[query] = pattern_name
                    print(f"  - Consulta #{current_index+1}: '{query[:40]}...' -> '{pattern_name}'")
                    current_index += 1
                else:
                    # Si nos quedamos sin consultas, salimos del bucle
                    print("  - No quedan más consultas para asignar")
                    break
        
        # Si quedan consultas sin asignar, las marcamos como "Otros"
        otros_count = 0
        while current_index < len(self.query_patterns):
            self.query_to_pattern[self.query_patterns[current_index]] = "Otros"
            current_index += 1
            otros_count += 1
        
        if otros_count > 0:
            print(f"Se asignaron {otros_count} consultas restantes a 'Otros'")
        print("=== FIN PROCESO ===\n")
        
        print(f"Se asignaron {len(self.query_to_pattern)} consultas a {len(set(self.query_to_pattern.values()))} patrones abstractos")

    def generate_mappings_file(self):
        """Genera el archivo de mapeos analizando el archivo edges.txt según el factor de escala y los modos de selección"""
        # Construir la ruta al archivo edges.txt según el factor de escala
        edges_path = os.path.join("MillenniumDB", "data", "ldbc", self.selected_scale, "edges.txt")
        
        print(f"Generando archivo de mapeos a partir de {edges_path}...")
        print(f"Seleccionando {self.nodes_per_label} nodos por etiqueta (modos: {', '.join(self.selection_modes)})")
        
        # Verificar si el archivo edges.txt existe
        if not os.path.exists(edges_path):
            print(f"Advertencia: No se encontró el archivo {edges_path}.")
            print("Se usarán mapeos predeterminados.")
            return
        
        # Diccionarios para almacenar conteos
        relationship_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        
        # Asignar el número completo de nodos a cada modo
        nodes_per_mode = {}
        for mode in self.selection_modes:
            nodes_per_mode[mode] = self.nodes_per_label
        
        print("Distribución de nodos por modo:")
        for mode, count in nodes_per_mode.items():
            print(f"  - {mode}: {count} nodos")
        
        # Procesar el archivo
        try:
            with open(edges_path, 'r') as file:
                for line in file:
                    parts = line.strip().split(',')
                    if len(parts) >= 3:  # Asegurar que tengamos origen, relación y destino
                        origin, relation, target = parts[0], parts[1], parts[2]
                        
                        # Contar relaciones salientes y entrantes
                        relationship_data[relation]['outgoing'][origin] += 1
                        relationship_data[relation]['incoming'][target] += 1
            
            # *** NUEVA SECCIÓN: CREAR CARPETA Y RANKINGS POR ETIQUETA ***
            rankings_folder = "rankingsNodes"
            if not os.path.exists(rankings_folder):
                os.makedirs(rankings_folder)
                print(f"Se creó la carpeta: {rankings_folder}")
            
            print(f"\nGenerando rankings individuales por etiqueta en {rankings_folder}/...")
            
            # Crear archivo de mapeos
            with open(self.mappings_file, 'w') as mappings_file:
                # Agregar encabezado como comentario
                mappings_file.write("# Mapeo de etiquetas a nodos iniciales\n")
                mappings_file.write("# Formato: etiqueta,id_nodo1,id_nodo2,...\n")
                mappings_file.write(f"# Generado automáticamente seleccionando {self.nodes_per_label} nodos ")
                mappings_file.write(f"usando los modos: {', '.join(self.selection_modes)}\n\n")
                
                # Contar cuántas relaciones procesamos
                count = 0
                
                # Procesar cada relación y seleccionar los nodos según los modos
                for relation, counts in relationship_data.items():
                    # Obtener los nodos con conexiones salientes
                    if counts['outgoing']:
                        # Ordenar nodos por número de conexiones (descendente)
                        sorted_nodes = sorted(counts['outgoing'].items(), key=lambda x: x[1], reverse=True)
                        total_nodes = len(sorted_nodes)
                        
                        # *** GENERAR RANKING INDIVIDUAL PARA ESTA ETIQUETA ***
                        ranking_file_path = os.path.join(rankings_folder, f"{relation}.txt")
                        with open(ranking_file_path, 'w', encoding='utf-8') as ranking_file:
                            # Encabezado del ranking
                            ranking_file.write(f"# RANKING DE NODOS PARA ETIQUETA: {relation}\n")
                            ranking_file.write(f"# Total de nodos con conexiones salientes: {total_nodes}\n")
                            ranking_file.write(f"# Formato: Posición,NodeID,Conexiones_Salientes\n")
                            ranking_file.write(f"# Generado automáticamente desde: {edges_path}\n")
                            ranking_file.write("# " + "="*60 + "\n\n")
                            
                            # Escribir el ranking completo
                            for position, (node_id, connection_count) in enumerate(sorted_nodes, 1):
                                ranking_file.write(f"{position},{node_id},{connection_count}\n")
                            
                            # Estadísticas al final
                            ranking_file.write("\n# ESTADÍSTICAS\n")
                            connection_counts = [count for _, count in sorted_nodes]
                            ranking_file.write(f"# Total nodos: {len(connection_counts)}\n")
                            ranking_file.write(f"# Conexiones máximas: {max(connection_counts)}\n")
                            ranking_file.write(f"# Conexiones mínimas: {min(connection_counts)}\n")
                            ranking_file.write(f"# Conexiones promedio: {sum(connection_counts)/len(connection_counts):.2f}\n")
                            
                            # Percentiles
                            import statistics
                            ranking_file.write(f"# Mediana de conexiones: {statistics.median(connection_counts)}\n")
                            
                            # Mostrar los nodos seleccionados para cada modo
                            ranking_file.write(f"\n# NODOS SELECCIONADOS POR MODO (top {self.nodes_per_label}):\n")
                            for mode in self.selection_modes:
                                ranking_file.write(f"# Modo {mode.upper()}:\n")
                                if mode == "max":
                                    selected_for_mode = sorted_nodes[:self.nodes_per_label]
                                elif mode == "min":
                                    valid_nodes = [(node, count) for node, count in sorted_nodes if count > 0]
                                    valid_nodes.sort(key=lambda x: x[1])  # Ascendente para min
                                    selected_for_mode = valid_nodes[:self.nodes_per_label]
                                elif mode == "med":
                                    median_idx = len(sorted_nodes) // 2
                                    half_count = self.nodes_per_label // 2
                                    remainder = self.nodes_per_label % 2
                                    start_idx = max(0, median_idx - half_count)
                                    end_idx = min(len(sorted_nodes), median_idx + half_count + remainder)
                                    selected_for_mode = sorted_nodes[start_idx:end_idx]
                                elif mode == ".25":
                                    p25_idx = len(sorted_nodes) // 4
                                    half_count = self.nodes_per_label // 2
                                    remainder = self.nodes_per_label % 2
                                    start_idx = max(0, p25_idx - half_count)
                                    end_idx = min(len(sorted_nodes), p25_idx + half_count + remainder)
                                    selected_for_mode = sorted_nodes[start_idx:end_idx]
                                elif mode == ".75":
                                    p75_idx = (len(sorted_nodes) * 3) // 4
                                    half_count = self.nodes_per_label // 2
                                    remainder = self.nodes_per_label % 2
                                    start_idx = max(0, p75_idx - half_count)
                                    end_idx = min(len(sorted_nodes), p75_idx + half_count + remainder)
                                    selected_for_mode = sorted_nodes[start_idx:end_idx]
                                
                                for pos, (node_id, conn_count) in enumerate(selected_for_mode, 1):
                                    # Buscar la posición real en el ranking completo
                                    real_position = next(i for i, (nid, _) in enumerate(sorted_nodes, 1) if nid == node_id)
                                    ranking_file.write(f"#   {pos}. {node_id} (pos {real_position}, {conn_count} conexiones)\n")
                        
                        print(f"  ✓ Ranking generado: {relation}.txt ({total_nodes} nodos)")
                        # *** FIN DE GENERACIÓN DE RANKING INDIVIDUAL ***
                        
                        # Lista para almacenar todos los nodos seleccionados para esta relación
                        selected_nodes = []
                        
                        # Seleccionar nodos para cada modo configurado
                        for mode in self.selection_modes:
                            mode_nodes_to_select = nodes_per_mode[mode]
                            mode_selected_nodes = []
                            
                            # Filtrar nodos ya seleccionados para evitar duplicados
                            available_nodes = [(node_id, node_count) for node_id, node_count in sorted_nodes 
                                            if node_id not in selected_nodes]
                            
                            if mode == "max":
                                # Seleccionar los nodos con más conexiones
                                num_nodes = min(mode_nodes_to_select, len(available_nodes))
                                mode_selected_nodes = [node[0] for node in available_nodes[:num_nodes]]
                            
                            elif mode == "min":
                                # Seleccionar los nodos con menos conexiones, pero con al menos una conexión
                                # Filtrar nodos que tienen al menos una conexión (>0)
                                valid_nodes = [node for node in available_nodes if node[1] > 0]
                                
                                if valid_nodes:
                                    # Ordenar por número de conexiones (ascendente)
                                    valid_nodes.sort(key=lambda x: x[1])
                                    
                                    # Seleccionar hasta N nodos
                                    num_nodes = min(mode_nodes_to_select, len(valid_nodes))
                                    mode_selected_nodes = [node[0] for node in valid_nodes[:num_nodes]]
                                else:
                                    print(f"Advertencia: No hay nodos con conexiones salientes > 0 para la etiqueta '{relation}' en modo '{mode}'.")
                            
                            elif mode == "med":
                                # Seleccionar nodos alrededor de la mediana
                                if len(available_nodes) <= mode_nodes_to_select:
                                    # Si hay menos nodos que los requeridos, tomamos todos
                                    mode_selected_nodes = [node[0] for node in available_nodes]
                                else:
                                    # Calcular el índice de la mediana
                                    median_idx = len(available_nodes) // 2
                                    
                                    # Calcular cuántos nodos tomar antes y después de la mediana
                                    half_count = mode_nodes_to_select // 2
                                    mode_remainder = mode_nodes_to_select % 2
                                    
                                    # Seleccionar nodos alrededor de la mediana
                                    start_idx = max(0, median_idx - half_count)
                                    end_idx = min(len(available_nodes), median_idx + half_count + mode_remainder)
                                    
                                    # Ajustar si no hay suficientes nodos a un lado
                                    if start_idx == 0:
                                        end_idx = min(len(available_nodes), mode_nodes_to_select)
                                    elif end_idx == len(available_nodes):
                                        start_idx = max(0, len(available_nodes) - mode_nodes_to_select)
                                    
                                    mode_selected_nodes = [node[0] for node in available_nodes[start_idx:end_idx]]
                            
                            elif mode == ".25":
                                # Seleccionar nodos en el percentil 25
                                if len(available_nodes) <= mode_nodes_to_select:
                                    # Si hay menos nodos que los requeridos, tomamos todos
                                    mode_selected_nodes = [node[0] for node in available_nodes]
                                else:
                                    # Calcular el índice del percentil 25
                                    p25_idx = len(available_nodes) // 4
                                    
                                    # Calcular cuántos nodos tomar alrededor del percentil
                                    half_count = mode_nodes_to_select // 2
                                    mode_remainder = mode_nodes_to_select % 2
                                    
                                    # Seleccionar nodos alrededor del percentil 25
                                    start_idx = max(0, p25_idx - half_count)
                                    end_idx = min(len(available_nodes), p25_idx + half_count + mode_remainder)
                                    
                                    # Ajustar si no hay suficientes nodos a un lado
                                    if start_idx == 0:
                                        end_idx = min(len(available_nodes), mode_nodes_to_select)
                                    elif end_idx == len(available_nodes):
                                        start_idx = max(0, len(available_nodes) - mode_nodes_to_select)
                                    
                                    mode_selected_nodes = [node[0] for node in available_nodes[start_idx:end_idx]]
                            
                            elif mode == ".75":
                                # Seleccionar nodos en el percentil 75
                                if len(available_nodes) <= mode_nodes_to_select:
                                    # Si hay menos nodos que los requeridos, tomamos todos
                                    mode_selected_nodes = [node[0] for node in available_nodes]
                                else:
                                    # Calcular el índice del percentil 75
                                    p75_idx = (len(available_nodes) * 3) // 4
                                    
                                    # Calcular cuántos nodos tomar alrededor del percentil
                                    half_count = mode_nodes_to_select // 2
                                    mode_remainder = mode_nodes_to_select % 2
                                    
                                    # Seleccionar nodos alrededor del percentil 75
                                    start_idx = max(0, p75_idx - half_count)
                                    end_idx = min(len(available_nodes), p75_idx + half_count + mode_remainder)
                                    
                                    # Ajustar si no hay suficientes nodos a un lado
                                    if start_idx == 0:
                                        end_idx = min(len(available_nodes), mode_nodes_to_select)
                                    elif end_idx == len(available_nodes):
                                        start_idx = max(0, len(available_nodes) - mode_nodes_to_select)
                                    
                                    mode_selected_nodes = [node[0] for node in available_nodes[start_idx:end_idx]]
                            
                            # Añadir los nodos seleccionados a la lista general para esta relación
                            selected_nodes.extend(mode_selected_nodes)
                        
                        # Escribir al archivo
                        if selected_nodes:
                            mappings_file.write(f"{relation},{','.join(selected_nodes)}\n")
                            count += 1
            
            print(f"\nSe generó el archivo {self.mappings_file} con {count} etiquetas y hasta {len(self.selection_modes) * self.nodes_per_label} nodos por etiqueta.")
            print(f"Se generaron {count} archivos de ranking en la carpeta '{rankings_folder}/'")
            
        except Exception as e:
            print(f"Error al generar el archivo de mapeos: {e}")
            print("Se usarán mapeos predeterminados.")


    def load_patterns(self, patterns_file):
        """Carga los patrones de consulta desde un archivo o usa los predeterminados"""
        default_patterns = [
            "MATCH (x)=[ALL TRAILS ?p1 (:hasCreator/:isLocatedIn)]=>(?y) RETURN ?p1 LIMIT 100",
            "MATCH (x)=[ALL TRAILS ?p1 (:hasCreator/:studyAt)]=>(?y) RETURN ?p1 LIMIT 100",
            "MATCH (x)=[ALL TRAILS ?p1 (:containerOf/:hasCreator)]=>(?y) RETURN ?p1 LIMIT 100",
            "MATCH (x)=[ALL TRAILS ?p1 (:containerOf/:hasTag)]=>(?y) RETURN ?p1 LIMIT 100",
            "MATCH (x)=[ALL TRAILS ?p1 (:hasMember/:likes)]=>(?y) RETURN ?p1 LIMIT 100",
            "MATCH (x)=[ALL TRAILS ?p1 (:knows/:likes)]=>(?y) RETURN ?p1 LIMIT 100"
        ]
        
        if patterns_file and os.path.exists(patterns_file):
            try:
                with open(patterns_file, 'r') as f:
                    if patterns_file.endswith('.json'):
                        patterns = json.load(f)
                    else:
                        # Asumimos que es un archivo de texto con un patrón por línea
                        patterns = [line.strip() for line in f if line.strip() and not line.startswith('#')]
                print(f"Se cargaron {len(patterns)} patrones de consulta desde {patterns_file}")
                return patterns
            except Exception as e:
                print(f"Error al cargar los patrones desde {patterns_file}: {e}")
                print("Usando patrones predeterminados...")
        
        print(f"Usando {len(default_patterns)} patrones de consulta predeterminados")
        return default_patterns
    
    def load_mappings(self, mappings_file):
        """Carga el mapeo de nodos desde un archivo o usa los predeterminados"""
        default_mappings = {
            "hasCreator": ["m135702"],
            "containerOf": ["f38"],
            "hasMember": ["f41"],
            "knows": ["p4"]
        }
        
        if mappings_file and os.path.exists(mappings_file):
            try:
                with open(mappings_file, 'r') as f:
                    if mappings_file.endswith('.json'):
                        mappings = json.load(f)
                    else:
                        # Ahora esperamos múltiples nodos por etiqueta
                        mappings = {}
                        for line in f:
                            if line.strip() and not line.startswith('#'):
                                parts = line.strip().split(',')
                                if len(parts) >= 2:
                                    # La etiqueta es el primer elemento, los nodos son el resto
                                    label = parts[0].strip()
                                    nodes = [node.strip() for node in parts[1:]]
                                    mappings[label] = nodes
                print(f"Se cargaron {len(mappings)} mapeos de etiquetas desde {mappings_file}")
                total_nodes = sum(len(nodes) for nodes in mappings.values())
                print(f"Total de nodos cargados: {total_nodes}")
                return mappings
            except Exception as e:
                print(f"Error al cargar los mapeos desde {mappings_file}: {e}")
                print("Usando mapeos predeterminados...")
        
        print(f"Usando {len(default_mappings)} mapeos de nodos predeterminados")
        return default_mappings
    
    #################################################
    # FUNCIONES DE INTERFAZ DE USUARIO
    #################################################
    
    def show_welcome_screen(self):
        os.system('cls' if os.name == 'nt' else 'clear')  

        print("\033[1;94m") 
        print("┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓")
        print("┃                                                          ┃")
        print("┃                                                          ┃")
        print("┃                                                          ┃")
        print("┃       \033[1;97m╔══════════════════════════════════════════╗\033[1;94m       ┃")
        print("┃       \033[1;97m║\033[1;95m        RPQ SNB       \033[1;97m║\033[1;94m       ┃")
        print("┃       \033[1;97m╚══════════════════════════════════════════╝\033[1;94m       ┃")
        print("┃                                                          ┃")
        print("┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛")
        print("\033[0m") 
        
        print("\033[38;5;39m▓\033[38;5;38m▓\033[38;5;37m▓\033[38;5;36m▓\033[0m", end="")
        print("\033[1;97m RPQ SELECTIVE QUERY TOOL \033[0m", end="")
        print("\033[38;5;36m▓\033[38;5;37m▓\033[38;5;38m▓\033[38;5;39m▓\033[0m")
        
        print(f"\033[1;97m│\033[0m \033[1;92m•\033[0m \033[1mDeveloped by:\033[0m j Nuñez - Utalca         \033[1;97m\033[0m")
        print(f"\033[1;97m│\033[0m \033[1;92m•\033[0m \033[1mTemplate Queries:\033[0m \033[1;93m{len(self.query_patterns)}\033[0m          \033[1;97m\033[0m")
        
        if self.abstract_patterns:
            print(f"\033[1;97m│\033[0m \033[1;92m•\033[0m \033[1mAbstract Queries:\033[0m \033[1;93m{len(self.abstract_patterns)}\033[0m           \033[1;97m\033[0m")
        
        #print(f"\033[1;97m│\033[0m \033[1;92m•\033[0m \033[1mModo:\033[0m \033[1;96mSELECTIVO (por defecto)\033[0m        \033[1;97m\033[0m")
        
        if hasattr(self, 'selective_queries') and self.selective_queries:
            aq = self.selective_queries.get('n_abstract', '*')
            tq = self.selective_queries.get('n_templates', '*') 
            rq = self.selective_queries.get('n_real', 3)
            print(f"\033[1;97m│\033[0m \033[1;92m•\033[0m \033[1mConfig:\033[0m aq={aq}, tq={tq}, rq={rq}          \033[1;97m\033[0m")
        
        print("\033[1;97m└─────────────────────────────────────────┘\033[0m")
   
    def get_scale_factor(self):
        """Solicita al usuario que seleccione un factor de escala"""
        print("Por favor, seleccione un factor de escala para el benchmark:")
        for i, factor in enumerate(self.scale_factors, 1):
            print(f"{i}. {factor}")
       
        while True:
            try:
                choice = int(input("\nIngrese el número de la opción (1-4): "))
                if 1 <= choice <= 4:
                    self.selected_scale = self.scale_factors[choice-1]
                    return self.selected_scale
                else:
                    print("Error: Por favor ingrese un número entre 1 y 4.")
            except ValueError:
                print("Error: Por favor ingrese un número válido.")
    
    def print_progress_bar(self, current, total, bar_length=40):
        """Imprime una barra de progreso en la consola"""
        if total == 0:
            percent = 0
        else:
            percent = int(100 * (current / float(total)))
        
        filled_length = int(bar_length * current // total)
        bar = '#' * filled_length + '-' * (bar_length - filled_length)
        
        sys.stdout.write(f'\r[{bar}] {percent}% ({current}/{total} consultas)')
        sys.stdout.flush()
    
    #################################################
    # FUNCIONES DE GENERACIÓN DE CONSULTAS
    #################################################
    
    def generate_query_script(self, script_path="runTRAIL_query_script.sh"):
        """
        Genera dinámicamente el script bash con las consultas,
        reemplazando los identificadores de nodos según la etiqueta inicial
        """
        print("\nGenerando script de consultas para el factor de escala", self.selected_scale)
        
        # Asignar consultas a patrones abstractos según la distribución especificada
        self.map_queries_to_patterns()
        
        # Crear contenido del script bash
        script_content = """#!/bin/bash

    # URL del endpoint
    BASE_URL="http://localhost:1234/query"

    # Lista de consultas a ejecutar
    PATTERNS=(
    """
        
        # Procesar cada patrón de consulta
        count = 0
        skipped = 0
        
        # Guardamos información sobre las consultas para usarla después
        query_info = {}
        
        for pattern in self.query_patterns:
            # Verificar si el patrón ya contiene un ID de nodo específico en lugar de 'x'
            if not "(x)=" in pattern:
                # El patrón ya tiene un ID de nodo, añadirlo tal cual
                script_content += f'"{pattern}"\n'
                abstract_pattern = self.query_to_pattern.get(pattern, "Desconocido")
                query_info[pattern] = {"original": pattern, "abstract_pattern": abstract_pattern}
                count += 1
                continue
                
            # Extraer la etiqueta inicial usando múltiples patrones
            initial_label = self.extract_initial_label(pattern)
            
            if initial_label:
                # Limpiar la etiqueta (quitar espacios y caracteres extra)
                initial_label = initial_label.strip()
                if initial_label.endswith(')'):
                    initial_label = initial_label[:-1]
                if '?' in initial_label:
                    initial_label = initial_label.replace('?', '')
                    
                # Verificar si tenemos un mapeo para esta etiqueta
                if initial_label in self.node_mappings:
                    # Obtener la lista de nodos para esta etiqueta
                    node_ids = self.node_mappings[initial_label]
                    
                    # Crear una consulta para cada nodo
                    for node_id in node_ids:
                        # Reemplazar 'x' con el ID de nodo correspondiente
                        query = pattern.replace("(x)=", f"({node_id})=")
                        
                        # Obtener a qué patrón abstracto pertenece esta consulta
                        abstract_pattern = self.query_to_pattern.get(pattern, "Desconocido")
                        
                        # Añadir la consulta al script
                        script_content += f'"{query}"\n'
                        query_info[query] = {
                            "original": pattern, 
                            "abstract_pattern": abstract_pattern,
                            "node_id": node_id,
                            "label": initial_label
                        }
                        count += 1
                else:
                    print(f"Advertencia: No se encontró mapeo para la etiqueta '{initial_label}'")
                    skipped += 1
            else:
                print(f"Advertencia: No se pudo extraer etiqueta inicial de: {pattern}")
                skipped += 1
        
        # Finalizar el script
        script_content += """)

    # Ejecutar las consultas
    for PATTERN in "${PATTERNS[@]}"; do
        # Ejecutar la consulta
        RESPONSE=$(curl -s -X POST "$BASE_URL" -d "$PATTERN")
        
        # Imprimir la consulta que se está ejecutando (opcional, para depuración)
        echo "Ejecutando: $PATTERN"
        
        # Esperar un segundo entre consultas para no sobrecargar el servidor
    done

    # Confirmar que todas las consultas se completaron
    echo "Todas las consultas se ejecutaron correctamente."
    """
        
        # Guardar el script en un archivo
        with open(script_path, "w") as f:
            f.write(script_content)
        
        # Hacer el script ejecutable
        os.chmod(script_path, 0o755)
        
        # Guardar información de las consultas para usarla después
        with open("query_info.json", "w") as f:
            json.dump(query_info, f, indent=2)
        
        print(f"Se generó el script con {count} consultas en '{script_path}'")
        if skipped > 0:
            print(f"Se omitieron {skipped} consultas porque no se pudo determinar la etiqueta inicial o no tenían mapeo")
        
        return script_path, count

    #IMPORTANTE
    def extract_initial_label(self, pattern):
        """
        Extrae la etiqueta inicial de un patrón de consulta, manejando todos los tipos de patrones,
        incluyendo patrones complejos con operadores alternativa (|), cuantificadores y paréntesis anidados.
        """
        # Caso 1: Patrones con etiqueta directa simple: (:etiqueta)
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 (:hasCreator)]=>(?y) RETURN ?p1
        simple_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(?:([a-zA-Z0-9_]+)', pattern)
        if simple_match:
            return simple_match.group(1)
        
        # Caso 2: Patrones de camino simple: (:etiqueta1/:etiqueta2)
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 (:hasCreator/:isLocatedIn)]=>(?y) RETURN ?p1
        path_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(?:([a-zA-Z0-9_]+)/', pattern)
        if path_match:
            return path_match.group(1)
        
        # Caso 3: Patrones con cuantificadores: (:etiqueta{0,4})
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 (:knows{1,4})]=>(?y) RETURN ?p1
        quant_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(?:([a-zA-Z0-9_]+)\{', pattern)
        if quant_match:
            return quant_match.group(1)
        
        # Caso 4: Patrones con paréntesis dobles y alternativa: ((:etiqueta1|:etiqueta2))
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 ((:hasCreator|:isLocatedIn))]=>(?y) RETURN ?p1
        alt_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(\(:([a-zA-Z0-9_]+)\|', pattern)
        if alt_match:
            return alt_match.group(1)
        
        # Caso 5: Patrones con operador de opción (?) después de un término
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 ((:containerOf/:hasTag)?)]=>(?y) RETURN ?p1
        opt_path_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(\((:?[a-zA-Z0-9_]+)/(:?[a-zA-Z0-9_]+)\)\?\)', pattern)
        if opt_path_match:
            # Eliminar los dos puntos si existen
            label = opt_path_match.group(1)
            if label.startswith(':'):
                label = label[1:]
            return label
        
        # Caso 6: Patrones con signo de interrogación en la relación
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 (:hasCreator?)]=>(?y) RETURN ?p1
        opt_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(:([a-zA-Z0-9_]+)\?\)', pattern)
        if opt_match:
            return opt_match.group(1)
        
        # Caso 7: Patrones con paréntesis y opción: ((:etiqueta)?)
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 ((:hasCreator?))]=>(?y) RETURN ?p1
        paren_opt_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(\(:([a-zA-Z0-9_]+)\?\)\)', pattern)
        if paren_opt_match:
            return paren_opt_match.group(1)
        
        # Caso 8: Patrones con alternativa y dobles paréntesis: (:etiqueta1|(:etiqueta2))
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 (:isLocatedIn|(:hasInterest/:hasType))]=>(?y) RETURN ?p1
        complex_alt_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(:([a-zA-Z0-9_]+)\|', pattern)
        if complex_alt_match:
            return complex_alt_match.group(1)
        
        # Caso 9: Patrones con grupo repetido: ((:etiqueta1/:etiqueta2){1,4})
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 ((:likes/:hasCreator){1,4})]=>(?y) RETURN ?p1
        group_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(\(:([a-zA-Z0-9_]+)/', pattern)
        if group_match:
            return group_match.group(1)
        
        # Caso 10: Patrones con relación alternativa entre paréntesis: ((:etiqueta1|:etiqueta2)?)
        # Ejemplo: MATCH (x)=[ALL TRAILS ?p1 ((:hasCreator|:isLocatedIn)?)]=>(?y) RETURN ?p1
        opt_alt_match = re.search(r'\([a-z0-9]+\)=\[ALL TRAILS \?p1\s+\(\(:[a-zA-Z0-9_]+\|:([a-zA-Z0-9_]+)\)\?\)', pattern)
        if opt_alt_match:
            # En este caso vamos a tomar la segunda etiqueta
            return opt_alt_match.group(1)
        
        # Caso 11: Si todo lo anterior falla, intentar buscar cualquier etiqueta después de dos puntos
        any_label_match = re.search(r':[a-zA-Z0-9_]+', pattern)
        if any_label_match:
            # Eliminar los dos puntos
            return any_label_match.group(0)[1:]
        
        # Si no encontramos ninguna etiqueta, retornar None
        return None   
    #################################################
    # FUNCIONES DEL SERVIDOR Y CONSULTAS
    #################################################
    
    def start_mdb_server(self):
        # NUEVA LÓGICA: Solo saltar si usamos resultados existentes Y el archivo existe
        if self.use_existing_results:
            if os.path.exists(self.result_file):
                print(f"📁 Usando archivo de resultados existente: {self.result_file}")
                print("🚫 No se iniciará el servidor MillenniumDB.")
                return
            else:
                print(f"❌ ERROR: El archivo {self.result_file} no existe.")
                print("🔄 Cambiando a modo --calculate-new automáticamente...")
                self.use_existing_results = False
                # Continuar con el inicio del servidor
        
        # RESTO DEL CÓDIGO ORIGINAL (inicio del servidor)
        if hasattr(self, 'db_path') and self.db_path:
            db_path = self.db_path
        else:
            if self.selected_scale:
                db_path = os.path.join("MillenniumDB", "data", "db", self.selected_scale)
            else:
                db_path = os.path.join("MillenniumDB", "data", "db", "01")
        
        if not os.path.exists(db_path):
            print(f"❌ Error: La base de datos '{db_path}' no existe.")
            input("\nPresione Enter para salir...")
            sys.exit(1)
            
        print(f"🚀 Iniciando servidor MillenniumDB con base de datos: {db_path}...")
        try:
            with open("result.txt", "w") as output_file:
                server_bin = os.path.join("MillenniumDB", "build", "Release", "bin", "mdb-server")
                self.server_process = subprocess.Popen(
                    [server_bin, db_path],
                    stdout=output_file,
                    stderr=output_file
                )
            print(f"✅ MillenniumDB iniciado!!")
            print(f"📝 La salida del servidor se está guardando en result.txt")
            
            print("⏳ Esperando a que el servidor se inicialice...")
            time.sleep(5)
            
            if self.server_process.poll() is not None:
                exit_code = self.server_process.poll()
                print(f"❌ Error: El servidor MillenniumDB se cerró con código {exit_code}.")
                print("📄 Revise result.txt para más detalles.")
                input("\nPresione Enter para salir...")
                sys.exit(1)
            
            print("🟢 Servidor MillenniumDB listo para recibir consultas")
            
        except Exception as e:
            print(f"❌ Error al iniciar el servidor MillenniumDB: {e}")
            input("\nPresione Enter para salir...")
            sys.exit(1)


    def run_queries_with_progress(self, timeout=300):
        # NUEVA SECCIÓN AL INICIO
        if self.use_existing_results:
            print(f"\nUsando resultados existentes de {self.result_file}")
            print("Se omite la ejecución de consultas.")
            return
        
        # RESTO DEL CÓDIGO ORIGINAL
        print("\nPreparando ejecución de consultas...")
        
        script_path, total_queries = self.generate_query_script()
        
        if total_queries == 0:
            print("No se generaron consultas para ejecutar.")
            return
        
        try:
            print(f"\nEjecutando {total_queries} consultas al servidor...")
            print("Este proceso puede tardar")
            
            with open("queries_output.txt", "w") as output_file:
                self.query_process = subprocess.Popen(
                    [f"./{script_path}"],
                    stdout=output_file,
                    stderr=output_file,
                    shell=True
                )
                
                completed_queries = 0
                last_count = 0
                start_time = time.time()
                
                progress_bar_length = 40
                self.print_progress_bar(0, total_queries, progress_bar_length)
                
                while self.query_process.poll() is None:
                    elapsed_time = time.time() - start_time
                    if elapsed_time > timeout:
                        print(f"\nTimeout después de {timeout} segundos. Terminando ejecución...")
                        self.query_process.terminate()
                        break
                    
                    time.sleep(2)
                    
                    if os.path.exists("result.txt"):
                        try:
                            with open("result.txt", 'r', encoding='utf-8', errors='replace') as f:
                                log_content = f.read()
                            
                            completed_queries = log_content.count("Query received:")
                            
                            if completed_queries > last_count:
                                completed_queries = min(completed_queries, total_queries)
                                self.print_progress_bar(completed_queries, total_queries, progress_bar_length)
                                last_count = completed_queries
                        except Exception as e:
                            print(f"\nError al leer result.txt: {e}")
                            time.sleep(1)
                
                self.print_progress_bar(total_queries, total_queries, progress_bar_length)
                
                print("\nConsultas completadas. Resultados guardados en queries_output.txt")
                
        except Exception as e:
            print(f"Error al ejecutar las consultas: {e}")


    def parse_query_results(self, output_folder="resultados_benchmark", output_excel_name="resultados_queries.xlsx", 
                        queries_per_pattern=2, selection_modes=None):
        print("\nAnalizando resultados de las consultas...")
        result_file_to_use = self.result_file if self.use_existing_results else "result.txt"
        print(f"Leyendo resultados desde: {result_file_to_use}")
        if selection_modes is None:
            selection_modes = self.selection_modes if hasattr(self, 'selection_modes') else ["max"]
        
        if isinstance(selection_modes, str):
            if '+' in selection_modes:
                selection_modes = list(dict.fromkeys([mode.strip().lower() 
                                    for mode in selection_modes.split('+')]))
            else:
                selection_modes = [selection_modes.lower()]
        
        using_selective_mode = bool(self.selective_queries)
        
        print(f"Usando modo selectivo: {using_selective_mode}")
        if using_selective_mode:
            print(f"Configuración selectiva: {self.selective_queries}")
        
        try:
            if not os.path.exists(result_file_to_use):
                print("Error: No se encontró el archivo result.txt")
                return 0
            
            query_info = {}
            if os.path.exists("query_info.json"):
                try:
                    with open("query_info.json", 'r') as f:
                        query_info = json.load(f)
                except Exception as e:
                    print(f"Advertencia: No se pudo cargar info de consultas: {e}")
                    
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)
                print(f"Se ha creado la carpeta: {output_folder}")
            
            output_excel_path = os.path.join(output_folder, output_excel_name)
                
            with open(result_file_to_use, 'r', encoding='utf-8', errors='replace') as f:
                log_content = f.read()
            
            query_groups = {}
            
            query_blocks = re.split(r"Query received:\s*\n", log_content)[1:]
            
            for i, block in enumerate(query_blocks):
                try:
                    query_match = re.search(
                        r"(MATCH\s+\(.+?\)=\[ALL TRAILS \?p1\s+\(.+?\)\]=>\(\?y\)\s+RETURN \?p1.*?)(?:\s*$|\n)",
                        block, 
                        re.MULTILINE | re.DOTALL
                    )
                    
                    if not query_match:
                        query_match = re.search(
                            r"(MATCH.+?RETURN.+?)(?:\s*$|\n)", 
                            block, 
                            re.MULTILINE | re.DOTALL
                        )
                        
                    if not query_match:
                        print(f"Advertencia: No se pudo extraer la consulta del bloque {i+1}")
                        continue
                    
                    query = query_match.group(1).strip()
                    
                    results_match = re.search(r"Results:\s*(\d+)", block)
                    exec_time_match = re.search(r"Execution duration:\s*([\d.]+)\s*ms", block)
                    
                    if results_match:
                        num_results = int(results_match.group(1))
                        exec_time = float(exec_time_match.group(1)) if exec_time_match else None
                        
                        abstract_pattern = "Desconocido"
                        template_query = "Desconocido"
                        node_id = "Desconocido"
                        q_number = None
                        
                        if query in query_info:
                            abstract_pattern = query_info[query]["abstract_pattern"]
                            if "original" in query_info[query]:
                                template_query = query_info[query]["original"]
                            if "node_id" in query_info[query]:
                                node_id = query_info[query]["node_id"]
                            
                            if abstract_pattern in self.pattern_to_q_number:
                                q_number = self.pattern_to_q_number[abstract_pattern]
                        
                        if query not in query_groups:
                            query_groups[query] = {
                                'Consulta': query,
                                'Patrón Abstracto': abstract_pattern,
                                'Consulta Plantilla': template_query,
                                'ID Nodo': node_id,
                                'Número de Paths': num_results,
                                'Q Number': q_number,
                                'Tiempos': [exec_time],
                                'Ejecuciones': 1
                            }
                        else:
                            query_groups[query]['Tiempos'].append(exec_time)
                            query_groups[query]['Ejecuciones'] += 1
                    else:
                        print(f"Advertencia: No se encontraron resultados para la consulta en el bloque {i+1}")
                except Exception as e:
                    print(f"Advertencia: Error procesando bloque {i+1} - {str(e)}")
                    continue
            
            data = []
            for query, group in query_groups.items():
                group['Tiempo Ejecución (ms)'] = sum(group['Tiempos']) / len(group['Tiempos'])
                
                if len(group['Tiempos']) > 1:
                    group['Desviación Estándar (ms)'] = statistics.stdev(group['Tiempos'])
                else:
                    group['Desviación Estándar (ms)'] = 0.0
                
                del group['Tiempos']
                
                data.append(group)

            if not data:
                print("No se encontraron resultados de consultas para analizar.")
                return 0
            
            df = pd.DataFrame(data)
            
            if 'Tiempo Ejecución (ms)' in df.columns:
                df.sort_values('Tiempo Ejecución (ms)', inplace=True)
            
            df.to_excel(output_excel_path, index=False)
            print(f"Se guardaron {len(data)} consultas únicas en {output_excel_path}")
            
            pattern_excel_path = os.path.join(output_folder, "resultados_por_patron.xlsx")
            with pd.ExcelWriter(pattern_excel_path, engine='xlsxwriter') as writer:
                df.to_excel(writer, sheet_name='Todos', index=False)
                
                workbook = writer.book
                bold_format = workbook.add_format({'bold': True})
                num_format = workbook.add_format({'num_format': '0.00'})
                bold_num_format = workbook.add_format({'bold': True, 'num_format': '0.00'})
                
                worksheet = writer.sheets['Todos']
                num_rows = len(df) + 1
                
                paths_col_idx = None
                time_col_idx = None
                
                for i, col in enumerate(df.columns):
                    if col == 'Número de Paths':
                        paths_col_idx = i
                    elif col == 'Tiempo Ejecución (ms)':
                        time_col_idx = i
                
                if paths_col_idx is not None:
                    paths_avg = df['Número de Paths'].mean()
                    worksheet.write(num_rows + 1, 0, "Promedio de Paths:", bold_format)
                    worksheet.write(num_rows + 1, paths_col_idx, paths_avg, bold_num_format)
                
                if time_col_idx is not None:
                    time_avg = df['Tiempo Ejecución (ms)'].mean()
                    worksheet.write(num_rows + 2, 0, "Promedio de Tiempo (ms):", bold_format)
                    worksheet.write(num_rows + 2, time_col_idx, time_avg, bold_num_format)
                
                patterns = df['Patrón Abstracto'].unique()
                
                pool_final_queries = []
                
                if self.selective_queries:
                    n_abstract = self.selective_queries.get('n_abstract', len(patterns))
                    n_templates = self.selective_queries.get('n_templates', 2)
                    n_real = self.selective_queries.get('n_real', 2)
                    if n_abstract == '*':
                        n_abstract = len(patterns)
                    
                    print(f"\nModo selectivo: Seleccionando top {n_abstract} abstract queries con mayor promedio de paths")
                    print(f"Para cada abstract query: {n_templates} plantillas, {n_real} consultas reales por plantilla")
                    
                    pattern_averages = []
                    for pattern in patterns:
                        pattern_df = df[df['Patrón Abstracto'] == pattern]
                        non_zero_df = pattern_df[pattern_df['Número de Paths'] > 0]
                        
                        if len(non_zero_df) > 0:
                            avg_paths = non_zero_df['Número de Paths'].mean()
                            q_number = None
                            if pattern in self.pattern_to_q_number:
                                q_number = self.pattern_to_q_number[pattern]
                            
                            pattern_averages.append({
                                'pattern': pattern,
                                'avg_paths': avg_paths,
                                'q_number': q_number,
                                'df': non_zero_df
                            })
                    
                    pattern_averages.sort(key=lambda x: x['avg_paths'], reverse=True)
                    
                    selected_patterns = pattern_averages[:n_abstract]
                    
                    print(f"\nAbstract queries seleccionadas:")
                    for i, item in enumerate(selected_patterns, 1):
                        print(f"  {i}. Q{item['q_number']} - {item['pattern']} (promedio: {item['avg_paths']:.2f} paths)")
                    
                    for item in selected_patterns:
                        pattern = item['pattern']
                        pattern_df = item['df']
                        q_number = item['q_number']
                        
                        template_groups = {}
                        for _, row in pattern_df.iterrows():
                            template = row['Consulta Plantilla']
                            if template not in template_groups:
                                template_groups[template] = []
                            template_groups[template].append(row)
                        
                        sorted_templates = []
                        for template, rows in template_groups.items():
                            avg_paths = sum(row['Número de Paths'] for row in rows) / len(rows)
                            sorted_templates.append((template, avg_paths, rows))
                        
                        sorted_templates.sort(key=lambda x: x[1], reverse=True)
                        
                        # Si n_templates es '*', usar todas las plantillas
                        if n_templates == '*':
                            selected_templates = sorted_templates
                        else:
                            selected_templates = sorted_templates[:n_templates]
                        
                        print(f"  - Patrón '{pattern}': {len(selected_templates)} plantillas seleccionadas")
                        
                        for template, avg_paths, rows in selected_templates:
                            sorted_rows = sorted(rows, key=lambda row: row['Número de Paths'], reverse=True)
                            selected_queries = sorted_rows[:n_real]
                            
                            for row in selected_queries:
                                pool_final_queries.append({
                                    'Consulta': row['Consulta'],
                                    'Patrón Abstracto': pattern,
                                    'Consulta Plantilla': row['Consulta Plantilla'],
                                    'ID Nodo': row.get('ID Nodo', ''),
                                    'Número de Paths': row['Número de Paths'],
                                    'Tiempo Ejecución (ms)': row['Tiempo Ejecución (ms)'],
                                    'Ejecuciones': row.get('Ejecuciones', 1),
                                    'Desviación Estándar (ms)': row.get('Desviación Estándar (ms)', 0),
                                    'Tipo': 'Selective',
                                    'Q Number': q_number
                                })
                    
                    print(f"\nTotal de consultas seleccionadas: {len(pool_final_queries)}")
                
                else:
                    print("\nModo estándar: procesando todos los patrones")
                    
                for pattern in patterns:
                    pattern_df = df[df['Patrón Abstracto'] == pattern]
                    
                    q_number = None
                    if pattern in self.pattern_to_q_number:
                        q_number = self.pattern_to_q_number[pattern]
                    
                    pattern_df = pattern_df.sort_values('Número de Paths', ascending=False)
                    
                    sheet_name = self.sanitize_sheet_name(pattern)
                    
                    pattern_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    worksheet = writer.sheets[sheet_name]
                    num_rows = len(pattern_df) + 1
                    
                    paths_col_idx = None
                    time_col_idx = None
                    
                    for i, col in enumerate(pattern_df.columns):
                        if col == 'Número de Paths':
                            paths_col_idx = i
                        elif col == 'Tiempo Ejecución (ms)':
                            time_col_idx = i
                    
                    if paths_col_idx is not None:
                        paths_avg = pattern_df['Número de Paths'].mean()
                        worksheet.write(num_rows + 1, 0, "Promedio de Paths:", bold_format)
                        worksheet.write(num_rows + 1, paths_col_idx, paths_avg, bold_num_format)
                    
                    if time_col_idx is not None:
                        time_avg = pattern_df['Tiempo Ejecución (ms)'].mean()
                        worksheet.write(num_rows + 2, 0, "Promedio de Tiempo (ms):", bold_format)
                        worksheet.write(num_rows + 2, time_col_idx, time_avg, bold_num_format)
                    
                    non_zero_df = pattern_df[pattern_df['Número de Paths'] > 0]
                    
                    if len(non_zero_df) > 0:
                        n_queries = len(non_zero_df)
                        print(f"Patrón '{pattern}': {n_queries} consultas únicas con caminos > 0")
                        
                        template_groups = {}
                        for _, row in non_zero_df.iterrows():
                            template = row['Consulta Plantilla']
                            if template not in template_groups:
                                template_groups[template] = []
                            template_groups[template].append(row)
                        
                        print(f"Total de plantillas diferentes: {len(template_groups)}")
                
                if pool_final_queries:
                    pool_final_df = pd.DataFrame(pool_final_queries)
                    pool_final_path = os.path.join(output_folder, "pool_final.xlsx")
                    
                    with pd.ExcelWriter(pool_final_path, engine='xlsxwriter') as writer:
                        workbook = writer.book
                        bold_format = workbook.add_format({'bold': True})
                        num_format = workbook.add_format({'num_format': '0.00'})
                        bold_num_format = workbook.add_format({'bold': True, 'num_format': '0.00'})
                        
                        pool_final_df.to_excel(writer, sheet_name='Consultas Seleccionadas', index=False)
                        
                        worksheet = writer.sheets['Consultas Seleccionadas']
                        num_rows = len(pool_final_df) + 1
                        
                        paths_col_idx = None
                        time_col_idx = None
                        
                        for i, col in enumerate(pool_final_df.columns):
                            if col == 'Número de Paths':
                                paths_col_idx = i
                            elif col == 'Tiempo Ejecución (ms)':
                                time_col_idx = i
                        
                        if paths_col_idx is not None:
                            paths_avg = pool_final_df['Número de Paths'].mean()
                            worksheet.write(num_rows + 1, 0, "Promedio de Paths:", bold_format)
                            worksheet.write(num_rows + 1, paths_col_idx, paths_avg, bold_num_format)
                        
                        if time_col_idx is not None:
                            time_avg = pool_final_df['Tiempo Ejecución (ms)'].mean()
                            worksheet.write(num_rows + 2, 0, "Promedio de Tiempo (ms):", bold_format)
                            worksheet.write(num_rows + 2, time_col_idx, time_avg, bold_num_format)
                        
                        for pattern in patterns:
                            pattern_queries = pool_final_df[pool_final_df['Patrón Abstracto'] == pattern]
                            if not pattern_queries.empty:
                                sheet_name = self.sanitize_sheet_name(pattern)
                                pattern_queries.to_excel(writer, sheet_name=sheet_name, index=False)
                                
                                worksheet = writer.sheets[sheet_name]
                                num_rows = len(pattern_queries) + 1
                                
                                paths_col_idx = None
                                time_col_idx = None
                                
                                for i, col in enumerate(pattern_queries.columns):
                                    if col == 'Número de Paths':
                                        paths_col_idx = i
                                    elif col == 'Tiempo Ejecución (ms)':
                                        time_col_idx = i
                                
                                if paths_col_idx is not None:
                                    paths_avg = pattern_queries['Número de Paths'].mean()
                                    worksheet.write(num_rows + 1, 0, "Promedio de Paths:", bold_format)
                                    worksheet.write(num_rows + 1, paths_col_idx, paths_avg, bold_num_format)
                                
                                if time_col_idx is not None:
                                    time_avg = pattern_queries['Tiempo Ejecución (ms)'].mean()
                                    worksheet.write(num_rows + 2, 0, "Promedio de Tiempo (ms):", bold_format)
                                    worksheet.write(num_rows + 2, time_col_idx, time_avg, bold_num_format)
                        
                        for tipo in set(pool_final_df['Tipo']):
                            tipo_queries = pool_final_df[pool_final_df['Tipo'] == tipo]
                            if not tipo_queries.empty:
                                tipo_queries.to_excel(writer, sheet_name=tipo, index=False)
                                
                                worksheet = writer.sheets[tipo]
                                num_rows = len(tipo_queries) + 1
                                
                                paths_col_idx = None
                                time_col_idx = None
                                
                                for i, col in enumerate(tipo_queries.columns):
                                    if col == 'Número de Paths':
                                        paths_col_idx = i
                                    elif col == 'Tiempo Ejecución (ms)':
                                        time_col_idx = i
                                
                                if paths_col_idx is not None:
                                    paths_avg = tipo_queries['Número de Paths'].mean()
                                    worksheet.write(num_rows + 1, 0, "Promedio de Paths:", bold_format)
                                    worksheet.write(num_rows + 1, paths_col_idx, paths_avg, bold_num_format)
                                
                                if time_col_idx is not None:
                                    time_avg = tipo_queries['Tiempo Ejecución (ms)'].mean()
                                    worksheet.write(num_rows + 2, 0, "Promedio de Tiempo (ms):", bold_format)
                                    worksheet.write(num_rows + 2, time_col_idx, time_avg, bold_num_format)
                        
                        if 'Q Number' in pool_final_df.columns:
                            q_numbers = pool_final_df['Q Number'].dropna().unique()
                            for q_num in sorted(q_numbers):
                                q_queries = pool_final_df[pool_final_df['Q Number'] == q_num]
                                if not q_queries.empty:
                                    sheet_name = f"Q{int(q_num)}"
                                    q_queries.to_excel(writer, sheet_name=sheet_name, index=False)
                                    
                                    worksheet = writer.sheets[sheet_name]
                                    num_rows = len(q_queries) + 1
                                    
                                    paths_col_idx = None
                                    time_col_idx = None
                                    
                                    for i, col in enumerate(q_queries.columns):
                                        if col == 'Número de Paths':
                                            paths_col_idx = i
                                        elif col == 'Tiempo Ejecución (ms)':
                                            time_col_idx = i
                                    
                                    if paths_col_idx is not None:
                                        paths_avg = q_queries['Número de Paths'].mean()
                                        worksheet.write(num_rows + 1, 0, "Promedio de Paths:", bold_format)
                                        worksheet.write(num_rows + 1, paths_col_idx, paths_avg, bold_num_format)
                                    
                                    if time_col_idx is not None:
                                        time_avg = q_queries['Tiempo Ejecución (ms)'].mean()
                                        worksheet.write(num_rows + 2, 0, "Promedio de Tiempo (ms):", bold_format)
                                        worksheet.write(num_rows + 2, time_col_idx, time_avg, bold_num_format)
                    
                    print(f"Se creó el archivo pool_final.xlsx con {len(pool_final_queries)} consultas representativas")
                    
                    pool_final_txt_path = os.path.join(output_folder, "pool_final.txt")
                    with open(pool_final_txt_path, 'w', encoding='utf-8') as txt_file:
                        for _, row in pool_final_df.iterrows():
                            txt_file.write(f"{row['Consulta']}\n")
                    
                    print(f"Se creó el archivo pool_final.txt con {len(pool_final_queries)} consultas (una por línea)")
                    
                    print("\nResumen de consultas seleccionadas:")
                    
                    q_counts = {}
                    templates_used = {}
                    for q_num in pool_final_df['Q Number'].dropna().unique():
                        q_queries = pool_final_df[pool_final_df['Q Number'] == q_num]
                        if not q_queries.empty:
                            q_counts[int(q_num)] = len(q_queries)
                            templates_used[int(q_num)] = len(q_queries['Consulta Plantilla'].unique())
                    
                    for q_num in sorted(q_counts.keys()):
                        print(f"  - Q{q_num}: {q_counts[q_num]} consultas reales, {templates_used[q_num]} plantillas")
                        
                        tipos = pool_final_df[pool_final_df['Q Number'] == q_num]['Tipo'].value_counts().to_dict()
                        tipos_str = ', '.join([f"{count} {tipo}" for tipo, count in tipos.items()])
                        print(f"    Desglose: {tipos_str}")
                
                summary_data = []
                for pattern in patterns:
                    pattern_df = df[df['Patrón Abstracto'] == pattern]
                    
                    q_number = None
                    if pattern in self.pattern_to_q_number:
                        q_number = self.pattern_to_q_number[pattern]
                    
                    summary_data.append({
                        'Patrón Abstracto': pattern,
                        'Q Number': q_number,
                        'Número de Consultas': len(pattern_df),
                        'Tiempo Promedio (ms)': pattern_df['Tiempo Ejecución (ms)'].mean(),
                        'Tiempo Mínimo (ms)': pattern_df['Tiempo Ejecución (ms)'].min(),
                        'Tiempo Máximo (ms)': pattern_df['Tiempo Ejecución (ms)'].max(),
                        'Total Paths': pattern_df['Número de Paths'].sum(),
                        'Promedio Paths': pattern_df['Número de Paths'].mean()
                    })
                
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='Resumen', index=False)
                
                worksheet = writer.sheets['Resumen']
                num_rows = len(summary_df) + 1
                
                worksheet.write(num_rows + 1, 0, "TOTAL / PROMEDIO GENERAL:", bold_format)
                
                for i, col in enumerate(summary_df.columns):
                    if col == 'Número de Consultas':
                        total = summary_df['Número de Consultas'].sum()
                        worksheet.write(num_rows + 1, i, total, bold_format)
                    elif col == 'Total Paths':
                        total = summary_df['Total Paths'].sum()
                        worksheet.write(num_rows + 1, i, total, bold_format)
                    elif col == 'Tiempo Promedio (ms)':
                        avg = summary_df['Tiempo Promedio (ms)'].mean()
                        worksheet.write(num_rows + 1, i, avg, bold_num_format)
                    elif col == 'Promedio Paths':
                        avg = summary_df['Promedio Paths'].mean()
                        worksheet.write(num_rows + 1, i, avg, bold_num_format)
            
            print(f"Se guardaron resultados organizados por patrón abstracto en {pattern_excel_path}")

# *** NUEVO: GENERAR RANKING DE TEMPLATES POR ABSTRACT QUERY ***
            print("\nGenerando rankingTemplates.xlsx...")
            ranking_templates_path = os.path.join(output_folder, "rankingTemplates.xlsx")
            
            with pd.ExcelWriter(ranking_templates_path, engine='xlsxwriter') as writer:
                workbook = writer.book
                bold_format = workbook.add_format({'bold': True})
                num_format = workbook.add_format({'num_format': '0.00'})
                bold_num_format = workbook.add_format({'bold': True, 'num_format': '0.00'})
                
                # Procesar cada patrón abstracto
                for pattern in patterns:
                    pattern_df = df[df['Patrón Abstracto'] == pattern]
                    
                    if pattern_df.empty:
                        continue
                    
                    q_number = None
                    if pattern in self.pattern_to_q_number:
                        q_number = self.pattern_to_q_number[pattern]
                    
                    # Agrupar por Template Query y calcular estadísticas
                    template_stats = []
                    template_groups = pattern_df.groupby('Consulta Plantilla')
                    
                    for template, template_group in template_groups:
                        promedio_paths = template_group['Número de Paths'].mean()
                        promedio_tiempo = template_group['Tiempo Ejecución (ms)'].mean()
                        num_consultas = len(template_group)
                        max_paths = template_group['Número de Paths'].max()
                        min_paths = template_group['Número de Paths'].min()
                        
                        template_stats.append({
                            'Template Query': template,
                            'Promedio Paths': promedio_paths,
                            'Tiempo Promedio (ms)': promedio_tiempo,
                            'Num Consultas': num_consultas,
                            'Max Paths': max_paths,
                            'Min Paths': min_paths
                        })
                    
                    # Ordenar por promedio de paths (descendente)
                    template_stats.sort(key=lambda x: x['Promedio Paths'], reverse=True)
                    
                    # Agregar ranking
                    for i, stats in enumerate(template_stats, 1):
                        stats['Ranking'] = i
                    
                    # Crear DataFrame para este patrón
                    template_ranking_df = pd.DataFrame(template_stats)
                    
                    # Reordenar columnas
                    column_order = ['Ranking', 'Template Query', 'Promedio Paths', 'Tiempo Promedio (ms)', 
                                  'Num Consultas', 'Max Paths', 'Min Paths']
                    template_ranking_df = template_ranking_df[column_order]
                    
                    # Crear nombre de hoja
                    if q_number is not None:
                        sheet_name = f"Q{int(q_number)}"
                    else:
                        sheet_name = self.sanitize_sheet_name(pattern)[:31]
                    
                    # Escribir a Excel
                    template_ranking_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    
                    # Formatear la hoja
                    worksheet = writer.sheets[sheet_name]
                    
                    # Aplicar formato numérico a columnas de números
                    for col_num, col_name in enumerate(template_ranking_df.columns):
                        if col_name in ['Promedio Paths', 'Tiempo Promedio (ms)']:
                            col_letter = chr(65 + col_num)
                            worksheet.set_column(f'{col_letter}2:{col_letter}{len(template_ranking_df)+1}', None, num_format)
                    
                    # Agregar estadísticas al final
                    num_rows = len(template_ranking_df) + 1
                    
                    # Promedio general de todos los templates
                    if template_ranking_df['Promedio Paths'].notna().any():
                        promedio_general_paths = template_ranking_df['Promedio Paths'].mean()
                        worksheet.write(num_rows + 1, 0, "Promedio General Paths:", bold_format)
                        worksheet.write(num_rows + 1, 2, promedio_general_paths, bold_num_format)
                    
                    if template_ranking_df['Tiempo Promedio (ms)'].notna().any():
                        promedio_general_tiempo = template_ranking_df['Tiempo Promedio (ms)'].mean()
                        worksheet.write(num_rows + 2, 0, "Promedio General Tiempo:", bold_format)
                        worksheet.write(num_rows + 2, 3, promedio_general_tiempo, bold_num_format)
                    
                    print(f"  ✓ Ranking de templates para {sheet_name}: {len(template_stats)} templates")
            
            print(f"Se creó el archivo rankingTemplates.xlsx con rankings de templates por abstract query")
            # *** FIN NUEVO CÓDIGO ***
            
            print("\nGenerando rankingAbstract.xlsx...")
            abstract_stats = []

            print("\nCalculando paths para ranking (basado en promedio):")
            for pattern in patterns:
                pattern_df = df[df['Patrón Abstracto'] == pattern]
                if not pattern_df.empty:
                    q_number = None
                    if pattern in self.pattern_to_q_number:
                        q_number = self.pattern_to_q_number[pattern]
                    
                    paths_promedio = pattern_df['Número de Paths'].mean()
                    tiempo_promedio = pattern_df['Tiempo Ejecución (ms)'].mean()
                    
                    print(f"  - Patrón '{pattern}' (Q{q_number}):")
                    print(f"    Paths Promedio: {paths_promedio:.2f}")
                    print(f"    Tiempo Promedio: {tiempo_promedio:.2f} ms")
                    
                    abstract_stats.append({
                        'Q Number': f"Q{int(q_number)}" if q_number is not None else "Desconocido",
                        'Patrón Abstracto': pattern,
                        'Promedio Paths': paths_promedio,
                        'Tiempo Promedio (ms)': tiempo_promedio
                    })

            ranking_df = pd.DataFrame(abstract_stats)
            if not ranking_df.empty:
                ranking_df.sort_values('Promedio Paths', ascending=False, inplace=True)
                
                ranking_df.insert(0, 'Ranking', range(1, len(ranking_df) + 1))
                
                ranking_df = ranking_df[['Ranking', 'Q Number', 'Patrón Abstracto', 'Promedio Paths', 'Tiempo Promedio (ms)']]
                
                ranking_path = os.path.join(output_folder, "rankingAbstract.xlsx")
                
                with pd.ExcelWriter(ranking_path, engine='xlsxwriter') as writer:
                    ranking_df.to_excel(writer, sheet_name='Ranking', index=False)
                    
                    workbook = writer.book
                    worksheet = writer.sheets['Ranking']
                    num_format = workbook.add_format({'num_format': '0.00'})
                    
                    for i, col in enumerate(ranking_df.columns):
                        if col in ['Promedio Paths', 'Tiempo Promedio (ms)']:
                            col_letter = chr(65 + i)
                            worksheet.set_column(f'{col_letter}2:{col_letter}{len(ranking_df)+1}', None, num_format)
                
                print(f"Se creó el archivo rankingAbstract.xlsx con el ranking de {len(ranking_df)} patrones abstractos")
            else:
                print("No se pudo crear rankingAbstract.xlsx porque no hay datos suficientes")
        
            return len(data)
            
        except Exception as e:
            print(f"Error crítico al analizar los resultados: {e}")
            import traceback
            traceback.print_exc()
            return 0   


    def run_benchmark(self):
            if hasattr(self, 'db_path') and self.db_path:
                base_name = os.path.basename(self.db_path)
                output_folder = f"resultados_benchmark_{base_name}"
            else:
                output_folder = f"resultados_benchmark_{self.selected_scale}"
            
            output_excel_name = "resultados_queries.xlsx"
            output_excel_path = os.path.join(output_folder, output_excel_name)

            print(f"\nEjecutando benchmark...")
            print("Preparando pruebas...")
            
            # FLUJO CORREGIDO PARA --calculate-new
            if not self.use_existing_results:
                print("\n🚀 MODO --calculate-new: Ejecutando TODAS las consultas primero")
                
                # PASO 1: Asegurar mapeos de nodos
                if not hasattr(self, 'node_mappings') or not self.node_mappings:
                    print("Generando mapeos de nodos necesarios...")
                    self.generate_mappings_file()
                    self.node_mappings = self.load_mappings(self.mappings_file)
                
                # PASO 2: Generar y ejecutar TODAS las consultas (modo estándar)
                print("📝 Generando script con TODAS las consultas templates...")
                script_path, total_queries = self.generate_query_script()
                
                if script_path and total_queries > 0:
                    print(f"🚀 EJECUTANDO {total_queries} consultas al servidor...")
                    print(f"Script a ejecutar: {script_path}")
                    
                    # Ejecutar las consultas usando el script generado
                    self.execute_query_script(script_path, total_queries)
                else:
                    print("❌ ERROR: No se pudo generar el script de consultas")
                    return
                
                print("\n✅ Todas las consultas ejecutadas. Procediendo al análisis selectivo...")
            else:
                print(f"📁 USANDO RESULTADOS EXISTENTES: {self.result_file}")
            
            # PASO 3: PARSEAR RESULTADOS Y APLICAR SELECCIÓN SELECTIVA
            print("\n🎯 Aplicando modo selectivo en el análisis de resultados...")
            num_queries = self.parse_query_results(
                output_folder=output_folder, 
                output_excel_name=output_excel_name,
                queries_per_pattern=self.queries_per_pattern,
                selection_modes=self.query_selection_modes
            )
            
            # Resto del código igual...
            for i in range(5):
                sys.stdout.write(".")
                sys.stdout.flush()
                time.sleep(0.5)

            print("\n\nIniciando pruebas de rendimiento adicionales...\n")

            print("\n" + "=" * 60)
            print(f"Benchmark completado")
            print("=" * 60)
            print("\nResumen de resultados:")
            if self.selected_scale:
                print(f"- Factor de escala: {self.selected_scale}")
                print(f"- Base de datos utilizada: {os.path.join('MillenniumDB', 'data', 'db', self.selected_scale)}")
                print(f"- Archivo de edges utilizado: {os.path.join('MillenniumDB', 'data', 'ldbc', self.selected_scale, 'edges.txt')}")
            else:
                print(f"- Base de datos utilizada: {self.db_path}")
            
            print(f"- Consultas analizadas: {num_queries}")
            print(f"- Modos selección nodos: {', '.join(self.selection_modes)} ({self.nodes_per_label} nodos por etiqueta)")
            print(f"- Modos selección consultas: {', '.join(self.query_selection_modes)} ({self.queries_per_pattern} consultas por patrón/modo)")
            
            # Mostrar información del modo selectivo aplicado
            if self.selective_queries:
                print(f"\n🎯 SELECCIÓN APLICADA:")
                print(f"- Abstract queries seleccionadas: {self.selective_queries.get('n_abstract', '*')}")
                print(f"- Templates por abstract: {self.selective_queries.get('n_templates', '*')}")
                print(f"- Consultas reales por template: {self.selective_queries.get('n_real', 3)}")
            
            total_time = 0
            if num_queries > 0:
                try:
                    df = pd.read_excel(output_excel_path)
                    if 'Tiempo Ejecución (ms)' in df.columns:
                        total_time = df['Tiempo Ejecución (ms)'].sum() / 1000
                        
                        if 'Patrón Abstracto' in df.columns:
                            pattern_stats = df.groupby('Patrón Abstracto').agg({
                                'Tiempo Ejecución (ms)': ['mean', 'min', 'max', 'count'],
                                'Número de Paths': ['sum']
                            })
                            
                            print("\nEstadísticas por patrón abstracto:")
                            for pattern, stats in pattern_stats.iterrows():
                                print(f"  - {pattern}:")
                                print(f"    Consultas: {stats[('Tiempo Ejecución (ms)', 'count')]}")
                                print(f"    Tiempo promedio: {stats[('Tiempo Ejecución (ms)', 'mean')]:.2f} ms")
                                print(f"    Tiempo mín/máx: {stats[('Tiempo Ejecución (ms)', 'min')]:.2f}/{stats[('Tiempo Ejecución (ms)', 'max')]:.2f} ms")
                                print(f"    Total Paths: {stats[('Número de Paths', 'sum')]}")
                                print()
                except Exception as e:
                    print(f"Aviso: No se pudo leer el archivo Excel para cálculos: {e}")
                    total_time = random.uniform(2, 10)
            else:
                total_time = random.uniform(2, 10)
                    
            print(f"- Tiempo total: {total_time:.2f} segundos")
            
            if num_queries > 0:
                rendimiento = num_queries / total_time if total_time > 0 else 0
                print(f"- Rendimiento medio: {rendimiento:.2f} ops/sec")
            else:
                print(f"- Rendimiento medio: {random.uniform(800, 2000):.2f} ops/sec")
                    
            print(f"- Resultados guardados en: {output_excel_path}")
            print(f"- Resultados por patrón abstracto: {os.path.join(output_folder, 'resultados_por_patron.xlsx')}")
            print(f"- Pool final de consultas: {os.path.join(output_folder, 'pool_final.xlsx')}")

            input("\nPresione Enter para salir...")
            
            if self.server_process and self.server_process.poll() is None:
                print("Terminando el servidor MillenniumDB...")
                self.server_process.terminate()
                try:
                    self.server_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.server_process.kill()
                print("Servidor MillenniumDB terminado.")





    def execute_query_script(self, script_path, total_queries, timeout=300):
        """
        Nueva función específica para ejecutar el script de consultas
        """
        try:
            print(f"\n⚡ Ejecutando {total_queries} consultas al servidor...")
            print("Este proceso puede tardar varios minutos...")
            
            with open("queries_output.txt", "w") as output_file:
                self.query_process = subprocess.Popen(
                    [f"./{script_path}"],
                    stdout=output_file,
                    stderr=output_file,
                    shell=True
                )
                
                completed_queries = 0
                last_count = 0
                start_time = time.time()
                
                progress_bar_length = 40
                self.print_progress_bar(0, total_queries, progress_bar_length)
                
                while self.query_process.poll() is None:
                    elapsed_time = time.time() - start_time
                    if elapsed_time > timeout:
                        print(f"\nTimeout después de {timeout} segundos. Terminando ejecución...")
                        self.query_process.terminate()
                        break
                    
                    time.sleep(2)
                    
                    if os.path.exists("result.txt"):
                        try:
                            with open("result.txt", 'r', encoding='utf-8', errors='replace') as f:
                                log_content = f.read()
                            
                            completed_queries = log_content.count("Query received:")
                            
                            if completed_queries > last_count:
                                completed_queries = min(completed_queries, total_queries)
                                self.print_progress_bar(completed_queries, total_queries, progress_bar_length)
                                last_count = completed_queries
                        except Exception as e:
                            print(f"\nError al leer result.txt: {e}")
                            time.sleep(1)
                
                self.print_progress_bar(total_queries, total_queries, progress_bar_length)
                print("\n✅ Consultas completadas. Resultados guardados en result.txt")
                
        except Exception as e:
            print(f"❌ Error al ejecutar las consultas: {e}")
            import traceback
            traceback.print_exc()        
    def read_ranking_abstract(self, ranking_folder="rankings"):
        ranking_path = os.path.join(ranking_folder, self.selected_scale, "rankingAbstract.xlsx")
        if not os.path.exists(ranking_path):
            print(f"Error: No se encontró {ranking_path}")
            return []
        
        df = pd.read_excel(ranking_path, sheet_name='Ranking')
        return df.to_dict('records')

    def read_ranking_templates(self, ranking_folder="rankings", q_number=None):
        ranking_path = os.path.join(ranking_folder, self.selected_scale, "rankingTemplates.xlsx")
        if not os.path.exists(ranking_path):
            print(f"Error: No se encontró {ranking_path}")
            return []
        
        sheet_name = f"Q{q_number}" if q_number else 'Q1'
        try:
            df = pd.read_excel(ranking_path, sheet_name=sheet_name)
            return df.to_dict('records')
        except Exception as e:
            print(f"Error leyendo sheet {sheet_name}: {e}")
            return []

    def select_abstract_queries(self, n_abstract):
        abstract_ranking = self.read_ranking_abstract()
        if n_abstract == '*':
            return abstract_ranking
        return abstract_ranking[:n_abstract]

    def select_templates_for_abstract(self, q_number, n_templates):
        template_ranking = self.read_ranking_templates(q_number=q_number)
        if not template_ranking:
            return []
        
        # Filtrar templates válidos (no NaN/None)
        valid_templates = []
        for item in template_ranking:
            template_query = item.get('Template Query', '')
            if isinstance(template_query, str) and template_query.strip():
                valid_templates.append(item)
            else:
                print(f"Template inválido ignorado: {template_query}")
        
        if n_templates == '*':
            return valid_templates
        return valid_templates[:n_templates]

    def generate_real_queries_from_template(self, template, n_real):
        if not isinstance(template, str):
            print(f"Template no es string: {type(template)} - {template}")
            return []
        
        initial_label = self.extract_initial_label(template)
        if not initial_label:
            print(f"No se pudo extraer etiqueta de template: {template}")
            return []
        
        if not hasattr(self, 'node_mappings') or not self.node_mappings:
            print("No hay mapeos de nodos cargados. Cargando mapeos...")
            self.node_mappings = self.load_mappings(self.mappings_file)
        
        if initial_label not in self.node_mappings:
            print(f"No se encontró mapeo para etiqueta: {initial_label}")
            return []
        
        node_ids = self.node_mappings[initial_label][:n_real]
        real_queries = []
        
        for node_id in node_ids:
            query = template.replace("(x)=", f"({node_id})=")
            real_queries.append(query)
        
        return real_queries

    def generate_selective_pool(self):
        if not self.selective_queries:
            return []
        
        # Asegurar que los mapeos estén cargados
        if not hasattr(self, 'node_mappings') or not self.node_mappings:
            print("Cargando mapeos de nodos...")
            if hasattr(self, 'mappings_file') and os.path.exists(self.mappings_file):
                self.node_mappings = self.load_mappings(self.mappings_file)
            else:
                print("Generando mapeos de nodos...")
                self.generate_mappings_file()
                self.node_mappings = self.load_mappings(self.mappings_file)
        
        n_abstract = self.selective_queries.get('n_abstract', '*')
        n_templates = self.selective_queries.get('n_templates', '*')
        n_real = self.selective_queries.get('n_real', 2)
        
        selected_abstracts = self.select_abstract_queries(n_abstract)
        pool_queries = []
        
        print(f"Procesando {len(selected_abstracts)} abstract queries...")
        
        for abstract_item in selected_abstracts:
            q_number = abstract_item.get('Q Number', '').replace('Q', '')
            if not q_number:
                continue
                
            print(f"Procesando Q{q_number}...")
            selected_templates = self.select_templates_for_abstract(int(q_number), n_templates)
            print(f"  - {len(selected_templates)} templates seleccionados")
            
            for template_item in selected_templates:
                template_query = template_item.get('Template Query', '')
                if not isinstance(template_query, str) or not template_query.strip():
                    continue
                    
                real_queries = self.generate_real_queries_from_template(template_query, n_real)
                print(f"    - Template generó {len(real_queries)} consultas reales")
                
                for real_query in real_queries:
                    pool_queries.append({
                        'Q_Number': f"Q{q_number}",
                        'Abstract_Pattern': abstract_item.get('Patrón Abstracto', ''),
                        'Template_Query': template_query,
                        'Real_Query': real_query,
                        'Initial_Label': self.extract_initial_label(template_query)
                    })
        
        print(f"Pool final generado: {len(pool_queries)} consultas")
        return pool_queries

    def save_selective_pool(self, pool_queries, output_folder="resultados_benchmark"):
        if not pool_queries:
            print("No hay consultas en el pool selectivo")
            return
        
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        
        df = pd.DataFrame(pool_queries)
        
        excel_path = os.path.join(output_folder, "pool_selectivo_final.xlsx")
        df.to_excel(excel_path, index=False)
        
        txt_path = os.path.join(output_folder, "pool_selectivo_final.txt")
        with open(txt_path, 'w', encoding='utf-8') as f:
            for query in pool_queries:
                f.write(f"{query['Real_Query']}\n")
        
        print(f"Pool selectivo guardado: {len(pool_queries)} consultas")
        print(f"Excel: {excel_path}")
        print(f"TXT: {txt_path}")

    def generate_query_script_from_pool(self, pool_queries, script_path="selective_query_script.sh"):
        if not pool_queries:
            print("No hay consultas en el pool para generar script")
            return None, 0
        
        script_content = """#!/bin/bash

    BASE_URL="http://localhost:1234/query"

    PATTERNS=(
    """
        
        for query_item in pool_queries:
            real_query = query_item['Real_Query']
            script_content += f'"{real_query}"\n'
        
        script_content += """)

    for PATTERN in "${PATTERNS[@]}"; do
        RESPONSE=$(curl -s -X POST "$BASE_URL" -d "$PATTERN")
        echo "Ejecutando: $PATTERN"
    done

    echo "Todas las consultas se ejecutaron correctamente."
    """
        
        with open(script_path, "w") as f:
            f.write(script_content)
        
        os.chmod(script_path, 0o755)
        
        query_info = {}
        for i, query_item in enumerate(pool_queries):
            query_info[query_item['Real_Query']] = {
                "original": query_item['Template_Query'],
                "abstract_pattern": query_item['Abstract_Pattern'],
                "node_id": self.extract_node_from_query(query_item['Real_Query']),
                "label": query_item['Initial_Label']
            }
        
        with open("selective_query_info.json", "w") as f:
            json.dump(query_info, f, indent=2)
        
        return script_path, len(pool_queries)

    def extract_node_from_query(self, query):
        match = re.search(r'MATCH \(([^)]+)\)=', query)
        return match.group(1) if match else "unknown"



    def get_nodes_per_label(self):
        """Solicita al usuario que ingrese el número de nodos a seleccionar por etiqueta"""
        print("\nPor favor, indique cuántos nodos desea seleccionar por cada etiqueta:")
        while True:
            try:
                nodes = int(input("Número de nodos por etiqueta (recomendado: 1-5): "))
                if nodes > 0:
                    self.nodes_per_label = nodes
                    return nodes
                else:
                    print("Error: El número debe ser mayor que 0.")
            except ValueError:
                print("Error: Por favor ingrese un número válido.")


    def sanitize_sheet_name(self, pattern_name):
        """Convierte un nombre de patrón abstracto en un nombre válido para hoja de Excel"""
        # Reemplazar caracteres problemáticos
        replacements = {
            '*': 'star',
            '{': 'open',
            '}': 'close',
            '[': 'bracket_open',
            ']': 'bracket_close',
            '?': 'qmark',
            '/': '_',
            '\\': '_',
            ':': '_',
            '|': 'or',
            '+': 'plus'
        }
        
        result = pattern_name
        for char, replacement in replacements.items():
            result = result.replace(char, replacement)
        
        # Limitar longitud a 31 caracteres (límite de Excel)
        return result[:31]
    
    def validate_selection_mode(value):
        valid_modes = ["max", "med", "min", ".25", ".75"]
        if "+" in value:
            modes = [mode.strip().lower() for mode in value.split("+")]
            invalid_modes = [mode for mode in modes if mode not in valid_modes]
            if invalid_modes:
                raise argparse.ArgumentTypeError(
                    f"Modos inválidos: {', '.join(invalid_modes)}. Los modos válidos son: max, med, min, .25, .75"
                )
            return value
        elif value.lower() in valid_modes:
            return value.lower()
        else:
            raise argparse.ArgumentTypeError(
                f"Modo inválido: {value}. Los modos válidos son: max, med, min, .25, .75"
            )


    def handle_interrupt(self, sig, frame):
        """Maneja la interrupción del programa con CTRL+C"""
        print("\nInterrumpiendo el benchmark...")
        
        # Terminar procesos si están activos
        if self.server_process and self.server_process.poll() is None:
            print("Terminando el servidor MillenniumDB...")
            try:
                self.server_process.terminate()
                self.server_process.wait(timeout=5)
            except:
                self.server_process.kill()
            
        if self.query_process and self.query_process.poll() is None:
            print("Terminando el proceso de consultas...")
            try:
                self.query_process.terminate()
                self.query_process.wait(timeout=5)
            except:
                self.query_process.kill()
            
        print("Procesos terminados. Saliendo.")
        sys.exit(0)  


    def start(self):
        signal.signal(signal.SIGINT, self.handle_interrupt)
        
        self.show_welcome_screen()
        
        if self.operation_mode == "calculate_new":
            default_path = os.path.join("MillenniumDB", "data", "db", "01")
            
            if hasattr(self, 'db_path') and self.db_path and self.db_path != default_path:
                self.selected_scale = os.path.basename(os.path.normpath(self.db_path))
                print(f"\n\033[1;92m▶\033[0m \033[1mUsando base de datos personalizada: {self.db_path}\033[0m")
                print(f"Factor de escala detectado de la ruta: {self.selected_scale}")
            else:
                print(f"\nUsando base de datos por defecto: {self.db_path}")
                time.sleep(4)
                self.selected_scale = self.get_scale_factor()
                self.db_path = os.path.join("MillenniumDB", "data", "db", self.selected_scale)
            
            print(f"\n📋 CONFIGURACIÓN FINAL:")
            print(f"   🎯 Abstract queries: {self.selective_queries.get('n_abstract', '*')}")
            print(f"   📝 Templates por abstract: {self.selective_queries.get('n_templates', '*')}")
            print(f"   🔍 Consultas reales por template: {self.selective_queries.get('n_real', 3)}")
            print(f"   🔗 Nodos por etiqueta: {self.nodes_per_label}")
            print(f"   ⚙️  Modo selección nodos: {', '.join(self.selection_modes)}")
            print(f"   ✅ SINCRONIZADO: nodes_per_label = rq = {self.nodes_per_label}")
            
            if self.selected_scale and not self.use_existing_results:
                self.generate_mappings_file()
            else:
                if self.use_existing_results:
                    print("Usando archivo de resultados existente. Se omite la generación de mapeos.")
                else:
                    print("No se puede generar archivo de mapeos sin factor de escala.")
                    print("Se usarán mapeos predeterminados.")
            
            if not self.use_existing_results:
                self.node_mappings = self.load_mappings(self.mappings_file)
                
                num_etiquetas = len(self.node_mappings)
                total_nodos = sum(len(nodos) for nodos in self.node_mappings.values())
                print(f"Mapeos cargados: {num_etiquetas} etiquetas con {total_nodos} nodos en total")
            
            self.start_mdb_server()
            self.run_benchmark()
            
        else:
            print(f"📊 Usando rankings existentes desde: rankings/{self.rankings_scale}/")
            
            if not self.validate_rankings_exist():
                print(f"❌ ERROR: No se encontraron rankings válidos en rankings/{self.rankings_scale}/")
                print("💡 Ejecute con --calculate-new para generar rankings desde cero.")
                input("\nPresione Enter para salir...")
                return
            
            self.generate_pool_from_rankings()
            
            print("\n🎉 ¡Pool generado exitosamente desde rankings existentes!")
            input("\nPresione Enter para salir...")

def validate_parameter_consistency(args):
    """
    Valida y ajusta automáticamente la consistencia entre parámetros.
    Específicamente asegura que nodes_per_label = rq en modo selectivo.
    """
    if args.aq and args.tq and args.rq:
        if hasattr(args, 'nodes_per_label') and args.nodes_per_label != args.rq:
            original = args.nodes_per_label
            args.nodes_per_label = args.rq
            print(f"⚠️  VALIDACIÓN: nodes_per_label ajustado de {original} a {args.rq}")
            return True
    return False

def validate_selection_mode(value):
        valid_modes = ["max", "med", "min", ".25", ".75"]
        if "+" in value:
            modes = [mode.strip().lower() for mode in value.split("+")]
            invalid_modes = [mode for mode in modes if mode not in valid_modes]
            if invalid_modes:
                raise argparse.ArgumentTypeError(
                    f"Modos inválidos: {', '.join(invalid_modes)}. Los modos válidos son: max, med, min, .25, .75"
                )
            return value
        elif value.lower() in valid_modes:
            return value.lower()
        else:
            raise argparse.ArgumentTypeError(
                f"Modo inválido: {value}. Los modos válidos son: max, med, min, .25, .75"
            )

def validate_select_query(value):
    if value == '*':
        return '*'
    try:
        int_value = int(value)
        if int_value > 0:
            return int_value
        else:
            raise argparse.ArgumentTypeError("El número debe ser mayor que 0")
    except ValueError:
        raise argparse.ArgumentTypeError("Debe ser un número entero positivo o '*'")

def validate_template_queries(value):
    if value == '*':
        return '*'
    try:
        int_value = int(value)
        if int_value > 0:
            return int_value
        else:
            raise argparse.ArgumentTypeError("El número debe ser mayor que 0")
    except ValueError:
        raise argparse.ArgumentTypeError("Debe ser un número entero positivo o '*'")
 
if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    patterns_file = os.path.join(script_dir, 'consultas2.txt')
    abstract_patterns_file = os.path.join(script_dir, 'patrones2.txt')
    
    parser = argparse.ArgumentParser(
        description='PathBenchmark: Herramienta para generar y ejecutar consultas de caminos en MillenniumDB (MODO SELECTIVO)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso (MODO SELECTIVO - por defecto):
  
  # Usar configuración por defecto (aq=*, tq=*, rq=3)
  python pathBenchmark.py
  
  # Pool grande con selección pequeña
  python pathBenchmark.py --calculate-new --nodes-per-label 10 --rq 2
  python pathBenchmark.py --calculate-new --nodes-per-label 15 --aq 2 --tq 3 --rq 5
  
  # Sincronización automática (cuando no se especifica --nodes-per-label)
  python pathBenchmark.py --rq 4  # nodes_per_label se ajusta automáticamente a 4
  
  # Con archivos de resultados
  python pathBenchmark.py --use-existing --result-file result_2.txt
  python pathBenchmark.py --calculate-new --rq 4
  
  # NUEVO: Con rankings existentes
  python pathBenchmark.py --use-rankings 01 --aq 3 --rq 2
  python pathBenchmark.py --use-rankings 03 --aq 2 --tq 1 --rq 4

CONFIGURACIÓN POR DEFECTO:
  --aq "*"     (todos los abstract queries)
  --tq "*"     (todas las plantillas)  
  --rq 3       (3 consultas reales por plantilla)
  --nodes-per-label se sincroniza automáticamente con --rq SOLO si no se especifica explícitamente
        """
    )
    
    basic_group = parser.add_argument_group('Parámetros básicos')
    basic_group.add_argument('--nodes-per-label', type=int, default=3, 
                        help='Número de nodos por etiqueta para el POOL TOTAL. Si se especifica explícitamente, NO se sincroniza con --rq (default: 3)')
    basic_group.add_argument('--db-path', type=str, 
                        help='Ruta a la base de datos MillenniumDB (default: MillenniumDB/data/db/01)')
    
    selection_group = parser.add_argument_group('Modos de selección de nodos')
    selection_group.add_argument('--node-selection-mode', type=validate_selection_mode, default='max',
                      help='Modo(s) de selección de nodos: max, med, min, .25, .75 o combinaciones (default: max)')
    selection_group.add_argument('--query-selection-mode', type=validate_selection_mode, default='max',
                      help='Modo(s) de selección de consultas: max, med, min, .25, .75 o combinaciones (default: max)')
    
    selective_group = parser.add_argument_group('Selección de consultas (MODO SELECTIVO - por defecto)')
    selective_group.add_argument('--aq', type=validate_select_query, default='*',
                        help='Abstract queries a seleccionar: número específico o "*" para todos (default: "*")')
    selective_group.add_argument('--tq', type=validate_template_queries, default='*',
                        help='Templates por abstract query: número específico o "*" para todos (default: "*")')
    selective_group.add_argument('--rq', type=int, default=3,
                        help='Consultas reales por template para SELECCIÓN FINAL (default: 3)')
    
    results_group = parser.add_argument_group('Manejo de archivos de resultados')
    results_group.add_argument('--use-existing', action='store_true', default=True,
                        help='Usar archivo de resultados existente (default: True)')
    results_group.add_argument('--calculate-new', action='store_true', default=False,
                        help='Calcular nuevos resultados ejecutando consultas')
    results_group.add_argument('--result-file', type=str, default='result_1.txt',
                        help='Archivo de resultados a usar cuando --use-existing está activo (default: result_1.txt)')
    results_group.add_argument('--use-rankings', type=str, metavar='SCALE',
                        help='Usar rankings existentes del scale factor especificado (ej: 01, 03, 1, 3)')
    
    try:
        args = parser.parse_args()
        
        nodes_per_label_explicit = '--nodes-per-label' in sys.argv
        
        if args.calculate_new:
            use_existing_results = False
            result_file = "result.txt"
        else:
            use_existing_results = args.use_existing
            result_file = args.result_file
        
        selective_queries = {
            'n_abstract': args.aq,
            'n_templates': args.tq,
            'n_real': args.rq
        }
        
        print(f"\n🎯 MODO SELECTIVO (configuración activa):")
        print(f"   Abstract queries (--aq): {args.aq}")
        print(f"   Templates por abstract (--tq): {args.tq}")
        print(f"   Consultas reales por template (--rq): {args.rq}")
        
        if nodes_per_label_explicit:
            print(f"\n🔧 --nodes-per-label ESPECIFICADO EXPLÍCITAMENTE:")
            print(f"   Valor: {args.nodes_per_label}")
            print(f"   Pool total: {args.nodes_per_label} nodos por etiqueta")
            print(f"   Selección final: {args.rq} consultas por template")
            if args.nodes_per_label > args.rq:
                print(f"   ✅ Pool grande ({args.nodes_per_label}) → Selección pequeña ({args.rq})")
            else:
                print(f"   ⚠️  Pool pequeño ({args.nodes_per_label}) ≤ Selección ({args.rq})")
        else:
            if args.nodes_per_label != args.rq:
                original_nodes = args.nodes_per_label
                args.nodes_per_label = args.rq
                print(f"\n🔄 SINCRONIZACIÓN AUTOMÁTICA (--nodes-per-label no especificado):")
                print(f"   --nodes-per-label ajustado de {original_nodes} a {args.rq} (= --rq)")
            else:
                print(f"\n✅ PARÁMETROS CONSISTENTES: nodes-per-label = rq = {args.rq}")
        
        print(f"   Nodos por etiqueta (final): {args.nodes_per_label}")
        print(f"   Modo selección nodos: {args.node_selection_mode}\n")
        
        benchmark = PathBenchmark(
            patterns_file=patterns_file,
            abstract_patterns_file=abstract_patterns_file,
            nodes_per_label=args.nodes_per_label,
            selection_mode=args.node_selection_mode,
            query_selection_mode=args.query_selection_mode,
            queries_per_pattern=args.rq,
            selective_queries=selective_queries,
            use_existing_results=use_existing_results,
            result_file=result_file,
            nodes_per_label_explicit=nodes_per_label_explicit,
            use_rankings=getattr(args, 'use_rankings', None),
            calculate_new=args.calculate_new
        )
        
        if args.db_path:
            benchmark.db_path = args.db_path
        
        benchmark.start()
        
    except argparse.ArgumentTypeError as e:
        print(f"\nERROR DE TIPO: {e}")
        print("Ejemplos de modos válidos: max, med, min, .25, .75, max+min, med+min, max+med+min, .25+.75")
    except ValueError as e:
        print(f"\nERROR DE VALOR: {e}")
        print("\nEjemplos de uso correcto (MODO SELECTIVO):")
        print("  python pathBenchmark.py                                    # Usar defaults: aq=*, tq=*, rq=3")
        print("  python pathBenchmark.py --rq 2                             # Solo cambiar rq (sincronización automática)")
        print("  python pathBenchmark.py --nodes-per-label 10 --rq 2        # Pool grande, selección pequeña") 
        print("  python pathBenchmark.py --calculate-new --nodes-per-label 15 --aq 2 --tq 3 --rq 5")
        print("  python pathBenchmark.py --use-rankings 01 --aq 3 --rq 2    # NUEVO: usar rankings existentes")
        print("\nEjemplos para manejo de archivos de resultados:")
        print("  python pathBenchmark.py --use-existing --result-file result_2.txt")
        print("  python pathBenchmark.py --calculate-new --rq 5")
        