from __future__ import annotations

import torch
import sqlite3
import re
from typing import Dict, List, Optional, Tuple
from transformers import (
    AutoTokenizer, 
    AutoModelForCausalLM,
    pipeline,
    BitsAndBytesConfig
)
from sentence_transformers import SentenceTransformer
import numpy as np
from dataclasses import dataclass
import logging
from auf_analyzer.storage.db import get_connection

logger = logging.getLogger(__name__)

# Modelos optimizados para español y recursos limitados
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
LLM_MODEL = "PlanTL-GOB-ES/gpt2-base-bne"  # Modelo español más liviano

@dataclass
class QueryContext:
    """Contexto extraído de la base de datos para la consulta"""
    equipos_relevantes: List[str]
    temporada: Optional[int] = None
    torneo: Optional[str] = None
    tipo_consulta: str = "general"  # equipo, jugador, comparacion, estadisticas

class ConversationalAIConsultor:
    def __init__(self):
        self.embedding_model = None
        self.llm_model = None
        self.tokenizer = None
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self._load_models()
    
    def _load_models(self):
        """Carga los modelos de embeddings y lenguaje"""
        try:
            logger.info("Cargando modelos de IA...")
            
            # Modelo de embeddings para búsqueda semántica
            self.embedding_model = SentenceTransformer(EMBEDDING_MODEL)
            
            # Modelo de lenguaje con configuración optimizada
            self.tokenizer = AutoTokenizer.from_pretrained(LLM_MODEL)
            
            # Configuración para reducir uso de memoria
            quantization_config = BitsAndBytesConfig(
                load_in_4bit=True,
                bnb_4bit_use_double_quant=True,
                bnb_4bit_quant_type="nf4",
                bnb_4bit_compute_dtype=torch.float16
            )
            
            self.llm_model = AutoModelForCausalLM.from_pretrained(
                LLM_MODEL,
                quantization_config=quantization_config,
                device_map="auto",
                torch_dtype=torch.float16,
                low_cpu_mem_usage=True
            )
            
            if self.tokenizer.pad_token is None:
                self.tokenizer.pad_token = self.tokenizer.eos_token
                
            logger.info("Modelos cargados exitosamente")
            
        except Exception as e:
            logger.error(f"Error cargando modelos: {e}")
            # Fallback a pipeline más simple
            self.fallback_generator = pipeline(
                "text-generation",
                model=LLM_MODEL,
                tokenizer=LLM_MODEL,
                device=0 if self.device == "cuda" else -1
            )

    def _analyze_query_intent(self, query: str) -> QueryContext:
        """Analiza la intención de la consulta y extrae entidades relevantes"""
        query_lower = query.lower()
        context = QueryContext(equipos_relevantes=[])
        
        # Detectar equipos mencionados
        equipos = self._get_all_teams()
        for equipo in equipos:
            if equipo.lower() in query_lower:
                context.equipos_relevantes.append(equipo)
        
        # Detectar temporada
        year_match = re.search(r'(20\d{2})', query)
        if year_match:
            context.temporada = int(year_match.group(1))
        
        # Detectar torneo
        if 'apertura' in query_lower:
            context.torneo = 'apertura'
        elif 'clausura' in query_lower:
            context.torneo = 'clausura'
        elif 'intermedio' in query_lower:
            context.torneo = 'intermedio'
        elif 'anual' in query_lower:
            context.torneo = 'anual'
        
        # Detectar tipo de consulta
        if any(palabra in query_lower for palabra in ['compar', 'vs', 'contra', 'enfrent']):
            context.tipo_consulta = 'comparacion'
        elif any(palabra in query_lower for palabra in ['jugador', 'goleador', 'arquero', 'delantero']):
            context.tipo_consulta = 'jugador'
        elif len(context.equipos_relevantes) == 1:
            context.tipo_consulta = 'equipo'
        elif any(palabra in query_lower for palabra in ['tabla', 'posición', 'clasific']):
            context.tipo_consulta = 'estadisticas'
        
        return context

    def _get_all_teams(self) -> List[str]:
        """Obtiene todos los nombres de equipos de la base de datos"""
        conn = get_connection()
        try:
            teams = conn.execute("SELECT name FROM teams").fetchall()
            return [team['name'] for team in teams]
        finally:
            conn.close()

    def _retrieve_relevant_data(self, context: QueryContext) -> Dict:
        """Recupera datos relevantes de la base de datos basado en el contexto"""
        conn = get_connection()
        data = {}
        
        try:
            # Datos por defecto si no se especifica temporada/torneo
            temporada = context.temporada or 2024
            torneo = context.torneo or 'apertura'
            
            if context.tipo_consulta == 'comparacion' and len(context.equipos_relevantes) >= 2:
                data['comparacion'] = self._get_team_comparison(
                    conn, context.equipos_relevantes[0], context.equipos_relevantes[1], temporada, torneo
                )
            
            elif context.tipo_consulta == 'equipo' and context.equipos_relevantes:
                data['equipo'] = self._get_team_details(
                    conn, context.equipos_relevantes[0], temporada, torneo
                )
            
            elif context.tipo_consulta == 'estadisticas':
                data['estadisticas'] = self._get_league_stats(conn, temporada, torneo)
            
            elif context.tipo_consulta == 'jugador':
                data['jugadores'] = self._get_player_stats(conn, temporada, torneo)
            
            # Datos generales siempre disponibles
            data['general'] = {
                'temporada': temporada,
                'torneo': torneo,
                'equipos_relevantes': context.equipos_relevantes
            }
            
        finally:
            conn.close()
        
        return data

    def _get_team_comparison(self, conn: sqlite3.Connection, equipo_a: str, equipo_b: str, 
                           temporada: int, torneo: str) -> Dict:
        """Obtiene comparación detallada entre dos equipos"""
        # Obtener datos de la tabla de posiciones
        from auf_analyzer.storage.db import compute_table, cards_by_team
        
        table_data = compute_table(temporada, torneo)
        cards_data = cards_by_team(conn, temporada, torneo)
        
        equipo_a_data = next((r for r in table_data['rows'] if r['team'] == equipo_a), None)
        equipo_b_data = next((r for r in table_data['rows'] if r['team'] == equipo_b), None)
        
        equipo_a_cards = next((c for c in cards_data if c['team'] == equipo_a), {'yellow': 0, 'red': 0})
        equipo_b_cards = next((c for c in cards_data if c['team'] == equipo_b), {'yellow': 0, 'red': 0})
        
        # Obtener enfrentamientos históricos
        historico = self._get_historical_matches(conn, equipo_a, equipo_b, temporada)
        
        return {
            'equipo_a': {
                'nombre': equipo_a,
                'data': equipo_a_data,
                'tarjetas': equipo_a_cards,
                'ultimos_partidos': equipo_a_data.get('last5', '') if equipo_a_data else ''
            },
            'equipo_b': {
                'nombre': equipo_b,
                'data': equipo_b_data,
                'tarjetas': equipo_b_cards,
                'ultimos_partidos': equipo_b_data.get('last5', '') if equipo_b_data else ''
            },
            'historico': historico
        }

    def _get_team_details(self, conn: sqlite3.Connection, equipo: str, temporada: int, torneo: str) -> Dict:
        """Obtiene detalles completos de un equipo"""
        from auf_analyzer.storage.db import compute_table, cards_by_team
        
        table_data = compute_table(temporada, torneo)
        cards_data = cards_by_team(conn, temporada, torneo)
        
        equipo_data = next((r for r in table_data['rows'] if r['team'] == equipo), None)
        equipo_cards = next((c for c in cards_data if c['team'] == equipo), {'yellow': 0, 'red': 0})
        
        # Obtener goleadores del equipo
        goleadores = conn.execute("""
            SELECT p.full_name, COUNT(*) as goles
            FROM match_events e
            JOIN matches m ON e.match_id = m.id
            JOIN players p ON e.player_id = p.id
            JOIN teams t ON p.team_id = t.id
            WHERE m.season_year = ? AND m.stage_code = ? 
            AND t.name = ? AND e.type = 'goal'
            GROUP BY p.id
            ORDER BY goles DESC
            LIMIT 5
        """, (temporada, torneo, equipo)).fetchall()
        
        return {
            'nombre': equipo,
            'estadisticas': equipo_data,
            'disciplina': equipo_cards,
            'goleadores': [{'jugador': row['full_name'], 'goles': row['goles']} for row in goleadores],
            'ultimos_partidos': equipo_data.get('last5', '') if equipo_data else ''
        }

    def _get_league_stats(self, conn: sqlite3.Connection, temporada: int, torneo: str) -> Dict:
        """Obtiene estadísticas generales de la liga"""
        from auf_analyzer.storage.db import compute_table
        
        table_data = compute_table(temporada, torneo)
        
        # Top goleadores general
        goleadores = conn.execute("""
            SELECT p.full_name, t.name as equipo, COUNT(*) as goles
            FROM match_events e
            JOIN matches m ON e.match_id = m.id
            JOIN players p ON e.player_id = p.id
            JOIN teams t ON p.team_id = t.id
            WHERE m.season_year = ? AND m.stage_code = ? AND e.type = 'goal'
            GROUP BY p.id, t.name
            ORDER BY goles DESC
            LIMIT 10
        """, (temporada, torneo)).fetchall()
        
        return {
            'tabla_posiciones': table_data['rows'][:10],  # Top 10
            'goleadores': [dict(row) for row in goleadores],
            'total_equipos': len(table_data['rows'])
        }

    def _get_player_stats(self, conn: sqlite3.Connection, temporada: int, torneo: str) -> Dict:
        """Obtiene estadísticas de jugadores"""
        from auf_analyzer.storage.db import player_standard_stats
        
        stats = player_standard_stats(temporada, torneo)
        return {
            'destacados': sorted(stats, key=lambda x: x.get('gls', 0), reverse=True)[:10]
        }

    def _get_historical_matches(self, conn: sqlite3.Connection, equipo_a: str, equipo_b: str, 
                              temporada: int) -> List[Dict]:
        """Obtiene historial de enfrentamientos entre dos equipos"""
        matches = conn.execute("""
            SELECT m.date, m.home_goals, m.away_goals, t1.name as home_team, t2.name as away_team
            FROM matches m
            JOIN teams t1 ON m.home_team_id = t1.id
            JOIN teams t2 ON m.away_team_id = t2.id
            WHERE m.season_year = ? 
            AND ((t1.name = ? AND t2.name = ?) OR (t1.name = ? AND t2.name = ?))
            ORDER BY m.date DESC
            LIMIT 5
        """, (temporada, equipo_a, equipo_b, equipo_b, equipo_a)).fetchall()
        
        return [dict(match) for match in matches]

    def _build_conversational_prompt(self, query: str, context: QueryContext, data: Dict) -> str:
        """Construye el prompt para la conversación"""
        
        prompt = f"""Eres un analista deportivo experto en el fútbol uruguayo. Responde de forma natural y conversacional a la pregunta del usuario.

CONTEXTO DE LA TEMPORADA:
- Temporada: {data['general']['temporada']}
- Torneo: {data['general']['torneo'].title()}
- Equipos mencionados: {', '.join(data['general']['equipos_relevantes']) if data['general']['equipos_relevantes'] else 'Ninguno específico'}

DATOS RELEVANTES:"""

        # Agregar datos específicos según el tipo de consulta
        if 'comparacion' in data:
            comp = data['comparacion']
            prompt += f"""

COMPARACIÓN ENTRE EQUIPOS:
{comp['equipo_a']['nombre']}:
- Posición: {comp['equipo_a']['data']['pos'] if comp['equipo_a']['data'] else 'N/A'}
- Puntos: {comp['equipo_a']['data']['pts'] if comp['equipo_a']['data'] else 'N/A'}
- Diferencia de goles: {comp['equipo_a']['data']['gd'] if comp['equipo_a']['data'] else 'N/A'}
- Racha: {comp['equipo_a']['ultimos_partidos']}

{comp['equipo_b']['nombre']}:
- Posición: {comp['equipo_b']['data']['pos'] if comp['equipo_b']['data'] else 'N/A'}
- Puntos: {comp['equipo_b']['data']['pts'] if comp['equipo_b']['data'] else 'N/A'}
- Diferencia de goles: {comp['equipo_b']['data']['gd'] if comp['equipo_b']['data'] else 'N/A'}
- Racha: {comp['equipo_b']['ultimos_partidos']}"""

        if 'equipo' in data:
            equipo = data['equipo']
            prompt += f"""

INFORMACIÓN DEL EQUIPO {equipo['nombre'].upper()}:
- Posición: {equipo['estadisticas']['pos']}
- Puntos: {equipo['estadisticas']['pts']} (Promedio: {equipo['estadisticas']['ppg']}/partido)
- Récord: {equipo['estadisticas']['w']}V-{equipo['estadisticas']['d']}E-{equipo['estadisticas']['l']}D
- Goles: {equipo['estadisticas']['gf']} a favor, {equipo['estadisticas']['ga']} en contra
- Disciplina: {equipo['disciplina']['yellow']} amarillas, {equipo['disciplina']['red']} rojas
- Racha actual: {equipo['ultimos_partidos']}
- Goleadores: {', '.join([f"{p['jugador']} ({p['goles']})" for p in equipo['goleadores']])}"""

        if 'estadisticas' in data:
            stats = data['estadisticas']
            prompt += f"""

ESTADÍSTICAS DE LA LIGA:
- Total equipos: {stats['total_equipos']}
- Top 3 tabla: {', '.join([f"{e['team']} ({e['pts']} pts)" for e in stats['tabla_posiciones'][:3]])}
- Goleadores: {', '.join([f"{g['full_name']} ({g['goles']})" for g in stats['goleadores'][:3]])}"""

        prompt += f"""

PREGUNTA DEL USUARIO: {query}

RESPUESTA (se natural, conversacional y basado en los datos proporcionados, máximo 150 palabras):
"""
        return prompt

    def generate_response(self, query: str) -> Dict:
        """Genera una respuesta conversacional a la consulta del usuario"""
        try:
            # 1. Analizar la intención de la consulta
            context = self._analyze_query_intent(query)
            
            # 2. Recuperar datos relevantes
            data = self._retrieve_relevant_data(context)
            
            # 3. Construir prompt conversacional
            prompt = self._build_conversational_prompt(query, context, data)
            
            # 4. Generar respuesta
            if hasattr(self, 'fallback_generator'):
                response = self.fallback_generator(
                    prompt,
                    max_length=400,
                    num_return_sequences=1,
                    temperature=0.7,
                    do_sample=True,
                    pad_token_id=self.tokenizer.eos_token_id
                )[0]['generated_text']
            else:
                inputs = self.tokenizer.encode(prompt, return_tensors="pt").to(self.device)
                
                with torch.no_grad():
                    outputs = self.llm_model.generate(
                        inputs,
                        max_length=400,
                        num_return_sequences=1,
                        temperature=0.7,
                        do_sample=True,
                        pad_token_id=self.tokenizer.eos_token_id,
                        top_p=0.9
                    )
                
                response = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
            
            # Extraer solo la respuesta (remover el prompt)
            respuesta_texto = response.replace(prompt, '').strip()
            
            return {
                "respuesta": respuesta_texto,
                "consulta_original": query,
                "equipos_relevantes": context.equipos_relevantes,
                "temporada": data['general']['temporada'],
                "torneo": data['general']['torneo'],
                "tipo_consulta": context.tipo_consulta
            }
            
        except Exception as e:
            logger.error(f"Error generando respuesta: {e}")
            return {
                "respuesta": "Lo siento, hubo un error procesando tu consulta. Por favor intenta con una pregunta más específica sobre equipos, jugadores o estadísticas del torneo uruguayo.",
                "consulta_original": query,
                "error": str(e)
            }

# Instancia global del consultor conversacional
_consultor_conversacional = None

def get_consultor_conversacional() -> ConversationalAIConsultor:
    global _consultor_conversacional
    if _consultor_conversacional is None:
        _consultor_conversacional = ConversationalAIConsultor()
    return _consultor_conversacional

def consulta_libre(query: str) -> Dict:
    """Función principal para consultas libres y naturales"""
    consultor = get_consultor_conversacional()
    return consultor.generate_response(query)

# Función de compatibilidad con el endpoint existente
def consultar_enfrentamiento(equipo_a: str, equipo_b: str, season: int, stage: str) -> Dict:
    """Mantener compatibilidad con el endpoint POST existente"""
    consulta = f"Compara {equipo_a} vs {equipo_b} en el {stage} {season}"
    return consulta_libre(consulta)