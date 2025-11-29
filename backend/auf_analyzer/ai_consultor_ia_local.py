from __future__ import annotations

import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import sqlite3
from typing import Dict, List
import logging
from auf_analyzer.storage.db import get_connection, compute_table, cards_by_team

logger = logging.getLogger(__name__)

# Modelo en español optimizado para CPU - pequeño pero efectivo
MODEL_NAME = "mrm8488/distiluse-base-multilingual-cased"
# Alternativas: 
# - "dccuchile/bert-base-spanish-wwm-uncased" (470MB)
# - "PlanTL-GOB-ES/roberta-base-bne" (500MB)
# - "mrm8488/distilbert-base-spanish-wwm-cased-finetuned-spa-squad2-es" (250MB)

class IALocalConsultor:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.tokenizer = None
        self.model = None
        self.generator = None
        self._cargar_modelo()
    
    def _cargar_modelo(self):
        """Carga el modelo de IA local"""
        try:
            logger.info("🤖 Cargando modelo de IA local...")
            
            # Opción 1: Usar pipeline para generación de texto
            self.generator = pipeline(
                "text-generation",
                model="microsoft/DialoGPT-small",  # Modelo conversacional pequeño
                tokenizer="microsoft/DialoGPT-small",
                device=-1 if self.device == "cpu" else 0
            )
            
            logger.info(f"✅ Modelo de IA local cargado en {self.device}")
            
        except Exception as e:
            logger.error(f"❌ Error cargando modelo: {e}")
            # Fallback a un modelo más simple
            try:
                self.generator = pipeline(
                    "text-generation",
                    model="gpt2",
                    device=-1 if self.device == "cpu" else 0
                )
            except Exception as e2:
                logger.error(f"❌ Error con fallback: {e2}")
                self.generator = None

    def _obtener_contexto_enfrentamiento(self, equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> str:
        """Obtiene datos contextuales del enfrentamiento"""
        conn = get_connection()
        try:
            tabla = compute_table(temporada, torneo)
            tarjetas = cards_by_team(conn, temporada, torneo)
            
            datos_a = next((r for r in tabla['rows'] if r['team'] == equipo_a), None)
            datos_b = next((r for r in tabla['rows'] if r['team'] == equipo_b), None)
            
            if not datos_a or not datos_b:
                return f"Datos insuficientes para {equipo_a} vs {equipo_b}"
            
            tarjetas_a = next((t for t in tarjetas if t['team'] == equipo_a), {'yellow': 0, 'red': 0})
            tarjetas_b = next((t for t in tarjetas if t['team'] == equipo_b), {'yellow': 0, 'red': 0})
            
            contexto = f"""
            Análisis de fútbol uruguayo - {torneo.title()} {temporada}:
            
            {equipo_a}:
            - Posición: {datos_a['pos']}
            - Puntos: {datos_a['pts']} (Promedio: {datos_a['ppg']}/partido)
            - Récord: {datos_a['w']}V-{datos_a['d']}E-{datos_a['l']}D
            - Goles: {datos_a['gf']} a favor, {datos_a['ga']} en contra (Diferencia: {datos_a['gd']})
            - Disciplina: {tarjetas_a.get('yellow', 0)} amarillas, {tarjetas_a.get('red', 0)} rojas
            - Racha reciente: {datos_a.get('last5', 'N/A')}
            
            {equipo_b}:
            - Posición: {datos_b['pos']}
            - Puntos: {datos_b['pts']} (Promedio: {datos_b['ppg']}/partido)
            - Récord: {datos_b['w']}V-{datos_b['d']}E-{datos_b['l']}D
            - Goles: {datos_b['gf']} a favor, {datos_b['ga']} en contra (Diferencia: {datos_b['gd']})
            - Disciplina: {tarjetas_b.get('yellow', 0)} amarillas, {tarjetas_b.get('red', 0)} rojas
            - Racha reciente: {datos_b.get('last5', 'N/A')}
            """
            
            return contexto
            
        except Exception as e:
            logger.error(f"Error obteniendo contexto: {e}")
            return f"Enfrentamiento: {equipo_a} vs {equipo_b} en {torneo} {temporada}"
        finally:
            conn.close()

    def _construir_prompt_inteligente(self, equipo_a: str, equipo_b: str, contexto: str) -> str:
        """Construye un prompt efectivo para el modelo de IA"""
        
        prompt = f"""
        Eres un experto analista de fútbol uruguayo. Analiza este enfrentamiento basándote en los datos proporcionados y da una recomendación natural en español.

        CONTEXTO:
        {contexto}

        INSTRUCCIONES:
        1. Analiza las fortalezas y debilidades de cada equipo
        2. Compara su rendimiento en la temporada
        3. Considera la racha reciente de resultados
        4. Evalúa la efectividad ofensiva y defensiva
        5. Da una recomendación sobre qué equipo parece tener ventaja
        6. Sé conciso pero informativo (máximo 150 palabras)
        7. Usa un lenguaje natural como si hablaras con un aficionado

        ANÁLISIS:
        """
        
        return prompt.strip()

    def generar_analisis_ia(self, prompt: str) -> str:
        """Genera análisis usando el modelo de IA local"""
        if self.generator is None:
            return "El modelo de IA no está disponible en este momento."
        
        try:
            # Generar respuesta con el modelo
            respuesta = self.generator(
                prompt,
                max_length=400,
                num_return_sequences=1,
                temperature=0.7,
                do_sample=True,
                pad_token_id=self.generator.tokenizer.eos_token_id,
                repetition_penalty=1.2
            )[0]['generated_text']
            
            # Extraer solo la parte nueva (remover el prompt)
            if prompt in respuesta:
                respuesta = respuesta.replace(prompt, "").strip()
            
            # Limpiar y formatear la respuesta
            respuesta = self._limpiar_respuesta(respuesta)
            
            return respuesta
            
        except Exception as e:
            logger.error(f"Error generando análisis IA: {e}")
            return "No pude generar un análisis en este momento. Por favor intenta más tarde."

    def _limpiar_respuesta(self, respuesta: str) -> str:
        """Limpia y formatea la respuesta del modelo"""
        # Remover textos repetitivos o incompletos
        lineas = respuesta.split('\n')
        lineas_limpias = []
        
        for linea in lineas:
            linea = linea.strip()
            if linea and not linea.startswith(('###', '---', '***')):
                if len(linea) > 10:  # Remover líneas muy cortas
                    lineas_limpias.append(linea)
        
        # Tomar solo las primeras 5 líneas para mantenerlo conciso
        respuesta_limpia = ' '.join(lineas_limpias[:5])
        
        # Asegurar que termine con punto
        if respuesta_limpia and not respuesta_limpia.endswith(('.', '!', '?')):
            respuesta_limpia += '.'
        
        return respuesta_limpia

    def analizar_enfrentamiento(self, equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> Dict:
        """Analiza un enfrentamiento usando IA local"""
        
        logger.info(f"🤖 IA Local analizando: {equipo_a} vs {equipo_b}")
        
        # Obtener datos estructurados para el frontend
        datos_equipos = self._obtener_datos_equipos(equipo_a, equipo_b, temporada, torneo)
        
        # Generar análisis con IA
        contexto = self._obtener_contexto_enfrentamiento(equipo_a, equipo_b, temporada, torneo)
        prompt = self._construir_prompt_inteligente(equipo_a, equipo_b, contexto)
        analisis_ia = self.generar_analisis_ia(prompt)
        
        return {
            **datos_equipos,
            "recomendacion": analisis_ia,
            "analisis_ia": analisis_ia,
            "modelo_utilizado": "IA Local - Transformers",
            "temporada": temporada,
            "torneo": torneo
        }

    def _obtener_datos_equipos(self, equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> Dict:
        """Obtiene datos estructurados de los equipos para el frontend"""
        conn = get_connection()
        try:
            tabla = compute_table(temporada, torneo)
            tarjetas = cards_by_team(conn, temporada, torneo)
            
            datos_a = next((r for r in tabla['rows'] if r['team'] == equipo_a), None)
            datos_b = next((r for r in tabla['rows'] if r['team'] == equipo_b), None)
            
            tarjetas_a = next((t for t in tarjetas if t['team'] == equipo_a), {'yellow': 0, 'red': 0})
            tarjetas_b = next((t for t in tarjetas if t['team'] == equipo_b), {'yellow': 0, 'red': 0})
            
            return {
                "equipo_a": {
                    "team": equipo_a,
                    "pts": datos_a['pts'] if datos_a else 0,
                    "dg": datos_a['gd'] if datos_a else 0,
                    "gf": datos_a['gf'] if datos_a else 0,
                    "gc": datos_a['ga'] if datos_a else 0,
                    "amarillas": tarjetas_a.get('yellow', 0),
                    "rojas": tarjetas_a.get('red', 0),
                    "last5": datos_a.get('last5', '') if datos_a else ''
                },
                "equipo_b": {
                    "team": equipo_b,
                    "pts": datos_b['pts'] if datos_b else 0,
                    "dg": datos_b['gd'] if datos_b else 0,
                    "gf": datos_b['gf'] if datos_b else 0,
                    "gc": datos_b['ga'] if datos_b else 0,
                    "amarillas": tarjetas_b.get('yellow', 0),
                    "rojas": tarjetas_b.get('red', 0),
                    "last5": datos_b.get('last5', '') if datos_b else ''
                }
            }
        except Exception as e:
            logger.error(f"Error obteniendo datos equipos: {e}")
            return {
                "equipo_a": {"team": equipo_a},
                "equipo_b": {"team": equipo_b}
            }
        finally:
            conn.close()

# Instancia global del consultor de IA
_ia_consultor_local = None

def get_ia_consultor_local() -> IALocalConsultor:
    global _ia_consultor_local
    if _ia_consultor_local is None:
        _ia_consultor_local = IALocalConsultor()
    return _ia_consultor_local

def analizar_enfrentamiento_ia_local(equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> Dict:
    """Función principal para análisis con IA local"""
    consultor = get_ia_consultor_local()
    return consultor.analizar_enfrentamiento(equipo_a, equipo_b, temporada, torneo)