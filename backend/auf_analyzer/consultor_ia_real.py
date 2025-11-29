from __future__ import annotations

import torch
import sqlite3
import json
from typing import Dict, List, Optional, Tuple
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
from sentence_transformers import SentenceTransformer
import numpy as np
from dataclasses import dataclass
import logging

from auf_analyzer.storage.db import (
    get_connection, 
    compute_table, 
    cards_by_team, 
    list_scorers, 
    player_standard_stats,
    teams_list
)

logger = logging.getLogger(__name__)

# 🤖 Modelos optimizados para español
EMBEDDING_MODEL = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
LLM_MODEL = "microsoft/DialoGPT-small"  # Liviano y conversacional

@dataclass
class QueryIntent:
    """Intención detectada por IA"""
    tipo: str  # comparacion, equipo, goleadores, tabla, general
    equipos: List[str]
    confianza: float
    embeddings: np.ndarray

class ConsultorIAReal:
    def __init__(self):
        print("🤖 Iniciando Consultor IA REAL - Con Transformers")
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"📱 Device: {self.device}")
        
        self.equipos = self._cargar_equipos()
        self.embedding_model = None
        self.llm_generator = None
        
        # 📚 Base de conocimiento de preguntas ejemplo
        self.knowledge_base = self._build_knowledge_base()
        
        self._cargar_modelos()
    
    def _cargar_equipos(self) -> List[str]:
        """Carga equipos desde la BD"""
        try:
            equipos = teams_list()
            print(f"✅ Equipos cargados: {len(equipos)}")
            return equipos
        except Exception as e:
            print(f"⚠️ Error cargando equipos: {e}")
            return ["Nacional", "Peñarol", "Danubio", "Defensor", "Liverpool"]
    
    def _build_knowledge_base(self) -> Dict[str, Dict]:
        """
        🧠 Base de conocimiento: preguntas de ejemplo con sus intenciones
        Esto ayuda al modelo a clasificar mejor
        """
        return {
            "comparacion_apuesta": {
                "ejemplos": [
                    "¿A quién le apuesto, Nacional o Peñarol?",
                    "¿Quién ganaría entre Danubio y Defensor?",
                    "Nacional o Liverpool, ¿cuál es mejor?",
                    "¿A qué equipo apostar: Cerro vs Racing?",
                    "Compara Nacional contra Peñarol",
                ],
                "tipo": "comparacion",
                "palabras_clave": ["apuest", "ganar", "vs", "contra", "o", "mejor", "compar"]
            },
            "equipo_individual": {
                "ejemplos": [
                    "¿Cómo va Peñarol?",
                    "¿Cómo está Danubio en el torneo?",
                    "Dame información de Nacional",
                    "¿Qué tal anda Liverpool?",
                    "Posición de Defensor",
                ],
                "tipo": "equipo",
                "palabras_clave": ["cómo va", "cómo está", "información de", "posición de"]
            },
            "goleadores": {
                "ejemplos": [
                    "¿Quién es el goleador?",
                    "Top 5 goleadores",
                    "¿Quién anota más goles?",
                    "Máximos anotadores",
                ],
                "tipo": "goleadores",
                "palabras_clave": ["goleador", "goles", "anota", "anotador"]
            },
            "tabla": {
                "ejemplos": [
                    "Muestra la tabla",
                    "¿Cómo está la clasificación?",
                    "Ranking de equipos",
                    "Tabla de posiciones",
                ],
                "tipo": "tabla",
                "palabras_clave": ["tabla", "clasificación", "ranking", "posiciones"]
            }
        }
    
    def _cargar_modelos(self):
        """🔥 Carga los modelos de IA"""
        try:
            print("📥 Cargando modelo de embeddings...")
            self.embedding_model = SentenceTransformer(EMBEDDING_MODEL)
            print("✅ Embedding model listo")
            
            print("📥 Cargando LLM generativo...")
            self.llm_generator = pipeline(
                "text-generation",
                model=LLM_MODEL,
                tokenizer=LLM_MODEL,
                device=0 if self.device == "cuda" else -1,
                max_length=200,
                truncation=True
            )
            print("✅ LLM generativo listo")
            
            # Pre-calcular embeddings de la base de conocimiento
            print("🧠 Pre-calculando embeddings de la base de conocimiento...")
            self._precompute_knowledge_embeddings()
            print("✅ Embeddings pre-calculados")
            
        except Exception as e:
            logger.error(f"❌ Error cargando modelos: {e}")
            print(f"⚠️ Usando modo fallback sin IA")
            self.embedding_model = None
            self.llm_generator = None
    
    def _precompute_knowledge_embeddings(self):
        """Pre-calcula embeddings de ejemplos para búsqueda semántica"""
        if not self.embedding_model:
            return
        
        self.knowledge_embeddings = {}
        for categoria, data in self.knowledge_base.items():
            ejemplos = data["ejemplos"]
            embeddings = self.embedding_model.encode(ejemplos)
            self.knowledge_embeddings[categoria] = {
                "embeddings": embeddings,
                "tipo": data["tipo"]
            }
    
    def _detectar_intencion_con_ia(self, pregunta: str) -> QueryIntent:
        """
        🧠 Detección de intención usando IA (sentence-transformers)
        Usa similitud semántica con la base de conocimiento
        """
        if not self.embedding_model:
            return self._detectar_intencion_fallback(pregunta)
        
        # 1. Generar embedding de la pregunta
        pregunta_embedding = self.embedding_model.encode([pregunta])[0]
        
        # 2. Buscar equipos mencionados
        equipos_detectados = self._detectar_equipos_en_pregunta(pregunta)
        
        # 3. Calcular similitud con cada categoría de la base de conocimiento
        similitudes = {}
        for categoria, data in self.knowledge_embeddings.items():
            # Calcular similitud coseno promedio con los ejemplos
            similitud = np.mean([
                np.dot(pregunta_embedding, ejemplo_emb) / 
                (np.linalg.norm(pregunta_embedding) * np.linalg.norm(ejemplo_emb))
                for ejemplo_emb in data["embeddings"]
            ])
            similitudes[categoria] = (similitud, data["tipo"])
        
        # 4. Seleccionar la categoría más similar
        mejor_categoria = max(similitudes.items(), key=lambda x: x[1][0])
        tipo_detectado = mejor_categoria[1][1]
        confianza = float(mejor_categoria[1][0])
        
        # 5. Ajustar tipo según equipos detectados
        if len(equipos_detectados) >= 2 and tipo_detectado != "comparacion":
            tipo_detectado = "comparacion"
            confianza = max(confianza, 0.85)
        elif len(equipos_detectados) == 1 and tipo_detectado == "general":
            tipo_detectado = "equipo"
        
        print(f"🎯 IA detectó: tipo={tipo_detectado}, confianza={confianza:.2f}, equipos={equipos_detectados}")
        
        return QueryIntent(
            tipo=tipo_detectado,
            equipos=equipos_detectados,
            confianza=confianza,
            embeddings=pregunta_embedding
        )
    
    def _detectar_equipos_en_pregunta(self, pregunta: str) -> List[str]:
        """Detecta equipos mencionados en la pregunta"""
        pregunta_lower = pregunta.lower()
        equipos_encontrados = []
        
        for equipo in self.equipos:
            equipo_lower = equipo.lower()
            
            # Búsqueda exacta
            if equipo_lower in pregunta_lower:
                equipos_encontrados.append(equipo)
                continue
            
            # Búsqueda parcial (mínimo 4 caracteres)
            if len(equipo_lower) >= 4:
                for i in range(4, len(equipo_lower) + 1):
                    if equipo_lower[:i] in pregunta_lower:
                        equipos_encontrados.append(equipo)
                        break
        
        return list(set(equipos_encontrados))
    
    def _detectar_intencion_fallback(self, pregunta: str) -> QueryIntent:
        """Fallback si no hay modelos cargados"""
        pregunta_lower = pregunta.lower()
        equipos = self._detectar_equipos_en_pregunta(pregunta)
        
        if len(equipos) >= 2 or any(w in pregunta_lower for w in ['vs', 'contra', 'o', 'apuest']):
            tipo = "comparacion"
        elif len(equipos) == 1:
            tipo = "equipo"
        elif any(w in pregunta_lower for w in ['goleador', 'goles']):
            tipo = "goleadores"
        elif any(w in pregunta_lower for w in ['tabla', 'clasificación']):
            tipo = "tabla"
        else:
            tipo = "general"
        
        return QueryIntent(
            tipo=tipo,
            equipos=equipos,
            confianza=0.5,
            embeddings=np.array([])
        )
    
    def _obtener_contexto_datos(self, intent: QueryIntent) -> Dict:
        """Obtiene datos relevantes de la BD según la intención"""
        conn = get_connection()
        contexto = {}
        
        try:
            temporada = 2024
            torneo = "apertura"
            
            if intent.tipo == "comparacion" and len(intent.equipos) >= 2:
                contexto["comparacion"] = self._get_comparacion(
                    conn, intent.equipos[0], intent.equipos[1], temporada, torneo
                )
            
            elif intent.tipo == "equipo" and intent.equipos:
                contexto["equipo"] = self._get_equipo_data(
                    conn, intent.equipos[0], temporada, torneo
                )
            
            elif intent.tipo == "goleadores":
                contexto["goleadores"] = list_scorers(temporada, torneo, top=5)
            
            elif intent.tipo == "tabla":
                tabla = compute_table(temporada, torneo)
                contexto["tabla"] = tabla["rows"][:5]
            
        finally:
            conn.close()
        
        return contexto
    
    def _get_comparacion(self, conn, equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> Dict:
        """Obtiene datos de comparación"""
        tabla = compute_table(temporada, torneo)
        tarjetas = cards_by_team(conn, temporada, torneo)
        
        datos_a = next((r for r in tabla['rows'] if r['team'] == equipo_a), None)
        datos_b = next((r for r in tabla['rows'] if r['team'] == equipo_b), None)
        
        tarjetas_a = next((t for t in tarjetas if t['team'] == equipo_a), {'yellow': 0, 'red': 0})
        tarjetas_b = next((t for t in tarjetas if t['team'] == equipo_b), {'yellow': 0, 'red': 0})
        
        return {
            "equipo_a": {"datos": datos_a, "tarjetas": tarjetas_a},
            "equipo_b": {"datos": datos_b, "tarjetas": tarjetas_b}
        }
    
    def _get_equipo_data(self, conn, equipo: str, temporada: int, torneo: str) -> Dict:
        """Obtiene datos de un equipo"""
        tabla = compute_table(temporada, torneo)
        datos = next((r for r in tabla['rows'] if r['team'] == equipo), None)
        return {"datos": datos}
    
    def _generar_respuesta_con_ia(self, pregunta: str, intent: QueryIntent, contexto: Dict) -> str:
        """
        🤖 Genera respuesta usando el LLM
        """
        if not self.llm_generator:
            return self._generar_respuesta_sin_ia(pregunta, intent, contexto)
        
        # 1. Construir prompt estructurado para el LLM
        prompt = self._construir_prompt(pregunta, intent, contexto)
        
        # 2. Generar respuesta con el LLM
        try:
            respuesta_raw = self.llm_generator(
                prompt,
                max_length=250,
                num_return_sequences=1,
                temperature=0.7,
                do_sample=True,
                pad_token_id=self.llm_generator.tokenizer.eos_token_id
            )[0]['generated_text']
            
            # 3. Limpiar la respuesta (remover el prompt)
            respuesta = respuesta_raw.replace(prompt, "").strip()
            
            # 4. Si la respuesta es muy corta o mala, usar fallback
            if len(respuesta) < 20:
                return self._generar_respuesta_sin_ia(pregunta, intent, contexto)
            
            return respuesta
            
        except Exception as e:
            logger.error(f"Error generando con LLM: {e}")
            return self._generar_respuesta_sin_ia(pregunta, intent, contexto)
    
    def _construir_prompt(self, pregunta: str, intent: QueryIntent, contexto: Dict) -> str:
        """Construye un prompt estructurado para el LLM"""
        
        prompt = "Eres un analista experto de fútbol uruguayo. Responde de forma natural y directa.\n\n"
        
        if intent.tipo == "comparacion" and "comparacion" in contexto:
            comp = contexto["comparacion"]
            datos_a = comp["equipo_a"]["datos"]
            datos_b = comp["equipo_b"]["datos"]
            
            if datos_a and datos_b:
                prompt += f"DATOS:\n"
                prompt += f"{datos_a['team']}: Pos {datos_a['pos']}, {datos_a['pts']} pts, {datos_a['gf']} GF, {datos_a['ga']} GC, racha {datos_a.get('last5', 'N/A')}\n"
                prompt += f"{datos_b['team']}: Pos {datos_b['pos']}, {datos_b['pts']} pts, {datos_b['gf']} GF, {datos_b['ga']} GC, racha {datos_b.get('last5', 'N/A')}\n\n"
        
        elif intent.tipo == "equipo" and "equipo" in contexto:
            datos = contexto["equipo"]["datos"]
            if datos:
                prompt += f"DATOS de {datos['team']}:\n"
                prompt += f"Posición {datos['pos']}, {datos['pts']} pts, récord {datos['w']}-{datos['d']}-{datos['l']}, "
                prompt += f"{datos['gf']} GF, {datos['ga']} GC\n\n"
        
        prompt += f"PREGUNTA: {pregunta}\n\nRESPUESTA:"
        
        return prompt
    
    def _generar_respuesta_sin_ia(self, pregunta: str, intent: QueryIntent, contexto: Dict) -> str:
        """Fallback: genera respuesta con templates"""
        
        if intent.tipo == "comparacion" and "comparacion" in contexto:
            return self._template_comparacion(contexto["comparacion"], pregunta)
        
        elif intent.tipo == "equipo" and "equipo" in contexto:
            return self._template_equipo(contexto["equipo"])
        
        elif intent.tipo == "goleadores" and "goleadores" in contexto:
            return self._template_goleadores(contexto["goleadores"])
        
        elif intent.tipo == "tabla" and "tabla" in contexto:
            return self._template_tabla(contexto["tabla"])
        
        return "🤖 Soy tu asistente de fútbol uruguayo. Pregúntame sobre equipos, goleadores o la tabla."
    
    def _template_comparacion(self, comp: Dict, pregunta: str) -> str:
        """Template para respuestas de comparación"""
        datos_a = comp["equipo_a"]["datos"]
        datos_b = comp["equipo_b"]["datos"]
        
        if not datos_a or not datos_b:
            return "No tengo datos completos para hacer la comparación."
        
        # Calcular favorito
        score_a = datos_a['pts'] * 1.5 + datos_a['gd'] + (datos_a['gf'] * 0.5)
        score_b = datos_b['pts'] * 1.5 + datos_b['gd'] + (datos_b['gf'] * 0.5)
        
        es_apuesta = any(w in pregunta.lower() for w in ['apuest', 'aposto', 'ganar'])
        
        nombre_a = datos_a['team']
        nombre_b = datos_b['team']
        
        if abs(score_a - score_b) < 5:
            return f"⚖️ **Duelo parejo** entre {nombre_a} y {nombre_b}.\n\n" \
                   f"📊 {nombre_a}: Pos {datos_a['pos']}, {datos_a['pts']} pts, racha {datos_a.get('last5', 'N/A')}\n" \
                   f"📊 {nombre_b}: Pos {datos_b['pos']}, {datos_b['pts']} pts, racha {datos_b.get('last5', 'N/A')}\n\n" \
                   f"💡 Partido disputado, puede ir para cualquier lado."
        else:
            favorito = nombre_a if score_a > score_b else nombre_b
            fav_data = datos_a if score_a > score_b else datos_b
            
            return f"🎯 **{favorito}** es favorito.\n\n" \
                   f"📊 Mejor posición (#{fav_data['pos']}), más puntos ({fav_data['pts']}), " \
                   f"mejor diferencia de goles ({fav_data['gd']:+d}).\n" \
                   f"Racha reciente: {fav_data.get('last5', 'N/A')}\n\n" \
                   f"💡 {'Apuesta segura.' if es_apuesta else 'Llega con mejor forma.'}"
    
    def _template_equipo(self, equipo_data: Dict) -> str:
        datos = equipo_data["datos"]
        if not datos:
            return "No tengo datos de este equipo."
        
        return f"📊 **{datos['team']}**\n" \
               f"• Posición: {datos['pos']}°\n" \
               f"• Puntos: {datos['pts']} ({datos['ppg']}/partido)\n" \
               f"• Récord: {datos['w']}W-{datos['d']}E-{datos['l']}L\n" \
               f"• Goles: {datos['gf']} a favor, {datos['ga']} en contra\n" \
               f"• Racha: {datos.get('last5', 'N/A')}"
    
    def _template_goleadores(self, goleadores: List) -> str:
        if not goleadores:
            return "No hay datos de goleadores."
        
        respuesta = "⚽ **Top Goleadores:**\n"
        for i, g in enumerate(goleadores[:5], 1):
            respuesta += f"{i}. {g['player']} ({g['team']}) - {g['goals']} goles\n"
        return respuesta
    
    def _template_tabla(self, tabla: List) -> str:
        if not tabla:
            return "No hay datos de la tabla."
        
        respuesta = "📊 **Tabla de Posiciones:**\n"
        for eq in tabla[:5]:
            respuesta += f"{eq['pos']}. {eq['team']} - {eq['pts']} pts\n"
        return respuesta
    
    def consultar(self, pregunta: str) -> Dict:
        """
        🎯 Método principal: procesa consulta con IA
        """
        try:
            print(f"\n{'='*60}")
            print(f"📩 CONSULTA: {pregunta}")
            print(f"{'='*60}")
            
            # 1. Detectar intención con IA (sentence-transformers)
            intent = self._detectar_intencion_con_ia(pregunta)
            print(f"🧠 Intención: {intent.tipo} (confianza: {intent.confianza:.2f})")
            print(f"👥 Equipos: {intent.equipos}")
            
            # 2. Obtener datos relevantes de la BD
            contexto = self._obtener_contexto_datos(intent)
            print(f"📊 Contexto obtenido: {list(contexto.keys())}")
            
            # 3. Generar respuesta con LLM
            respuesta = self._generar_respuesta_con_ia(pregunta, intent, contexto)
            print(f"✅ Respuesta generada ({len(respuesta)} chars)")
            print(f"{'='*60}\n")
            
            return {
                "respuesta": respuesta,
                "consulta_original": pregunta,
                "intencion_detectada": {
                    "tipo": intent.tipo,
                    "equipos": intent.equipos,
                    "confianza": intent.confianza
                },
                "modelo_usado": "IA Real (Transformers + LLM)" if self.embedding_model else "Fallback (Reglas)"
            }
            
        except Exception as e:
            logger.error(f"❌ Error en consulta: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                "respuesta": "❌ Hubo un error procesando tu consulta. Intenta de nuevo.",
                "consulta_original": pregunta,
                "error": str(e)
            }


# ========== INSTANCIA GLOBAL ==========
_consultor_ia = None

def get_consultor_ia() -> ConsultorIAReal:
    global _consultor_ia
    if _consultor_ia is None:
        _consultor_ia = ConsultorIAReal()
    return _consultor_ia

def consulta_libre_ia(pregunta: str) -> Dict:
    """Función principal para consultas con IA real"""
    consultor = get_consultor_ia()
    return consultor.consultar(pregunta)

def analizar_enfrentamiento(equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> Dict:
    """Compatibilidad con endpoint existente"""
    consulta = f"Compara {equipo_a} vs {equipo_b} en el {torneo} {temporada}"
    return consulta_libre_ia(consulta)