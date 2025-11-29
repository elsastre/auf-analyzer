from __future__ import annotations

import sqlite3
import re
from typing import Dict, List, Optional
import logging
from auf_analyzer.storage.db import get_connection, compute_table, cards_by_team, list_scorers, player_standard_stats

logger = logging.getLogger(__name__)

class ConsultorLibre:
    def __init__(self):
        print("🚀 Iniciando Consultor Libre - IA Rápida")
        self.equipos = self._cargar_equipos_ultra_seguro()
    
    def _cargar_equipos_ultra_seguro(self) -> List[str]:
        """Carga equipos usando la función teams_list() de db.py"""
        print("🛡️ Cargando equipos con protección ABSOLUTA...")
        try:
            # ✅ USA LA FUNCIÓN OFICIAL DE db.py QUE YA RETORNA List[str]
            from auf_analyzer.storage.db import teams_list
            equipos = teams_list()
            
            print(f"📊 Equipos cargados desde db.teams_list(): {equipos}")
            print(f"🔍 Verificación de tipos:")
            for i, eq in enumerate(equipos):
                print(f"   Equipo {i}: '{eq}' (tipo: {type(eq)})")
            
            # Doble verificación: asegurar que todos sean strings
            lista_final = []
            for equipo in equipos:
                if isinstance(equipo, str) and equipo.strip():
                    lista_final.append(equipo.strip())
                else:
                    print(f"   ⚠️ Equipo inválido detectado: {equipo} (tipo: {type(equipo)})")
            
            print(f"🎯 LISTA FINAL: {lista_final}")
            return lista_final
            
        except Exception as e:
            print(f"💥 ERROR catastrófico: {e}")
            import traceback
            traceback.print_exc()
            return ["Nacional", "Peñarol"]

    def _detectar_intencion_ultra_segura(self, pregunta: str) -> Dict:
        """Detección de intención con protección EXTREMA"""
        print(f"🎯 INICIANDO DETECCIÓN")
        
        # Garantizar que pregunta sea string
        pregunta_str = str(pregunta).strip() if pregunta else ""
        pregunta_lower = pregunta_str.lower()
        
        intencion = {
            "tipo": "general",
            "equipos": [],
            "temporada": 2024,
            "torneo": "apertura", 
            "accion": "responder"
        }
        
        # Buscar equipos en la pregunta
        for equipo in self.equipos:
            # Ya garantizado que equipo es string desde _cargar_equipos_ultra_seguro()
            equipo_lower = equipo.lower()
            
            # Comparación segura - ambos son strings
            if equipo_lower in pregunta_lower:
                intencion["equipos"].append(equipo)
                print(f"   ✅ Equipo detectado: {equipo}")
        
        # Detectar tipo de consulta
        if any(palabra in pregunta_lower for palabra in ['compar', 'vs', 'contra', 'enfrent']):
            intencion["tipo"] = "comparacion"
        elif any(palabra in pregunta_lower for palabra in ['goleador', 'goleadores', 'anota', 'gol']):
            intencion["tipo"] = "goleadores"
        elif any(palabra in pregunta_lower for palabra in ['jugador', 'jugadores']):
            intencion["tipo"] = "jugadores"
        elif any(palabra in pregunta_lower for palabra in ['tabla', 'posicion', 'clasificacion']):
            intencion["tipo"] = "tabla"
        elif any(palabra in pregunta_lower for palabra in ['partido', 'partidos', 'fixture', 'calendario']):
            intencion["tipo"] = "partidos"
        elif len(intencion["equipos"]) == 1:
            intencion["tipo"] = "equipo"
        
        print(f"🎯 Intención final: {intencion}")
        return intencion

    def _obtener_datos_relevantes(self, intencion: Dict) -> Dict:
        """Obtiene datos relevantes según la intención"""
        print(f"📊 Obteniendo datos para intención: {intencion}")
        
        # Si hay error en equipos, usar datos básicos
        if not hasattr(self, 'equipos') or not self.equipos:
            print("⚠️  No hay equipos cargados, usando datos básicos")
            return {
                "general": {
                    "temporada": 2024,
                    "torneo": "apertura",
                    "equipos_liga": ["Nacional", "Peñarol"]
                }
            }
        
        conn = get_connection()
        datos = {}
        
        try:
            temporada = intencion["temporada"]
            torneo = intencion["torneo"]
            
            if intencion["tipo"] == "comparacion" and len(intencion["equipos"]) >= 2:
                datos["comparacion"] = self._obtener_comparacion_equipos(
                    conn, intencion["equipos"][0], intencion["equipos"][1], temporada, torneo
                )
            elif intencion["tipo"] == "equipo" and intencion["equipos"]:
                datos["equipo"] = self._obtener_datos_equipo(
                    conn, intencion["equipos"][0], temporada, torneo
                )
            elif intencion["tipo"] == "goleadores":
                datos["goleadores"] = list_scorers(temporada, torneo, top=5)
            elif intencion["tipo"] == "tabla":
                tabla = compute_table(temporada, torneo)
                datos["tabla"] = tabla["rows"][:5]
            elif intencion["tipo"] == "jugadores":
                datos["jugadores"] = player_standard_stats(temporada, torneo)[:5]
            
            datos["general"] = {
                "temporada": temporada,
                "torneo": torneo,
                "equipos_liga": self.equipos
            }
            
        except Exception as e:
            print(f"❌ Error obteniendo datos: {e}")
        finally:
            conn.close()
        
        return datos

    def _obtener_comparacion_equipos(self, conn: sqlite3.Connection, equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> Dict:
        """Obtiene comparación entre dos equipos"""
        try:
            tabla = compute_table(temporada, torneo)
            tarjetas = cards_by_team(conn, temporada, torneo)
            
            datos_a = next((r for r in tabla['rows'] if r['team'] == equipo_a), None)
            datos_b = next((r for r in tabla['rows'] if r['team'] == equipo_b), None)
            
            return {
                "equipo_a": {"datos": datos_a},
                "equipo_b": {"datos": datos_b}
            }
        except Exception as e:
            print(f"❌ Error en comparación: {e}")
            return {"equipo_a": {"datos": None}, "equipo_b": {"datos": None}}

    def _obtener_datos_equipo(self, conn: sqlite3.Connection, equipo: str, temporada: int, torneo: str) -> Dict:
        """Obtiene datos completos de un equipo"""
        try:
            tabla = compute_table(temporada, torneo)
            datos = next((r for r in tabla['rows'] if r['team'] == equipo), None)
            return {"datos": datos}
        except Exception as e:
            print(f"❌ Error obteniendo datos equipo: {e}")
            return {"datos": None}

    def _generar_respuesta_inteligente(self, pregunta: str, intencion: Dict, datos: Dict) -> str:
        """Genera respuesta conversacional basada en los datos"""
        try:
            if intencion["tipo"] == "comparacion" and "comparacion" in datos:
                return self._generar_respuesta_comparacion(pregunta, datos["comparacion"])
            elif intencion["tipo"] == "equipo" and "equipo" in datos:
                return self._generar_respuesta_equipo(pregunta, datos["equipo"])
            elif intencion["tipo"] == "goleadores" and "goleadores" in datos:
                return self._generar_respuesta_goleadores(datos["goleadores"])
            elif intencion["tipo"] == "tabla" and "tabla" in datos:
                return self._generar_respuesta_tabla(datos["tabla"])
            else:
                return self._generar_respuesta_general(pregunta, datos)
        except Exception as e:
            print(f"❌ Error generando respuesta: {e}")
            return "🤖 Hola! Soy tu asistente de fútbol uruguayo. Puedo ayudarte con información sobre equipos, jugadores, goleadores y la tabla de posiciones."

    def _generar_respuesta_comparacion(self, pregunta: str, comparacion: Dict) -> str:
        equipo_a = comparacion["equipo_a"]["datos"]
        equipo_b = comparacion["equipo_b"]["datos"]
        
        if not equipo_a or not equipo_b:
            return "No tengo datos completos de ambos equipos para hacer la comparación."
        
        return f"📊 Comparación: {equipo_a['team']} (posición {equipo_a['pos']}) vs {equipo_b['team']} (posición {equipo_b['pos']}). {equipo_a['team']} tiene {equipo_a['pts']} puntos y {equipo_b['team']} tiene {equipo_b['pts']} puntos."

    def _generar_respuesta_equipo(self, pregunta: str, equipo_data: Dict) -> str:
        datos = equipo_data["datos"]
        if not datos:
            return "No tengo datos de este equipo en este momento."
        
        return f"📊 {datos['team']}: Posición {datos['pos']}, {datos['pts']} puntos. Récord: {datos['w']}W {datos['d']}E {datos['l']}L. Goles: {datos['gf']} a favor, {datos['ga']} en contra."

    def _generar_respuesta_goleadores(self, goleadores: List) -> str:
        if not goleadores:
            return "No tengo datos de goleadores en este momento."
        
        respuesta = "⚽ Top goleadores:\n"
        for i, gol in enumerate(goleadores[:5], 1):
            respuesta += f"{i}. {gol['player']} ({gol['team']}) - {gol['goals']} goles\n"
        return respuesta

    def _generar_respuesta_tabla(self, tabla: List) -> str:
        if not tabla:
            return "No tengo datos de la tabla en este momento."
        
        respuesta = "🏆 Top 5 de la tabla:\n"
        for equipo in tabla[:5]:
            respuesta += f"{equipo['pos']}. {equipo['team']} - {equipo['pts']} pts\n"
        return respuesta

    def _generar_respuesta_general(self, pregunta: str, datos: Dict) -> str:
        return "🤖 Hola! Soy tu asistente de fútbol uruguayo. Puedo ayudarte con información sobre equipos, jugadores, goleadores y la tabla de posiciones. ¿Sobre qué quieres saber?"

    def consultar(self, pregunta: str) -> Dict:
        """Procesa una consulta libre y devuelve respuesta"""
        try:
            print(f"🎯 CONSULTA RECIBIDA: '{pregunta}'")
            
            intencion = self._detectar_intencion_ultra_segura(pregunta)
            datos = self._obtener_datos_relevantes(intencion)
            respuesta = self._generar_respuesta_inteligente(pregunta, intencion, datos)
            
            return {
                "respuesta": respuesta,
                "consulta_original": pregunta,
                "intencion_detectada": intencion,
                "datos_utilizados": list(datos.keys()) if datos else []
            }
            
        except Exception as e:
            print(f"💥 ERROR en consulta: {e}")
            import traceback
            traceback.print_exc()
            return {
                "respuesta": "❌ Error procesando tu consulta. Por favor intenta con una pregunta más específica.",
                "consulta_original": pregunta,
                "error": str(e)
            }

# Instancia global
_consultor_libre = None

def get_consultor_libre() -> ConsultorLibre:
    global _consultor_libre
    if _consultor_libre is None:
        _consultor_libre = ConsultorLibre()
    return _consultor_libre

def consulta_libre(pregunta: str) -> Dict:
    """Función principal para consultas libres"""
    consultor = get_consultor_libre()
    return consultor.consultar(pregunta)

def analizar_enfrentamiento(equipo_a: str, equipo_b: str, temporada: int, torneo: str) -> Dict:
    """Para mantener compatibilidad con el frontend actual"""
    consulta = f"Compara {equipo_a} vs {equipo_b} en el {torneo} {temporada}"
    return consulta_libre(consulta)