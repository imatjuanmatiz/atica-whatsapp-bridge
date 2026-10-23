# Piloto Decisions en la línea existente · 22-oct-2026

Puente 3.10.1. El clasificador se aloja en SICETAC-AL-INSTANTE y solo recibe el
mensaje actual. El motor sigue en `https://sicetac-api-mcp.onrender.com`.
No hay contexto nuevo ni cambios de cálculo, Meta, números o permisos privados.
El flujo público y la consulta empresarial con prueba verificada se conservan.

La presencia de `SICETAC_INSTANT_DECISIONS_BRIDGE_KEY` habilita la conexión y
suspende los fallbacks OpenAI/Gemini anteriores para evitar gasto fuera del tope.
Las consultas simples siguen por el parser; texto largo, contenedores y
circuitos pueden usar el piloto. Error, apagado o agotamiento siguen por el
parser determinista. No se reintenta la clasificación ni un tramo ya calculado.

El interruptor autoritativo y el presupuesto compartido de USD 5 totales viven
en `instant_decisions.pilot` del proyecto de portal. No hay renovación automática.
Apagar con `enabled=false` sin reiniciar contadores. El procedimiento y la
migración están en el repositorio dueño:
`/Users/atiemppoia/codex/SICETAC-AL-INSTANTE/docs/DECISIONS-WHATSAPP-2026-10-22.md`.

60 pruebas del puente pasan en modo sin red y con envíos simulados; 3 intentos
bind/DNS de fixtures previos fueron bloqueados. Esto no equivale a una
conversación humana de WhatsApp. El resultado de publicación se añadirá tras
verificar estado live y salud del commit exacto.

La reproducción sintética HTTP encontró que varias rutas tienen alternativas.
La presentación del piloto ahora conserva cada variante con su ID, periodo y
valor para las horas solicitadas; no selecciona la primera ni suma alternativas.
Los mensajes se dividen antes del límite de WhatsApp para evitar truncarlos.
Se mantiene el rechazo de vehículo vacío cuando falta referencia VACIO oficial.
Las tres regresiones nuevas validan variantes H6, división íntegra y estado vacío.
