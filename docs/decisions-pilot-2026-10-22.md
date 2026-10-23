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
conversación humana de WhatsApp. El recibo operativo central está en el handoff del repositorio propietario de
Instant, enlazado arriba. Código 4c0fc3b publicado como
`dep-dbdcduu1egvs738h8ecg`, live; salud 3.10.1 con piloto configurado y
fallbacks externos previos suspendidos.

La reproducción sintética HTTP encontró que varias rutas tienen alternativas.
La presentación del piloto ahora conserva cada variante con su ID, periodo y
valor para las horas solicitadas; no selecciona la primera ni suma alternativas.
Los mensajes se dividen antes del límite de WhatsApp para evitar truncarlos.
Se mantiene el rechazo de vehículo vacío cuando falta referencia VACIO oficial.
Las tres regresiones nuevas validan variantes H6, división íntegra y estado vacío.

## Aceptación publicada

Instant 0.4.15, commit f855196, despliegue
`dpl_FE36SrgxSLFZtudSNmUMzQjXYMkq`, Ready y promovido al dominio canónico.
Cuatro consultas sintéticas publicadas produjeron planes correctos y siete
tramos consultaron el motor real. Se repitió únicamente el caso del contenedor
para verificar su formato: todas las variantes conservadas, sin sumar importes.
Con el interruptor de la base realmente apagado, el webhook local volvió al
parser y obtuvo respuesta 200 del motor, con envíos y captura comercial simulados.
El contador no cambió durante ese apagado.

El titular autorizó la activación final: USD 5 totales, sin recarga automática.
El control leído al activarlo registraba USD 0,0016837 en siete intentos sintéticos
liquidados y cero pendientes. No se enviaron mensajes a contactos reales.
Queda pendiente la comprobación humana de ida y vuelta en WhatsApp; la salud
y los replays HTTP no la sustituyen. Este commit solo registra el recibo.


## Principal primero y variantes bajo demanda · 3.10.3

Juan aprobó mostrar solo la principal de cada tramo y ofrecer «Ver otras
variantes». El puente respeta `seleccion_variante.rutasid_principal` publicado por
el motor; no elige por precio o posición del arreglo. El motor actual documenta
su criterio como menor RUTASID. Una variante explícita del usuario prevalece.
Si la fuente no identifica una principal inequívoca, se presentan las opciones.

El botón de cada sentido contiene su OD DANE, configuración resuelta,
carrocería, modo, estado del contenedor, horas, periodo y peso declarado cuando
aplica. Los parámetros públicos están firmados con HMAC-SHA256 de 96 bits y una
etiqueta de dominio propia, usando la clave de canal ya configurada. No contiene
cuentas, teléfonos, empresa, prueba de acceso ni secretos. La firma protege la
integridad de parámetros; no concede autorización empresarial. Cada clic usa la
prueba del webhook actual y conserva el control de acceso previo.

El handler valida firma y parámetros antes de consultar, omite el clasificador y
no escribe una última ruta ni captura comercial. Funciona con historial vacío o
una ruta anterior distinta, y también con Decisions apagado mientras la clave
siga configurada. La rotación de esa clave invalida botones anteriores de forma
segura. No se agregó almacenamiento ni otra credencial.

Solo la principal se usa también en el mensaje empresarial inicial. Al pulsar
el botón se presentan las variantes de ese tramo. Las condiciones de contenedor
vacío transportado y H6 no cambian entre ida y regreso; no se mezclan sus botones.
No se cambiaron algoritmos, motor, presupuesto o permisos. El alcance de esta
presentación es el flujo del piloto; el parser clásico conserva su flujo previo.

Verificación: 74 pruebas del puente en modo sin red y envíos simulados. El replay
con datos publicados confirmó las principales 154 / 95, los valores H6
$3.217.526 / $2.676.872 y la expansión a cuatro variantes por sentido después de
vaciar el estado local. Cero llamadas Decisions y cero envíos Meta en el replay.
El recibo desplegado y el pendiente humano del botón se registran en el handoff
central de Instant para evitar otro despliegue solo documental de este puente.

El resumen principal final tiene cuatro líneas: OD, configuración y carga con
horas, valor SICETAC destacado y periodo/ID. Reutiliza el cálculo y formato COP
existentes; si no hay importe, usa el formato de detalle sin inventar cero.


## Candidato local 3.10.4 · 23 oct 2026 · sin publicar

Rama `codex/decisions-roundtrip-defaults-v1`. Instant 0.4.16 acompaña esta
actualización; producción permanece en Instant 0.4.15 / puente 3.10.3.
Contrato propietario en SICETAC-AL-INSTANTE:
`docs/DECISIONS-TRAMOS-MISMA-CONFIGURACION-2026-10-23.md`.

Sin cambios expresos, el perfil del primer tramo se usa en los siguientes.
Una cifra de horas aplica por tramo y se muestra en el resumen. La metadata
solo se acepta tras validar que los parámetros compartidos coincidan. En ida
y vuelta cargada con estado heredado se propone Regreso sin carga (H0), o
Contenedor vacío (mantiene las horas y vehículo cargado). La alternativa solo
se calcula al pulsarla; se conserva Ver otras variantes. No se suman variantes.

Codec autosuficiente `sii:return:v1:…` con HMAC de dominio separado, clave
existente, tipos/rangos y límites Meta. No guarda conversación. Handler antes
de captura comercial; no clasifica de nuevo; prueba privada del webhook actual.
No se cambió motor, Meta, credenciales, permisos ni presupuesto.

84 pruebas PASS, solo fixtures y envíos simulados; el verificador bloqueó tres
intentos bind/DNS preexistentes. No hubo llamadas pagadas ni envíos reales.
Tests nuevos: `tests/test_stateless_return_options.py`.
Pendiente publicar ambos candidatos y probar los botones en WhatsApp real.
No empujar a main sin intención de desplegar: Render publica automáticamente.
