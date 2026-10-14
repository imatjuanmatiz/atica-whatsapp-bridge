# Ficha de valores de empresa — SICETAC Instant

Implementación preparada en `codex/instant-company-card`, desde `70b6180`. Pendiente de publicación en el bridge WhatsApp.

La ficha presenta Valores de empresa, las definiciones VPT/VF y una línea por importe con COP/viaje y COP/tn cuando se conoce una base positiva y finita. Usa las toneladas indicadas en la consulta o las toneladas de configuración devueltas por el motor, siempre identificando la base. No inventa peso ni divide otra vez tarifas que ya vienen en COP/tn.

Ejemplo ficticio:

```text
Valores de empresa · EMPRESA DEMO
Valor pagado a transportador (VPT) · Valor Flete (VF)

VPT: $ 1.700.000,00 COP/viaje · $ 50.000,00/tn
VF: $ 3.400.000,00 COP/viaje · $ 100.000,00/tn
Base por tonelada: 34 t de referencia
Vigencia: 2026-10-01 a 2026-10-31
```

Se conserva la simulación cuando está presente, el perfil y la proyección de datos autorizados. Una generadora solo recibe VF. La leyenda incluye únicamente los tipos disponibles. Variantes múltiples conservan su identificación y peso. Vigencias diferentes conservan la asociación VPT/VF. Se retiran de esta ficha la diferencia de tarifas y el ID cuando hay una sola variante.

El encabezado VPT solicitado por el propietario es presentación de la tarifa empresarial: no se cambió la fuente para acreditar pagos ejecutados.

Validación: 49 pruebas de bridge pasan usando `/Users/atiemppoia/codex/SICETAC-API-MCP/.venv/bin/python -m unittest discover -s tests -v`; `git diff --check` pasa. Requests de envío/consulta de las pruebas están simulados. Python del sistema carecía de FastAPI; se usó el entorno existente sin instalar dependencias nuevas.

Archivos: `enterprise_rates.py`, punto de llamada en `main.py`, y `tests/test_enterprise_rates.py`. No se cambiaron el portal, las tablas, los permisos ni la configuración productiva.
