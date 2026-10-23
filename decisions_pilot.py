"""Optional stateless interpreter for WhatsApp. No session or contact is sent."""
import os
import re

import requests

BODY_NAMES={
    'GENERAL_ESTACAS_CARGADO':'General - Estacas',
    'GENERAL_FURGON_CARGADO':'General - Furgon',
    'CONTENEDOR_PORTACONTENEDORES_CARGADO':'Portacontenedores',
}
URL='https://sicetac-al-instante-atiemppo.vercel.app/internal/decisions/interpret'


def configured():
    return bool(os.getenv('SICETAC_INSTANT_DECISIONS_BRIDGE_KEY',''))


def interpret(text,*,legacy_route_found,post=requests.post):
    key=os.getenv('SICETAC_INSTANT_DECISIONS_BRIDGE_KEY','')
    if not key:return None
    if not isinstance(text,str) or not 1<=len(text)<=2400:return None
    # Legacy/default and bulk flows remain unchanged. Spend on difficult input.
    if re.search(r'granel|volco|volqueta',text,re.I):return None
    if legacy_route_found and len(text)<100 and not re.search(r'contenedor|circuito|circular|retorno|regreso|ida y vuelta',text,re.I):return None
    try:
        r=post(URL,json={'texto':text},headers={'X-Instant-Decisions-Key':key},timeout=(3,10),allow_redirects=False)
        if r.status_code!=200:return None
        data=r.json()
        if not isinstance(data,dict) or data.get('context_stored') is not False:return None
        if data.get('status') in ('clarify','out_of_scope'):
            questions=data.get('questions',[])
            if not questions or len(questions)>12 or any(not isinstance(q,str) or len(q)>500 for q in questions):return None
            return {'status':'clarify','questions':questions}
        if data.get('status')!='draft' or data.get('operation')!='reference':return None
        queries=data.get('queries');names=data.get('municipality_names',{})
        if not isinstance(queries,list) or not 1<=len(queries)<=5 or not isinstance(names,dict):return None
        validated=[]
        for q in queries:
            if not isinstance(q,dict) or q.get('modo_ruta','CATALOGO')!='CATALOGO':return None
            origin,destination=q.get('origen'),q.get('destino')
            if any(not isinstance(c,str) or not re.fullmatch(r'\d{8}',c) for c in (origin,destination)):return None
            if any(not isinstance(names.get(c),str) or not 1<=len(names[c])<=120 for c in (origin,destination)):return None
            vehicle=q.get('configuracion')
            if not isinstance(vehicle,str) or not re.fullmatch(r'[A-Z0-9]{1,12}',vehicle):return None
            body=BODY_NAMES.get(q.get('carroceria'))
            if not body or q.get('modo_viaje') not in ('CARGADO','VACIO'):return None
            hours=q.get('horas_logisticas')
            if type(hours) not in (int,float) or not 0<=hours<=48:return None
            if q.get('mes') is not None and (type(q['mes']) is not int or not 200001<=q['mes']<=210012 or not 1<=q['mes']%100<=12):return None
            if q.get('rutasid') is not None and not re.fullmatch(r'\d{1,12}',str(q['rutasid'])):return None
            container=q.get('tipo_contenedor')
            if container not in (None,'CARGADO','VACIO'):return None
            if container is not None and (body!='Portacontenedores' or q['modo_viaje']!='CARGADO'):return None
            validated.append(dict(origen=names[origin],destino=names[destination],
                codigo_dane_origen=origin,codigo_dane_destino=destination,vehiculo=vehicle,
                carroceria=body,modo_viaje=q['modo_viaje'],tipo_contenedor=container,
                horas_logisticas=hours,mes=q.get('mes'),rutasid=q.get('rutasid'),
                peso_carga_t=q.get('peso_carga_t')))
        return {'status':'draft','queries':validated}
    except (requests.RequestException,ValueError,TypeError,KeyError):return None


def send_parts(send,recipient,text):
    # The existing sender truncates at 4096. Keep every variant without truncation.
    remaining=text
    while remaining:
        if len(remaining)<=3800:
            send(to=recipient,body=remaining);return
        cut=remaining.rfind('\n',0,3800)
        if cut<1:cut=3800
        send(to=recipient,body=remaining[:cut])
        remaining=remaining[cut:]


def reference_text(result,q,format_hours):
    parts=[f"Carrocería: {q['carroceria']}",f"Modo: {q['modo_viaje']}"]
    if q.get('tipo_contenedor'):parts.append(f"Contenedor: {q['tipo_contenedor']}")
    if result.get('mes'):parts.append(f"Periodo: {result['mes']}")
    if result.get('estimado'):parts.append('VALOR ESTIMADO: no es una referencia de ruta publicada.')
    variants=result.get('variantes') or []
    rows=variants or [None]
    if len(rows)>1:parts.append(f"Variantes disponibles: {len(rows)}; no se seleccionó una automáticamente.")
    for variant in rows:
        row={k:v for k,v in result.items() if k!='variantes'}
        if variant:
            row.update(variant)
            # A variant without its own totals cannot inherit another route's price.
            row['totales']=variant.get('totales',{})
            label=str(variant.get('NOMBRE_SICE') or 'Ruta publicada')
            rid=variant.get('ID_SICE') or variant.get('RUTASID')
            if rid:label+=f" (ID {rid})"
            parts.append(label)
        parts.append(format_hours(resultado=row,horas=q['horas_logisticas'],vehiculo=q['vehiculo'],
            toneladas=q.get('peso_carga_t'),incluir_por_tonelada=False,include_closing=False))
    parts.append('Escribe otra consulta con la ruta completa, vehículo y horas.')
    return '\n'.join(parts)


def respond(plan,*,query,format_result,format_hours,format_market,private_message,proof,send,recipient):
    """All queries validated first. Execute each leg once; never save last_route."""
    if plan['status']=='clarify':
        send(to=recipient,body='Para consultar esta solicitud:\n'+'\n'.join(plan['questions']))
        return {'status':'decisions clarification'}
    for index,q in enumerate(plan['queries'],1):
        fields={k:v for k,v in q.items() if k!='peso_carga_t' and v is not None}
        try:result=query(**fields)
        except Exception:
            send(to=recipient,body='No pude confirmar este tramo. No repetí los cálculos anteriores; vuelve a indicar la ruta que falta.')
            return {'status':'decisions partial result','completed_legs':index-1}
        if not isinstance(result,dict) or result.get('_error'):
            send(to=recipient,body=format_result(result or {}))
            return {'status':'decisions unavailable','completed_legs':index-1}
        if q['modo_viaje']=='VACIO' and result.get('metodo')!='lookup_vacio_oficial' and not result.get('estimado'):
            send(to=recipient,body='No hay una referencia oficial de vehículo vacío para este tramo y carrocería.')
            return {'status':'decisions unavailable','completed_legs':index-1}
        text=reference_text(result,q,format_hours)
        if len(plan['queries'])>1:text=f'Tramo {index}/{len(plan["queries"])}\n'+text
        send_parts(send,recipient,text)
        market=format_market(result)
        if market:send(to=recipient,body=market)
        private=private_message(proof=proof,result=result,route=q,vehicle=q['vehiculo'],body=q['carroceria'],
            travel_mode=q['modo_viaje'],toneladas=q.get('peso_carga_t'))
        if private:send(to=recipient,body=private)
    return {'status':'ok','mode':'decisions-stateless','completed_legs':len(plan['queries'])}
