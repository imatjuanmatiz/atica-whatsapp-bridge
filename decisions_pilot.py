"""Optional stateless interpreter for WhatsApp. No session or contact is sent."""
import base64
import hashlib
import hmac
import json
import math
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
        result={'status':'draft','queries':validated}
        grounding=data.get('grounding')
        if isinstance(grounding,dict) and grounding.get('return_inherited') is True and len(validated)>1:
            shared=('vehiculo','carroceria','modo_viaje','tipo_contenedor','horas_logisticas','mes','peso_carga_t')
            if all(all(q.get(k)==validated[0].get(k) for k in shared) for q in validated[1:]):
                result['same_as_first']=True
        return result
    except (requests.RequestException,ValueError,TypeError,KeyError):return None


ACTION_PREFIX='sii:variants:v1:'
RETURN_ACTION_PREFIX='sii:return:v1:'
BODY_CODES={'E':'General - Estacas','F':'General - Furgon','P':'Portacontenedores'}


def action_query(values):
    """Fixed public-reference parameters, never identity or permissions."""
    if not isinstance(values,list) or len(values)!=9:return None
    origin,destination,vehicle,body,mode,container,hours,month,weight=values
    if any(not isinstance(c,str) or not re.fullmatch(r'\d{8}',c) for c in (origin,destination)):return None
    if not isinstance(vehicle,str) or not re.fullmatch(r'[A-Z0-9]{1,12}',vehicle):return None
    if not isinstance(body,str) or body not in BODY_CODES or mode not in ('CARGADO','VACIO'):return None
    if container not in (None,'CARGADO','VACIO'):return None
    if body=='P' and (mode!='CARGADO' or container not in ('CARGADO','VACIO')):return None
    if body!='P' and container is not None:return None
    if type(hours) not in (int,float) or not math.isfinite(hours) or not 0<=hours<=48:return None
    if mode=='VACIO' and hours!=0:return None
    if type(month) is not int or not 200001<=month<=210012 or not 1<=month%100<=12:return None
    if weight is not None and (type(weight) not in (int,float) or not math.isfinite(weight) or not 0<weight<=60):return None
    if (container=='VACIO' or mode=='VACIO') and weight is not None:return None
    return dict(origen=origin,destino=destination,codigo_dane_origen=origin,codigo_dane_destino=destination,
        vehiculo=vehicle,carroceria=BODY_CODES[body],modo_viaje=mode,tipo_contenedor=container,
        horas_logisticas=hours,mes=month,peso_carga_t=weight)


def signed_query_action(q,period,prefix):
    key=os.getenv('SICETAC_INSTANT_DECISIONS_BRIDGE_KEY','')
    body=next((k for k,v in BODY_CODES.items() if v==q.get('carroceria')),None)
    container=q.get('tipo_contenedor')
    if body=='P' and container is None:container='CARGADO'
    values=[q.get('codigo_dane_origen'),q.get('codigo_dane_destino'),q.get('vehiculo'),body,
        q.get('modo_viaje'),container,q.get('horas_logisticas'),period,q.get('peso_carga_t')]
    if not key or action_query(values) is None:return None
    payload=base64.urlsafe_b64encode(json.dumps(values,separators=(',',':'),allow_nan=False).encode()).decode().rstrip('=')
    signature=hmac.new(key.encode(),(prefix+payload).encode(),hashlib.sha256).hexdigest()[:24]
    action=prefix+payload+':'+signature
    return action if len(action)<=256 else None


def variant_action(q,period):
    return signed_query_action(q,period,ACTION_PREFIX)


def return_action(q,period):
    # The alternative is calculated only on click; it does not change the trip.
    if q.get('modo_viaje')!='CARGADO' or q.get('tipo_contenedor')=='VACIO':return None
    alternate={**q,'peso_carga_t':None}
    if q.get('carroceria')=='Portacontenedores':
        alternate['tipo_contenedor']='VACIO'
    else:
        alternate.update(modo_viaje='VACIO',tipo_contenedor=None,horas_logisticas=0)
    return signed_query_action(alternate,period,RETURN_ACTION_PREFIX)


def decode_query_action(text,prefix):
    key=os.getenv('SICETAC_INSTANT_DECISIONS_BRIDGE_KEY','')
    if not key or not isinstance(text,str) or not text.startswith(prefix) or len(text)>256:return None
    try:
        payload,signature=text[len(prefix):].split(':')
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,220}',payload) or not re.fullmatch(r'[0-9a-f]{24}',signature):return None
        expected=hmac.new(key.encode(),(prefix+payload).encode(),hashlib.sha256).hexdigest()[:24]
        if not hmac.compare_digest(signature,expected):return None
        values=json.loads(base64.urlsafe_b64decode(payload+'='*(-len(payload)%4)))
        return action_query(values)
    except (ValueError,TypeError):return None


def decode_variant_action(text):
    q=decode_query_action(text,ACTION_PREFIX)
    return {'status':'draft','queries':[q],'view_all_variants':True} if q else None


def decode_return_action(text):
    q=decode_query_action(text,RETURN_ACTION_PREFIX)
    if not q or not (q['modo_viaje']=='VACIO' or q['tipo_contenedor']=='VACIO'):return None
    return {'status':'draft','queries':[q],'alternative_return':True}


def selected_principal(result,q):
    variants=result.get('variantes') or []
    if len(variants)==1:return variants[0]
    selection=result.get('seleccion_variante') or {}
    rid=q.get('rutasid') or selection.get('rutasid_principal') or result.get('rutasid') or (result.get('detalle_lookup') or {}).get('rutasid')
    if rid is None:return None
    matches=[v for v in variants if str(v.get('ID_SICE') or v.get('RUTASID') or '')==str(rid)]
    return matches[0] if len(matches)==1 else None


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


def reference_text(result,q,format_hours,*,principal=None):
    parts=[f"Carrocería: {q['carroceria']}",f"Modo: {q['modo_viaje']}"]
    if q.get('tipo_contenedor'):parts.append(f"Contenedor: {q['tipo_contenedor']}")
    if result.get('mes'):parts.append(f"Periodo: {result['mes']}")
    if result.get('estimado'):parts.append('VALOR ESTIMADO: no es una referencia de ruta publicada.')
    variants=result.get('variantes') or []
    rows=[principal] if principal is not None else (variants or [None])
    if principal is not None and len(variants)>1:parts.append('Ruta seleccionada' if q.get('rutasid') else 'Ruta principal')
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
    if principal is None:parts.append('Escribe otra consulta con la ruta completa, vehículo y horas.')
    return '\n'.join(parts)


def respond(plan,*,query,format_result,format_hours,format_market,private_message,proof,send,recipient,send_buttons=None,format_primary=None):
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
        principal=None;action=None
        if not plan.get('view_all_variants'):
            principal=selected_principal(result,q)
            if principal is not None and len(result.get('variantes') or [])>1:
                action=variant_action(q,result.get('mes')) if send_buttons is not None else None
                if action is None:principal=None  # Preserve access to every alternative.
        text=None
        if principal is not None and format_primary is not None:
            row={k:v for k,v in result.items() if k!='variantes'}
            row.update(principal);row['totales']=principal.get('totales',{})
            text=format_primary(row,q)
        if not text:text=reference_text(result,q,format_hours,principal=principal)
        if len(plan['queries'])>1:text=f'Tramo {index}/{len(plan["queries"])}\n'+text
        if plan.get('same_as_first') and index==1:
            text+='\nMisma configuración en todos los tramos · '+str(q['horas_logisticas']).removesuffix('.0')+' horas por tramo.'
        if plan.get('alternative_return'):
            text='Alternativa de regreso: '+('contenedor vacío transportado.' if q.get('tipo_contenedor')=='VACIO' else 'vehículo sin carga · H0.')+'\n'+text
        buttons=[{'id':action,'title':'Ver otras variantes'}] if action else []
        round_trip=(len(plan['queries'])==2 and
            plan['queries'][0].get('codigo_dane_origen')==q.get('codigo_dane_destino') and
            plan['queries'][0].get('codigo_dane_destino')==q.get('codigo_dane_origen'))
        if send_buttons is not None and index==2 and round_trip and plan.get('same_as_first'):
            alternate=return_action(q,result.get('mes'))
            if alternate:
                title='Contenedor vacío' if q.get('carroceria')=='Portacontenedores' else 'Regreso sin carga'
                buttons.append({'id':alternate,'title':title})
        # Existing interactive sender accepts a 1024-character body.
        if buttons and len(text)<=1024:
            send_buttons(to=recipient,body=text,buttons=buttons)
        else:
            send_parts(send,recipient,text)
            if buttons:send_buttons(to=recipient,body='Opciones de este tramo: se calculan al elegirlas.',buttons=buttons)
        market=format_market(result)
        if market:send(to=recipient,body=market)
        private_result=result
        if principal is not None:
            private_result={**result,'variantes':[principal],'totales':principal.get('totales',{})}
        private=private_message(proof=proof,result=private_result,route=q,vehicle=q['vehiculo'],body=q['carroceria'],
            travel_mode=q['modo_viaje'],toneladas=q.get('peso_carga_t'))
        if private:send(to=recipient,body=private)
    return {'status':'ok','mode':'decisions-stateless','completed_legs':len(plan['queries'])}
