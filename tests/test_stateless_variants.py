import asyncio
import base64
import copy
import hashlib
import hmac
import json
import os
import unittest
from unittest.mock import Mock,patch

import decisions_pilot as d
import main

KEY={'SICETAC_INSTANT_DECISIONS_BRIDGE_KEY':'fixture-key'}
QUERY=dict(origen='BUENAVENTURA',destino='BOGOTÁ',codigo_dane_origen='76109000',codigo_dane_destino='11001000',
    vehiculo='C2S2',carroceria='Portacontenedores',modo_viaje='CARGADO',tipo_contenedor='CARGADO',
    horas_logisticas=6,mes=202610,peso_carga_t=5.7)
RESULT={'mes':202610,'origen':'BUENAVENTURA','destino':'BOGOTA',
    'seleccion_variante':{'rutasid_principal':'154','criterio':'menor_rutasid'},
    'variantes':[{'ID_SICE':11361,'NOMBRE_SICE':'VÍA ALTERNA','totales':{'H6':4319478}},
                 {'ID_SICE':154,'NOMBRE_SICE':'RUTA BASE','totales':{'H6':3217526}}]}


def response(plan=None,result=None,**kwargs):
    query=Mock(return_value=copy.deepcopy(result or RESULT));send=Mock();buttons=Mock();private=Mock(return_value=None)
    formatter=Mock(side_effect=lambda **kw:'H6 '+str(kw['resultado']['totales'].get('H6')))
    r=d.respond(plan or {'status':'draft','queries':[QUERY]},query=query,send=send,send_buttons=buttons,
        format_result=Mock(),format_hours=formatter,format_market=lambda _:None,
        private_message=private,proof={'verified-fixture':True},recipient={'user_id':'fixture'},**kwargs)
    return r,query,send,buttons,private


class StatelessVariantTests(unittest.TestCase):
    def test_primary_comes_from_engine_not_array_order_or_cheapest_price(self):
        with patch.dict(os.environ,KEY):r,query,send,buttons,private=response()
        self.assertEqual(r['completed_legs'],1);query.assert_called_once();send.assert_not_called()
        text=buttons.call_args.kwargs['body']
        self.assertIn('Ruta principal',text);self.assertIn('3217526',text);self.assertNotIn('4319478',text)
        self.assertEqual(buttons.call_args.kwargs['buttons'][0]['title'],'Ver otras variantes')
        self.assertEqual([v['ID_SICE'] for v in private.call_args.kwargs['result']['variantes']],[154])
        self.assertEqual(private.call_args.kwargs['proof'],{'verified-fixture':True})

    def test_action_roundtrip_keeps_exact_period_hours_and_weight_without_identity(self):
        with patch.dict(os.environ,KEY):
            action=d.variant_action(QUERY,202610);plan=d.decode_variant_action(action)
        self.assertLessEqual(len(action),256);self.assertTrue(plan['view_all_variants'])
        q=plan['queries'][0]
        for k in ('codigo_dane_origen','codigo_dane_destino','vehiculo','carroceria','modo_viaje','tipo_contenedor','horas_logisticas','mes','peso_carga_t'):
            self.assertEqual(q[k],QUERY[k])
        self.assertFalse({'proof','phone','user_id','company','rutasid'} & q.keys())

    def test_action_distinguishes_return_empty_container_from_empty_vehicle(self):
        q={**QUERY,'codigo_dane_origen':'11001000','codigo_dane_destino':'76109000','tipo_contenedor':'VACIO','peso_carga_t':None}
        with patch.dict(os.environ,KEY):plan=d.decode_variant_action(d.variant_action(q,202610))
        r=plan['queries'][0]
        self.assertEqual((r['codigo_dane_origen'],r['codigo_dane_destino']),('11001000','76109000'))
        self.assertEqual((r['modo_viaje'],r['tipo_contenedor'],r['horas_logisticas']),('CARGADO','VACIO',6))
        self.assertIsNone(r['peso_carga_t'])

    def test_tampered_missing_key_rotated_key_and_truncated_action_fail_closed(self):
        with patch.dict(os.environ,KEY):
            action=d.variant_action(QUERY,202610)
            for invalid in (action[:-1],action[:-1]+('0' if action[-1]!='0' else '1'),action+'x'*257,d.ACTION_PREFIX+'junk'):
                self.assertIsNone(d.decode_variant_action(invalid))
        for key in ('','different-fixture-key'):
            with patch.dict(os.environ,{'SICETAC_INSTANT_DECISIONS_BRIDGE_KEY':key}):self.assertIsNone(d.decode_variant_action(action))

    def test_signed_but_invalid_parameters_are_rejected(self):
        base=['76109000','11001000','C2S2','P','CARGADO','CARGADO',6,202610,5.7]
        for index,value in [(0,'https://invalid'),(2,'C2S2;SQL'),(3,{}),(4,'ADMIN'),(5,'BAD'),(6,49),(6,True),(6,float('inf')),(7,202613),(8,-1),(8,True),(8,61)]:
            values=base.copy();values[index]=value
            payload=base64.urlsafe_b64encode(json.dumps(values,separators=(',',':')).encode()).decode().rstrip('=')
            sig=hmac.new(KEY['SICETAC_INSTANT_DECISIONS_BRIDGE_KEY'].encode(),(d.ACTION_PREFIX+payload).encode(),hashlib.sha256).hexdigest()[:24]
            with patch.dict(os.environ,KEY):self.assertIsNone(d.decode_variant_action(d.ACTION_PREFIX+payload+':'+sig))

    def test_expansion_shows_every_variant_once_and_keeps_private_permission_check(self):
        with patch.dict(os.environ,KEY):
            plan=d.decode_variant_action(d.variant_action(QUERY,202610))
            _,query,send,buttons,private=response(plan)
        text=''.join(c.kwargs['body'] for c in send.call_args_list)
        self.assertIn('3217526',text);self.assertIn('4319478',text)
        query.assert_called_once();buttons.assert_not_called()
        self.assertEqual(len(private.call_args.kwargs['result']['variantes']),2)
        self.assertEqual(private.call_args.kwargs['proof'],{'verified-fixture':True})

    def test_missing_or_unknown_primary_is_not_guessed(self):
        for result in [{**RESULT,'seleccion_variante':{}},{**RESULT,'seleccion_variante':{'rutasid_principal':'404'}}]:
            with patch.dict(os.environ,KEY):_,_,send,buttons,_=response(result=result)
            buttons.assert_not_called()
            self.assertIn('4319478',''.join(c.kwargs['body'] for c in send.call_args_list))

    def test_single_variant_does_not_offer_redundant_button(self):
        with patch.dict(os.environ,KEY):_,_,send,buttons,_=response(result={**RESULT,'variantes':[RESULT['variantes'][1]]})
        buttons.assert_not_called();send.assert_called_once()

    def test_each_leg_has_an_independent_button_and_response_does_not_mutate_input(self):
        plan={'status':'draft','queries':[QUERY,{**QUERY,'codigo_dane_origen':'11001000','codigo_dane_destino':'76109000','tipo_contenedor':'VACIO','peso_carga_t':None}]}
        before=copy.deepcopy(plan)
        with patch.dict(os.environ,KEY):
            _,query,_,buttons,_=response(plan)
            decoded=[d.decode_variant_action(c.kwargs['buttons'][0]['id']) for c in buttons.call_args_list]
        self.assertEqual(query.call_count,2);self.assertEqual(buttons.call_count,2);self.assertEqual(plan,before)
        self.assertEqual([p['queries'][0]['tipo_contenedor'] for p in decoded],['CARGADO','VACIO'])

    def test_button_payload_is_supported_and_does_not_truncate_the_action(self):
        with patch.dict(os.environ,KEY):action=d.variant_action(QUERY,202610)
        with patch.object(main,'send_whatsapp_payload') as send:
            main.send_whatsapp_buttons({'user_id':'fixture'},'Resumen',[{'id':action,'title':'Ver otras variantes'}])
        payload=send.call_args.args[1]
        self.assertEqual(payload['interactive']['action']['buttons'][0]['reply']['id'],action)
        self.assertLessEqual(len(payload['interactive']['action']['buttons'][0]['reply']['title']),20)

    def test_webhook_expansion_ignores_previous_route_and_never_uses_classifier_or_commercial_capture(self):
        user='fixture-signed-variants'
        state=main.get_state(user_id=user);state['last_route']={'origen':'UNRELATED','destino':'UNRELATED'}
        state['last_result']={'stale':True};before=copy.deepcopy(state)
        with patch.dict(os.environ,KEY):action=d.variant_action(QUERY,202610)
        message={'id':'fixture-button','from_user_id':user,'type':'interactive',
            'interactive':{'type':'button_reply','button_reply':{'id':action,'title':'Ver otras variantes'}}}
        payload={'entry':[{'changes':[{'value':{'messages':[message],'contacts':[{'user_id':user}]}}]}]}
        with patch.dict(os.environ,KEY),patch.object(main,'merge_lead_data') as merge,patch.object(main,'capture_lead_event') as capture,\
            patch.object(d,'interpret') as classifier,patch.object(main,'consultar_sicetac',return_value=copy.deepcopy(RESULT)) as query,\
            patch.object(main,'send_whatsapp_message') as send,patch.object(main,'send_whatsapp_buttons') as buttons,\
            patch.object(main,'formatear_valor_plaza',return_value=None),patch.object(main,'private_rates_message',return_value=None) as private,\
            patch.object(main,'current_proof',return_value={'current-request-proof':True}),\
            patch.object(main,'formatear_valor_personalizado_por_horas',return_value='fixture amount'):
            result=asyncio.run(main._process_whatsapp_payload(payload))
        self.assertEqual(result['completed_legs'],1);query.assert_called_once();send.assert_called_once()
        self.assertEqual(query.call_args.kwargs['codigo_dane_origen'],'76109000')
        self.assertEqual(private.call_args.kwargs['proof'],{'current-request-proof':True})
        classifier.assert_not_called();merge.assert_not_called();capture.assert_not_called();buttons.assert_not_called()
        self.assertEqual(state,before)

    def test_invalid_action_does_not_call_engine_or_classifier(self):
        message={'id':'fixture-invalid','from_user_id':'fixture-invalid','type':'text','text':{'body':d.ACTION_PREFIX+'junk'}}
        payload={'entry':[{'changes':[{'value':{'messages':[message],'contacts':[{'user_id':'fixture-invalid'}]}}]}]}
        with patch.dict(os.environ,KEY),patch.object(main,'send_whatsapp_message') as send,patch.object(main,'consultar_sicetac') as query,patch.object(d,'interpret') as classifier:
            result=asyncio.run(main._process_whatsapp_payload(payload))
        self.assertEqual(result['status'],'invalid variants action');send.assert_called_once();query.assert_not_called();classifier.assert_not_called()


class CompactPrimaryTests(unittest.TestCase):
    def test_primary_summary_preserves_h6_value_period_id_and_container_state(self):
        data={'origen':'Buenaventura','destino':'Bogota','mes':202610,'ID_SICE':154,'totales':{'H6':3217526}}
        text=main.formatear_principal_sicetac(data,QUERY)
        self.assertIn('*Valor SICETAC: $3.217.526*',text)
        self.assertIn('C2S2 · Contenedor cargado · 6 horas',text)
        self.assertIn('Periodo: 202610 · Ruta principal · ID 154',text)
        self.assertEqual(len(text.splitlines()),4)
        empty={**QUERY,'tipo_contenedor':'VACIO','peso_carga_t':None}
        self.assertIn('Contenedor vacío',main.formatear_principal_sicetac(data,empty))
        self.assertLessEqual(len(text),1024)

    def test_missing_amount_uses_detailed_fallback_instead_of_showing_zero(self):
        self.assertIsNone(main.formatear_principal_sicetac({'totales':{}},QUERY))
