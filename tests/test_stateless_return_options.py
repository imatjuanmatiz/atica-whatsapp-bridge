"""Same-profile disclosure and optional returns, entirely synthetic/offline."""
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
from test_stateless_variants import KEY,QUERY,RESULT,response


def journey(container=True):
    first={**QUERY}
    if not container:first.update(carroceria='General - Estacas',tipo_contenedor=None,vehiculo='C3S3')
    last={**first,'origen':first['destino'],'destino':first['origen'],
        'codigo_dane_origen':first['codigo_dane_destino'],'codigo_dane_destino':first['codigo_dane_origen']}
    return dict(status='draft',queries=[first,last],same_as_first=True)


class SameProfileReturnTests(unittest.TestCase):
    def test_loaded_return_is_calculated_once_and_offers_both_options_without_more_calls(self):
        plan=journey();before=copy.deepcopy(plan)
        with patch.dict(os.environ,KEY):
            result,query,send,buttons,private=response(plan)
            last=buttons.call_args.kwargs
            variant,alternate=last['buttons']
            decoded=d.decode_return_action(alternate['id'])
        self.assertEqual(query.call_count,2)
        self.assertEqual([c.kwargs['modo_viaje'] for c in query.call_args_list],['CARGADO','CARGADO'])
        self.assertTrue(all(c.kwargs['horas_logisticas']==6 for c in query.call_args_list))
        self.assertIn('6 horas por tramo',buttons.call_args_list[0].kwargs['body'])
        self.assertEqual([variant['title'],alternate['title']],['Ver otras variantes','Contenedor vacío'])
        self.assertEqual(decoded['queries'][0]['tipo_contenedor'],'VACIO')
        self.assertEqual(decoded['queries'][0]['horas_logisticas'],6)
        self.assertIsNone(decoded['queries'][0]['peso_carga_t'])
        self.assertEqual(plan,before)

    def test_general_alternative_is_vehicle_empty_h0_with_no_cargo_weight(self):
        with patch.dict(os.environ,KEY):
            _,query,_,buttons,_=response(journey(False))
            button=buttons.call_args.kwargs['buttons'][-1]
            p=d.decode_return_action(button['id'])
        self.assertEqual(button['title'],'Regreso sin carga')
        q=p['queries'][0]
        self.assertEqual((q['modo_viaje'],q['tipo_contenedor'],q['horas_logisticas']),('VACIO',None,0))
        self.assertIsNone(q['peso_carga_t'])
        self.assertEqual(q['vehiculo'],'C3S3')
        self.assertEqual(q['codigo_dane_origen'],QUERY['codigo_dane_destino'])
        self.assertEqual(query.call_count,2)

    def test_options_are_not_reoffered_for_explicit_empty_return_or_one_way(self):
        for plan in [dict(status='draft',queries=[QUERY]),journey()]:
            plan.pop('same_as_first',None)
            if len(plan['queries'])==2:plan['queries'][1]['tipo_contenedor']='VACIO'
            with patch.dict(os.environ,KEY):_,_,_,buttons,_=response(plan)
            self.assertTrue(all(all(b['title']=='Ver otras variantes' for b in c.kwargs['buttons']) for c in buttons.call_args_list))

    def test_no_empty_return_option_for_open_routes_or_circuits_or_empty_first(self):
        plans=[]
        p=journey();p['queries'][1]['codigo_dane_destino']='76001000';plans.append(p)
        p=journey();p['queries'].append(copy.deepcopy(QUERY));plans.append(p)
        p=journey();p['queries'][1]['tipo_contenedor']='VACIO';plans.append(p)
        for p in plans:
            with patch.dict(os.environ,KEY):_,_,_,buttons,_=response(p)
            self.assertTrue(all(all(b['title']=='Ver otras variantes' for b in c.kwargs['buttons']) for c in buttons.call_args_list))

    def test_single_geographic_variant_can_still_offer_optional_return(self):
        with patch.dict(os.environ,KEY):
            _,query,send,buttons,_=response(journey(),result={**RESULT,'variantes':[RESULT['variantes'][1]]})
        self.assertEqual(query.call_count,2)
        self.assertEqual(buttons.call_count,1)
        self.assertEqual(buttons.call_args.kwargs['buttons'][0]['title'],'Contenedor vacío')

    def test_signed_return_is_domain_separated_tamper_protected_and_has_no_authority(self):
        with patch.dict(os.environ,KEY):
            action=d.return_action(journey()['queries'][1],202610)
            p=d.decode_return_action(action)
            self.assertIsNone(d.decode_return_action(action[:-1]+'x'))
            self.assertIsNone(d.decode_variant_action(action))
            self.assertIsNone(d.decode_return_action(action.replace(d.RETURN_ACTION_PREFIX,d.ACTION_PREFIX)))
            self.assertIsNone(d.decode_return_action(d.variant_action(QUERY,202610)))
            self.assertIsNone(d.decode_return_action(d.signed_query_action(QUERY,202610,d.RETURN_ACTION_PREFIX)))
        self.assertLessEqual(len(action),256)
        self.assertFalse({'proof','user_id','phone','company','token'} & p['queries'][0].keys())
        for key in ('','rotated-fixture'):
            with patch.dict(os.environ,{'SICETAC_INSTANT_DECISIONS_BRIDGE_KEY':key}):self.assertIsNone(d.decode_return_action(action))

    def test_decode_rejects_signed_invalid_return_parameters(self):
        base=['11001000','76109000','C3S3','E','VACIO',None,0,202610,None]
        for index,value in [(6,6),(8,5),(0,True),(6,True),(7,202613),(3,'X')]:
            raw=base.copy();raw[index]=value
            payload=base64.urlsafe_b64encode(json.dumps(raw,separators=(',',':')).encode()).decode().rstrip('=')
            sig=hmac.new(KEY['SICETAC_INSTANT_DECISIONS_BRIDGE_KEY'].encode(),(d.RETURN_ACTION_PREFIX+payload).encode(),hashlib.sha256).hexdigest()[:24]
            with patch.dict(os.environ,KEY):self.assertIsNone(d.decode_return_action(d.RETURN_ACTION_PREFIX+payload+':'+sig))

    def test_webhook_click_with_empty_history_requeries_only_return_and_rechecks_private_proof(self):
        user='fixture-stateless-return';main.CONVERSATION_STATE.clear()
        with patch.dict(os.environ,KEY):action=d.return_action(journey()['queries'][1],202610)
        value={'messages':[{'id':'fixture-return-button','from_user_id':user,'type':'interactive',
            'interactive':{'type':'button_reply','button_reply':{'id':action,'title':'Contenedor vacío'}}}],
            'contacts':[{'user_id':user}]}
        with patch.dict(os.environ,KEY),patch.object(d,'interpret') as classifier,            patch.object(main,'merge_lead_data') as merge,patch.object(main,'capture_lead_event') as capture,            patch.object(main,'consultar_sicetac',return_value=copy.deepcopy(RESULT)) as query,            patch.object(main,'send_whatsapp_message') as send,patch.object(main,'send_whatsapp_buttons') as buttons,            patch.object(main,'formatear_valor_plaza',return_value=None),            patch.object(main,'formatear_valor_personalizado_por_horas',return_value='fixture amount'),            patch.object(main,'current_proof',return_value={'fresh-request':True}),            patch.object(main,'private_rates_message',return_value=None) as private:
            result=asyncio.run(main._process_whatsapp_payload({'entry':[{'changes':[{'value':value}]}]}))
        self.assertEqual(result['completed_legs'],1)
        query.assert_called_once();classifier.assert_not_called();merge.assert_not_called();capture.assert_not_called()
        self.assertEqual(query.call_args.kwargs['codigo_dane_origen'],'11001000')
        self.assertEqual(query.call_args.kwargs['tipo_contenedor'],'VACIO')
        self.assertEqual(private.call_args.kwargs['proof'],{'fresh-request':True})
        state=main.get_state(user_id=user)
        self.assertIsNone(state.get('last_route'));self.assertIsNone(state.get('last_result'))
        self.assertIn('Alternativa de regreso',buttons.call_args.kwargs['body'])
        self.assertTrue(all(b['title']=='Ver otras variantes' for b in buttons.call_args.kwargs['buttons']))

    def test_return_button_stays_within_whatsapp_limits(self):
        with patch.dict(os.environ,KEY):_,_,_,buttons,_=response(journey(False))
        for call in buttons.call_args_list:
            self.assertLessEqual(len(call.kwargs['body']),1024)
            self.assertLessEqual(len(call.kwargs['buttons']),3)
            for b in call.kwargs['buttons']:
                self.assertLessEqual(len(b['title']),20)
                self.assertLessEqual(len(b['id']),256)

    def test_interpreter_propagates_inheritance_only_for_consistent_validated_parameters(self):
        p=journey()
        names={q['codigo_dane_origen']:q['origen'] for q in p['queries']}
        queries=[dict(origen=q['codigo_dane_origen'],destino=q['codigo_dane_destino'],
            configuracion=q['vehiculo'],carroceria='CONTENEDOR_PORTACONTENEDORES_CARGADO',
            modo_viaje=q['modo_viaje'],tipo_contenedor=q['tipo_contenedor'],
            horas_logisticas=q['horas_logisticas'],peso_carga_t=q['peso_carga_t']) for q in p['queries']]
        data=dict(status='draft',operation='reference',context_stored=False,queries=queries,
            municipality_names=names,grounding={'return_inherited':True})
        post=Mock(return_value=Mock(status_code=200,json=lambda:data))
        with patch.dict(os.environ,KEY):r=d.interpret('Bogotá Cali ida y vuelta contenedor 20 pies',legacy_route_found=False,post=post)
        self.assertTrue(r['same_as_first'])
        data['queries'][1]['horas_logisticas']=4
        with patch.dict(os.environ,KEY):r=d.interpret('Bogotá Cali ida y vuelta contenedor 20 pies',legacy_route_found=False,post=post)
        self.assertNotIn('same_as_first',r)
        self.assertEqual(post.call_count,2)
