import asyncio
import copy
import os
import unittest
from unittest.mock import Mock,patch

import decisions_pilot as d
import main

KEY={'SICETAC_INSTANT_DECISIONS_BRIDGE_KEY':'fixture-key'}
RAW=dict(origen='11001000',destino='08001000',configuracion='C3S3',carroceria='GENERAL_ESTACAS_CARGADO',
    horas_logisticas=4,modo_viaje='CARGADO',modo_ruta='CATALOGO')
NAMES={'11001000':'BOGOTÁ','08001000':'BARRANQUILLA'}
TEXT='Estoy preparando un despacho de mercancía que ya está empacada, desde Bogotá hasta Barranquilla con C3S3, estacas y 4 horas logísticas.'


def remote(**changes):
    return dict(status='draft',operation='reference',context_stored=False,queries=[RAW],municipality_names=NAMES,**changes)


class BridgeDecisionsTests(unittest.TestCase):
    def test_unconfigured_bridge_never_calls_the_classifier(self):
        post=Mock()
        with patch.dict(os.environ,{},clear=True):self.assertIsNone(d.interpret(TEXT,legacy_route_found=True,post=post))
        post.assert_not_called()
    def test_simple_queries_stay_in_existing_parser(self):
        post=Mock()
        with patch.dict(os.environ,KEY):self.assertIsNone(d.interpret('Bogotá a Cali',legacy_route_found=True,post=post))
        post.assert_not_called()
    def test_switch_budget_timeout_redirect_and_errors_fall_back_without_retry(self):
        for reply in [dict(status='fallback',context_stored=False,reason='disabled'),dict(status='fallback',context_stored=False,reason='budget_denied')]:
            post=Mock(return_value=Mock(status_code=200,json=Mock(return_value=reply)))
            with patch.dict(os.environ,KEY):self.assertIsNone(d.interpret(TEXT,legacy_route_found=False,post=post))
            self.assertEqual(post.call_count,1)
        for status in [301,401,500]:
            post=Mock(return_value=Mock(status_code=status))
            with patch.dict(os.environ,KEY):self.assertIsNone(d.interpret(TEXT,legacy_route_found=False,post=post))
            self.assertEqual(post.call_count,1)
        post=Mock(side_effect=d.requests.Timeout('fixture'))
        with patch.dict(os.environ,KEY):self.assertIsNone(d.interpret(TEXT,legacy_route_found=False,post=post))
        self.assertEqual(post.call_count,1)
    def test_provider_gets_only_current_text_no_phone_lead_or_context(self):
        post=Mock(return_value=Mock(status_code=200,json=Mock(return_value=remote())))
        with patch.dict(os.environ,KEY):r=d.interpret(TEXT,legacy_route_found=False,post=post)
        self.assertEqual(r['status'],'draft')
        self.assertEqual(post.call_args.kwargs['json'],{'texto':TEXT})
        self.assertFalse(post.call_args.kwargs['allow_redirects'])
    def test_entire_plan_is_validated_before_costs(self):
        for change in [dict(origen='bad'),dict(configuracion='SQL'),dict(modo_ruta='MANUAL'),dict(horas_logisticas=99),dict(tipo_contenedor='VACIO')]:
            data=remote();data['queries']=[RAW,{**RAW,**change}]
            if change==dict(configuracion='SQL'):change={'configuracion':'bad value'};data['queries'][1].update(change)
            post=Mock(return_value=Mock(status_code=200,json=Mock(return_value=data)))
            with patch.dict(os.environ,KEY):self.assertIsNone(d.interpret(TEXT,legacy_route_found=False,post=post))
    def test_success_keeps_h4_and_private_access_check_and_does_not_mutate_plan(self):
        post=Mock(return_value=Mock(status_code=200,json=Mock(return_value=remote())))
        with patch.dict(os.environ,KEY):plan=d.interpret(TEXT,legacy_route_found=False,post=post)
        before=copy.deepcopy(plan);query=Mock(return_value={'totales':{'H4':1400}});send=Mock();private=Mock(return_value=None)
        result=d.respond(plan,query=query,format_result=Mock(),format_hours=Mock(return_value='H4 $1.400'),
            format_market=Mock(return_value=''),private_message=private,proof={'fixture':True},send=send,recipient={'user_id':'qa'})
        self.assertEqual(result['mode'],'decisions-stateless');self.assertEqual(query.call_count,1)
        self.assertEqual(query.call_args.kwargs['horas_logisticas'],4)
        self.assertEqual(private.call_args.kwargs['proof'],{'fixture':True})
        self.assertEqual(plan,before)
    def test_webhook_handles_long_query_without_storing_last_route(self):
        value={'messages':[{'id':'qa-decision-msg','from_user_id':'qa-decision-user','type':'text','text':{'body':TEXT}}],
            'contacts':[{'user_id':'qa-decision-user','profile':{'name':'QA'}}]}
        state=main.get_state(user_id='qa-decision-user');before=copy.deepcopy(state)
        result_query={'origen':'BOGOTÁ','destino':'BARRANQUILLA','configuracion':'C3S3','totales':{'H4':1400}}
        plan={'status':'draft','queries':[dict(origen='BOGOTÁ',destino='BARRANQUILLA',codigo_dane_origen='11001000',codigo_dane_destino='08001000',
            vehiculo='C3S3',carroceria='General - Estacas',modo_viaje='CARGADO',horas_logisticas=4)]}
        with patch.dict(os.environ,KEY),patch.object(main,'merge_lead_data'),patch.object(main,'capture_lead_event'),patch.object(main,'parsear_ruta',return_value={'origen':'BOGOTÁ','destino':'BARRANQUILLA'}),\
             patch.object(main.decisions_pilot,'interpret',return_value=plan),patch.object(main,'consultar_sicetac',return_value=result_query) as query,\
             patch.object(main,'send_whatsapp_message') as send,patch.object(main,'formatear_valor_plaza',return_value=''),\
             patch.object(main,'formatear_valor_personalizado_por_horas',return_value='H4 $1.400'),patch.object(main,'private_rates_message',return_value=None):
            result=asyncio.run(main._process_whatsapp_payload({'entry':[{'changes':[{'value':value}]}]}))
        self.assertEqual(result['mode'],'decisions-stateless');self.assertEqual(query.call_count,1);send.assert_called_once()
        self.assertIsNone(state.get('last_route'));self.assertIsNone(state.get('last_result'))
        # Only existing inbound lead bookkeeping may change before the pilot hook.
        self.assertEqual(state.get('preferred_vehicle'),before.get('preferred_vehicle'))

    def test_no_unbudgeted_model_fallback_after_decisions_is_disabled(self):
        with patch.dict(os.environ,KEY),patch.object(main,'OPENAI_API_KEY','fixture'),patch.object(main,'OPENAI_FALLBACK_ENABLED',True),patch.object(main,'GEMINI_API_KEY','fixture'),patch.object(main,'GEMINI_ENABLED',True):
            self.assertFalse(main.should_try_openai_route_fallback(TEXT,{}))
            self.assertFalse(main.should_try_gemini_route_fallback(TEXT,{}))
            self.assertIsNone(main.generar_respuesta_ia(phone='qa',profile_name=None,user_text=TEXT,state={},ruta=None,resultado_sicetac=None,deterministic_reply='fixture'))
