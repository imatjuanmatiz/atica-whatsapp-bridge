import unittest
from unittest.mock import Mock,patch
import enterprise_rates as e

CODE='A'*32
ENV={'SICETAC_ENTERPRISE_URL':'https://fixture.invalid/api/empresas/whatsapp','CAPTURE_WEBHOOK_SECRET':'fixture-secret'}
class LinkingTests(unittest.TestCase):
 def test_routes_never_trigger_linking(self):
  with patch.object(e.requests,'post') as post:
   self.assertIsNone(e.link_command_message(text='Bogotá a Cali',proof=None));post.assert_not_called()
 def test_malformed_command_does_not_send_a_code(self):
  with patch.object(e.requests,'post') as post:
   self.assertIn('código',e.link_command_message(text='VINCULAR 1234',proof=None));post.assert_not_called()
 def test_missing_proof_does_not_grant_access(self):
  with patch.dict(e.os.environ,ENV),patch.object(e.requests,'post') as post:
   self.assertIn('verificar',e.link_command_message(text='VINCULAR '+CODE,proof=None));post.assert_not_called()
 def test_signed_proof_sent_unchanged_and_code_not_repeated_in_response(self):
  proof={'webhook':'fixture-original-bytes','signature':'fixture-signature'};response=Mock(status_code=200);response.json.return_value={'linked':True,'company':'QA'}
  with patch.dict(e.os.environ,ENV),patch.object(e.requests,'post',return_value=response) as post:
   result=e.link_command_message(text='VINCULAR '+CODE,proof=proof);self.assertIn('vinculado a QA',result);self.assertNotIn(CODE,result);self.assertEqual(post.call_args.args[0],ENV['SICETAC_ENTERPRISE_URL']+'/vincular');self.assertEqual(post.call_args.kwargs['json'],proof)
 def test_expired_and_conflicting_code_does_not_claim_success(self):
  for status in (403,409,410):
   response=Mock(status_code=status);response.json.return_value={'error':'fixture denied'}
   with patch.dict(e.os.environ,ENV),patch.object(e.requests,'post',return_value=response):
    result=e.link_command_message(text='VINCULAR '+CODE,proof={'webhook':'fixture','signature':'fixture'});self.assertEqual(result,'fixture denied')
 def test_command_is_intercepted_before_logging_capture_or_route_model(self):
  import asyncio
  import main
  value={'metadata':{'phone_number_id':'qa-line'},'messages':[{'id':'qa-msg','from_user_id':'qa-link-only','timestamp':'1','type':'text','text':{'body':'VINCULAR '+CODE}}],'contacts':[{'user_id':'qa-link-only','profile':{'name':'QA'}}]}
  response=Mock(status_code=200);response.json.return_value={'linked':True,'company':'QA'}
  token=e.set_proof(b'fixture','fixture-signature')
  try:
   with patch.dict(e.os.environ,ENV),patch.object(e.requests,'post',return_value=response),patch.object(main,'send_whatsapp_message') as send,patch.object(main,'merge_lead_data') as merge,patch.object(main,'capture_lead_event') as capture,patch.object(main.logger,'info') as log,patch.object(main,'extraer_json_ruta_openai') as llm:
    result=asyncio.run(main._process_whatsapp_payload({'entry':[{'changes':[{'value':value}]}]}));self.assertEqual(result['status'],'WhatsApp linking command handled');send.assert_called_once();merge.assert_not_called();capture.assert_not_called();log.assert_not_called();llm.assert_not_called()
  finally:e.reset_proof(token)
