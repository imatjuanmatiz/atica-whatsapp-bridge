import os
import unittest
from unittest.mock import Mock,patch
import enterprise_rates as e

class EnterpriseTests(unittest.TestCase):
    def query(self,proof={"webhook":"fixture","signature":"fixture"}):
        return e.private_rates_message(proof=proof,result={"detalle_lookup":{"rutasid":"243"}},route={"origen":"Cartagena","destino":"Barranquilla"},vehicle="C3S3",body="General - Estacas",travel_mode="CARGADO")

    def test_no_unsigned_proof_no_private_request(self):
        with patch.dict(os.environ,{"SICETAC_ENTERPRISE_URL":"https://fixture.invalid","CAPTURE_WEBHOOK_SECRET":"fixture-secret"}),patch.object(e.requests,"post") as post:
            self.assertIsNone(self.query(None));post.assert_not_called()

    def test_denied_identity_keeps_public_flow_without_financial_output(self):
        response=Mock(status_code=200);response.json.return_value={"authorized":False}
        with patch.dict(os.environ,{"SICETAC_ENTERPRISE_URL":"https://fixture.invalid","CAPTURE_WEBHOOK_SECRET":"fixture-secret"}),patch.object(e.requests,"post",return_value=response):
            self.assertIsNone(self.query())

    def test_authorized_tariff_is_labelled_and_not_converted_to_tonnage(self):
        rate={"amount":1608025,"unit":"COP/viaje","effective_from":"2026-10-01","effective_to":"2026-10-31","is_simulated":True}
        response=Mock(status_code=200);response.json.return_value={"authorized":True,"company":{"name":"ATIEMPPO"},"results":[{"route_id":"243","vehicle":"C3S3","private":{"carrier":rate,"customer":{**rate,"amount":2172584.70}}}]}
        with patch.dict(os.environ,{"SICETAC_ENTERPRISE_URL":"https://fixture.invalid","CAPTURE_WEBHOOK_SECRET":"fixture-secret"}),patch.object(e.requests,"post",return_value=response) as post:
            text=self.query();self.assertIn("SIMULACIÓN",text);self.assertIn("2.172.584,70 COP/viaje",text)
            payload=post.call_args.kwargs['json'];self.assertNotIn("company",payload);self.assertEqual(payload['queries'][0]['route_id'],'243')

    def test_conversation_proof_does_not_survive_its_request(self):
        token=e.set_proof(b"fixture","signature");self.assertIsNotNone(e.current_proof());e.reset_proof(token);self.assertIsNone(e.current_proof())

    def test_shipper_only_receives_negotiated_freight_even_if_legacy_payload_contains_carrier(self):
        rate={"amount":1234567,"unit":"COP/viaje","effective_from":"2026-10-01"}
        response=Mock(status_code=200);response.json.return_value={"authorized":True,"business_profile":"generadora","company":{"name":"QA"},"results":[{"route_id":"243","vehicle":"C3S3","private":{"carrier":rate,"customer":{**rate,"amount":2000000}},"indicative_difference":{"amount":765433,"unit":"COP/viaje"}}]}
        with patch.dict(os.environ,{"SICETAC_ENTERPRISE_URL":"https://fixture.invalid","CAPTURE_WEBHOOK_SECRET":"fixture-secret"}),patch.object(e.requests,"post",return_value=response):
            text=self.query();self.assertIn("Flete negociado",text);self.assertNotIn("1.234.567",text);self.assertNotIn("Diferencia de tarifas",text);self.assertNotIn("transportador",text)

    def test_transport_profile_keeps_carrier_and_customer_labels(self):
        rate={"amount":1234567,"unit":"COP/viaje","effective_from":"2026-10-01"}
        response=Mock(status_code=200);response.json.return_value={"authorized":True,"company":{"name":"QA","business_profile":"transporte"},"results":[{"route_id":"243","vehicle":"C3S3","private":{"carrier":rate,"customer":rate}}]}
        with patch.dict(os.environ,{"SICETAC_ENTERPRISE_URL":"https://fixture.invalid","CAPTURE_WEBHOOK_SECRET":"fixture-secret"}),patch.object(e.requests,"post",return_value=response):
            text=self.query();self.assertIn("Valor a pagar al transportador",text);self.assertIn("Flete al cliente",text)
