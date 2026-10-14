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
            text=self.query();self.assertIn("Valor Flete (VF)",text);self.assertNotIn("1.234.567",text);self.assertNotIn("Diferencia de tarifas",text);self.assertNotIn("transportador",text)

    def test_transport_profile_keeps_carrier_and_customer_labels(self):
        rate={"amount":1234567,"unit":"COP/viaje","effective_from":"2026-10-01"}
        response=Mock(status_code=200);response.json.return_value={"authorized":True,"company":{"name":"QA","business_profile":"transporte"},"results":[{"route_id":"243","vehicle":"C3S3","private":{"carrier":rate,"customer":rate}}]}
        with patch.dict(os.environ,{"SICETAC_ENTERPRISE_URL":"https://fixture.invalid","CAPTURE_WEBHOOK_SECRET":"fixture-secret"}),patch.object(e.requests,"post",return_value=response):
            text=self.query();self.assertIn("Valor pagado a transportador (VPT)",text);self.assertIn("Valor Flete (VF)",text)

    def card(self, unit="COP/viaje", profile="transporte"):
        rate={"amount":1700000,"unit":unit,"effective_from":"2026-10-01","effective_to":"2026-10-31"}
        return {"company":{"name":"DEMO","business_profile":profile},"results":[{"route_id":"243","vehicle":"C3S3","private":{"carrier":rate,"customer":{**rate,"amount":3400000}},"indicative_difference":{"amount":1700000,"unit":unit}}]}

    def test_reference_tons_and_compact_shared_validity(self):
        text=e.format_company_values(self.card(),{"toneladas_configuracion":34})
        self.assertIn("*VPT:* $ 1.700.000,00 COP/viaje · $ 50.000,00/tn",text)
        self.assertIn("*VF:* $ 3.400.000,00 COP/viaje · $ 100.000,00/tn",text)
        self.assertIn("34 t de referencia",text)
        self.assertEqual(text.count("Vigencia:"),1)
        self.assertNotIn("Diferencia",text)
        self.assertNotIn("Ruta ID",text)

    def test_explicit_tons_override_nominal(self):
        text=e.format_company_values(self.card(),{"toneladas_configuracion":34},20)
        self.assertIn("$ 85.000,00/tn",text)
        self.assertIn("$ 170.000,00/tn",text)
        self.assertIn("20 t indicadas",text)
        self.assertNotIn("referencia",text)

    def test_invalid_or_missing_tons_never_invent_a_conversion(self):
        for weight in [None,0,-2,"NaN","Infinity",True,"unknown"]:
            text=e.format_company_values(self.card(),{"toneladas_configuracion":weight})
            self.assertNotIn("/tn",text)
            self.assertIn("sin base de peso",text)
        text=e.format_company_values(self.card(),{"toneladas_configuracion":34},0)
        self.assertNotIn("/tn",text)

    def test_already_per_ton_is_not_divided_again(self):
        text=e.format_company_values(self.card(unit="COP/tn"),{"toneladas_configuracion":34})
        self.assertIn("$ 1.700.000,00 COP/tn",text)
        self.assertNotIn("$ 50.000,00",text)

    def test_shipper_has_no_carrier_label_or_simulation_from_hidden_carrier(self):
        data=self.card(profile="generadora")
        data["results"][0]["private"]["carrier"]["is_simulated"]=True
        text=e.format_company_values(data,{"toneladas_configuracion":34})
        self.assertNotIn("VPT",text)
        self.assertNotIn("SIMULACIÓN",text)
        self.assertIn("*VF:*",text)

    def test_variants_keep_identity_and_their_own_weight(self):
        data=self.card()
        data["results"].append({**data["results"][0],"route_id":"244"})
        text=e.format_company_values(data,{"toneladas_configuracion":34,"variantes":[{"ID_SICE":"244","toneladas_configuracion":20}]})
        self.assertIn("Ruta ID 243",text)
        self.assertIn("Ruta ID 244",text)
        self.assertIn("$ 50.000,00/tn",text)
        self.assertIn("$ 85.000,00/tn",text)

    def test_different_validities_are_not_collapsed(self):
        data=self.card()
        data["results"][0]["private"]["customer"]["effective_from"]="2026-10-10"
        text=e.format_company_values(data,{"toneladas_configuracion":34})
        self.assertIn("VPT 2026-10-01 a 2026-10-31",text)
        self.assertIn("VF 2026-10-10 a 2026-10-31",text)
