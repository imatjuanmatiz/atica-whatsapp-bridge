import unittest
from unittest.mock import patch, Mock
import main


def model():
    return {"origen":"BOGOTA", "destino":"BOGOTA", "configuracion":"C3S3", "carroceria":"GENERAL", "modo_viaje":"CARGADO", "mes":202609, "total_km":30, "estimado":True,
        "detalle_costos":dict(total_galones=10,combustible=120000,horas_recorrido=1,horas_logisticas=4,horas_totales=5,rotaciones_calculadas=46,costo_fijo=500000,costos_variables=152250,peajes=0,mantenimiento=30000,imprevistos=2250,otros_costos=146641.45,total_viaje=798891.45,mes_costo_fijo=202608,costo_fijo_mensual=23000000),
        "detalle_consumo":{"por_terreno":{"ondulado":{"km":30,"gal":10,"costo_combustible":120000}}}}

class CostDetailFlowTests(unittest.TestCase):
    def test_commands(self):
        for text in ['detalle costos','detalle de costos','DETALLE_COSTOS']:
            self.assertEqual(main.tipo_detalle_modelo(text),'costos')
        self.assertEqual(main.tipo_detalle_modelo('detalle de consumo'),'consumo')
        self.assertIsNone(main.tipo_detalle_modelo('detalle de peajes'))

    def test_context_retained_and_published_result_preserved(self):
        published={'totales':{'H4':999},'mes':202609}
        state={'last_route':{'origen':'BOGOTA','destino':'BOGOTA','vehiculo':'C3S3','carroceria':'GENERAL','modo_viaje':'CARGADO','mes':202609,'horas_logisticas':6,'route_id':'93'},'last_result':published}
        with patch.object(main,'consultar_sicetac',return_value=model()) as api:
            answer=main.responder_detalle_modelo_desde_contexto('detalle de costos',state)
        payload=api.call_args.kwargs
        self.assertEqual(payload['rutasid'],'93')
        self.assertEqual(payload['mes'],202609)
        self.assertEqual(payload['horas_logisticas'],6)
        self.assertFalse(payload['resumen'])
        self.assertTrue(payload['detalle_costos'])
        self.assertIs(state['last_result'],published)
        self.assertIn('VALOR ESTIMADO',answer)
        self.assertIn('30 km',answer)
        self.assertIn('Costos variables:',answer)
        self.assertIn('Rotaciones calculadas',answer)

    def test_fuel_terrain_format(self):
        answer=main.formatear_detalle_modelo(model(),'consumo')
        self.assertIn('Ondulado: 30 km | 10.00 gal',answer)
        self.assertIn('Combustible total:',answer)

    @patch.object(main,'ensure_vehiculos_cache')
    @patch.object(main,'resolver_municipio_cache',side_effect=lambda text: {'nombre_oficial':text,'codigo_dane':'11001000' if text.lower()=='bogota' else '8001000'})
    def test_direct_cost_and_consumption_include_traditional_total(self, municipalities, vehicles):
        for kind in ('costos','consumo'):
            for estimated in (False,True):
                result=model()
                result['sicetac_tradicional']={'total_viaje':7821531,'horas_logisticas':6,'mes':202609,'estimado':estimated}
                with self.subTest(kind=kind,estimated=estimated), patch.object(main,'consultar_sicetac',return_value=result) as api:
                    answer=main.responder_detalle_modelo_desde_contexto(f'detalle de {kind} Bogota a Barranquilla C3S3 6 horas',{})
                self.assertEqual(main.normalizar_texto_libre(api.call_args.kwargs['origen']),'BOGOTA')
                self.assertEqual(main.normalizar_texto_libre(api.call_args.kwargs['destino']),'BARRANQUILLA')
                self.assertEqual(api.call_args.kwargs['horas_logisticas'],6)
                self.assertIn('Total SICETAC estimado:' if estimated else 'Total SICETAC:',answer)
                self.assertIn(main.fmt_cop(7821531),answer)
                self.assertIn('6 horas logisticas',answer)

    def test_no_context_requires_route_without_api_call(self):
        with patch.object(main,'consultar_sicetac') as api:
            answer=main.responder_detalle_modelo_desde_contexto('detalle de costos',{})
        api.assert_not_called()
        self.assertIn('Primero',answer)

    def test_summary_labels_urban_estimate_and_km(self):
        r=model();r['totales']={'H2':1,'H4':2,'H8':3}
        text=main.formatear_respuesta(r,include_closing=False)
        self.assertIn('VALOR ESTIMADO',text)
        self.assertIn('Distancia: 30 km',text)
        self.assertIn('Valores estimados',text)

    @patch('main.requests.post')
    def test_transport_forwards_detail_and_selected_context(self, post):
        post.return_value=Mock(status_code=200)
        post.return_value.json.return_value=model()
        main.consultar_sicetac('BOGOTA','BOGOTA','C3S3',detalle_consumo=True,mes=202609,rutasid='93',horas_logisticas=6)
        payload=post.call_args.kwargs['json']
        self.assertFalse(payload['resumen']);self.assertTrue(payload['detalle_consumo'])
        self.assertEqual(payload['rutasid'],'93');self.assertEqual(payload['horas_logisticas'],6)
