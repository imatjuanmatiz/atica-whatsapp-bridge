from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

import main


class VacioFlowTests(unittest.TestCase):
    def test_parses_vehicle_empty_trip_without_losing_body_type(self) -> None:
        text = "Bogotá a Buenaventura C2S2 vacío portacontenedores"
        self.assertEqual(main.parsear_modo_viaje(text), "VACIO")
        self.assertEqual(main.parsear_carroceria(text), "Portacontenedores")
        self.assertFalse(main.usuario_pide_contenedor_vacio(text))

    def test_distinguishes_transported_empty_container(self) -> None:
        self.assertTrue(
            main.usuario_pide_contenedor_vacio(
                "Buenaventura a Bogotá C2S2 con un contenedor vacío"
            )
        )

    def test_formats_vehicle_empty_trip_explicitly(self) -> None:
        message = main.formatear_respuesta(
            {
                "origen": "Bogotá",
                "destino": "Buenaventura",
                "configuracion": "C2S2",
                "carroceria": "Portacontenedores",
                "modo_viaje": "VACIO",
                "mes": 202607,
                "totales": {"H2": 2_867_384, "H4": 2_867_384, "H8": 2_867_384},
                "metodo": "lookup_vacio_oficial",
            },
            include_closing=False,
        )
        self.assertIn("Modo: Vacio (sin mercancia ni contenedor)", message)
        self.assertIn("Carroceria: Portacontenedores", message)

    def test_formats_transported_empty_container_as_loaded(self) -> None:
        message = main.formatear_respuesta(
            {
                "origen": "Bogotá",
                "destino": "Buenaventura",
                "configuracion": "C2S2",
                "carroceria": "Portacontenedores",
                "modo_viaje": "CARGADO",
                "tipo_contenedor": "VACIO",
                "valor_plaza_no_aplica": "CONTENEDOR_VACIO",
                "mes": 202607,
                "totales": {"H2": 100, "H4": 120, "H8": 160},
                "metodo": "lookup_contenedor_vacio_oficial",
            },
            include_closing=False,
        )
        self.assertIn("Modo: Cargado | Carga: Contenedor vacio", message)
        self.assertIn("Valor en plaza: no aplica para contenedor vacio.", message)
        self.assertNotIn("sin mercancia ni contenedor", message)

    def test_round_trip_trigger_requires_full_phrase(self) -> None:
        self.assertTrue(main.usuario_pide_viaje_redondo("viaje redondo"))
        self.assertFalse(main.usuario_pide_viaje_redondo_con_vacio("viaje redondo"))
        self.assertTrue(
            main.usuario_pide_viaje_redondo_con_vacio(
                "Buenaventura a Bogotá viaje redondo con vacío"
            )
        )
        self.assertEqual(main.recortar_destino("Bogotá viaje redondo con vacío"), "Bogotá")

    def test_parses_selected_route_ids_for_round_trip(self) -> None:
        self.assertEqual(
            main.parsear_ids_viaje_redondo("ida 106 regreso 11367"),
            ("106", "11367"),
        )
        self.assertEqual(
            main.parsear_ids_viaje_redondo("salida id 106 vuelta id 11367"),
            ("106", "11367"),
        )

    @patch("main.requests.post")
    def test_sends_empty_container_as_loaded_to_api(self, post) -> None:
        response = Mock(status_code=200)
        response.json.return_value = {
            "origen": "Buenaventura",
            "destino": "Bogotá",
            "metodo": "lookup_contenedor_vacio_oficial",
            "totales": {"H2": 100, "H4": 120, "H8": 160},
        }
        post.return_value = response

        main.consultar_sicetac(
            origen="Buenaventura",
            destino="Bogotá",
            vehiculo="C2S2",
            carroceria="Portacontenedores",
            modo_viaje="CARGADO",
            tipo_contenedor="VACIO",
        )

        payload = post.call_args.kwargs["json"]
        self.assertEqual(payload["modo_viaje"], "CARGADO")
        self.assertEqual(payload["carroceria"], "Portacontenedores")
        self.assertEqual(payload["tipo_contenedor"], "VACIO")

    @patch("main.consultar_sicetac")
    def test_round_trip_queries_loaded_outbound_and_empty_container_return(self, consultar) -> None:
        resultado_api = {
            "tipo_consulta": "VIAJE_REDONDO",
            "configuracion": "C2S2",
            "carroceria": "Portacontenedores",
            "ida": {
                "origen": "Buenaventura",
                "destino": "Bogotá",
                "totales": {"H2": 100, "H4": 120, "H8": 160},
            },
            "regreso": {
                "origen": "Bogotá",
                "destino": "Buenaventura",
                "tipo_contenedor": "VACIO",
                "totales": {"H2": 70, "H4": 90, "H8": 130},
                "valor_plaza_no_aplica": "CONTENEDOR_VACIO",
            },
            "totales": {"H2": 170, "H4": 210, "H8": 290},
        }
        consultar.return_value = resultado_api

        resultado = main.consultar_viaje_redondo_con_vacio(
            ruta={
                "origen": "Buenaventura",
                "destino": "Bogotá",
                "codigo_dane_origen": "76109000",
                "codigo_dane_destino": "11001000",
            },
            vehiculo="C2S2",
            carroceria="Portacontenedores",
            rutasid_ida="154",
            rutasid_regreso="95",
        )

        self.assertEqual(resultado, resultado_api)
        kwargs = consultar.call_args.kwargs
        self.assertEqual(kwargs["modo_viaje"], "CARGADO")
        self.assertEqual(kwargs["tipo_contenedor"], "CARGADO")
        self.assertTrue(kwargs["viaje_redondo"])
        self.assertEqual(kwargs["tipo_contenedor_regreso"], "VACIO")
        self.assertEqual(kwargs["rutasid_ida"], "154")
        self.assertEqual(kwargs["rutasid_regreso"], "95")
        self.assertNotIn("peajes", kwargs)

        message = main.formatear_viaje_redondo_con_vacio(resultado)
        self.assertIn("VIAJE REDONDO CON CONTENEDOR VACÍO", message)
        self.assertIn("H2 total: $170", message)
        self.assertIn("H4 total: $210", message)
        self.assertIn("H8 total: $290", message)
        self.assertIn("plaza no aplica", message)


if __name__ == "__main__":
    unittest.main()
