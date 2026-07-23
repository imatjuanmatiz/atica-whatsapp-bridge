from __future__ import annotations

import unittest
from unittest.mock import patch

import main


class VacioFlowTests(unittest.TestCase):
    def test_parses_empty_trip_without_losing_body_type(self) -> None:
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

    def test_formats_empty_trip_explicitly(self) -> None:
        message = main.formatear_respuesta(
            {
                "origen": "Bogotá",
                "destino": "Buenaventura",
                "configuracion": "C2S2",
                "carroceria": "Portacontenedores",
                "modo_viaje": "VACIO",
                "mes": 202607,
                "totales": {
                    "H2": 2_867_384,
                    "H4": 2_867_384,
                    "H8": 2_867_384,
                },
                "metodo": "lookup_vacio_oficial",
            },
            include_closing=False,
        )
        self.assertIn("Modo: Vacio (sin mercancia ni contenedor)", message)
        self.assertIn("Carroceria: Portacontenedores", message)

    def test_round_trip_trigger_requires_full_phrase(self) -> None:
        self.assertTrue(main.usuario_pide_viaje_redondo("viaje redondo"))
        self.assertFalse(
            main.usuario_pide_viaje_redondo_con_vacio("viaje redondo")
        )
        self.assertTrue(
            main.usuario_pide_viaje_redondo_con_vacio(
                "Buenaventura a Bogotá viaje redondo con vacío"
            )
        )
        self.assertEqual(
            main.recortar_destino("Bogotá viaje redondo con vacío"),
            "Bogotá",
        )

    def test_pairs_inverse_corridors_with_loaded_h8_and_empty_logistics(self) -> None:
        ida = {
            "origen": "Buenaventura",
            "destino": "Bogotá",
            "variantes": [
                {
                    "ID_SICE": "11533",
                    "NOMBRE_SICE": "BUENAVENTURA - BOGOTA VIA LA PAILA CARTAGO PEREIRA",
                    "totales": {"H2": 100, "H4": 120, "H8": 160},
                }
            ],
        }
        regreso = {
            "origen": "Bogotá",
            "destino": "Buenaventura",
            "variantes": [
                {
                    "ID_SICE": "11456",
                    "NOMBRE_SICE": "BOGOTA - BUENAVENTURA VIA PEREIRA CARTAGO LA PAILA",
                    "totales": {"H2": 70, "H4": 70, "H8": 70},
                    "detalle_lookup": {"movilizacion": 50, "valor_hora": 10},
                }
            ],
        }

        pares, ida_sin_pareja, regreso_sin_pareja = (
            main.emparejar_variantes_viaje_redondo(ida, regreso)
        )

        self.assertEqual(len(pares), 1)
        self.assertFalse(ida_sin_pareja)
        self.assertFalse(regreso_sin_pareja)
        self.assertEqual(
            pares[0]["total_viaje"],
            {
                "cargado_h8": 160.0,
                "vacio_logistica": 50.0,
                "total": 210.0,
            },
        )

    @patch("main.consultar_sicetac")
    def test_round_trip_queries_loaded_outbound_and_empty_return(self, consultar) -> None:
        ida = {
            "origen": "Buenaventura",
            "destino": "Bogotá",
            "metodo": "lookup_consolidado",
            "mes": 202607,
            "totales": {"H2": 100, "H4": 120, "H8": 160},
            "detalle_lookup": {"rutasid": "154", "nombre_sice": "Ruta directa"},
        }
        regreso = {
            "origen": "Bogotá",
            "destino": "Buenaventura",
            "metodo": "lookup_vacio_oficial",
            "mes": 202607,
            "totales": {"H2": 70, "H4": 70, "H8": 70},
            "detalle_lookup": {
                "rutasid": "95",
                "nombre_sice": "Ruta directa",
                "movilizacion": 50,
                "valor_hora": 10,
            },
        }
        consultar.side_effect = [ida, regreso]

        resultado = main.consultar_viaje_redondo_con_vacio(
            ruta={
                "origen": "Buenaventura",
                "destino": "Bogotá",
                "codigo_dane_origen": "76109000",
                "codigo_dane_destino": "11001000",
            },
            vehiculo="C2S2",
            carroceria="Portacontenedores",
        )

        self.assertFalse(resultado.get("_error"))
        self.assertEqual(resultado["pares"][0]["total_viaje"]["total"], 210.0)
        self.assertEqual(consultar.call_args_list[0].kwargs["modo_viaje"], "CARGADO")
        self.assertEqual(consultar.call_args_list[1].kwargs["modo_viaje"], "VACIO")
        self.assertEqual(consultar.call_args_list[1].kwargs["origen"], "Bogotá")

        message = main.formatear_viaje_redondo_con_vacio(resultado)
        self.assertIn("$160 cargado H8 + $50 vacio logistica = $210 total", message)
        self.assertNotIn("H2:", message)
        self.assertNotIn("H4:", message)


if __name__ == "__main__":
    unittest.main()
