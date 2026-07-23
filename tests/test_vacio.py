from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
