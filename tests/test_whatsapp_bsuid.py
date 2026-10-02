import asyncio
import unittest
from unittest.mock import Mock, patch

import main


class FakeRequest:
    def __init__(self, body):
        self.body = body

    async def json(self):
        return self.body


class WhatsAppBsuidTests(unittest.TestCase):
    def setUp(self):
        self.state_backup = dict(main.CONVERSATION_STATE)
        main.CONVERSATION_STATE.clear()

    def tearDown(self):
        main.CONVERSATION_STATE.clear()
        main.CONVERSATION_STATE.update(self.state_backup)

    def test_resolves_username_sender_without_phone(self):
        message = {
            "from_user_id": "bsuid-123",
            "type": "text",
            "text": {"body": "Bogota a Medellin"},
        }
        contact = {
            "profile": {"name": "Daniela Cardona", "username": "@dacardona18"},
            "user_id": "bsuid-123",
        }

        recipient = main.resolve_whatsapp_recipient(message, contact)

        self.assertEqual(recipient["user_id"], "bsuid-123")
        self.assertEqual(recipient["username"], "@dacardona18")
        self.assertNotIn("phone", recipient)
        self.assertEqual(main.extract_incoming_message(message), ("Bogota a Medellin", "text"))

    def test_keeps_legacy_phone_state_when_bsuid_appears(self):
        phone_state = main.get_state("573045717979")
        phone_state["pending_selection"] = "vehicle"

        state = main.get_state(
            phone="573045717979",
            user_id="bsuid-123",
            username="@dacardona18",
        )

        self.assertIs(state, phone_state)
        self.assertNotIn("573045717979", main.CONVERSATION_STATE)
        self.assertEqual(main.CONVERSATION_STATE["bsuid:bsuid-123"], phone_state)
        self.assertEqual(state["lead"]["phone"], "573045717979")
        self.assertEqual(state["lead"]["wa_user_id"], "bsuid-123")
        self.assertEqual(state["lead"]["wa_username"], "@dacardona18")
        self.assertEqual(state["pending_selection"], "vehicle")

    def test_builds_bsuid_address_without_inventing_a_phone(self):
        outgoing = main.build_whatsapp_send_payload(
            {"user_id": "bsuid-123"},
            {"type": "text", "text": {"body": "Respuesta"}},
        )

        self.assertEqual(outgoing["recipient"], "bsuid-123")
        self.assertEqual(outgoing["recipient_type"], "individual")
        self.assertNotIn("to", outgoing)
        self.assertEqual(outgoing["messaging_product"], "whatsapp")

    def test_phone_address_remains_unchanged_when_phone_is_available(self):
        outgoing = main.build_whatsapp_send_payload(
            {"phone": "573045717979", "user_id": "bsuid-123"},
            {"type": "text", "text": {"body": "Respuesta"}},
        )

        self.assertEqual(outgoing["to"], "573045717979")
        self.assertNotIn("recipient", outgoing)

    @patch("main.requests.post")
    def test_phone_less_help_message_gets_processed_and_replied_to(self, post):
        post.return_value = Mock(status_code=200, text="{}")
        body = {
            "entry": [{
                "changes": [{
                    "value": {
                        "messages": [{
                            "from_user_id": "bsuid-123",
                            "type": "text",
                            "text": {"body": "ayuda"},
                        }],
                        "contacts": [{
                            "user_id": "bsuid-123",
                            "profile": {"name": "Daniela Cardona", "username": "@dacardona18"},
                        }],
                    }
                }]
            }]
        }
        with patch.object(main, "WHATSAPP_TOKEN", "test-token"), patch.object(
            main, "WHATSAPP_PHONE_ID", "phone-id"
        ):
            result = asyncio.run(main.receive_message(FakeRequest(body)))

        self.assertEqual(result["status"], "help sent")
        self.assertGreaterEqual(post.call_count, 1)
        for call in post.call_args_list:
            payload = call.kwargs["json"]
            self.assertEqual(payload["recipient"], "bsuid-123")
            self.assertNotIn("to", payload)
        state = main.CONVERSATION_STATE["bsuid:bsuid-123"]
        self.assertEqual(state["lead"]["wa_username"], "@dacardona18")

    @patch("main.requests.post")
    def test_send_uses_bsuid_request_shape(self, post):
        post.return_value = Mock(status_code=200, text="{}")
        with patch.object(main, "WHATSAPP_TOKEN", "test-token"), patch.object(
            main, "WHATSAPP_PHONE_ID", "phone-id"
        ):
            sent = main.send_whatsapp_payload(
                {"user_id": "bsuid-123"},
                {"type": "text", "text": {"body": "Respuesta"}},
            )

        self.assertTrue(sent)
        self.assertEqual(post.call_args.args[0], "https://graph.facebook.com/v25.0/phone-id/messages")
        self.assertEqual(post.call_args.kwargs["json"]["recipient"], "bsuid-123")
        self.assertNotIn("to", post.call_args.kwargs["json"])

    @patch("main.requests.post")
    def test_phone_less_lead_is_captured_with_bsuid_and_username(self, post):
        payload = {
            "event": "route_query",
            "lead": {
                "wa_user_id": "CO.bsuid-123",
                "wa_username": "@dacardona18",
            },
        }
        with patch.object(main, "LEAD_CAPTURE_WEBHOOK_URL", "https://capture.invalid"):
            main.capture_lead_event(payload)

        post.assert_called_once()
        captured = post.call_args.kwargs["json"]["lead"]
        self.assertEqual(captured["wa_user_id"], "CO.bsuid-123")
        self.assertEqual(captured["wa_username"], "@dacardona18")
        self.assertNotIn("phone", captured)

    @patch("main.requests.post")
    def test_event_without_phone_or_bsuid_is_skipped(self, post):
        with patch.object(main, "LEAD_CAPTURE_WEBHOOK_URL", "https://capture.invalid"):
            main.capture_lead_event({"event": "route_query", "lead": {"wa_username": "@dacardona18"}})

        post.assert_not_called()


if __name__ == "__main__":
    unittest.main()
