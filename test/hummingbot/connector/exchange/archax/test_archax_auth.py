import asyncio
from typing import Awaitable
from unittest import TestCase
from unittest.mock import MagicMock

import hummingbot.connector.exchange.archax.archax_constants as CONSTANTS
from hummingbot.connector.exchange.archax.archax_auth import ArchaxAuth
from hummingbot.core.web_assistant.connections.data_types import RESTMethod


class ArchaxAuthTests(TestCase):

    def setUp(self) -> None:
        super().setUp()
        self.archax_email = "testApiKey"
        self.archax_password = "testPassphrase"
        self.domain = "domain"

        self.auth = ArchaxAuth(
            archax_email=self.archax_email,
            archax_password=self.archax_password,
            domain=self.domain,
        )

        self.successful_jwt = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJwcmltYXJ5T3JnIjoxMjR9.ydkBt9gJegOwq9WnfDoJWWOjVJCeorXAjMJhImJBazI"

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: int = 1):
        ret = asyncio.get_event_loop().run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def async_return(self, result):
        f = asyncio.Future()
        f.set_result(result)
        return f

    def test_generate_ws_authentication_message_success(self):
        ret_val = {
            "status": "OK",
            "data": {
                "jwt": self.successful_jwt
            }
        }

        mock = MagicMock(return_value=self.async_return(ret_val))

        auth_message = self.async_run_with_timeout(self.auth.generate_ws_authentication_message(rest_client=lambda path_url, domain, method, params: mock(path_url, domain, method, params)))
        self.assertEqual(self.successful_jwt, auth_message["token"])
        self.assertEqual("login", auth_message["action"])
        self.assertEqual("core", auth_message["service"])
        mock.assert_called_once_with(CONSTANTS.LOGIN_PATH_URL, self.domain, RESTMethod.POST, {'email': self.archax_email, 'password': self.archax_password})

    def test_generate_ws_authentication_message_login_fail(self):
        ret_val = {
            "status": "ERROR",
            "error": "login failed"
        }

        mock = MagicMock(return_value=self.async_return(ret_val))

        auth_message = self.async_run_with_timeout(self.auth.generate_ws_authentication_message(rest_client=lambda path_url, domain, method, params: mock(path_url, domain, method, params)))
        self.assertEqual("", auth_message["token"])
        self.assertEqual("login", auth_message["action"])
        self.assertEqual("core", auth_message["service"])
