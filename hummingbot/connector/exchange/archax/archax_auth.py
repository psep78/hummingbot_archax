import asyncio
import time
from collections import OrderedDict
from typing import Any, Dict, Optional

import hummingbot.connector.exchange.archax.archax_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class ArchaxAuth(AuthBase):

    login_lock = asyncio.Lock()

    def __init__(self, archax_email: str, archax_password: str, web_assistants_factory: WebAssistantsFactory, domain: str):
        self.archax_email = archax_email
        self.archax_password = archax_password
        self.web_assistants_factory = web_assistants_factory
        self.domain = domain
        self.archax_jwt = str()

    @staticmethod
    def keysort(dictionary: Dict[str, str]) -> Dict[str, str]:
        return OrderedDict(sorted(dictionary.items(), key=lambda t: t[0]))

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.
        :param request: the request to be configured for authenticated interaction
        """
        return request  # pass-through

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. Archax does not use this
        functionality
        """
        return request  # pass-through

    async def _api_request(self,
                           path_url,
                           method: RESTMethod = RESTMethod.GET,
                           params: Optional[Dict[str, Any]] = None,
                           data: Optional[Dict[str, Any]] = None
                           ) -> Dict[str, Any]:
        last_exception = None
        rest_assistant = await self.web_assistants_factory.get_rest_assistant()
        url = CONSTANTS.REST_URLS[self.domain] + path_url
        local_headers = {
            "Content-Type": "application/x-www-form-urlencoded"}
        for _ in range(2):
            try:
                request_result = await rest_assistant.execute_request(
                    url=url,
                    params=params,
                    data=data,
                    method=method,
                    is_auth_required=False,
                    return_err=True,
                    headers=local_headers,
                    throttler_limit_id=path_url,
                )
                return request_result
            except IOError as request_exception:
                last_exception = request_exception
                raise

        # Failed even after the last retry
        raise last_exception

    async def _login(self):
        async with ArchaxAuth.login_lock:
            if len(self.archax_jwt) == 0:
                params = {
                    "email": self.archax_email,
                    "password": self.archax_password
                }

                login_response = await self._api_request(
                    method=RESTMethod.POST,
                    path_url=CONSTANTS.LOGIN_PATH_URL,
                    params=params)
                self.archax_jwt = login_response["data"]["jwt"]

            return self.archax_jwt

    async def generate_ws_authentication_message(self):
        """
        Generates the authentication message to start receiving messages from
        the 3 private ws channels
        """
        await self._login()

        auth_message = {
            "action": "login",
            "service": "core",
            "token": self.archax_jwt
        }
        return auth_message

    def _time(self):
        return time.time()
