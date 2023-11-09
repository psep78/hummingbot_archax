import asyncio
import logging

import jwt

import hummingbot.connector.exchange.archax.archax_constants as CONSTANTS
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest
from hummingbot.logger.logger import HummingbotLogger


class ArchaxAuth(AuthBase):

    login_lock = asyncio.Lock()
    _logger = None

    def __init__(self, archax_email: str, archax_password: str, domain: str):
        self.archax_email = archax_email
        self.archax_password = archax_password
        self.domain = domain
        self.archax_jwt = str()
        self.primary_org = 0

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(HummingbotLogger.logger_name_for_class(cls))
        return cls._logger

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

    async def _login(self, rest_client):
        async with ArchaxAuth.login_lock:
            if len(self.archax_jwt) == 0:
                params = {
                    "email": self.archax_email,
                    "password": self.archax_password
                }

                login_response = await rest_client(
                    path_url=CONSTANTS.LOGIN_PATH_URL,
                    domain=self.domain,
                    method=RESTMethod.POST,
                    params=params
                )

                if login_response["status"] == "OK":
                    self.archax_jwt = login_response["data"]["jwt"]
                    self.extract_org_id()
                else:
                    self.logger().error(f'Unable to log in: {login_response["error"]}')

            return self.archax_jwt

    async def generate_ws_authentication_message(self, rest_client):
        """
        Generates the authentication message to start receiving messages from
        the 3 private ws channels
        """
        await self._login(rest_client)

        auth_message = {
            "action": "login",
            "service": "core",
            "token": self.archax_jwt
        }
        return auth_message

    def extract_org_id(self):
        decoded_data = jwt.decode(jwt=self.archax_jwt, verify=False, algorithms=['RS256', ])
        self.primary_org = decoded_data["primaryOrg"]
