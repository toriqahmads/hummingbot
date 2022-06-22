import logging

import grpc

from hummingbot.client.config.global_config_map import global_config_map
from hummingbot.client.controller.hummingbot_controller import HummingbotController
from hummingbot.client.controller.rpc.hummingbot_controller_pb2_grpc import add_HummingbotControllerServicer_to_server
from hummingbot.client.controller.rpc.hummingbot_controller_rpc_server import HummingbotControllerRPCServer
from hummingbot.client.hummingbot_application import HummingbotApplication


async def start_controller(app: HummingbotApplication) -> grpc.aio.server:
    server = grpc.aio.server()
    controller = HummingbotController(app)
    server_handler = HummingbotControllerRPCServer(controller)
    add_HummingbotControllerServicer_to_server(server_handler, server)
    port = global_config_map["controller_server_port"].value
    listen_addr = f"[::]:{port}"
    server.add_insecure_port(listen_addr)
    logging.info("Starting controller server on %s", listen_addr)
    await server.start()
    return server
