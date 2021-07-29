from google.protobuf import descriptor_pool as _descriptor_pool, symbol_database as _symbol_database
from google.protobuf.descriptor_pool import DescriptorPool
from google.protobuf.symbol_database import SymbolDatabase
from homi.test_case import HomiRealServerTestCase

try:
    from unittest import IsolatedAsyncioTestCase as AsyncTestCaseClass
except ImportError:
    from aiounittest import AsyncTestCase as AsyncTestCaseClass
from ..grpc_requests.client import reset_cached_client


class RealServerTestCase(HomiRealServerTestCase):
    reset_descriptor_pool = True

    @property
    def default_endpoint(self):
        return f"{self.default_server_config['host']}:{self.default_server_config['port']}"

    def reset_grpc_db(self):
        reset_cached_client()
        if self.reset_descriptor_pool:
            _descriptor_pool._DEFAULT = DescriptorPool()
            _symbol_database._DEFAULT = SymbolDatabase(pool=_descriptor_pool.Default())

    def setUp(self):
        self.reset_grpc_db()
        super().setUp()

    def tearDown(self):
        self.test_server.stop(1)


class RealTLSServerTestCase(HomiRealServerTestCase):
    reset_descriptor_pool = True
    tls = True

    @property
    def default_endpoint(self):
        return f"{self.default_server_config['host']}:{self.default_server_config['port']}"

    def reset_grpc_db(self):
        reset_cached_client()
        if self.reset_descriptor_pool:
            _descriptor_pool._DEFAULT = DescriptorPool()
            _symbol_database._DEFAULT = SymbolDatabase(pool=_descriptor_pool.Default())

    def setUp(self):
        self.reset_grpc_db()
        super().setUp()

    def tearDown(self):
        self.test_server.stop(1)



class RealServerAsyncTestCase(RealServerTestCase, AsyncTestCaseClass):
    pass
