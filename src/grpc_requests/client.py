import logging
from enum import Enum
from functools import partial
from typing import Any, Dict, Iterable, List, NamedTuple, Tuple, TypeVar

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool as _descriptor_pool, symbol_database as _symbol_database
from google.protobuf.descriptor import MethodDescriptor, ServiceDescriptor
from google.protobuf.descriptor_pb2 import ServiceDescriptorProto
from google.protobuf.json_format import MessageToDict, ParseDict
from grpc_reflection.v1alpha import reflection_pb2, reflection_pb2_grpc


class DescriptorImport:
    def __init__(self, ):
        pass


def make_request(*requests):
    for r in requests:
        yield r


def reflection_request(channel, requests):
    stub = reflection_pb2_grpc.ServerReflectionStub(channel)
    responses = stub.ServerReflectionInfo(make_request(requests))
    try:
        for resp in responses:
            yield resp
    except grpc._channel._Rendezvous as err:
        print(err)


class BaseClient:
    def __init__(self, endpoint, symbol_db=None, descriptor_pool=None, channel_options=None, ssl_credentials=None,
                 compression=None, **kwargs):
        self.endpoint = endpoint
        self._symbol_db = symbol_db or _symbol_database.Default()
        self._desc_pool = descriptor_pool or _descriptor_pool.Default()
        self.compression = compression
        self.channel_options = channel_options
        if ssl_credentials:
            self._channel = grpc.secure_channel(endpoint, credentials=ssl_credentials, options=self.channel_options,
                                                compression=self.compression)
        else:
            self._channel = grpc.insecure_channel(endpoint, options=self.channel_options, compression=self.compression)

    @property
    def channel(self):
        return self._channel

    @classmethod
    def get_by_endpoint(cls, endpoint, **kwargs):
        global _cached_clients
        if endpoint not in _cached_clients:
            _cached_clients[endpoint] = cls(endpoint, **kwargs)
        return _cached_clients[endpoint]

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self._channel._close()
        except Exception:  # pylint: disable=bare-except
            pass
        return False

    def __del__(self):
        if self._channel:
            try:
                del self._channel
            except Exception:  # pylint: disable=bare-except
                pass


def parse_request_data(reqeust_data, input_type):
    _data = reqeust_data or {}
    if isinstance(_data, dict):
        request = ParseDict(_data, input_type())
    else:
        request = _data
    return request


def parse_stream_requests(stream_requests_data: Iterable, input_type):
    for request_data in stream_requests_data:
        yield parse_request_data(request_data or {}, input_type)


def parse_response(response):
    return MessageToDict(response, preserving_proto_field_name=True)


def parse_stream_responses(responses: Iterable):
    for resp in responses:
        yield parse_response(resp)


class MethodType(Enum):
    UNARY_UNARY = 'unary_unary'
    STREAM_UNARY = 'stream_unary'
    UNARY_STREAM = 'unary_stream'
    STREAM_STREAM = 'stream_stream'

    @property
    def is_unary_request(self):
        return 'unary_' in self.value

    @property
    def request_parser(self):
        return parse_request_data if self.is_unary_request else parse_stream_requests

    @property
    def is_unary_response(self):
        return '_unary' in self.value

    @property
    def response_parser(self):
        return parse_response if self.is_unary_response else parse_stream_responses


class MethodMetaData(NamedTuple):
    input_type: Any
    output_type: Any
    method_type: MethodType
    handler: Any


IS_REQUEST_STREAM = TypeVar("IS_REQUEST_STREAM")
IS_RESPONSE_STREAM = TypeVar("IS_RESPONSE_STREAM")

MethodTypeMatch: Dict[Tuple[IS_REQUEST_STREAM, IS_RESPONSE_STREAM], MethodType] = {
    (False, False): MethodType.UNARY_UNARY,
    (True, False): MethodType.STREAM_UNARY,
    (False, True): MethodType.UNARY_STREAM,
    (True, True): MethodType.STREAM_STREAM,
}


class BaseGrpcClient(BaseClient):

    def __init__(self, endpoint, symbol_db=None, descriptor_pool=None, lazy=False, ssl_credentials=None, compression=None,
                 **kwargs):
        super().__init__(endpoint, symbol_db, descriptor_pool, ssl_credentials=ssl_credentials, compression=compression, **kwargs)
        self._service_names: list = None
        self._lazy = lazy
        self.has_server_registered = False
        self._services_module_name = {}
        self._service_methods_meta: Dict[str, Dict[str, MethodMetaData]] = {}

        self._unary_unary_handler = {}
        self._unary_stream_handler = {}
        self._stream_unary_handler = {}
        self._stream_stream_handler = {}

    def _get_service_names(self):
        raise NotImplementedError()

    def check_method_available(self, service, method, method_type: MethodType = None):
        if not self.has_server_registered:
            self.register_all_service()
        methods_meta = self._service_methods_meta.get(service)
        if not methods_meta:
            raise ValueError(
                f"{self.endpoint} server doesn't support {service}. Available services {self.service_names}")

        if method not in methods_meta:
            raise ValueError(
                f"{service} doesn't support {method} method. Available methods {methods_meta.keys()}")
        if method_type and method_type != methods_meta[method].method_type:
            raise ValueError(
                f"{method} is {methods_meta[method].method_type.value} not {method_type.value}")
        return True

    def _register_methods(self, service_descriptor: ServiceDescriptor) -> Dict[str, MethodMetaData]:
        svc_desc_proto = ServiceDescriptorProto()
        service_descriptor.CopyToProto(svc_desc_proto)
        service_full_name = service_descriptor.full_name
        metadata: Dict[str, MethodMetaData] = {}
        for method_proto in svc_desc_proto.method:
            method_name = method_proto.name
            method_desc: MethodDescriptor = service_descriptor.methods_by_name[method_name]

            input_type = self._symbol_db.GetPrototype(method_desc.input_type)
            output_type = self._symbol_db.GetPrototype(method_desc.output_type)
            method_type = MethodTypeMatch[(method_proto.client_streaming, method_proto.server_streaming)]

            method_register_func = getattr(self.channel, method_type.value)
            handler = method_register_func(
                method=self._make_method_full_name(service_full_name, method_name),
                request_serializer=input_type.SerializeToString,
                response_deserializer=output_type.FromString
            )
            metadata[method_name] = MethodMetaData(
                method_type=method_type,
                input_type=input_type,
                output_type=output_type,
                handler=handler
            )
        return metadata

    def register_service(self, service_name):
        logging.debug(f"start {service_name} register")
        svc_desc = self._desc_pool.FindServiceByName(service_name)
        self._service_methods_meta[service_name] = self._register_methods(svc_desc)
        logging.debug(f"end {service_name} register")

    def register_all_service(self):
        for service in self.service_names:
            self.register_service(service)
        self.has_server_registered = True

    @property
    def service_names(self):
        if self._service_names is None:
            self._service_names = self._get_service_names()
        return self._service_names

    def get_methods_meta(self, service_name: str):

        if self._lazy and service_name in self.service_names and service_name not in self._service_methods_meta:
            self.register_service(service_name)

        try:
            return self._service_methods_meta[service_name]
        except KeyError:
            raise ValueError(f"{service_name} service can't find")

    @staticmethod
    def _make_method_full_name(service, method):
        return f"/{service}/{method}"

    def _request(self, service, method, request, raw_output=False, **kwargs):
        # does not check request is available
        method_meta = self.get_method_meta(service, method)

        _request = method_meta.method_type.request_parser(request, method_meta.input_type)
        result = method_meta.handler(_request, **kwargs)

        if raw_output:
            return result
        else:
            return method_meta.method_type.response_parser(result)

    def request(self, service, method, request=None, raw_output=False, **kwargs):
        self.check_method_available(service, method)
        return self._request(service, method, request, raw_output, **kwargs)

    def unary_unary(self, service, method, request=None, raw_output=False, **kwargs):
        self.check_method_available(service, method, MethodType.UNARY_UNARY)
        return self._request(service, method, request, raw_output, **kwargs)

    def unary_stream(self, service, method, request=None, raw_output=False, **kwargs):
        self.check_method_available(service, method, MethodType.UNARY_STREAM)
        return self._request(service, method, request, raw_output, **kwargs)

    def stream_unary(self, service, method, requests, raw_output=False, **kwargs):
        self.check_method_available(service, method, MethodType.STREAM_UNARY)
        return self._request(service, method, requests, raw_output, **kwargs)

    def stream_stream(self, service, method, requests, raw_output=False, **kwargs):
        self.check_method_available(service, method, MethodType.STREAM_STREAM)
        return self._request(service, method, requests, raw_output, **kwargs)

    def get_service_descriptor(self, service):
        return self._desc_pool.FindServiceByName(service)

    def get_method_descriptor(self, service, method):
        svc_desc = self.get_service_descriptor(service)
        return svc_desc.FindMethodByName(method)

    def get_method_meta(self, service: str, method: str) -> MethodMetaData:
        # add lazy mode & exception
        return self._service_methods_meta[service][method]

    def make_handler_argument(self, service: str, method: str):
        data_type = self.get_method_meta(service, method)
        return {
            'method': self._make_method_full_name(service, method),
            'request_serializer': data_type.input_type.SerializeToString,
            'response_deserializer': data_type.output_type.FromString,
        }

    def service(self, name):
        if name in self.service_names:
            return ServiceClient(client=self, service_name=name)
        else:
            raise ValueError(f"{name} doesn't support. Available service {self.service_names}")


class ReflectionClient(BaseGrpcClient):

    def __init__(self, endpoint, symbol_db=None, descriptor_pool=None, lazy=False, ssl_credentials=None, compression=None,
                 **kwargs):
        super().__init__(endpoint, symbol_db, descriptor_pool, ssl_credentials=ssl_credentials, lazy=lazy, compression=compression, **kwargs)
        self.reflection_stub = reflection_pb2_grpc.ServerReflectionStub(self.channel)
        self.registered_file_names = set()
        if not self._lazy:
            self.register_all_service()

    def _reflection_request(self, *requests):
        responses = self.reflection_stub.ServerReflectionInfo((r for r in requests))
        return responses

    def _reflection_single_request(self, request):
        results = list(self._reflection_request(request))
        if len(results) > 1:
            raise ValueError('response have more then one result')
        return results[0]

    def _get_service_names(self):
        request = reflection_pb2.ServerReflectionRequest(list_services="")
        resp = self._reflection_single_request(request)
        services = tuple([s.name for s in resp.list_services_response.service])
        return services

    def _get_file_descriptor_by_name(self, name):
        request = reflection_pb2.ServerReflectionRequest(file_by_filename=name)
        result = self._reflection_single_request(request)
        proto = result.file_descriptor_response.file_descriptor_proto[0]
        return descriptor_pb2.FileDescriptorProto.FromString(proto)

    def _get_file_descriptor_by_symbol(self, symbol):
        request = reflection_pb2.ServerReflectionRequest(file_containing_symbol=symbol)
        result = self._reflection_single_request(request)
        proto = result.file_descriptor_response.file_descriptor_proto[0]
        return descriptor_pb2.FileDescriptorProto.FromString(proto)

    def _register_file_descriptor(self, file_descriptor):
        logging.debug(f"start {file_descriptor.name} register")
        dependencies = list(file_descriptor.dependency)
        logging.debug(f"find {len(dependencies)} dependency in {file_descriptor.name}")
        for dep_file_name in dependencies:
            if dep_file_name not in self.registered_file_names:
                dep_desc = self._get_file_descriptor_by_name(dep_file_name)
                self._register_file_descriptor(dep_desc)
                self.registered_file_names.add(dep_file_name)
            else:
                logging.debug(f'{dep_file_name} already registered')

        self._desc_pool.Add(file_descriptor)
        logging.debug(f"end {file_descriptor.name} register")

    def register_service(self, service_name):
        logging.debug(f"start {service_name} register")
        file_descriptor = self._get_file_descriptor_by_symbol(service_name)
        self._register_file_descriptor(file_descriptor)
        super(ReflectionClient, self).register_service(service_name)


class StubClient(BaseGrpcClient):

    def __init__(self, endpoint, service_descriptors: List[ServiceDescriptor], symbol_db=None, lazy=False,
                 descriptor_pool=None, ssl_credentials=None, compression=None,
                 **kwargs):
        super().__init__(endpoint, symbol_db, descriptor_pool, ssl_credentials=ssl_credentials, compression=compression, lazy=lazy, **kwargs)
        self.service_descriptors = service_descriptors

        if not self._lazy:
            self.register_all_service()

    def _get_service_names(self):
        svcs = [x.full_name for x in self.service_descriptors]
        return svcs


class ServiceClient:
    def __init__(self, client: BaseGrpcClient, service_name: str):
        self.client = client
        self.name = service_name
        self._methods_meta = self.client.get_methods_meta(self.name)
        self._method_names = tuple(self._methods_meta.keys())
        self._register_methods()

    def _register_methods(self):
        for method in self._method_names:
            setattr(self, method, partial(self.client.request, self.name, method))

    @property
    def method_names(self):
        return self._method_names


Client = ReflectionClient

_cached_clients = {}  # Dict[str, Client] type (for 3.6,3.7 compatibility https://bugs.python.org/issue34939)


def get_by_endpoint(endpoint, service_descriptors=None, **kwargs) -> Client:
    global _cached_clients
    if endpoint not in _cached_clients:
        if service_descriptors:
            _cached_clients[endpoint] = StubClient(endpoint, service_descriptors=service_descriptors, **kwargs)
        else:
            _cached_clients[endpoint] = Client(endpoint, **kwargs)
    return _cached_clients[endpoint]


def reset_cached_client(endpoint=None):
    global _cached_clients
    if endpoint:
        if endpoint in _cached_clients:
            del _cached_clients[endpoint]
    else:
        _cached_clients = {}
