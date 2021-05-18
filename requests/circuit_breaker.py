import pybreaker
import os
import json
import warnings
from urllib3.util.url import get_host
from .exceptions import ApiCircuitBreakerError, CustomHttpCircuitBreakerError


class CircuitBreakerConfig(object):

    def __init__(self, fail_max_to_open, sleep_time_to_half_open, http_failed_status_code_list):
        self.fail_max_to_open = fail_max_to_open
        self.sleep_time_to_half_open = sleep_time_to_half_open
        self.http_failed_status_code_list = http_failed_status_code_list or []

    @staticmethod
    def from_json(json_data):
        configs = {}
        try:
            for config in json_data:
                try:
                    if config["domain_name"] in configs:
                        warnings.warn(
                            "Config already present once overriding :" + config["domain_name"])
                    configs[config["domain_name"]] = CircuitBreakerConfig(config["fail_max_to_open"],
                                                                          config["sleep_time_to_half_open"],
                                                                          config["http_failed_status_code_list"])
                except:
                    warnings.warn("JSON File has wrong format circuit breaker functionality wont be used :" + config)
        except:
            warnings.warn("JSON File has wrong format circuit breaker functionality wont be used : JSON_PARSE_ERROR")
        return configs


class CircuitBreaker(object):

    def __init__(self):
        self.__circuit_breaker_factory_per_domain = {}
        self.__circuit_breaker_config_per_domain = {}
        self.__load_from_json_file()
        self.__register_circuit_breaker()

    def __load_from_json_file(self):

        json_file_path = os.environ.get("CB_JSON_FILE_PATH") or None
        if not json_file_path:
            warnings.warn("JSON File path not found circuit breaker functionality wont be used : JSON_FILE_PATH")
        try:
            with open(json_file_path, ) as f:
                data = json.load(f)
                self.__circuit_breaker_config_per_domain = CircuitBreakerConfig.from_json(data)
        except:
            warnings.warn("JSON File has wrong format circuit breaker functionality wont be used : JSON_FILE_PATH")

    def __register_circuit_breaker(self):

        for key, config in self.__circuit_breaker_config_per_domain.iteritems():
            self.__circuit_breaker_factory_per_domain[key] = pybreaker.CircuitBreaker(
                fail_max=config.fail_max_to_open,
                reset_timeout=config.sleep_time_to_half_open,
                state_storage=pybreaker.CircuitMemoryStorage(pybreaker.STATE_CLOSED))

    def __get_circuit_breaker_by_url(self, url):
        try:
            _, domain_name, _ = get_host(url)
            cfg = self.__circuit_breaker_config_per_domain.get(domain_name)
            return self.__circuit_breaker_factory_per_domain.get(domain_name), cfg.http_failed_status_code_list
        except:
            return None

    def execute_with_circuit_breaker(self, func, method, url, **kwargs):

        cb, status_code_list = self.__get_circuit_breaker_by_url(url)
        if not cb:
            return False, None

        try:
            return True, cb.call(CircuitBreaker.basic_request_cb, status_code_list, func, method, url, kwargs)
        except pybreaker.CircuitBreakerError:
            raise ApiCircuitBreakerError(
                "Requests are closed because of too many failures".format(url)
            )
        except CustomHttpCircuitBreakerError as e:
            return True, e.http_response

    @staticmethod
    def basic_request_cb(status_code_list, func, method, url, **kwargs):
        response = func(method, url, **kwargs)
        if response.status_code in status_code_list:
            raise CustomHttpCircuitBreakerError(response)
        return response


default_circuit_breaker = CircuitBreaker()