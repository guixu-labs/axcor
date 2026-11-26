# Copyright (c) 2026 Workflow Controller Contributors
#
# Licensed under the MIT License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://opensource.org/licenses/MIT
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import ipaddress
import os
import shutil
import socket
import tempfile
from functools import partial
from pathlib import Path
from typing import Any

import pytest

_DNS_API_NAMES = (
    "getaddrinfo",
    "gethostbyname",
    "gethostbyname_ex",
    "gethostbyaddr",
    "getfqdn",
)
_ORIGINAL_APIS = {
    (socket.socket, "connect"): socket.socket.connect,
    (socket.socket, "connect_ex"): socket.socket.connect_ex,
    (socket, "create_connection"): socket.create_connection,
    **{(socket, name): getattr(socket, name) for name in _DNS_API_NAMES},
}

# 测试内裸 git 提交的身份：CI 无全局 git 身份、主机名不可解析时自动推导失败
_TEST_GIT_IDENTITY = {
    "GIT_AUTHOR_NAME": "workflow-controller-pytest",
    "GIT_AUTHOR_EMAIL": "workflow-controller-pytest@local",
    "GIT_COMMITTER_NAME": "workflow-controller-pytest",
    "GIT_COMMITTER_EMAIL": "workflow-controller-pytest@local",
}


class ExternalNetworkBlockedError(RuntimeError):
    pass


def _host_is_local(host: object) -> bool:
    if host in {None, "", "localhost", "localhost.localdomain"}:
        return True
    if isinstance(host, bytes):
        host = host.decode(errors="ignore")
    if not isinstance(host, str):
        return False
    try:
        return ipaddress.ip_address(host.split("%", 1)[0]).is_loopback
    except ValueError:
        return False


def _address_is_local(address: object) -> bool:
    if isinstance(address, (str, bytes, Path)):
        return True
    return isinstance(address, tuple) and bool(address) and _host_is_local(address[0])


def _blocked(address: object) -> ExternalNetworkBlockedError:
    return ExternalNetworkBlockedError(
        f"External network and DNS are disabled in tests: {address!r}. "
        "Use loopback/Unix sockets, or mark a real integration test and pass --live."
    )


def _guarded_connect(sock: socket.socket, address: Any) -> Any:
    if not _address_is_local(address):
        raise _blocked(address)
    return _ORIGINAL_APIS[(socket.socket, "connect")](sock, address)


def _guarded_connect_ex(sock: socket.socket, address: Any) -> int:
    if not _address_is_local(address):
        raise _blocked(address)
    return _ORIGINAL_APIS[(socket.socket, "connect_ex")](sock, address)


def _guarded_create_connection(
    address: tuple[str, int], *args: Any, **kwargs: Any
) -> socket.socket:
    if not _address_is_local(address):
        raise _blocked(address)
    original = _ORIGINAL_APIS[(socket, "create_connection")]
    return original(address, *args, **kwargs)


def _guarded_dns_call(original: Any, host: Any, *args: Any, **kwargs: Any) -> Any:
    if not _host_is_local(host):
        raise _blocked(host)
    return original(host, *args, **kwargs)


def _replace_api(owner: Any, name: str, value: Any) -> None:
    setattr(owner, name, value)


def _install_network_guard() -> None:
    _replace_api(socket.socket, "connect", _guarded_connect)
    _replace_api(socket.socket, "connect_ex", _guarded_connect_ex)
    _replace_api(socket, "create_connection", _guarded_create_connection)
    for name in _DNS_API_NAMES:
        original = _ORIGINAL_APIS[(socket, name)]
        _replace_api(socket, name, partial(_guarded_dns_call, original))


def _restore_network() -> None:
    for (owner, name), original in _ORIGINAL_APIS.items():
        _replace_api(owner, name, original)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--live",
        action="store_true",
        default=False,
        help="Allow tests marked live_llm/reviewer_recall to use real external services.",
    )


def pytest_configure(config: pytest.Config) -> None:
    setattr(config, "_workflow_controller_original_environment", dict(os.environ))  # type: ignore[attr-defined]
    config_dir = Path(tempfile.mkdtemp(prefix="workflow-controller-pytest-config-"))
    setattr(config, "_workflow_controller_config_dir", config_dir)  # type: ignore[attr-defined]
    os.environ["HOME"] = str(config_dir)
    os.environ["XDG_CONFIG_HOME"] = str(config_dir)
    os.environ.update(_TEST_GIT_IDENTITY)
    if not config.getoption("--live"):
        _install_network_guard()


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if config.getoption("--live"):
        return
    skip = pytest.mark.skip(reason="needs --live (uses a real external service)")
    for item in items:
        if "live_llm" in item.keywords or "reviewer_recall" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(autouse=True)
def _restore_environment_after_test() -> Any:
    before = dict(os.environ)
    yield
    after = dict(os.environ)
    ignored = {"PYTEST_CURRENT_TEST"}
    comparable_before = {key: value for key, value in before.items() if key not in ignored}
    comparable_after = {key: value for key, value in after.items() if key not in ignored}
    if comparable_after == comparable_before:
        return
    added = sorted(comparable_after.keys() - comparable_before.keys())
    removed = sorted(comparable_before.keys() - comparable_after.keys())
    changed = sorted(
        key
        for key in comparable_before.keys() & comparable_after.keys()
        if comparable_before[key] != comparable_after[key]
    )
    os.environ.clear()
    os.environ.update(before)
    pytest.fail(
        "Test leaked environment changes; use monkeypatch.setenv/delenv. "
        f"added={added}, removed={removed}, changed={changed}",
        pytrace=False,
    )


def pytest_sessionfinish(session: pytest.Session) -> None:
    config = session.config
    original = getattr(config, "_workflow_controller_original_environment")  # type: ignore[attr-defined]
    config_dir = getattr(config, "_workflow_controller_config_dir")  # type: ignore[attr-defined]
    current = dict(os.environ)
    expected = dict(original)
    expected["HOME"] = str(config_dir)
    expected["XDG_CONFIG_HOME"] = str(config_dir)
    expected.update(_TEST_GIT_IDENTITY)
    if current != expected and session.exitstatus == pytest.ExitCode.OK:
        session.exitstatus = pytest.ExitCode.TESTS_FAILED
    os.environ.clear()
    os.environ.update(original)
    _restore_network()
    shutil.rmtree(config_dir, ignore_errors=True)
