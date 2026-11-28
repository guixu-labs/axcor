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

"""观测路由行为断言（health 形状）。"""

from __future__ import annotations

from types import SimpleNamespace

from workflow_controller.api.http.routes import overview_routes
from workflow_controller.api.session_api import SessionAPI


def _request_with(orch) -> SimpleNamespace:
    # 真实 SessionAPI：经 api 面取健康快照
    api = SimpleNamespace(session=SessionAPI(orch))
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(api=api)))


async def test_health_shape_covers_hook_server_flag_and_session_count() -> None:
    running = SimpleNamespace(
        hook_server_active=True,
        pool=SimpleNamespace(list_sessions=lambda: [object(), object()]),
    )
    assert await overview_routes.health_check(_request_with(running)) == {
        "ok": True,
        "orchestrator": "running",
        "hook_server": True,
        "sessions": 2,
    }

    no_hook_server = SimpleNamespace(
        hook_server_active=False,
        pool=SimpleNamespace(list_sessions=lambda: []),
    )
    resp = await overview_routes.health_check(_request_with(no_hook_server))
    assert resp["hook_server"] is False
    assert resp["sessions"] == 0
