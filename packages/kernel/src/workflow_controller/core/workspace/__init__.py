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

"""工作区作用域树：私有仓、作用域账本、写入闸门与三阶段操作日志。"""

from workflow_controller.core.workspace.actions import (
    AbandonScope,
    CheckpointRecorded,
    IntegrateScope,
    IntegrationLineRetracted,
    PrepareScopeWorkspace,
    RestoreCommitted,
    RestoreScope,
    ScopeAbandoned,
    ScopeIntegrated,
    ScopeIntegrationRejected,
    ScopeRestored,
    ScopeWorkspacePrepared,
    ScopeWorkspaceSpec,
)
from workflow_controller.core.workspace.claims import (
    WORKSPACE_CLAIM_REJECTED_EVENT,
    ClaimRejectedError,
    ClaimsRegistry,
    claims_overlap,
)
from workflow_controller.core.workspace.gate import (
    DEFAULT_BACKGROUND_DRAIN_TIMEOUT_S,
    GateState,
    QuiesceOutcome,
    ScopeGate,
    ScopeGateError,
)
from workflow_controller.core.workspace.integration import (
    MERGE_TREE_MIN_GIT_VERSION,
    ConflictPolicy,
    IntegrationConflict,
    IntegrationCoordinator,
    IntegrationCoordinatorError,
    IntegrationSuccess,
    WorkspaceTarget,
)
from workflow_controller.core.workspace.manager import (
    ScopeQuiesceFailedError,
    WorkspaceManager,
    WorkspaceManagerError,
)
from workflow_controller.core.workspace.manifest import (
    DEFAULT_WRITER_LOCAL_PATHSPECS,
    CheckpointKind,
    InheritBinding,
    InheritDeclaration,
    IntegrationPolicy,
    PathPolicy,
    RebuildScript,
    RollbackPolicy,
    WorkspaceManifest,
    WorkspaceManifestError,
)
from workflow_controller.core.workspace.oplog import (
    OPLOG_FILENAME,
    CommitFacts,
    OperationPhase,
    OplogError,
    RecoveryAction,
    RecoveryDecision,
    SagaOperation,
    SagaOplog,
    operation_marker,
)
from workflow_controller.core.workspace.repository import (
    INNER_GIT_RESERVED_PATH,
    ROOT_INTEGRATION_REF,
    ROOT_SEED_REF,
    RootRefs,
    ScopeWorkspace,
    WorkspaceRepository,
    WorkspaceRepositoryError,
)
from workflow_controller.core.workspace.scope_tree import (
    INITIAL_EPOCH,
    AbandonedScope,
    ScopeGateState,
    ScopeKind,
    ScopeRecord,
    ScopeState,
    ScopeTree,
    ScopeTreeError,
)

__all__ = [
    "DEFAULT_BACKGROUND_DRAIN_TIMEOUT_S",
    "DEFAULT_WRITER_LOCAL_PATHSPECS",
    "INITIAL_EPOCH",
    "INNER_GIT_RESERVED_PATH",
    "MERGE_TREE_MIN_GIT_VERSION",
    "OPLOG_FILENAME",
    "ROOT_INTEGRATION_REF",
    "ROOT_SEED_REF",
    "WORKSPACE_CLAIM_REJECTED_EVENT",
    "AbandonScope",
    "AbandonedScope",
    "CheckpointKind",
    "CheckpointRecorded",
    "ClaimRejectedError",
    "ClaimsRegistry",
    "CommitFacts",
    "ConflictPolicy",
    "GateState",
    "InheritBinding",
    "InheritDeclaration",
    "IntegrateScope",
    "IntegrationConflict",
    "IntegrationCoordinator",
    "IntegrationCoordinatorError",
    "IntegrationLineRetracted",
    "IntegrationPolicy",
    "IntegrationSuccess",
    "OperationPhase",
    "OplogError",
    "PathPolicy",
    "PrepareScopeWorkspace",
    "QuiesceOutcome",
    "RebuildScript",
    "RecoveryAction",
    "RecoveryDecision",
    "RestoreCommitted",
    "RestoreScope",
    "RollbackPolicy",
    "RootRefs",
    "SagaOperation",
    "SagaOplog",
    "ScopeAbandoned",
    "ScopeGate",
    "ScopeGateError",
    "ScopeGateState",
    "ScopeIntegrated",
    "ScopeIntegrationRejected",
    "ScopeKind",
    "ScopeQuiesceFailedError",
    "ScopeRecord",
    "ScopeRestored",
    "ScopeState",
    "ScopeTree",
    "ScopeTreeError",
    "ScopeWorkspace",
    "ScopeWorkspacePrepared",
    "ScopeWorkspaceSpec",
    "WorkspaceManager",
    "WorkspaceManagerError",
    "WorkspaceManifest",
    "WorkspaceManifestError",
    "WorkspaceRepository",
    "WorkspaceRepositoryError",
    "WorkspaceTarget",
    "claims_overlap",
    "operation_marker",
    "pathspec_matches",
]
