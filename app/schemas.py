"""请求/响应模型（Pydantic v2）。"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class MessageIn(BaseModel):
    message_id: str = Field(..., min_length=1, description="RFC 2822 Message-ID")
    in_reply_to: Optional[str] = Field(None, description="直接回复的邮件 ID")
    references: list[str] = Field(default_factory=list, description="引用链，按邮件头顺序")
    sent_at: datetime = Field(..., description="发送时间，无时区时按 UTC 处理")
    subject: str
    retain_until: Optional[datetime] = Field(
        None, description="保留截止时间（可选）；截止后且未被冻结保护的邮件可被清理；无时区按 UTC 处理"
    )

    @field_validator("message_id", "in_reply_to", "subject")
    @classmethod
    def _strip(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        v = v.strip()
        return v or None

    @field_validator("references")
    @classmethod
    def _strip_refs(cls, v: list[str]) -> list[str]:
        return [r.strip() for r in v if r and r.strip()]

    def normalized(self) -> "MessageIn":
        """把时间统一换算到 UTC（naive datetime 视为 UTC）。"""

        update: dict[str, datetime] = {}
        dt = self.sent_at
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        update["sent_at"] = dt.astimezone(timezone.utc)
        if self.retain_until is not None:
            ru = self.retain_until
            if ru.tzinfo is None:
                ru = ru.replace(tzinfo=timezone.utc)
            update["retain_until"] = ru.astimezone(timezone.utc)
        return self.model_copy(update=update)


class BatchIn(BaseModel):
    messages: list[MessageIn] = Field(..., min_length=1)


class RelinkIn(BaseModel):
    """POST /relinks：待补链的 message_id 列表。"""

    message_ids: list[str] = Field(
        ..., min_length=1, description="待修复断链邮件的 message_id（自动去空白、去重，保序处理）"
    )

    @field_validator("message_ids")
    @classmethod
    def _strip_ids(cls, v: list[str]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for raw in v:
            mid = raw.strip()
            if mid and mid not in seen:
                seen.add(mid)
                out.append(mid)
        return out

    @model_validator(mode="after")
    def _require_nonempty(self) -> "RelinkIn":
        if not self.message_ids:
            raise ValueError("message_ids must contain at least one non-empty id")
        return self


class TreeNode(BaseModel):
    message_id: str
    parent_id: Optional[str]
    in_reply_to: Optional[str]
    references: list[str]
    sent_at: datetime
    subject: str
    children: list["TreeNode"] = Field(default_factory=list)


class DuplicateOut(BaseModel):
    message_id: str
    archived: bool = Field(..., description="True 表示该 ID 已在此前批次归档")
    first_submitted_seq: Optional[int]
    occurrences: list[int]


class MissingRefsOut(BaseModel):
    message_id: str
    missing: list[str]


class CycleOut(BaseModel):
    index: int
    members: list[str]
    invalid_messages: list[str]


class InvalidMessageOut(BaseModel):
    message_id: str
    in_reply_to: Optional[str]
    references: list[str]
    sent_at: datetime
    subject: str
    reason: str
    cycle_index: int


class BatchSummary(BaseModel):
    batch_id: str
    submitted_at: datetime
    total_count: int
    unique_count: int
    invalid_count: int
    tree_count: int


class BatchResult(BaseModel):
    batch_id: str
    submitted_at: datetime
    totals: dict
    threads: list[TreeNode]
    duplicates: list[DuplicateOut]
    missing_references: list[MissingRefsOut]
    cycles: list[CycleOut]
    invalid_messages: list[InvalidMessageOut]


class MessageOut(BaseModel):
    message_id: str
    in_reply_to: Optional[str]
    references: list[str]
    sent_at: datetime
    subject: str
    parent_id: Optional[str]
    is_invalid: bool
    batch_id: str
    retain_until: Optional[datetime] = None
    frozen: bool = Field(False, description="当前是否被任一会话冻结保护")


class RelinkItem(BaseModel):
    message_id: str
    old_parent_id: Optional[str]
    new_parent_id: Optional[str]
    old_root_id: str
    new_root_id: str


class RelinkResult(BaseModel):
    relinked: int = Field(..., description="实际发生父节点变更的邮件数")
    results: list[RelinkItem] = Field(..., description="按请求顺序逐封给出新旧父/根；未变更者新旧值相同")


# ---------------------------------------------------------------------------
# 保留期：会话冻结 / 解除冻结 / 批量清理
# ---------------------------------------------------------------------------


class FreezeIn(BaseModel):
    """POST /retention/freezes：以某封有效邮件为锚点冻结其当前整棵会话。"""

    message_id: str = Field(..., min_length=1, description="冻结锚点邮件 ID")

    @field_validator("message_id")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("message_id must not be empty")
        return v


class FreezeResult(BaseModel):
    anchor_id: str = Field(..., description="冻结锚点邮件 ID（会话内任一封邮件）")
    root_id: str = Field(..., description="冻结时刻该会话的根节点 ID")
    frozen_at: datetime = Field(..., description="冻结（或最近一次刷新）时间，UTC")
    created: bool = Field(
        ..., description="True=本次新建冻结；False=锚点已有冻结，已按当前会话刷新成员快照"
    )
    members: list[str] = Field(..., description="受保护的整棵会话成员 ID，按先序排列")


class UnfreezeIn(BaseModel):
    """POST /retention/unfreezes：解除以某封邮件为锚点的冻结。"""

    message_id: str = Field(..., min_length=1, description="冻结锚点邮件 ID（非保护成员）")

    @field_validator("message_id")
    @classmethod
    def _strip(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("message_id must not be empty")
        return v


class UnfreezeResult(BaseModel):
    anchor_id: str
    root_id: str = Field(..., description="冻结记录中的会话根 ID")
    frozen_at: datetime
    released_members: list[str] = Field(..., description="本次解除后不再被该冻结覆盖的成员 ID")
    still_protected: list[str] = Field(
        ..., description="释放成员中仍被其它冻结覆盖、清理时依旧受保护的 ID"
    )


class CleanupIn(BaseModel):
    """POST /retention/cleanups：批量清理到期且未受保护的有效邮件。"""

    cutoff: Optional[datetime] = Field(
        None, description="保留截止判定时刻，缺省为当前 UTC；retain_until <= cutoff 视为到期"
    )
    dry_run: bool = Field(
        False, description="true 时使用同一算法计算结果但不写库"
    )

    def normalized_cutoff(self) -> Optional[datetime]:
        """cutoff 统一换算到 UTC（naive datetime 视为 UTC）。"""

        if self.cutoff is None:
            return None
        c = self.cutoff
        if c.tzinfo is None:
            c = c.replace(tzinfo=timezone.utc)
        return c.astimezone(timezone.utc)


class CleanupItem(BaseModel):
    """压缩后父节点或根节点发生变化的存活节点。"""

    message_id: str
    old_parent_id: Optional[str]
    new_parent_id: Optional[str]
    old_root_id: str
    new_root_id: str


class CleanupResult(BaseModel):
    dry_run: bool
    cutoff: datetime = Field(..., description="本次判定实际使用的截止时刻，UTC")
    deleted_ids: list[str] = Field(..., description="将被/已被删除的邮件 ID，升序")
    affected: list[CleanupItem] = Field(
        ..., description="存活但父节点或根节点变化的邮件，按 message_id 升序"
    )
    deleted_count: int
    affected_count: int
    expired_count: int = Field(..., description="有效邮件中已到期（retain_until <= cutoff）的数量")
    protected_count: int = Field(
        ..., description="已到期但被冻结保护而未删除的数量"
    )


TreeNode.model_rebuild()
