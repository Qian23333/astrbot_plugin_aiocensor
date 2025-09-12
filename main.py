import os
import secrets
import time
from multiprocessing import Process

from apscheduler.schedulers.asyncio import AsyncIOScheduler  # type:ignore

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Image, Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.message.components import BaseMessageComponent
from astrbot.core.provider.entites import LLMResponse
from astrbot.core.star.filter.event_message_type import EventMessageType

from .censor_flow import CensorFlow  # type:ignore
from .common import CensorResult, RiskLevel, admin_check, dispose_msg  # type:ignore
from .db import DBManager  # type:ignore
from .webui import run_server  # type:ignore


@register(
    "astrbot_plugin_aiocensor", "Raven95676", "Astrbot综合内容安全+群管插件", "v0.1.3"
)
class AIOCensor(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.web_ui_process: Process | None = None
        self.scheduler: AsyncIOScheduler | None = None

        # 初始化内容审查流
        self.censor_flow = CensorFlow(config)
        data_path = os.path.join(os.getcwd(), "data", "aiocensor")
        os.makedirs(data_path, exist_ok=True)
        self.db_mgr = DBManager(os.path.join(data_path, "censor.db"))

        # 存储 (group_id_str, user_id_str) -> expiry_ts
        self.new_member_watchlist: dict[tuple[str, str], int] = {}

    async def initialize(self):
        logger.debug("初始化 AIOCensor 组件")
        # 生成 Web UI 密钥（如果未设置）
        if not self.config["webui"].get("secret"):
            self.config["webui"]["secret"] = secrets.token_urlsafe(32)
            self.config.save_config()

        # 初始化数据库和审查器
        self.db_mgr.initialize()

        await self._update_censors()

        # 设置定时任务，每 5 分钟更新审查器数据
        self.scheduler = AsyncIOScheduler(timezone="Asia/Shanghai")
        self.scheduler.add_job(
            self._update_censors,
            "interval",
            minutes=5,
            id="update_censors",
            misfire_grace_time=60,
        )
        # 设置定时任务，每 5 分钟清理过期的新成员监听条目
        self.scheduler.add_job(
            self._cleanup_watchlist,
            "interval",
            minutes=5,
            id="cleanup_watchlist",
            misfire_grace_time=60,
        )
        self.scheduler.start()

        # 启动 Web UI 服务
        self.web_ui_process = Process(
            target=run_server,
            args=(
                self.config["webui"]["secret"],
                self.config["webui"]["password"],
                self.config["webui"].get("host", "0.0.0.0"),
                self.config["webui"].get("port", 9966),
            ),
            daemon=True,
        )
        self.web_ui_process.start()

    async def _update_censors(self):
        """定期更新审查器数据"""
        try:
            black_list = self.db_mgr.get_blacklist_entries()
            await self.censor_flow.userid_censor.build(
                {entry.identifier for entry in black_list}
            )
            if hasattr(self.censor_flow.text_censor, "build"):
                sensitive_words = self.db_mgr.get_sensitive_words()
                await self.censor_flow.text_censor.build(
                    {entry.word for entry in sensitive_words}
                )
            logger.debug("审查器数据已更新")
        except Exception as e:
            logger.error(f"更新审查器数据失败: {e!s}")

    async def _cleanup_watchlist(self):
        """定时清理过期的新成员监听条目"""
        now = int(time.time())
        items = list(self.new_member_watchlist.items()) 
        remove_keys = [k for k, ts in items if ts <= now]
        for k in remove_keys:
            self.new_member_watchlist.pop(k, None)

    async def _handle_aiocqhttp_group_message(
        self, event: AstrMessageEvent, res: CensorResult
    ):
        """处理 aiocqhttp 平台的群消息"""
        from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import (
            AiocqhttpMessageEvent,
        )

        if not isinstance(event, AiocqhttpMessageEvent):
            return

        group_id = int(event.get_group_id())
        user_id = int(event.get_sender_id())
        self_id = int(event.get_self_id())
        message_id = int(event.message_obj.message_id)

        res.extra.update(
            {
                "group_id": group_id,
                "user_id": user_id,
                "self_id": self_id,
                "message_id": message_id,
            }
        )

        if (
            res.risk_level == RiskLevel.Block
            and (self.config.get("enable_group_msg_censor") or self.config.get("enable_review_new_members"))
            and await admin_check(user_id, group_id, self_id, event.bot)
        ):
            try:
                await dispose_msg(
                    message_id=message_id,
                    group_id=group_id,
                    user_id=user_id,
                    self_id=self_id,
                    duration=int(self.config.get("mute_duration", 300)),
                    client=event.bot,
                )
            except Exception as e:
                logger.error(f"消息处置失败: {e!s}")

    async def handle_message(
        self, event: AstrMessageEvent, chain: list[BaseMessageComponent]
    ):
        """核心消息内容审查逻辑"""
        try:
            # 遍历消息组件进行审计
            for comp in chain:
                res = None
                if isinstance(comp, Plain):
                    res = await self.censor_flow.submit_text(
                        comp.text, event.unified_msg_origin
                    )
                elif isinstance(comp, Image) and self.config.get("enable_image_censor"):
                    res = await self.censor_flow.submit_image(
                        comp.url, event.unified_msg_origin
                    )
                else:
                    continue

                if res and res.risk_level != RiskLevel.Pass:
                    res.extra = {"user_id_str": event.get_sender_id()}
                    # 只有在启用日志记录时才添加审核日志
                    if self.config.get("enable_audit_log", True):
                        self.db_mgr.add_audit_log(res)

                    if res.risk_level == RiskLevel.Block:
                        if (
                            event.get_platform_name() == "aiocqhttp"
                            and event.get_group_id()
                        ):
                            await self._handle_aiocqhttp_group_message(event, res)
                        else:
                            logger.warning("非 aiocqhttp 平台的群消息，无法自动处置")
                        event.stop_event()
                        break
        except Exception as e:
            logger.error(f"消息审查失败: {e!s}")

    @filter.event_message_type(EventMessageType.ALL)
    async def on_all_message(self, event: AstrMessageEvent):
        """检查黑名单和全输入审查"""
        if self.config.get("enable_blacklist"):
            res = await self.censor_flow.submit_userid(
                event.get_sender_id(), event.unified_msg_origin
            )
            if res.risk_level == RiskLevel.Block:
                if self.config.get("enable_audit_log", True):
                    self.db_mgr.add_audit_log(res)
                event.stop_event()
                return
        if (
            self.config.get("enable_all_input_censor")
            or self.config.get("enable_input_censor")
            and event.is_at_or_wake_command
        ):
            await self.handle_message(event, event.message_obj.message)

    @filter.event_message_type(EventMessageType.ALL)
    async def handle_group_increase_for_review(self, event: AstrMessageEvent):
        """检测 aiocqhttp 的 group_increase 通知并将新成员加入短期审查监听表"""
        if not self.config.get("enable_review_new_members"):
            return
        raw_message = event.message_obj.raw_message
        post_type = raw_message.get("post_type")
        if post_type == "notice" and raw_message.get("notice_type") == "group_increase":
            group_id = str(raw_message.get("group_id", ""))
            user_id = str(raw_message.get("user_id", ""))
            # 群组白名单判断
            group_list = self.config.get("group_list", [])
            if group_list and group_id not in group_list:
                return
            expiry_ts = int(time.time()) + int(self.config.get("review_new_members_duration", 300))
            self.new_member_watchlist[(group_id, user_id)] = expiry_ts
            logger.info(f"已将新成员{user_id}在群{group_id}登记为短期审查，直到{expiry_ts}")

    @filter.event_message_type(EventMessageType.GROUP_MESSAGE)
    async def group_censor(self, event: AstrMessageEvent):
        """群消息审查"""
        group_list = self.config.get("group_list", [])
        group_id = event.get_group_id()
        if group_list and group_id not in group_list:
            return

        # 新成员短期审查：如果发送者在监听表且未过期，则强制审查
        sender_key = (group_id, event.get_sender_id())
        should_run = False
        expiry = self.new_member_watchlist.get(sender_key)
        now_ts = int(time.time())
        if self.config.get("enable_review_new_members") and expiry and expiry > now_ts:
            # 在审查期内
            should_run = True
        elif expiry and expiry <= now_ts:
            # 已过期，清理
            self.new_member_watchlist.pop(sender_key, None)

        # 若既不在短期审查期，也未启用常规群消息审查，则直接返回
        if not should_run and not self.config.get("enable_group_msg_censor"):
            return

        await self.handle_message(event, event.message_obj.message)

    @filter.event_message_type(EventMessageType.PRIVATE_MESSAGE)
    async def private_censor(self, event: AstrMessageEvent):
        """私聊消息审查"""
        if self.config.get("enable_private_msg_censor"):
            await self.handle_message(event, event.message_obj.message)

    @filter.on_llm_response()
    async def output_censor(self, event: AstrMessageEvent, response: LLMResponse):
        """审核模型输出"""
        if self.config.get("enable_output_censor"):
            if not response.result_chain:
                res = await self.censor_flow.submit_text(
                    response.completion_text, event.unified_msg_origin
                )
                if res and res.risk_level != RiskLevel.Pass:
                    res.extra = {"user_id_str": event.get_sender_id()}
                    if self.config.get("enable_audit_log", True):
                        self.db_mgr.add_audit_log(res)
                    if res.risk_level == RiskLevel.Block:
                        if (
                            event.get_platform_name() == "aiocqhttp"
                            and event.get_group_id()
                        ):
                            await self._handle_aiocqhttp_group_message(event, res)
                        else:
                            logger.warning("非 aiocqhttp 平台的群消息，无法自动处置")
                        event.stop_event()
            elif response.result_chain:
                await self.handle_message(event, response.result_chain.chain)

    async def terminate(self):
        logger.debug("开始清理 AIOCensor 资源...")
        try:
            self.db_mgr.close()
            await self.censor_flow.close()
            if self.scheduler:
                self.scheduler.shutdown()
            if self.web_ui_process:
                self.web_ui_process.terminate()
                self.web_ui_process.join(5)
                if self.web_ui_process.is_alive():
                    self.web_ui_process.kill()
                    logger.warning("web_ui_process 未在 5 秒内退出，强制终止")
        except Exception as e:
            logger.error(f"资源清理失败: {e!s}")
