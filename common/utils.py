import asyncio
from asyncio.log import logger
import base64
from functools import wraps
from typing import Any, Awaitable, Callable, TypeVar
from aiocqhttp import CQHttp  # type: ignore
import aiohttp

from .types import CensorError

T = TypeVar("T")


def censor_retry(
    max_retries: int = 3,
    base_delay: float = 0.5,
):
    """
    审核重试装饰器。

    用于包装一个异步函数，使其在遇到特定异常时自动重试。

    Args:
        max_retries (int): 最大重试次数，默认为3。
        base_delay (float): 初始重试延迟时间（秒），默认为0.5秒。

    Returns:
        Callable[..., Awaitable[T]]: 包装后的异步函数。

    Raises:
        CensorError: 当达到最大重试次数或发生未知错误时抛出。
    """

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except aiohttp.ClientError:
                    if attempt < max_retries - 1:
                        await asyncio.sleep(base_delay * (2**attempt))
                        continue
                except Exception as e:
                    raise CensorError(f"发生未知错误: {e!s}")

            raise CensorError(f"请求失败，已达到最大重试次数 ({max_retries})")

        return wrapper

    return decorator


def get_image_format(img_b64: str):
    data = base64.b64decode(img_b64)
    if data.startswith(b"\x89\x50\x4e\x47\x0d\x0a\x1a\x0a"):
        return "png"
    elif data.startswith(b"\xff\xd8\xff"):
        return "jpeg"
    elif data.startswith(b"GIF87a") or data.startswith(b"GIF89a"):
        return "gif"
    elif data.startswith(b"BM"):
        return "bmp"
    elif data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "webp"
    elif data.startswith(b"\x00\x00\x01\x00"):
        return "ico"
    elif data.startswith(b"icns"):
        return "icns"
    elif (
        data.startswith(b"\x49\x49\x2a\x00")
        or data.startswith(b"\x4d\x4d\x00\x2a")
        or data.startswith(b"\x49\x49\x2b\x00")
        or data.startswith(b"\x4d\x4d\x00\x2b")
    ):
        return "tiff"
    elif data.startswith(b"\x00\x00\x00\x0c\x6a\x50\x20\x20\x0d\x0a\x87\x0a"):
        return "jp2"
    else:
        return None


# 本段代码来自于https://raw.githubusercontent.com/zouyonghe/astrbot_plugin_anti_porn/refs/heads/master/main.py
async def admin_check(
    sender_id: int, group_id: int, bot_id: int, client: CQHttp
) -> bool:
    """检查当前 bot 是否是群管理员或群主并且消息发送者不是管理员或群主"""
    try:
        bot_info = await client.get_group_member_info(
            group_id=group_id,
            user_id=bot_id,
            no_cache=True,
            self_id=int(bot_id),
        )
        sender_info = await client.get_group_member_info(
            group_id=group_id,
            user_id=sender_id,
            no_cache=True,
            self_id=int(bot_id),
        )
        return bot_info.get("role") in ["admin", "owner"] and sender_info.get(
            "role"
        ) not in ["admin", "owner"]
    except Exception as e:
        logger.error(f"获取群成员信息失败: {e}")
        return False


# 本段代码修改自https://raw.githubusercontent.com/zouyonghe/astrbot_plugin_anti_porn/refs/heads/master/main.py
async def dispose_msg(
    message_id: int, group_id: int, user_id: int, duration: int, self_id: int, client: CQHttp
):
    """删除消息并禁言用户

    Args:
        message_id: 消息ID
        group_id: 群组ID
        user_id: 用户ID
        duraion: 禁言时长
        self_id: 机器人ID
        client: CQHttp客户端
    """
    try:
        await client.delete_msg(
            message_id=message_id,
            self_id=self_id,
        )

        if duration <= 0:
            return

        await client.set_group_ban(
            group_id=group_id,
            user_id=user_id,
            duration=duration,
            self_id=self_id,
        )
    except Exception as e:
        logger.error(f"发生错误，无法禁言及撤回： {e}")
