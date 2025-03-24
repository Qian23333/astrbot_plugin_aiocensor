import time
import uuid

import aiosqlite

from ..common.types import DBError, SensitiveWordEntry  # type: ignore


class SensitiveWordMixin:
    """敏感词相关功能"""

    _db: aiosqlite.Connection | None

    async def _create_tables(self) -> None:
        """
        创建敏感词表。

        Raises:
            DBError: 数据库未初始化或创建表失败。
        """
        if not self._db:
            raise DBError("数据库未初始化")
        try:
            await self._db.execute("""
            CREATE TABLE IF NOT EXISTS sensitive_words (
                id TEXT PRIMARY KEY,
                word TEXT UNIQUE NOT NULL,
                updated_at INTEGER NOT NULL
            )""")
            await self._db.commit()
        except aiosqlite.Error as e:
            await self._db.rollback()
            raise DBError(f"创建敏感词表失败: {e!s}")

    async def add_sensitive_word(self, word: str) -> str:
        """
        添加一个敏感词。

        Args:
            word: 要添加的敏感词。

        Returns:
            新添加的敏感词的ID。

        Raises:
            DBError: 数据库未初始化或添加敏感词失败。
        """
        if not self._db:
            raise DBError("数据库未初始化或连接已关闭")
        word_id = str(uuid.uuid4())
        current_time = int(time.time())
        try:
            async with self._db.cursor() as cursor:
                await cursor.execute(
                    "INSERT INTO sensitive_words (id, word, updated_at) VALUES (?, ?, ?) ON CONFLICT(word) DO UPDATE SET updated_at = ? RETURNING id",
                    (word_id, word, current_time, current_time),
                )
                result = await cursor.fetchone()
                await self._db.commit()
                return result[0] if result else word_id
        except aiosqlite.Error as e:
            await self._db.rollback()
            raise DBError(f"添加敏感词失败：{e!s}")

    async def get_sensitive_words(
        self, limit: int = 100, offset: int = 0
    ) -> list[SensitiveWordEntry]:
        """
        获取敏感词列表。

        Args:
            limit: 返回的最大敏感词数，默认为100。
            offset: 偏移量，用于分页，默认为0。

        Returns:
            敏感词列表。

        Raises:
            DBError: 数据库未初始化或获取敏感词失败。
        """
        if not self._db:
            raise DBError("数据库未初始化或连接已关闭")
        try:
            async with self._db.execute(
                "SELECT id, word, updated_at FROM sensitive_words ORDER BY word LIMIT ? OFFSET ?",
                (limit, offset),
            ) as cursor:
                rows = await cursor.fetchall()
            return [
                SensitiveWordEntry(id=row[0], word=row[1], updated_at=row[2])
                for row in rows
            ]
        except aiosqlite.Error as e:
            raise DBError(f"获取敏感词失败：{e!s}")

    async def get_sensitive_words_count(self) -> int:
        """
        获取敏感词的总数。

        Returns:
            敏感词的总数。

        Raises:
            DBError: 数据库未初始化或获取敏感词总数失败。
        """
        if not self._db:
            raise DBError("数据库未初始化或连接已关闭")
        try:
            async with self._db.execute(
                "SELECT COUNT(*) FROM sensitive_words"
            ) as cursor:
                result = await cursor.fetchone()
            return result[0] if result else 0
        except aiosqlite.Error as e:
            raise DBError(f"获取敏感词总数失败：{e!s}")

    async def delete_sensitive_word(self, word_id: str) -> bool:
        """
        删除一个敏感词。

        Args:
            word_id: 要删除的敏感词的ID。

        Returns:
            如果删除成功返回True，否则返回False。

        Raises:
            DBError: 数据库未初始化或删除敏感词失败。
        """
        if not self._db:
            raise DBError("数据库未初始化或连接已关闭")
        try:
            async with self._db.cursor() as cursor:
                await cursor.execute(
                    "DELETE FROM sensitive_words WHERE id = ?", (word_id,)
                )
                deleted = cursor.rowcount > 0
                await self._db.commit()
                return deleted
        except aiosqlite.Error as e:
            await self._db.rollback()
            raise DBError(f"删除敏感词失败：{e!s}")
