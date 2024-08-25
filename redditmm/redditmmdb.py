import os
import asyncio
import logging
import sqlite3

class RedditMMDB():
    _lock = asyncio.Lock()

    def __init__(self, data_path):
        self.data_path = data_path
        self.filepath = os.path.join(self.data_path, "datadb.sqlite3")
        self.conn = None

    async def init(self):
        async with RedditMMDB._lock:
            self.conn = sqlite3.connect(self.filepath)

        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=wal")
        await self.prepare_seen_urls_table(cur)
        await self.prepare_ignored_redditors(cur)
        await self.prepare_favorites(cur)
        cur.close()

    async def prepare_seen_urls_table(self, cur):
        # check if table exists
        cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='seen_urls'")

        #if the table does not exist, create it
        if cur.fetchone()[0] == 0:
            async with RedditMMDB._lock:
                # create seen urls table to store urls of posts we've already seen
                cur.execute("CREATE TABLE seen_urls (id INTEGER PRIMARY KEY AUTOINCREMENT, guildID INTEGER, url TEXT, seentime DATETIME DEFAULT CURRENT_TIMESTAMP, CONSTRAINT UC_GuildURL UNIQUE (guildID, url))")
                # create multi-index on it
                cur.execute("CREATE INDEX seen_urls_idx_guildID ON seen_urls(guildID, url)")

                self.conn.commit()

    async def prepare_ignored_redditors(self, cur):
        # check if table exists
        cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='ignored_redditors'")

        #if the table does not exist, create it
        if cur.fetchone()[0] == 0:
            async with RedditMMDB._lock:
                # create seen urls table to store urls of posts we've already seen
                cur.execute("CREATE TABLE ignored_redditors (id INTEGER PRIMARY KEY AUTOINCREMENT, guildID INTEGER, redditor TEXT, ignoretime DATETIME DEFAULT CURRENT_TIMESTAMP, CONSTRAINT UC_GuildRedditor UNIQUE (guildID, redditor))")
                # create multi-index on it
                cur.execute("CREATE INDEX ignored_redditors_idx ON ignored_redditors(guildID, redditor)")

                self.conn.commit()

    async def prepare_favorites(self, cur):
        # check if table exists
        cur.execute("SELECT count(name) FROM sqlite_master WHERE type='table' AND name='favorites'")

        #if the table does not exist, create it
        if cur.fetchone()[0] == 0:
            async with RedditMMDB._lock:
                cur.execute("CREATE TABLE favorites (id INTEGER PRIMARY KEY AUTOINCREMENT, guildID INTEGER, userID INTEGER, redditor TEXT, url TEXT, favtime DATETIME DEFAULT CURRENT_TIMESTAMP, CONSTRAINT UC_GuildRedditorURLUser UNIQUE (guildID, redditor, url, userID))")
                # create multi-index on it
                cur.execute("CREATE INDEX favorites_idx ON favorites(guildID, redditor, url, userID)")

                self.conn.commit()

    #
    # SEEN URLS
    #

    async def get_seen_url(self, guildID, url):
        cur = self.conn.cursor()
        res = cur.execute(f"SELECT id FROM seen_urls WHERE guildID = {guildID} AND url = '{url}'")
        rt = res.fetchone()
        rowid = None
        if rt is not None:
            rowid = rt[0]
        cur.close()
        return rowid

    async def add_seen_url(self, guildID, url):
        cur = self.conn.cursor()
        cnt = None
        async with RedditMMDB._lock:
            cur.execute(f"INSERT INTO seen_urls (guildID, url) VALUES ({guildID}, '{url}')")
            cnt = cur.rowcount
            self.conn.commit()

        cur.close()
        return cnt

    #
    # IGNORED REDDITORS
    #

    # return row id or None
    async def get_ignored_redditor(self, guildID, redditor):
        cur = self.conn.cursor()
        res = cur.execute(f"SELECT id FROM ignored_redditors WHERE guildID = {guildID} AND redditor = '{redditor}'")
        rt = res.fetchone()
        rowid = None
        if rt is not None:
            rowid = rt[0]
        cur.close()
        return rowid

    # return list of redditor names
    async def get_all_ignored_redditors(self, guildID):
        cur = self.conn.cursor()
        res = cur.execute(f"SELECT redditor FROM ignored_redditors WHERE guildID = {guildID}")
        rt = res.fetchall()
        if rt is None:
            cur.close()
            return None

        cur.close()
        return rt

    async def add_ignored_redditor(self, guildID, redditor):
        cur = self.conn.cursor()
        cnt = None
        async with RedditMMDB._lock:
            cur.execute(f"INSERT INTO ignored_redditors (guildID, redditor) VALUES ({guildID}, '{redditor}')")
            cnt = cur.rowcount
            self.conn.commit()

        cur.close()
        return cnt

    async def del_ignored_redditor(self, guildID, redditor):
        cur = self.conn.cursor()
        cnt = None
        async with RedditMMDB._lock:
            res = cur.execute(f"DELETE FROM ignored_redditors WHERE guildID = {guildID} AND redditor = '{redditor}'")
            cnt = cur.rowcount
            self.conn.commit()
            if cnt == 0:
                cnt = None

        cur.close()
        return cnt

    #
    # FAVORITES
    #

    async def get_favorite(self, guildID, redditor, url=None, userID=None):
        cur = self.conn.cursor()
        query = f"SELECT count(id) FROM favorites WHERE guildID = {guildID} AND redditor = '{redditor}'"
        if url is not None:
            query += " AND url = '{url}'"
        if userID is not None:
            query += " AND userID = {userID}"

        res = cur.execute(query)
        rt = res.fetchone()
        cnt = None
        if rt is not None:
            cnt = rt[0]
            if cnt == 0:
                cnt = None
        cur.close()
        return cnt

    async def add_favorite(self, guildID, redditor, url, userID):
        cur = self.conn.cursor()
        cnt = None
        async with RedditMMDB._lock:
            cur.execute(f"INSERT INTO favorites (guildID, redditor, url, userID) VALUES ({guildID}, '{redditor}', '{url}', {userID})")
            cnt = cur.rowcount
            self.conn.commit()

        cur.close()
        return cnt

    async def del_favorite(self, guildID, redditor, url=None, userID=None):
        cur = self.conn.cursor()
        cnt = None
        async with RedditMMDB._lock:
            query = f"DELETE FROM favorites WHERE guildID = {guildID} AND redditor = '{redditor}'"
            if url is not None:
                query += " AND url = '{url}'"
            if userID is not None:
                query += " AND userID = {userID}"

            res = cur.execute(query)
            cnt = cur.rowcount
            self.conn.commit()

            if cnt == 0:
                cnt = None

        cur.close()
        return cnt
