from anthill.common.model import Model
from anthill.common.database import DatabaseError
from anthill.common.cluster import Cluster, NoClusterError, ClusterError
from anthill.common.options import options

import logging
import ujson


class LeaderboardAdapter(object):
    def __init__(self, data):
        self.leaderboard_id = data.get("leaderboard_id")


class RecordAdapter(object):
    def __init__(self, data, rank):
        self.account = data.get("account_id")
        self.cluster_id = data.get("cluster_id")
        self.score = data.get("score")
        self.name = data.get("display_name")
        self.profile = data.get("profile", {})
        self.rank = rank

    def dump(self):
        return {
            "rank": self.rank,
            "score": self.score,
            "account": self.account,
            "display_name": self.name,
            "profile": self.profile
        }


class LeaderboardError(Exception):
    def __init__(self, code, message):
        self.code = code
        self.message = message

    def __str__(self):
        return str(self.message) + ": " + self.message


class LeaderboardNotFound(Exception):

    def __init__(self, leaderboard_name):
        self.leaderboard_name = leaderboard_name


class LeaderboardsModel(Model):
    """
    Leaderboard model. Manages leaderboards itself, and user records in such leaderboards.

    Please note that MySQL 5.1 Events feature is used (please see /sql/records_expiration.sql).
    https://dev.mysql.com/doc/refman/5.7/en/event-scheduler.html

    """

    LEADERBOARD_CLUSTERED_TRIGGER = "@"

    @staticmethod
    def is_clustered(leaderboard_name):
        return leaderboard_name.startswith(LeaderboardsModel.LEADERBOARD_CLUSTERED_TRIGGER)

    def __init__(self, db):
        self.db = db
        self.cluster = Cluster(db, "leaderboard_clusters", "leaderboard_cluster_accounts")
        self.cluster_size = options.cluster_size

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["leaderboards", "records", "leaderboard_clusters", "leaderboard_cluster_accounts"]

    def get_setup_events(self):
        return ["records_expiration"]

    def has_delete_account_event(self):
        return True

    async def accounts_deleted(self, gamespace, accounts, gamespace_only):

        if gamespace_only:
            async with self.db.acquire() as db:
                await db.execute("""
                    DELETE 
                    FROM `leaderboard_cluster_accounts`
                    WHERE `gamespace_id`=%s AND `account_id` IN %s;
                """, gamespace, accounts)
                await db.execute("""
                    DELETE 
                    FROM `records`
                    WHERE `gamespace_id`=%s AND `account_id` IN %s;
                """, gamespace, accounts)
        else:
            async with self.db.acquire() as db:
                await db.execute("""
                    DELETE 
                    FROM `leaderboard_cluster_accounts`
                    WHERE `account_id` IN %s;
                """, accounts)
                await db.execute("""
                    DELETE 
                    FROM `records`
                    WHERE `account_id` IN %s;
                """, accounts)

    async def delete_entry(self, leaderboard_name, gamespace_id, account_id, sort_order):
        async with self.db.acquire() as db:

            leaderboard = await self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            await db.execute(
                """
                    DELETE FROM `records`
                    WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                """, leaderboard.leaderboard_id, account_id, gamespace_id)

            if LeaderboardsModel.is_clustered(leaderboard_name):
                try:
                    await self.cluster.leave_cluster(gamespace_id, account_id, leaderboard.leaderboard_id)
                except ClusterError as e:
                    raise LeaderboardError(500, e.message)

    async def delete_leaderboard(self, leaderboard_id, gamespace_id):

        async with self.db.acquire() as db:
            await db.execute(
                """
                    DELETE FROM `records`
                    WHERE `leaderboard_id` = %s AND `gamespace_id` = %s;
                """, leaderboard_id, gamespace_id)

            await db.execute(
                """
                    DELETE FROM `leaderboards`
                    WHERE `leaderboard_id` = %s AND `gamespace_id` = %s;
                """, leaderboard_id, gamespace_id)

            await self.cluster.delete_clusters_db(
                gamespace_id, leaderboard_id, db=db)

    async def find_leaderboard(self, gamespace_id, leaderboard_name, sort_order, db=None):

        leaderboard = await (db or self.db).get(
            """
                SELECT `leaderboard_id`, `leaderboard_name`
                FROM `leaderboards`
                WHERE `leaderboard_name` = %s AND `gamespace_id` = %s AND `leaderboard_sort_order` = %s
                LIMIT 1;
            """, leaderboard_name, gamespace_id, sort_order)

        if leaderboard is None:
            raise LeaderboardNotFound(leaderboard_name)

        return LeaderboardAdapter(leaderboard)

    async def list_around_me_records(self, user_id, leaderboard_name, gamespace_id, sort_order, offset, limit):

        limit = int(limit)
        async with self.db.acquire() as db:
            brd = await self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            user_score = await db.get(
                """
                    SELECT `score`
                    FROM `records`
                    WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                """, brd.leaderboard_id, user_id, gamespace_id)

            if not user_score:
                return None

            user_score = user_score["score"]

            records = await db.query(
                """
                    (SELECT `account_id`, `display_name`, `score`, `profile`
                        FROM `records`
                        WHERE `leaderboard_id`=%s AND `gamespace_id`=%s AND `score`<%s
                        ORDER BY `score` DESC
                        LIMIT %s)
                    UNION
                    (SELECT `account_id`, `display_name`, `score`, `profile`
                        FROM `records`
                        WHERE `leaderboard_id`=%s AND `gamespace_id`=%s AND `score` >= %s
                        ORDER BY `score` ASC
                        LIMIT %s)
                    ORDER BY `score` {0} LIMIT %s, %s;
                """.format(sort_order.upper()),

                brd.leaderboard_id,
                gamespace_id,
                user_score,
                limit / 2,

                brd.leaderboard_id,
                gamespace_id,
                user_score,
                limit / 2,

                offset,
                limit)

            return list(map(RecordAdapter, records))

    async def list_friends_records(self, friends_ids, leaderboard_name, gamespace_id, sort_order, offset, limit):

        async with self.db.acquire() as db:
            leaderboard = await self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            records = await db.query(
                """
                    SELECT `account_id`, `display_name`, `score`, `profile`
                    FROM `records`
                    WHERE `leaderboard_id`=%s AND `gamespace_id`=%s AND `account_id` IN %s
                    ORDER BY `score` {0}
                    LIMIT %s, %s;
                """.format(sort_order.upper()),
                leaderboard.leaderboard_id, gamespace_id, friends_ids, offset, limit)

            return list(map(RecordAdapter, records))

    # noinspection PyBroadException
    async def list_top_all_clusters(self, leaderboard_name, gamespace_id, sort_order):

        async with self.db.acquire() as db:
            leaderboard = await self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            if not LeaderboardsModel.is_clustered(leaderboard_name):
                data = await self.__list_top_records_cluster__(
                    leaderboard.leaderboard_id, gamespace_id, 0, sort_order, 0, 1000)

                return {
                    0: data
                }

            cluster_ids = await self.cluster.list_clusters(
                gamespace_id, leaderboard.leaderboard_id, db)

            try:
                data = await self.list_top_records_clusters(
                    leaderboard.leaderboard_id, gamespace_id,
                    cluster_ids, sort_order)
            except Exception:
                logging.exception("Error during requesting top clusters")
                return

            return data

    async def __list_top_records_cluster__(self, leaderboard_id, gamespace_id, cluster_id, sort_order, offset, limit):
        async with self.db.acquire() as db:
            try:
                records = await db.query(
                    """
                        SELECT `account_id`, `display_name`, `score`, `profile`
                        FROM `records`
                        WHERE `gamespace_id`=%s AND `leaderboard_id`=%s AND `cluster_id`=%s
                        ORDER BY score {0}
                        LIMIT %s, %s;
                    """.format(sort_order.upper()),
                    gamespace_id, leaderboard_id, cluster_id, int(offset), int(limit))
            except DatabaseError as e:
                raise LeaderboardError(500, "Failed to get top records: " + e.args[1])
            else:

                result = [
                    RecordAdapter(data, index)
                    for index, data in enumerate(records, start=1)
                ]

                return result

    async def list_top_records_clusters(self, leaderboard_id, gamespace_id, cluster_ids, sort_order):

        if not cluster_ids:
            raise LeaderboardError(400, "Empty cluster_ids")

        async with self.db.acquire() as db:
            try:
                records = await db.query(
                    """
                        SELECT `account_id`, `display_name`, `score`, `profile`, `cluster_id`
                        FROM `records`
                        WHERE `gamespace_id`=%s AND `leaderboard_id`=%s AND `cluster_id` IN %s
                        ORDER BY `score` {0};
                    """.format(sort_order.upper()),
                    gamespace_id, leaderboard_id, cluster_ids)
            except DatabaseError as e:
                raise LeaderboardError(500, "Failed to get top records: " + e.args[1])
            else:

                result = {}

                for record in records:
                    cluster_id = record["cluster_id"]

                    try:
                        existing = result[cluster_id]
                        existing.append(RecordAdapter(record, len(existing) + 1))
                    except KeyError:
                        result[cluster_id] = [RecordAdapter(record, 1)]

                return result

    async def list_top_records_account(self, leaderboard_name, gamespace_id,
                                       account_id, sort_order, offset=0, limit=1000):
        async with self.db.acquire() as db:

            leaderboard = await self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            if LeaderboardsModel.is_clustered(leaderboard_name):
                try:
                    cluster_id = await self.cluster.get_cluster(
                        gamespace_id, account_id, leaderboard.leaderboard_id,
                        cluster_size=self.cluster_size, auto_create=False)
                except NoClusterError:
                    raise LeaderboardNotFound(leaderboard_name)
            else:
                cluster_id = 0

            result = await self.__list_top_records_cluster__(
                leaderboard.leaderboard_id, gamespace_id, cluster_id,
                sort_order, offset, limit)

            return result

    async def list_top_records(self, leaderboard_name, gamespace_id, sort_order, offset=0, limit=1000):
        async with self.db.acquire() as db:

            leaderboard = await self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            if LeaderboardsModel.is_clustered(leaderboard_name):
                raise LeaderboardNotFound(leaderboard_name)
            else:
                cluster_id = 0

            result = await self.__list_top_records_cluster__(
                leaderboard.leaderboard_id, gamespace_id, cluster_id,
                sort_order, offset, limit)

            return result

    async def insert_record(self, gamespace_id, leaderboard_id, account_id,
                            time_to_live, profile, score, display_name, cluster_id=0, db=None):

        result = await (db or self.db).insert(
            """
                INSERT INTO `records`
                (`account_id`, `leaderboard_id`, `gamespace_id`, `expire_at`,
                `profile`, `score`, `display_name`, `cluster_id`)
                VALUES (%s, %s, %s, NOW() + INTERVAL %s SECOND, %s, %s, %s, %s);
            """,
            account_id, leaderboard_id, gamespace_id, time_to_live,
            ujson.dumps(profile), score, display_name, cluster_id)

        return result

    async def add_entry(self, gamespace_id, leaderboard_name, sort_order, account_id,
                        display_name, score, time_to_live, profile):

        clustered = LeaderboardsModel.is_clustered(leaderboard_name)

        async with self.db.acquire() as db:
            try:

                try:
                    leaderboard = await self.find_leaderboard(
                        gamespace_id, leaderboard_name,
                        sort_order, db=db)

                except LeaderboardNotFound:
                    leaderboard_id = await db.insert(
                        """
                            INSERT INTO `leaderboards`
                            (`leaderboard_name`, `gamespace_id`, `leaderboard_sort_order`)
                            VALUES (%s, %s, %s);
                        """, leaderboard_name, gamespace_id, sort_order)

                    if clustered:
                        cluster_id = await self.cluster.get_cluster(
                            gamespace_id, account_id, leaderboard_id,
                            cluster_size=self.cluster_size, auto_create=True)
                    else:
                        cluster_id = 0

                    await self.insert_record(
                        gamespace_id, leaderboard_id, account_id,
                        time_to_live, profile, score, display_name,
                        cluster_id=cluster_id, db=db)
                else:

                    if clustered:
                        cluster_id = await self.cluster.get_cluster(
                            gamespace_id, account_id, leaderboard.leaderboard_id,
                            cluster_size=self.cluster_size, auto_create=True)
                    else:
                        cluster_id = 0

                    record = await db.get(
                        """
                            SELECT *
                            FROM `records`
                            WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s AND `cluster_id`=%s;
                        """,
                        leaderboard.leaderboard_id,
                        account_id,
                        gamespace_id,
                        cluster_id
                    )

                    if not record:
                        await self.insert_record(
                            gamespace_id, leaderboard.leaderboard_id, account_id,
                            time_to_live, profile, score, display_name,
                            cluster_id=cluster_id, db=db
                        )
                    else:
                        await db.execute(
                            """
                                UPDATE `records`
                                SET `expire_at`=NOW() + INTERVAL %s SECOND, `profile`=%s,
                                    `score`=%s, `display_name`=%s
                                WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s AND `cluster_id`=%s;
                            """,
                            time_to_live, ujson.dumps(profile),
                            score, display_name, leaderboard.leaderboard_id, account_id, gamespace_id, cluster_id)
            except DatabaseError as e:
                raise LeaderboardError(500, "Failed add entry: " + e.args[1])
        return "OK"
