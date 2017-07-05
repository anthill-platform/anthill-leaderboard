
import ujson

from tornado.gen import coroutine, Return

from common.model import Model
from common.database import DatabaseError
from common.cluster import Cluster, NoClusterError, ClusterError
from common.options import options

import logging


class LeaderboardAdapter(object):
    def __init__(self, data):
        self.leaderboard_id = data.get("leaderboard_id")


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

    @coroutine
    def delete_entry(self, leaderboard_name, gamespace_id, account_id, sort_order):
        with (yield self.db.acquire()) as db:

            leaderboard = yield self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            yield db.execute(
                """
                    DELETE FROM `records`
                    WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                """, leaderboard.leaderboard_id, account_id, gamespace_id)

            if LeaderboardsModel.is_clustered(leaderboard_name):
                try:
                    yield self.cluster.leave_cluster(gamespace_id, account_id, leaderboard.leaderboard_id)
                except ClusterError as e:
                    raise LeaderboardError(500, e.message)

    @coroutine
    def delete_leaderboard(self, leaderboard_id, gamespace_id):

        with (yield self.db.acquire()) as db:
            yield db.execute(
                """
                    DELETE FROM `records`
                    WHERE `leaderboard_id` = %s AND `gamespace_id` = %s;
                """, leaderboard_id, gamespace_id)

            yield db.execute(
                """
                    DELETE FROM `leaderboards`
                    WHERE `leaderboard_id` = %s AND `gamespace_id` = %s;
                """, leaderboard_id, gamespace_id)

            yield self.cluster.delete_clusters_db(
                gamespace_id, leaderboard_id, db=db)

    @coroutine
    def find_leaderboard(self, gamespace_id, leaderboard_name, sort_order, db=None):

        leaderboard = yield (db or self.db).get(
            """
                SELECT `leaderboard_id`, `leaderboard_name`
                FROM `leaderboards`
                WHERE `leaderboard_name` = %s AND `gamespace_id` = %s AND `leaderboard_sort_order` = %s
                LIMIT 1;
            """, leaderboard_name, gamespace_id, sort_order)

        if leaderboard is None:
            raise LeaderboardNotFound(leaderboard_name)

        raise Return(LeaderboardAdapter(leaderboard))

    @coroutine
    def list_around_me_records(self, user_id, leaderboard_name, gamespace_id, sort_order, offset, limit):

        limit = int(limit)
        with (yield self.db.acquire()) as db:
            brd = yield self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            user_score = yield db.get(
                """
                    SELECT `score`
                    FROM `records`
                    WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                """, brd.leaderboard_id, user_id, gamespace_id)

            if not user_score:
                raise Return(None)

            user_score = user_score["score"]

            records = yield db.query(
                """
                    (SELECT `account_id` AS user, `display_name`, `score`, `profile`
                        FROM `records`
                        WHERE `leaderboard_id`=%s AND `gamespace_id`=%s AND `score`<%s
                        ORDER BY `score` DESC
                        LIMIT %s)
                    UNION
                    (SELECT `account_id` AS user, `display_name`, `score`, `profile`
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

            raise Return({
                "entries": len(records),
                "data": LeaderboardsModel.render_records(records)
            })

    @coroutine
    def list_friends_records(self, friends_ids, leaderboard_name, gamespace_id, sort_order, offset, limit):

        with (yield self.db.acquire()) as db:
            brd = yield self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            records = yield db.query(
                """
                    SELECT `account_id` AS user, `display_name`, `score`, `profile`
                    FROM `records`
                    WHERE `leaderboard_id`=%s AND `gamespace_id`=%s AND `account_id` IN %s
                    ORDER BY `score` {0}
                    LIMIT %s, %s;
                """.format(sort_order.upper()),
                brd.leaderboard_id, gamespace_id, friends_ids, offset, limit)

            raise Return({
                "entries": len(records),
                "data": LeaderboardsModel.render_records(records)
            })

    # noinspection PyBroadException
    @coroutine
    def list_top_all_clusters(self, leaderboard_name, gamespace_id, sort_order, limit):

        with (yield self.db.acquire()) as db:
            leaderboard = yield self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            if not LeaderboardsModel.is_clustered(leaderboard_name):
                data = yield self.list_top_records_cluster(
                    leaderboard.leaderboard_id, gamespace_id, 0, sort_order, 0, limit)

                raise Return([data])

            clusters = yield self.cluster.list_clusters(
                gamespace_id, leaderboard.leaderboard_id, db)

            clusters_data = []

            for cluster_id in clusters:
                try:
                    data = yield self.list_top_records_cluster(
                        leaderboard.leaderboard_id, gamespace_id,
                        cluster_id, sort_order, 0, limit)
                except Exception:
                    logging.exception("Error during requesting top clusters")
                else:
                    clusters_data.append(data)

            raise Return(clusters_data)

    @coroutine
    def list_top_records_cluster(self, leaderboard_id, gamespace_id, cluster_id, sort_order, offset, limit):
        with (yield self.db.acquire()) as db:
            try:
                records = yield db.query(
                    """
                        SELECT `account_id` AS `user`, `display_name`, `score`, `profile`
                        FROM `records`
                        WHERE `gamespace_id`=%s AND `leaderboard_id`=%s AND `cluster_id`=%s
                        ORDER BY score {0}
                        LIMIT %s, %s;
                    """.format(sort_order.upper()),
                    gamespace_id, leaderboard_id, cluster_id, int(offset), int(limit))
            except DatabaseError as e:
                raise LeaderboardError(500, "Failed to get top records: " + e.args[1])
            else:
                raise Return({
                    "entries": len(records),
                    "data": LeaderboardsModel.render_records(records)
                })

    @coroutine
    def list_top_records(self, leaderboard_name, gamespace_id, account_id, sort_order, offset, limit):
        with (yield self.db.acquire()) as db:

            leaderboard = yield self.find_leaderboard(
                gamespace_id, leaderboard_name,
                sort_order, db=db)

            if LeaderboardsModel.is_clustered(leaderboard_name):
                try:
                    cluster_id = yield self.cluster.get_cluster(
                        gamespace_id, account_id, leaderboard.leaderboard_id,
                        cluster_size=self.cluster_size, auto_create=False)
                except NoClusterError:
                    raise LeaderboardNotFound(leaderboard_name)
            else:
                cluster_id = 0

            result = yield self.list_top_records_cluster(
                leaderboard.leaderboard_id, gamespace_id, cluster_id,
                sort_order, offset, limit)

            raise Return(result)

    @coroutine
    def insert_record(self, gamespace_id, leaderboard_id, account_id,
                      time_to_live, profile, score, display_name, cluster_id=0, db=None):

        result = yield (db or self.db).insert(
            """
                INSERT INTO `records`
                (`account_id`, `leaderboard_id`, `gamespace_id`, `expire_at`,
                `profile`, `score`, `display_name`, `cluster_id`)
                VALUES (%s, %s, %s, NOW() + INTERVAL %s SECOND, %s, %s, %s, %s);
            """,
            account_id, leaderboard_id, gamespace_id, time_to_live,
            ujson.dumps(profile), score, display_name, cluster_id)

        raise Return(result)

    @staticmethod
    def render_records(leaderboard_data):
        return [
            {
                "account": record["user"],
                "rank": rank,
                "score": record["score"],
                "display_name": record["display_name"],
                "profile": record["profile"] if isinstance(record["profile"], dict) else ujson.loads(record["profile"])
            }
            for rank, record in enumerate(leaderboard_data, start=1)
        ]

    @coroutine
    def add_entry(self, gamespace_id, leaderboard_name, sort_order, account_id,
                  display_name, score, time_to_live, profile):

        clustered = LeaderboardsModel.is_clustered(leaderboard_name)

        with (yield self.db.acquire()) as db:
            try:

                try:
                    leaderboard = yield self.find_leaderboard(
                        gamespace_id, leaderboard_name,
                        sort_order, db=db)

                except LeaderboardNotFound:
                    leaderboard_id = yield db.insert(
                        """
                            INSERT INTO `leaderboards`
                            (`leaderboard_name`, `gamespace_id`, `leaderboard_sort_order`)
                            VALUES (%s, %s, %s);
                        """, leaderboard_name, gamespace_id, sort_order)

                    if clustered:
                        cluster_id = yield self.cluster.get_cluster(
                            gamespace_id, account_id, leaderboard_id,
                            cluster_size=self.cluster_size, auto_create=True)
                    else:
                        cluster_id = 0

                    yield self.insert_record(
                        gamespace_id, leaderboard_id, account_id,
                        time_to_live, profile, score, display_name,
                        cluster_id=cluster_id, db=db)
                else:

                    if clustered:
                        cluster_id = yield self.cluster.get_cluster(
                            gamespace_id, account_id, leaderboard.leaderboard_id,
                            cluster_size=self.cluster_size, auto_create=True)
                    else:
                        cluster_id = 0

                    record = yield db.get(
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
                        yield self.insert_record(
                            gamespace_id, leaderboard.leaderboard_id, account_id,
                            time_to_live, profile, score, display_name,
                            cluster_id=cluster_id, db=db
                        )
                    else:
                        yield db.execute(
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
        raise Return("OK")
