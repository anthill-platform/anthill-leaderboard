
import ujson

from tornado.gen import coroutine, Return

from common.database import DatabaseError
from common.model import Model


class LeaderboardNotFound(Exception):

    def __init__(self, leaderboard_name):
        self.leaderboard_name = leaderboard_name


class LeaderboardsModel(Model):

    def __init__(self, db):
        self.db = db

    def get_setup_db(self):
        return self.db

    def get_setup_tables(self):
        return ["leaderboards", "records"]

    @coroutine
    def delete_entry(self, leaderboard_id, gamespace_id, user_id, sort_order):
        with (yield self.db.acquire()) as db:

            brd_id = yield self.find_leaderboard(
                leaderboard_id,
                gamespace_id,
                sort_order,
                db)

            yield db.execute(
                """
                    DELETE FROM `records`
                    WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                """, brd_id, user_id, gamespace_id)

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

    @coroutine
    def find_leaderboard(self, leaderboard_name, gamespace_id, sort_order, db=None):

        brd_id = yield (db or self.db).get(
            """
                SELECT `leaderboard_id`
                FROM `leaderboards`
                WHERE `leaderboard_name` = %s AND `gamespace_id` = %s AND `leaderboard_sort_order` = %s;
            """, leaderboard_name, gamespace_id, sort_order)

        if brd_id is None:
            raise LeaderboardNotFound(leaderboard_name)

        raise Return(brd_id["leaderboard_id"])

    @coroutine
    def list_around_me_records(self, user_id, leaderboard_name, gamespace_id, sort_order, offset, limit):

        limit = int(limit)
        with (yield self.db.acquire()) as db:
            brd_id = yield self.find_leaderboard(
                leaderboard_name,
                gamespace_id,
                sort_order,
                db)

            user_score = yield db.get(
                """
                    SELECT `score`
                    FROM `records`
                    WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                """, brd_id, user_id, gamespace_id)

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

                brd_id,
                gamespace_id,
                user_score,
                limit / 2,

                brd_id,
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
            brd_id = yield self.find_leaderboard(
                leaderboard_name,
                gamespace_id,
                sort_order,
                db)

            records = yield db.query(
                """
                    SELECT `account_id` AS user, `display_name`, `score`, `profile`
                    FROM `records`
                    WHERE `leaderboard_id`=%s AND `gamespace_id`=%s AND `account_id` IN %s
                    ORDER BY `score` {0}
                    LIMIT %s, %s;
                """.format(sort_order.upper()),
                brd_id, gamespace_id, friends_ids, offset, limit)

            raise Return({
                "entries": len(records),
                "data": LeaderboardsModel.render_records(records)
            })

    @coroutine
    def list_top_records(self, leaderboard_name, gamespace_id, sort_order, offset, limit):
        with (yield self.db.acquire()) as db:
            brd_id = yield self.find_leaderboard(leaderboard_name, gamespace_id, sort_order, db)

            records = yield db.query(
                """
                    SELECT `account_id` AS `user`, `display_name`, `score`, `profile`
                    FROM `records`
                    WHERE `leaderboard_id`=%s
                    ORDER BY score {0}
                    LIMIT %s, %s;
                """.format(sort_order.upper()), brd_id, offset, int(limit))

            raise Return({
                "entries": len(records),
                "data": LeaderboardsModel.render_records(records)
            })

    @coroutine
    def insert_record(self, account_id, leaderboard_id, gamespace_id,
                      time_to_live, profile, score, display_name, db=None):

        result = yield (db or self.db).insert(
            """
                INSERT INTO `records`
                (`account_id`, `leaderboard_id`, `gamespace_id`, `published_at`, `time_to_live`,
                `profile`, `score`, `display_name`)
                VALUES (%s, %s, %s, NOW(), %s, %s, %s, %s);
            """,
            account_id, leaderboard_id, gamespace_id, time_to_live,
            ujson.dumps(profile), score, display_name)

        raise Return(result)

    @staticmethod
    def render_records(leaderboard_data):
        return [
            {
                "account": record["user"],
                "score": record["score"],
                "display_name": record["display_name"],
                "profile": record["profile"] if isinstance(record["profile"], dict) else ujson.loads(record["profile"])
            }
            for record in leaderboard_data
        ]

    @coroutine
    def set_top_entries(self, leaderboard_name, gamespace_id, user_id, display_name,
                        sort_order, score, time_to_live, profile):
        with (yield self.db.acquire()) as db:

            try:
                brd_id = yield self.find_leaderboard(
                    leaderboard_name,
                    gamespace_id,
                    sort_order,
                    db)

            except LeaderboardNotFound:
                leaderboard_name = yield db.insert(
                    """
                        INSERT INTO `leaderboards`
                        (`leaderboard_name`, `gamespace_id`, `leaderboard_sort_order`)
                        VALUES (%s, %s, %s);
                    """, leaderboard_name, gamespace_id, sort_order)

                yield self.insert_record(
                    user_id,
                    leaderboard_name,
                    gamespace_id,
                    time_to_live,
                    profile,
                    score,
                    display_name,
                    db
                )
            else:
                record = yield db.get(
                    """
                        SELECT *
                        FROM `records`
                        WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                    """,
                    brd_id,
                    user_id,
                    gamespace_id
                )

                if not record:
                    yield self.insert_record(
                        user_id,
                        brd_id,
                        gamespace_id,
                        time_to_live,
                        profile,
                        score,
                        display_name, db
                    )
                else:
                    yield db.execute(
                        """
                            UPDATE `records`
                            SET `published_at`=NOW(), `time_to_live`=%s, `profile`=%s,
                                `score`=%s, `display_name`=%s
                            WHERE `leaderboard_id`=%s AND `account_id`=%s AND `gamespace_id`=%s;
                        """,
                        time_to_live, ujson.dumps(profile), score, display_name, brd_id, user_id, gamespace_id)

        raise Return("OK")
