
import ujson

from tornado.gen import coroutine, Return
from tornado.web import HTTPError

from common.access import scoped, AccessToken
from common.handler import AuthenticatedHandler

from model.leaderboard import LeaderboardNotFound


class InternalHandler(object):
    def __init__(self, application):
        self.application = application

    @coroutine
    def delete(self, gamespace, sort_order, leaderboard_name):

        leaderboards = self.application.leaderboards

        try:
            leaderboard_id = yield leaderboards.find_leaderboard(
                leaderboard_name,
                gamespace,
                sort_order)

        except LeaderboardNotFound:
            pass

        else:
            yield leaderboards.delete_leaderboard(
                leaderboard_id,
                gamespace)

        raise Return("OK")
    
    @coroutine
    def get_leaderboard(self, name, order, gamespace, **other):
        try:
            leaderboards = self.application.leaderboards

            offset = other.get("offset", 0)
            limit = other.get("limit", self.application.limit)

            leaderboard_records = yield leaderboards.list_top_records(
                name, gamespace, order,
                offset, limit)

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % name)

        else:
            raise Return(leaderboard_records)

    @coroutine
    def post(self, account, gamespace, sort_order, leaderboard_name, score, display_name, expire_in, profile):

        leaderboards = self.application.leaderboards

        response = yield leaderboards.set_top_entries(
            leaderboard_name, gamespace, account,
            display_name, sort_order, score, expire_in, profile)

        raise Return(response)


class LeaderboardAroundMeHandler(AuthenticatedHandler):
    @coroutine
    @scoped()
    def get(self, sort_order, leaderboard_id):
        try:
            leaderboards = self.application.leaderboards

            offset = self.get_argument("offset", 0)
            limit = self.get_argument("limit", self.application.limit)

            account_id = self.current_user.token.account
            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            leaderboard_records = yield leaderboards.list_around_me_records(
                account_id,
                leaderboard_id,
                gamespace_id,
                sort_order,
                offset,
                limit) or {}

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)

        else:
            self.write(ujson.dumps(leaderboard_records))


class LeaderboardEntryHandler(AuthenticatedHandler):
    @coroutine
    @scoped()
    def delete(self, sort_order, leaderboard_id):
        try:
            leaderboards = self.application.leaderboards

            account_id = self.current_user.token.account
            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            yield leaderboards.delete_entry(
                leaderboard_id,
                gamespace_id,
                account_id,
                sort_order)

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)


class LeaderboardFriendsHandler(AuthenticatedHandler):
    @coroutine
    @scoped()
    def get(self, sort_order, leaderboard_id):
        try:
            offset = self.get_argument("offset", 0)
            limit = self.get_argument("limit", self.application.limit)

            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)
            account_id = self.current_user.token.account

            user_friends = yield self.application.social_service.list_friends(
                gamespace_id, account_id, profile_fields=[])

            if user_friends:
                leaderboard_records = yield self.application.leaderboards.list_friends_records(
                    user_friends,
                    leaderboard_id,
                    gamespace_id,
                    sort_order,
                    offset,
                    limit)
            else:
                leaderboard_records = {}

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)
        else:
            self.write(ujson.dumps(leaderboard_records))


class LeaderboardTopHandler(AuthenticatedHandler):
    @coroutine
    @scoped()
    def get(self, sort_order, leaderboard_id):
        try:
            leaderboards = self.application.leaderboards

            offset = self.get_argument("offset", 0)
            limit = self.get_argument("limit", self.application.limit)

            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            leaderboard_records = yield leaderboards.list_top_records(
                leaderboard_id,
                gamespace_id,
                sort_order,
                offset,
                limit)

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)

        else:
            self.write(leaderboard_records)

    @coroutine
    @scoped()
    def post(self, sort_order, leaderboard_id):

        leaderboards = self.application.leaderboards

        score = self.get_argument("score")
        display_name = self.get_argument("display_name")
        expire_in = self.get_argument("expire_in")

        try:
            profile = ujson.loads(self.get_argument("profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted 'profile' JSON")

        account_id = self.current_user.token.account
        gamespace_id = self.current_user.token.get(
            AccessToken.GAMESPACE)

        yield leaderboards.set_top_entries(
            leaderboard_id,
            gamespace_id,
            account_id,
            display_name,
            sort_order,
            score,
            expire_in,
            profile)
