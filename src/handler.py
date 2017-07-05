
import ujson

from tornado.gen import coroutine, Return
from tornado.web import HTTPError

from common.access import scoped, AccessToken, InternalError
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
                gamespace, leaderboard_name, sort_order)
        except LeaderboardNotFound:
            raise InternalError(404, "No such leaderboard")

        else:
            yield leaderboards.delete_leaderboard(
                gamespace, leaderboard_id)

        raise Return("OK")

    @coroutine
    def post(self, account, gamespace, sort_order, leaderboard_name, score, display_name, expire_in, profile):

        leaderboards = self.application.leaderboards

        response = yield leaderboards.add_entry(
            gamespace, leaderboard_name, sort_order, account,
            display_name, score, expire_in, profile)

        raise Return(response)

    @coroutine
    def get_top(self, gamespace, sort_order, leaderboard_name, limit=1000):

        leaderboards = self.application.leaderboards

        try:
            response = yield leaderboards.list_top_all_clusters(
                leaderboard_name, gamespace, sort_order, limit)
        except LeaderboardNotFound:
            raise InternalError(404, "No such leaderboard")

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
                account_id, leaderboard_id, gamespace_id,
                sort_order, offset, limit) or {}

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)

        else:
            self.dumps(leaderboard_records)


class LeaderboardEntryHandler(AuthenticatedHandler):

    def options(self, *args, **kwargs):
        self.set_header("Access-Control-Allow-Methods", "POST,DELETE,OPTIONS")

    @coroutine
    @scoped()
    def delete(self, sort_order, leaderboard_id):
        try:
            leaderboards = self.application.leaderboards

            account_id = self.current_user.token.account
            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            yield leaderboards.delete_entry(
                leaderboard_id, gamespace_id,
                account_id, sort_order)

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
                    user_friends, leaderboard_id,
                    gamespace_id, sort_order,
                    offset, limit)
            else:
                leaderboard_records = {}

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)
        else:
            self.dumps(leaderboard_records)


class LeaderboardTopHandler(AuthenticatedHandler):
    @coroutine
    @scoped()
    def get(self, sort_order, leaderboard_name):
        try:
            leaderboards = self.application.leaderboards

            offset = self.get_argument("offset", 0)
            limit = self.get_argument("limit", self.application.limit)

            account_id = self.current_user.token.account
            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            leaderboard_records = yield leaderboards.list_top_records(
                leaderboard_name, gamespace_id,
                account_id, sort_order,
                offset, limit)

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_name)

        else:
            self.dumps(leaderboard_records)

    @coroutine
    @scoped()
    def post(self, sort_order, leaderboard_name):

        leaderboards = self.application.leaderboards

        score = self.get_argument("score")
        display_name = self.get_argument("display_name")
        expire_in = self.get_argument("expire_in", 604800)
        force_account_id = self.get_argument("force_account_id", 0)

        account_id = self.current_user.token.account

        if force_account_id:
            if self.token.has_scope("lb_arbitrary_account"):
                account_id = force_account_id
            else:
                raise HTTPError(403, "Not allowed")

        try:
            profile = ujson.loads(self.get_argument("profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted 'profile' JSON")

        gamespace_id = self.current_user.token.get(
            AccessToken.GAMESPACE)

        yield leaderboards.add_entry(
            gamespace_id, leaderboard_name, sort_order, account_id,
            display_name, score, expire_in, profile)
