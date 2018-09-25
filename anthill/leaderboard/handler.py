
from tornado.web import HTTPError

from anthill.common.access import scoped, AccessToken, InternalError
from anthill.common.handler import AuthenticatedHandler

from . model.leaderboard import LeaderboardNotFound

import ujson


class InternalHandler(object):
    def __init__(self, application):
        self.application = application

    async def delete(self, gamespace, sort_order, leaderboard_name):

        leaderboards = self.application.leaderboards

        try:
            leaderboard_id = await leaderboards.find_leaderboard(
                gamespace, leaderboard_name, sort_order)
        except LeaderboardNotFound:
            raise InternalError(404, "No such leaderboard")

        else:
            await leaderboards.delete_leaderboard(
                gamespace, leaderboard_id)

        return "OK"

    async def post(self, account, gamespace, sort_order, leaderboard_name, score, display_name, expire_in, profile):

        leaderboards = self.application.leaderboards

        response = await leaderboards.add_entry(
            gamespace, leaderboard_name, sort_order, account,
            display_name, score, expire_in, profile)

        return response

    async def get_top(self, gamespace, sort_order, leaderboard_name, offset=0, limit=1000):

        leaderboards = self.application.leaderboards

        try:
            data = await leaderboards.list_top_records(
                leaderboard_name, gamespace, sort_order, offset=offset, limit=limit)
        except LeaderboardNotFound:
            raise InternalError(404, "No such leaderboard")

        return {
            "entries": len(data),
            "data": [
                item.dump()
                for item in data
            ]
        }

    async def get_top_account(self, gamespace, account_id, sort_order, leaderboard_name, offset=0, limit=1000):

        leaderboards = self.application.leaderboards

        try:
            data = await leaderboards.list_top_records_account(
                leaderboard_name, gamespace, account_id,
                sort_order, offset=offset, limit=limit)
        except LeaderboardNotFound:
            raise InternalError(404, "No such leaderboard")

        return {
            "entries": len(data),
            "data": [
                item.dump()
                for item in data
            ]
        }

    async def get_top_all_clusters(self, gamespace, sort_order, leaderboard_name):

        leaderboards = self.application.leaderboards

        try:
            data = await leaderboards.list_top_all_clusters(
                leaderboard_name, gamespace, sort_order)
        except LeaderboardNotFound:
            raise InternalError(404, "No such leaderboard")

        return {
            cluster_id: {
                "entries": len(cluster),
                "data": [
                    item.dump()
                    for item in cluster
                ]
            }
            for cluster_id, cluster in data.items()
        }


class LeaderboardAroundMeHandler(AuthenticatedHandler):
    @scoped()
    async def get(self, sort_order, leaderboard_id):
        try:
            leaderboards = self.application.leaderboards

            offset = self.get_argument("offset", 0)
            limit = self.get_argument("limit", self.application.limit)

            account_id = self.current_user.token.account
            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            leaderboard_records = await leaderboards.list_around_me_records(
                account_id, leaderboard_id, gamespace_id,
                sort_order, offset, limit) or {}

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)

        else:
            self.dumps({
                "entries": len(leaderboard_records),
                "data": [
                    record.dump()
                    for record in leaderboard_records
                ]
            })


class LeaderboardEntryHandler(AuthenticatedHandler):

    def options(self, *args, **kwargs):
        self.set_header("Access-Control-Allow-Methods", "POST,DELETE,OPTIONS")

    @scoped()
    async def delete(self, sort_order, leaderboard_id):
        try:
            leaderboards = self.application.leaderboards

            account_id = self.current_user.token.account
            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            await leaderboards.delete_entry(
                leaderboard_id, gamespace_id,
                account_id, sort_order)

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)


class LeaderboardFriendsHandler(AuthenticatedHandler):
    @scoped()
    async def get(self, sort_order, leaderboard_id):
        try:
            offset = self.get_argument("offset", 0)
            limit = self.get_argument("limit", self.application.limit)

            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)
            account_id = self.current_user.token.account

            user_friends = await self.application.social_service.list_friends(
                gamespace_id, account_id, profile_fields=[])

            if user_friends:
                leaderboard_records = await self.application.leaderboards.list_friends_records(
                    user_friends, leaderboard_id,
                    gamespace_id, sort_order,
                    offset, limit)
            else:
                leaderboard_records = []

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_id)
        else:
            self.dumps({
                "entries": len(leaderboard_records),
                "data": [
                    record.dump()
                    for record in leaderboard_records
                ]
            })


class LeaderboardTopHandler(AuthenticatedHandler):
    @scoped()
    async def get(self, sort_order, leaderboard_name):
        try:
            leaderboards = self.application.leaderboards

            offset = self.get_argument("offset", 0)
            limit = self.get_argument("limit", self.application.limit)

            account_id = self.get_argument("arbitrary_account", self.current_user.token.account)

            gamespace_id = self.current_user.token.get(
                AccessToken.GAMESPACE)

            leaderboard_records = await leaderboards.list_top_records_account(
                leaderboard_name, gamespace_id,
                account_id, sort_order,
                offset, limit)

        except LeaderboardNotFound:
            raise HTTPError(
                404, "Leaderboard '%s' was not found." % leaderboard_name)

        else:
            result = {
                "entries": len(leaderboard_records),
                "data": [
                    record.dump()
                    for record in leaderboard_records
                ]
            }

            self.dumps(result)

    @scoped()
    async def post(self, sort_order, leaderboard_name):

        leaderboards = self.application.leaderboards

        score = self.get_argument("score")
        display_name = self.get_argument("display_name")
        expire_in = self.get_argument("expire_in", 604800)
        arbitrary_account_id = self.get_argument("arbitrary_account", 0)

        account_id = self.current_user.token.account

        if arbitrary_account_id:
            if self.has_scopes(["lb_arbitrary_account"]):
                account_id = arbitrary_account_id
            else:
                raise HTTPError(403, "Scope 'lb_arbitrary_account' is required for posting for arbitrary account")

        try:
            profile = ujson.loads(self.get_argument("profile", "{}"))
        except (KeyError, ValueError):
            raise HTTPError(400, "Corrupted 'profile' JSON")

        gamespace_id = self.current_user.token.get(
            AccessToken.GAMESPACE)

        await leaderboards.add_entry(
            gamespace_id, leaderboard_name, sort_order, account_id,
            display_name, score, expire_in, profile)
