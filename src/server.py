
from tornado.gen import coroutine
from common.options import options

import handler as h
import admin

import common.server
import common.discover
import common.database
import common.access
import common.sign
import common.keyvalue

from model.leaderboard import LeaderboardsModel
from model.social import  SocialModel

import options as _opts


class LeaderboardServer(common.server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(LeaderboardServer, self).__init__()

        self.db = common.database.Database(
            host=options.db_host,
            database=options.db_name,
            user=options.db_username,
            password=options.db_password)

        self.leaderboards = LeaderboardsModel(self.db)

        self.limit = options.default_limit

        self.social_service = None

    def get_models(self):
        return [self.leaderboards]

    def get_metadata(self):
        return {
            "title": "Leaderboard",
            "description": "See and edit player ranking",
            "icon": "sort-numeric-asc"
        }

    def get_internal_handler(self):
        return h.InternalHandler(self)

    def get_handlers(self):
        return [
            (r"/leaderboard/(asc|desc)/(.*)/entry", h.LeaderboardEntryHandler),
            (r"/leaderboard/(asc|desc)/(.*)/around", h.LeaderboardAroundMeHandler),
            (r"/leaderboard/(asc|desc)/(.*)/friends", h.LeaderboardFriendsHandler),
            (r"/leaderboard/(asc|desc)/([^/]*)", h.LeaderboardTopHandler),
        ]

    def get_admin(self):
        return {
            "index": admin.RootAdminController
        }

    @coroutine
    def started(self, *args, **kwargs):
        yield super(LeaderboardServer, self).started(*args, **kwargs)

        self.social_service = SocialModel()

if __name__ == "__main__":
    stt = common.server.init()
    common.access.AccessToken.init([common.access.public()])
    common.server.start(LeaderboardServer)
