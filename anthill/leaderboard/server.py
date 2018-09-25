
from anthill.common.options import options
from anthill.common import server, discover, database, access, sign, keyvalue

from . import handler as h
from . import admin
from . model.leaderboard import LeaderboardsModel
from . model.social import SocialModel
from . import options as _opts


class LeaderboardServer(server.Server):
    # noinspection PyShadowingNames
    def __init__(self):
        super(LeaderboardServer, self).__init__()

        self.db = database.Database(
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

    async def started(self):
        await super(LeaderboardServer, self).started()

        self.social_service = SocialModel()


if __name__ == "__main__":
    stt = server.init()
    access.AccessToken.init([access.public()])
    server.start(LeaderboardServer)
