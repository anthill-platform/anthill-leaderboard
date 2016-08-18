
from tornado.gen import coroutine, Return

from common.internal import Internal


class SocialModel(object):

    def __init__(self):
        self.internal = Internal()

    @coroutine
    def get_friends(self, gamespace, account_id, profile_fields):

        response = yield self.internal.request(
            "social", "get_connections",
            account_id=account_id,
            gamespace=gamespace,
            profile_fields=profile_fields)

        friends_ids = [
            user_info["account"]
            for user_info in response
        ]

        raise Return(friends_ids)
