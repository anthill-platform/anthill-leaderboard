
from anthill.common.internal import Internal


class SocialModel(object):

    def __init__(self):
        self.internal = Internal()

    async def get_friends(self, gamespace, account_id, profile_fields):

        response = await self.internal.request(
            "social", "get_connections",
            account_id=account_id,
            gamespace=gamespace,
            profile_fields=profile_fields)

        friends_ids = [
            user_info["account"]
            for user_info in response
        ]

        return friends_ids
