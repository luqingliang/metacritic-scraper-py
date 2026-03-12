import unittest

from gamecritic.client import slug_from_game_url


class SlugFromUrlTestCase(unittest.TestCase):
    def test_slug_from_game_url(self) -> None:
        self.assertEqual(
            slug_from_game_url("https://www.metacritic.com/game/the-legend-of-zelda-breath-of-the-wild/"),
            "the-legend-of-zelda-breath-of-the-wild",
        )
        self.assertEqual(slug_from_game_url("https://www.metacritic.com/game/foo"), "foo")
        self.assertIsNone(slug_from_game_url("https://www.metacritic.com/movie/foo/"))


if __name__ == "__main__":
    unittest.main()
