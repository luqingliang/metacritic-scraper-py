import unittest

from gamecritic.client import (
    MetacriticClient,
    catalog_image_url_from_bucket_path,
    cover_bucket_path_from_product,
)


class CoverUrlHelpersTestCase(unittest.TestCase):
    def test_cover_bucket_path_prefers_card_image(self) -> None:
        payload = {
            "data": {
                "item": {
                    "images": [
                        {"typeName": "mainImage", "bucketPath": "/provider/6/12/main.jpg"},
                        {"typeName": "cardImage", "bucketPath": "/provider/6/3/card.jpg"},
                    ]
                }
            }
        }
        self.assertEqual(cover_bucket_path_from_product(payload), "/provider/6/3/card.jpg")

    def test_catalog_image_url_from_bucket_path(self) -> None:
        self.assertEqual(
            catalog_image_url_from_bucket_path("/provider/7/2/7-1695949825.jpg"),
            "https://www.metacritic.com/a/img/catalog/provider/7/2/7-1695949825.jpg",
        )
        self.assertIsNone(catalog_image_url_from_bucket_path(None))

    def test_resolve_cover_url_returns_catalog_url(self) -> None:
        payload = {
            "data": {
                "item": {
                    "images": [
                        {"typeName": "cardImage", "bucketPath": "/provider/7/2/7-1695949825.jpg"},
                    ]
                }
            }
        }

        client = MetacriticClient(delay_seconds=0)
        try:
            url = client.resolve_cover_url(product_payload=payload)
        finally:
            client.close()

        self.assertEqual(
            url,
            "https://www.metacritic.com/a/img/catalog/provider/7/2/7-1695949825.jpg",
        )

    def test_resolve_cover_url_returns_none_without_bucket_path(self) -> None:
        payload = {"data": {"item": {"images": []}}}
        client = MetacriticClient(delay_seconds=0)
        try:
            url = client.resolve_cover_url(product_payload=payload)
        finally:
            client.close()

        self.assertIsNone(url)

if __name__ == "__main__":
    unittest.main()
