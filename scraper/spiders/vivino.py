from math import ceil
from typing import Iterable, Any
from urllib.parse import urlencode

import jsonlines
import scrapy
from scrapy import Request
from scrapy.exceptions import DropItem
from scrapy.http import Response

BASE_URL = "https://www.vivino.com/api"
MAX_PAGES = 60  # warn for any searches with more than this number of pages of results
# https www.vivino.com/api/grapes  "User-Agent:"   Accept:application/json | jq '[.grapes[].id]'
OUT_FILE = "wines.jsonl"
MAX_PRICE = 2000


class NoImagePipeline:
    def process_item(self, item, spider):
        if not item["image_urls"]:
            raise DropItem(f"No images found for item: {item['id']}")
        return item


class DeduplicatePipeline:
    def __init__(self):
        try:
            with jsonlines.open(OUT_FILE) as wines:
                self.seen = set(wine["id"] for wine in wines)
        except FileNotFoundError:
            self.seen = set()

    def process_item(self, item, spider):
        if item["id"] in self.seen:
            raise DropItem(f"Duplicate item found: {item['id']}")
        self.seen.add(item["id"])
        return item


class VivinoSpider(scrapy.Spider):
    name = "vivino"
    allowed_domains = ["vivino.com"]
    custom_settings = {
        "IMAGES_STORE": "gs://trade_vino_data/vivino-images/",
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json",
        },
        "ITEM_PIPELINES": {
            NoImagePipeline: 200,
            DeduplicatePipeline: 300,
            "scrapy.pipelines.images.ImagesPipeline": 500,
        },
        "FEEDS": {
            OUT_FILE: {
                "format": "jsonlines",
            },
        }
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.first_wine = {}
        self.price_range = {}
        self.state = {}

    def start_requests(self) -> Iterable[Request]:
        yield Request(
            url=f"{BASE_URL}/grapes",
            callback=self.on_grapes,
            dont_filter=True,
        )

    def on_grapes(self, response: Response, **kwargs: Any) -> Any:
        data = response.json()
        grapes = data['grapes']
        self.logger.info("Found %d grapes", len(grapes))
        # for grape in grapes:
        for grape in grapes[:3]:
            # get current page for this grape
            grape_id = grape['id']
            yield from self.build_compute_filterset(grape_id)

    def build_compute_filterset(self, grape_id: int, min_price: float = 0, spread: float = -1):
        if spread == -1:
            default_spread = 1.05 ** min_price
            # try to retrieve spread from state
            spread = self.state.get('spread', {}).get(grape_id, {}).get(min_price, default_spread)

        max_price = min_price + spread
        if max_price > MAX_PRICE:
            self.logger.info("Max price ($%d) reached for grape %d, we will stop.", MAX_PRICE, grape_id)
            return

        params = {
            "min_rating": 0,
            "price_range_min": min_price,
            "price_range_max": max_price,
            "grape_ids[]": grape_id,
            "order_by": "ratings_count",
            "page": 1,
        }
        yield Request(
            url=f"{BASE_URL}/explore/explore?{urlencode(params)}",
            cb_kwargs={
                "grape_id": grape_id,
                "min_price": min_price,
                "max_price": max_price,
                "spread": spread,
                "page": 1,
            },
            callback=self.on_compute_filterset,
            dont_filter=True,
        )

    def on_compute_filterset(self, response: Response, **kwargs: Any) -> Any:
        grape_id = kwargs['grape_id']
        min_price = kwargs['min_price']
        max_price = kwargs['max_price']
        spread = kwargs['spread']
        page = kwargs['page']

        data = response.json()
        records = data['explore_vintage']['records_matched']
        wines = data['explore_vintage']['matches']

        self.logger.info("Current filters for grape %d: $%d-$%d (%d results)", grape_id, min_price, max_price, records)

        if not wines:
            self.logger.info("No results between $%d and $%d for grape %d, we will double the spread.", min_price, max_price, grape_id)
            yield from self.build_compute_filterset(grape_id, min_price, spread * 2)
            return

        records_in_page = len(wines)
        approx_pages = ceil(records / records_in_page)

        if approx_pages > MAX_PAGES:
            self.logger.warning("> %d pages of results for grape %d between $%d and $%d, we will reduce spread.", MAX_PAGES, grape_id, min_price, max_price)
            yield from self.build_compute_filterset(grape_id, min_price, spread * 0.75)
            return

        # we found a working range!
        self.state.setdefault('spread', {}).setdefault(grape_id, {})[min_price] = spread
        yield from self.build_explore(grape_id, page + 1, min_price, max_price)

    def build_explore(self, grape_id: int, page: int, min_price: float, max_price: float):
        params = {
            "min_rating": 0,
            "grape_ids[]": grape_id,
            "order_by": "ratings_count",
            "price_range_min": min_price,
            "price_range_max": max_price,
            "page": page,
        }
        yield Request(
            url=f"{BASE_URL}/explore/explore?{urlencode(params)}",
            cb_kwargs={
                "grape_id": grape_id,
                "page": page,
                "min_price": min_price,
                "max_price": max_price
            },
            callback=self.on_explore,
            dont_filter=True,
        )

    def on_explore(self, response: Response, **kwargs: Any) -> Any:
        grape_id = kwargs['grape_id']
        min_price = kwargs['min_price']
        max_price = kwargs['max_price']
        page = kwargs['page']

        data = response.json()
        records = data['explore_vintage']['records_matched']
        wines = data['explore_vintage']['matches']

        if not wines:
            self.logger.warning("No results between $%d and $%d for grape %d.", min_price, max_price, grape_id)
            yield from self.build_compute_filterset(grape_id, max_price + 0.01)
            return

        records_in_page = len(wines)
        approx_pages = ceil(records / records_in_page)
        first_wine_id = wines[0]['vintage']['id']

        self.logger.info("Current page for grape %d: %d/%d (%d results)", grape_id, page, approx_pages, records)
        if approx_pages > MAX_PAGES:
            self.logger.warning("More than %d pages of results for grape %d between $%d and $%d.", MAX_PAGES, grape_id, min_price, max_price)

        if first_wine_id == self.first_wine.get(grape_id):
            # when the pages start repeating, we are done with this filterset
            self.logger.info("No more wines left on page %d for grape %d between $%d and $%d.", page, grape_id, min_price, max_price)
            yield from self.build_compute_filterset(grape_id, max_price + 0.01)
            return

        for wine in wines:
            try:
                label_image_url = wine['vintage']['image']['variations']['label']
                label_image_url = f"https://{label_image_url[2:]}"
                image_urls = [label_image_url]
            except KeyError:
                image_urls = []

            yield {
                "id": wine['vintage']['id'],
                "url": f"{BASE_URL}/w/{wine['vintage']['wine']['id']}",
                "winery": wine['vintage']['wine']['winery']['name'],
                "region": wine['vintage']['wine']['region']['name'],
                "name": wine['vintage']['name'],
                "year": wine['vintage']['year'],
                "image_urls": image_urls,
            }

        self.state.setdefault('current_page', {}).setdefault(grape_id, {})[(min_price, max_price)] = page
        self.first_wine[grape_id] = first_wine_id
        yield from self.build_explore(grape_id, page + 1, min_price, max_price)
