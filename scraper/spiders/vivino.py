import pprint
from math import ceil
from typing import Iterable, Any
from urllib.parse import urlencode

import jsonlines
import scrapy
from scrapy import Request
from scrapy.exceptions import DropItem
from scrapy.http import Response

BASE_URL = "https://www.vivino.com/api"
BUCKET = "trade_vino_data"
BUCKET_PREFIX = "vivino-images"
OUT_FILE = "wines.jsonl"
MAX_PAGES = 60
MAX_PRICE = 10000


class NoImagePipeline:
    def process_item(self, item, spider):
        if not item["image_urls"]:
            raise DropItem(f"No images found for item: {item['vintage_id']}")
        return item


class DeduplicatePipeline:
    def __init__(self):
        try:
            with jsonlines.open(OUT_FILE) as wines:
                self.seen = set(wine["vintage_id"] for wine in wines)
        except FileNotFoundError:
            self.seen = set()

    def process_item(self, item, spider):
        if item["vintage_id"] in self.seen:
            raise DropItem(f"Duplicate item found: {item['vintage_id']}")
        self.seen.add(item["vintage_id"])
        return item

class FixLabelPipeline:
    def process_item(self, item, spider):
        image_path = item["images"][0]['path']
        bucket_path = f"https://storage.cloud.google.com/{BUCKET}/{BUCKET_PREFIX}"
        item["vintage_label_image_url"] = f'{bucket_path}/{image_path}'
        del item["image_urls"]
        del item["images"]
        return item

class VivinoSpider(scrapy.Spider):
    name = "vivino"
    allowed_domains = ["vivino.com"]
    custom_settings = {
        "IMAGES_STORE": f"gs://{BUCKET}/{BUCKET_PREFIX}/",
        "DEFAULT_REQUEST_HEADERS": {
            "Accept": "application/json",
        },
        "ITEM_PIPELINES": {
            NoImagePipeline: 200,
            DeduplicatePipeline: 300,
            "scrapy.pipelines.images.ImagesPipeline": 500,
            FixLabelPipeline: 501,
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

    def start_requests(self):
        yield Request(
            url=f"{BASE_URL}/grapes",
            callback=self.on_grapes,
            dont_filter=True,
        )

    def on_grapes(self, response: Response, **kwargs: Any):
        data = response.json()
        grapes = data['grapes']
        self.logger.info("Found %d grapes", len(grapes))
        for grape in grapes:
            # get current page for this grape
            grape_id = grape['id']
            yield from self.build_compute_filterset(grape_id)

    def build_compute_filterset(self, grape_id: int, min_price: float = -1, spread: float = -1):
        if min_price >= MAX_PRICE:
            self.logger.info("Max price ($%d) reached for grape %d, we will stop.", MAX_PRICE, grape_id)
            return

        if min_price == -1:
            min_price = self.state.setdefault('min_price', {}).setdefault(grape_id, 0.0)

        if spread == -1:
            max_spread = MAX_PRICE - min_price
            default_spread = min(max_spread, max(1.13 ** min_price, 12))
            # try to retrieve spread from state
            spread = self.state.get('spread', {}).get(grape_id, {}).get(min_price, default_spread)

        max_price = min_price + spread

        page = self.state.get('current_page', {}).get(grape_id, {}).get((min_price, max_price), 1)

        params = {
            "min_rating": 0,
            "price_range_min": min_price,
            "price_range_max": max_price,
            "grape_ids[]": grape_id,
            "order_by": "ratings_count",
            "page": page,
        }
        yield Request(
            url=f"{BASE_URL}/explore/explore?{urlencode(params)}",
            cb_kwargs={
                "grape_id": grape_id,
                "min_price": min_price,
                "max_price": max_price,
                "spread": spread,
                "page": page,
            },
            callback=self.on_compute_filterset,
            dont_filter=True,
        )

    def on_compute_filterset(self, response: Response, **kwargs: Any):
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
            self.logger.info("No results between $%d and $%d for grape %d, we will increase the minimum price.", min_price, max_price, grape_id)
            new_min_price = self.state.setdefault('min_price', {})[grape_id] = max_price + 0.01
            yield from self.build_compute_filterset(grape_id, new_min_price)
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

    def on_explore(self, response: Response, **kwargs: Any):
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
                self.logger.debug("No label image found for wine %d, skipping", wine['vintage']['id'])
                continue

            if not wine['vintage']['wine']:
                continue

            if not wine['vintage']['wine']['style']:
                wine['vintage']['wine']['style'] = {}

            if not wine['vintage']['wine']['region']:
                wine['vintage']['wine']['region'] = {}

            if not wine['vintage']['wine']['winery']:
                wine['vintage']['wine']['winery'] = {}

            wine_id = wine['vintage']['wine']['id']
            vintage_year = wine['vintage']['year']

            yield {
                "image_urls": image_urls,
                "vintage_id": wine['vintage']['id'],
                "vintage_url": f'https://www.vivino.com/api/w/{wine_id}?year={vintage_year}',
                "vintage_seo_name": wine['vintage']['seo_name'],
                "vintage_name": wine['vintage']['name'],
                "vintage_year": vintage_year,
                "wine_id": wine_id,
                "wine_url": f'https://www.vivino.com/api/wine/{wine_id}',
                "wine_name": wine['vintage']['wine']['name'],
                "wine_seo_name": wine['vintage']['wine']['seo_name'],
                "wine_type_id": wine['vintage']['wine']['type_id'],
                "wine_vintage_type": wine['vintage']['wine']['vintage_type'],
                "wine_is_natural": wine['vintage']['wine']['is_natural'],
                "wine_region_id": wine['vintage']['wine']['region'].get('id'),
                "wine_region_name": wine['vintage']['wine']['region'].get('name'),
                "wine_region_name_en": wine['vintage']['wine']['region'].get('name_en'),
                "wine_region_seo_name": wine['vintage']['wine']['region'].get('seo_name'),
                "wine_region_country_code": wine['vintage']['wine']['region'].get('country', {}).get('code'),
                "wine_region_country_name": wine['vintage']['wine']['region'].get('country', {}).get('name'),
                "wine_region_country_native_name": wine['vintage']['wine']['region'].get('country', {}).get('native_name'),
                "wine_region_country_seo_name": wine['vintage']['wine']['region'].get('country', {}).get('seo_name'),
                "style_id": wine['vintage']['wine']['style'].get('id'),
                "style_name": wine['vintage']['wine']['style'].get('name'),
                "style_regional_name": wine['vintage']['wine']['style'].get('regional_name'),
                "style_varietal_name": wine['vintage']['wine']['style'].get('varietal_name'),
                "style_seo_name": wine['vintage']['wine']['style'].get('seo_name'),
                "style_description": wine['vintage']['wine']['style'].get('description'),
                "style_body": wine['vintage']['wine']['style'].get('body'),
                "style_body_description": wine['vintage']['wine']['style'].get('body_description'),
                "style_acidity": wine['vintage']['wine']['style'].get('acidity'),
                "style_acidity_description": wine['vintage']['wine']['style'].get('acidity_description'),
                "winery_id": wine['vintage']['wine']['winery'].get('id'),
                "winery_name": wine['vintage']['wine']['winery'].get('name'),
                "winery_seo_name": wine['vintage']['wine']['winery'].get('seo_name'),
                "taste": wine['vintage']['wine']['taste'],
            }

        self.state.setdefault('current_page', {}).setdefault(grape_id, {})[(min_price, max_price)] = page
        self.first_wine[grape_id] = first_wine_id
        yield from self.build_explore(grape_id, page + 1, min_price, max_price)
