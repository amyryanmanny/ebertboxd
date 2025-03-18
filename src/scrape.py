import datetime
import json
from pathlib import Path
import re
from urllib.parse import parse_qs, urlparse

import scrapy

class RogerEbertSpider(scrapy.Spider):
    name = "Roger Ebert"
    base_url = 'https://www.rogerebert.com'
    start_urls = [f'{self.base_url}/contributors/roger-ebert']

    def parse(self, response):
        yield scrapy.Request(
            url=get_page_url(1),
            headers={'Accept': 'application/json'},
            callback=self.parse_json,
            cb_kwargs=dict(page=1),
        )

    def parse_json(self, response, page):
        json_response = json.loads(response.text)
        html = json_response['html']
        more = json_response['more']

        response = response.replace(body=html)

        review_links = response.xpath('//h5/a/@href').getall()
        for review_link in review_links:
            yield response.follow(review_link, self.parse_review)

        if more:
            # Read next page
            yield scrapy.Request(
                url=get_page_url(page + 1),
                headers={'Accept': 'application/json'},
                callback=self.parse_json,
                cb_kwargs={'page': page + 1},
            )

    def get_page_url(self, page):
        return f"{self.base_url}/contributors/roger-ebert?filters%5Btitle%5D=&sort%5Border%5D=newest&filters%5Byears%5D%5B%5D=1914&filters%5Byears%5D%5B%5D=2024&filters%5Bstar_rating%5D%5B%5D=0.0&filters%5Bstar_rating%5D%5B%5D=4.0&filters%5Bno_stars%5D=1&page={page}"

    def parse_review(self, response):
        # We can extract the TMDB ID from the JustWatch widget
        # <div data-id-type="tmdb" data-id="180383" data-jw-widget>
        tmdb_id = None
        justwatch_div = response.xpath('//div[@data-jw-widget]')
        if justwatch_div:
            id_type = justwatch_div.attrib['data-id-type']
            if id_type == "tmdb":
                tmdb_id = justwatch_div.attrib['data-id']

        # Buffer italicized head/footnotes to avoid complicated edge case handling later
        response = response.replace(body=response.text.replace('<i>', '\r<i>').replace('</i>', "</i>\r"))

        # Review
        movie_title = response.css('.cast-and-crew--movie-title::text').get().strip()
        dateline = response.css('.time::text').get()

        article_title = response.css('.page-content--title::text').get().strip()
        article_sections = response.css('.page-content--block_editor-content').xpath("string(.)").getall()
        article_copy = "\n\n".join([
            # Clean up paragraph breaks within sections
            s.replace('\xa0', '').replace('\n', '').replace('\r', '\n\n').strip()
            for s in article_sections
        ]).replace("\n\n\n\n", "\n\n")  # Clean up doubled breaks between sections

        is_great_movie = response.css('.gm-drop-cap')

        # Calculate Star Rating
        star_div = response.css('.page-content--star-rating').xpath("span/i")
        rating = len(star_div) - 1
        try:
            last_star_type = star_div[-1].attrib['title']
            if last_star_type == 'star-full':
                rating += 1
            elif last_star_type == 'star-half':
                rating += 0.5
            elif last_star_type == 'thumbsdown':
                # e.g. Deuce Bigalow
                rating = None
            else:
                raise NotImplementedError()
        except Exception:
            # Some movies have no rating, such as Human Centipede
            # Does introduce slight ambiguity with the thumbs down, nbd
            rating = None

        # Reformat Date
        reviewed_on = datetime.datetime.strptime(dateline, "%B %d, %Y").strftime("%Y-%m-%d")

        # Build Review
        review = f"<b>{article_title}</b>\n\n{article_copy}"

        yield {
            'tmdbID': tmdb_id,
            'Title': movie_title,
            'Rating': rating,
            'Review': review,
            'WatchedDate': reviewed_on,
            'Tags': None if not is_great_movie else "great movies"
        }
