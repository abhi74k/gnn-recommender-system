import os


class AmazonProduct:
    """Represents an Amazon product."""

    def __init__(self, product_dict, image_dir):
        self.product_dict = product_dict
        self.image_dir = image_dir

        self.asin = product_dict['asin']
        self.title = product_dict['title']
        self.price = product_dict['price']
        self.imUrl = product_dict['imUrl']
        self.brand = product_dict['brand']
        self.description = product_dict['description']
        self.categories = product_dict['categories']
        self.category = product_dict['category']
        self.also_bought = product_dict['also_bought']
        self.also_viewed = product_dict['also_viewed']
        self.bought_together = product_dict['bought_together']

    def get_asin(self):
        return self.asin

    def get_title(self):
        return self.title

    def get_price(self):
        return self.price

    def get_imUrl(self):
        return self.imUrl

    def get_brand(self):
        return self.brand

    def get_description(self):
        return self.description

    def get_categories(self):
        return self.categories

    def get_category(self):
        return self.category

    def get_also_bought(self):
        return self.also_bought

    def get_also_viewed(self):
        return self.also_viewed

    def get_bought_together(self):
        return self.bought_together

    def get_image_path(self):
        return os.path.join(self.image_dir, os.path.basename(self.imUrl))
