import numpy as np

from AmazonProduct import AmazonProduct
from ThreadedDownloader import ThreadedDownloader


class AmazonCategory:

    def __init__(self, category, reviews_df, products_df, image_dir):
        self.category = category
        self.review_df = reviews_df
        self.product_df = products_df

        self.out_dir = image_dir
        self.downloader = ThreadedDownloader(list(self.product_df['imUrl']), self.out_dir)

    def download_images(self, num_of_workers=10):
        self.downloader.download(num_of_workers)

    def is_valid_product(self, asin):
        return len(self.product_df[self.product_df['asin'] == asin]) > 0

    def get_product(self, asin):
        product_details = self.product_df[self.product_df['asin'] == asin].to_dict(orient='records')[0]

        return AmazonProduct(product_details, self.out_dir)

    def get_random_product(self):
        idx = np.random.randint(len(self.product_df))
        product_details = self.product_df.iloc[idx].to_dict()
        return AmazonProduct(product_details, self.out_dir)