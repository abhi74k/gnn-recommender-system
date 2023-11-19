import requests
import os
import tqdm as tqdm
from concurrent.futures import ThreadPoolExecutor
import numpy as np

from AmazonProduct import AmazonProduct


class AmazonCategory:

    def __init__(self, category, reviews_df, products_df, image_dir):
        self.category = category
        self.review_df = reviews_df
        self.product_df = products_df

        self.out_dir = image_dir

    def download_images(self):
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        img_urls = list(self.product_df['imUrl'])
        self.download(img_urls, 10, self.out_dir)

    def saving_op(self, var):
        url_list, out_dir, idx = var

        print("Starting download: " + str(idx))

        for img_url in tqdm.tqdm(url_list, desc='Thread ' + str(idx) + ''):
            img_name = os.path.basename(img_url)
            img_path = os.path.join(out_dir, img_name)
            if not os.path.exists(img_path):
                r = requests.get(img_url)
                if r.status_code == 200:
                    with open(img_path, 'wb') as f:
                        f.write(r.content)
                else:
                    print('Failed to download image: ' + img_url)

    def download(self, url_list, num_of_workers, output_folder):
        idx = len(url_list) // num_of_workers if len(url_list) > 9 else len(url_list)
        param = []
        for i in range(num_of_workers):
            param.append((url_list[((i * idx)):(idx * (i + 1))], output_folder, i))
        with ThreadPoolExecutor(max_workers=num_of_workers) as executor:
            executor.map(self.saving_op, param)

    def is_valid_product(self, asin):
        return len(self.product_df[self.product_df['asin'] == asin]) > 0

    def get_product(self, asin):
        product_details = self.product_df[self.product_df['asin'] == asin].to_dict(orient='records')[0]

        return AmazonProduct(product_details, self.out_dir)

    def get_random_product(self):
        idx = np.random.randint(len(self.product_df))
        product_details = self.product_df.iloc[idx].to_dict()
        return AmazonProduct(product_details, self.out_dir)