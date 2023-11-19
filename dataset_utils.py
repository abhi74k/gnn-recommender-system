from AmazonCategory import AmazonCategory
from AmazonCategoryMetadata import AmazonCategoryMetadata, create_amazon_category_metadata
import os
import io
import requests
import json
import gzip
import pandas as pd
import math


def download_url(url, out_dir, filename):
    if os.path.exists(out_dir) is False:
        os.makedirs(out_dir)

    filepath = os.path.join(out_dir, filename)
    if os.path.exists(filepath) is False:
        response = requests.get(url, allow_redirects=True)
        if response.status_code == 200:
            open(filepath, 'wb').write(response.content)
        else:
            raise Exception('Failed to download url: ' + url)

    return filepath


def clean_reviews_data(reviews_path, from_date='2013-01-01'):
    data = []
    with gzip.open(reviews_path, 'r') as f:
        with io.TextIOWrapper(f, encoding='utf-8') as s:
            lines = s.readlines()
            data = list(map(lambda x: json.loads(x), lines))

    data_pd = pd.DataFrame(data)
    data_pd = data_pd[['overall', 'reviewerID', 'asin', 'unixReviewTime', 'helpful']]

    data_pd['date'] = pd.to_datetime(data_pd['unixReviewTime'], unit='s').dt.date
    data_pd = data_pd[data_pd['date'] > pd.Timestamp(from_date).date()]
    data_pd.drop(columns=['unixReviewTime'], inplace=True)

    data_pd['helpfulness_ratio'] = data_pd['helpful'].apply(lambda x: x[0] / x[1] if x[1] > 0 else 0)
    data_pd.drop(columns=['helpful'], inplace=True)

    data_pd['overall'] = data_pd['overall'].astype(int)
    data_pd['reviewerID'] = data_pd['reviewerID'].astype(str)
    data_pd['asin'] = data_pd['asin'].astype(str)

    data_pd.reset_index(inplace=True, drop=True)

    return data_pd


def clean_products_data(reviews_path, category):
    data = []
    with gzip.open(reviews_path, 'r') as f:
        with io.TextIOWrapper(f, encoding='utf-8') as s:
            for line in s:
                json_item = json.loads(json.dumps(eval(line)))
                if "related" not in json_item:
                    continue

                json_data_item = {}
                json_data_item['asin'] = json_item['asin']
                json_data_item['title'] = json_item['title'] if 'title' in json_item else None
                json_data_item['price'] = json_item['price'] if 'price' in json_item else None
                json_data_item['imUrl'] = json_item['imUrl'] if 'imUrl' in json_item else None
                json_data_item['brand'] = json_item['brand'] if 'brand' in json_item else None
                json_data_item['description'] = json_item['description'] if 'description' in json_item else None
                json_data_item['categories'] = json_item['categories'] if 'categories' in json_item else None
                json_data_item['category'] = category

                if "also_bought" in json_item["related"]:
                    json_data_item['also_bought'] = json_item['related']['also_bought']

                if "also_viewed" in json_item["related"]:
                    json_data_item['also_viewed'] = json_item['related']['also_viewed']

                if "bought_together" in json_item["related"]:
                    json_data_item['bought_together'] = json_item['related']['bought_together']

                data.append(json_data_item)

    data_pd = pd.DataFrame(data)

    return data_pd


def create_amazon_category(metadata: AmazonCategoryMetadata, category, out_dir):
    """
     Downloads the dataset from the internet and stores it in out_dir.
     Create a category object with the reviews and products data.
    """

    category_data = metadata.get_category_data(category)
    review_url = category_data['review_url']
    product_url = category_data['product_url']

    category_path = os.path.join(out_dir, category)

    reviews_path = download_url(review_url, category_path, 'reviews.json.gz')
    products_path = download_url(product_url, category_path, 'products.json.gz')

    reviews_pd = clean_reviews_data(reviews_path)
    products_pd = clean_products_data(products_path, category)

    print("Number of reviews: " + str(len(reviews_pd)))
    print("Number of products: " + str(len(products_pd)))

    merged_reviews = pd.merge(reviews_pd, products_pd, on='asin', how='inner')
    products_in_reviews = merged_reviews['asin'].unique()
    filtered_products_id_pd = pd.DataFrame(products_in_reviews, columns=['asin'])
    products_pd = pd.merge(filtered_products_id_pd, products_pd, on='asin', how='inner')

    def filter_list(x):
        if isinstance(x, float) and math.isnan(x):
            return None
        else:
            return list(filter(lambda y: y in products_in_reviews, x))

    # Filter out also_bought, also_viewed and also_viewed that are not present in the products
    products_pd['also_bought'] = products_pd['also_bought'].apply(filter_list)
    products_pd['also_viewed'] = products_pd['also_viewed'].apply(filter_list)
    products_pd['bought_together'] = products_pd['bought_together'].apply(filter_list)

    print("Number of reviews: " + str(len(reviews_pd)))
    print("Number of products: " + str(len(products_pd)))

    amazon_category = AmazonCategory(category, reviews_pd, products_pd)
    return amazon_category


if __name__ == '__main__':
    amazon_category = create_amazon_category(create_amazon_category_metadata(),
                                             'garden',
                                             r"C:\Users\josep\Downloads\amazon_dataset")
