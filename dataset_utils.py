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


def create_product_filter(products):
    def filter_list(x):
        if isinstance(x, float) and math.isnan(x):
            return None
        else:
            return list(filter(lambda y: y in products, x))

    return filter_list


def create_all_products_csv(metadata: AmazonCategoryMetadata, category, out_dir):
    category_path = os.path.join(out_dir, category)
    all_products_csv_path = os.path.join(category_path, "all_products.csv")

    if os.path.exists(all_products_csv_path):
        return pd.read_csv(all_products_csv_path)

    category_data = metadata.get_category_data(category)

    product_url = category_data['product_url']
    products_filename = 'products.json.gz'
    products_path = os.path.join(out_dir, products_filename)

    if os.path.exists(products_path) is False:
        products_path = download_url(product_url, out_dir, products_filename)

    products_pd = clean_products_data(products_path, category)

    print("Filtering also_bought, also_viewed and also_viewed that are not present in the products")
    product_filter = create_product_filter(products_pd['asin'].unique())
    products_pd['also_bought'] = products_pd['also_bought'].apply(product_filter)
    products_pd['also_viewed'] = products_pd['also_viewed'].apply(product_filter)
    products_pd['bought_together'] = products_pd['bought_together'].apply(product_filter)
    print("Done!")

    print("Generating all_products.csv")
    products_pd.to_csv(all_products_csv_path, index=False)

    return products_pd


def create_amazon_category(metadata: AmazonCategoryMetadata, category, out_dir):
    """
     Downloads the dataset from the internet and stores it in out_dir.
     Create a category object with the reviews and products data.
    """

    category_path = os.path.join(out_dir, category)
    products_csv_path = os.path.join(category_path, "products.csv")
    users_csv_path = os.path.join(category_path, "users.csv")

    category_data = metadata.get_category_data(category)

    if os.path.exists(users_csv_path):
        reviews_pd = pd.read_csv(users_csv_path)
    else:
        review_url = category_data['review_url']
        reviews_path = download_url(review_url, category_path, 'reviews.json.gz')
        reviews_pd = clean_reviews_data(reviews_path)
        print("Generating reviews.csv")
        reviews_pd.to_csv(users_csv_path, index=False)

    if os.path.exists(products_csv_path):
        products_pd = pd.read_csv(products_csv_path)
    else:
        product_url = category_data['product_url']
        products_path = download_url(product_url, category_path, 'products.json.gz')
        products_pd = clean_products_data(products_path, category)

        # Filter out products that are not present in the reviews
        merged_reviews = pd.merge(reviews_pd, products_pd, on='asin', how='inner')

        # Products for which there are corresponding reviews
        products_in_reviews = merged_reviews['asin'].unique()
        products_in_reviews_pd = pd.DataFrame(products_in_reviews, columns=['asin'])

        # Filter out products that are not present in the reviews
        products_pd = pd.merge(products_in_reviews_pd, products_pd, on='asin', how='inner')

        # Filter out also_bought, also_viewed and also_viewed that are not present in the products
        print("Filtering also_bought, also_viewed and also_viewed that are not present in the products")
        product_filter = create_product_filter(products_pd['asin'].unique())
        products_pd['also_bought'] = products_pd['also_bought'].apply(product_filter)
        products_pd['also_viewed'] = products_pd['also_viewed'].apply(product_filter)
        products_pd['bought_together'] = products_pd['bought_together'].apply(product_filter)
        print("Done!")

        print("Generating products.csv")
        products_pd.to_csv(products_csv_path, index=False)

    amazon_category = AmazonCategory(category, reviews_pd, products_pd, os.path.join(out_dir, category, 'images'))
    return amazon_category


if __name__ == '__main__':
    amazon_category = create_amazon_category(create_amazon_category_metadata(),
                                             'garden',
                                             r"C:\Users\josep\Downloads\amazon_dataset")
