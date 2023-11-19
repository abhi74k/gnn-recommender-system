import io
import gzip
import json


class AmazonCategoryMetadata:

    def __init__(self):
        self.category_store = {}

    def add_category(self, category, review_url, product_url, image_features):
        self.name = category
        self.review_url = review_url
        self.product_url = product_url
        self.image_features = image_features

        self.category_store[category] = {
            'review_url': review_url,
            'product_url': product_url,
            'image_features': image_features
        }

    def get_category_data(self, category):
        return self.category_store[category]

    def get_review_url(self, category):
        return self.category_store[category]['review_url']

    def get_product_url(self, category):
        return self.category_store[category]['product_url']

    def get_image_features_url(self, category):
        return self.category_store[category]['image_features']

    def get_categories(self):
        return list(self.category_store.keys())


def create_amazon_category_metadata():
    metadata = AmazonCategoryMetadata()

    metadata.add_category('clothing_shoes_jewellery',
                          r'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Clothing_Shoes_and_Jewelry.json.gz',
                         r'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Clothing_Shoes_and_Jewelry.json.gz',
                       r'http://snap.stanford.edu/data/amazon/productGraph/image_features/categoryFiles/image_features_Clothing_Shoes_and_Jewelry.b')

    metadata.add_category('video',
                            r'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Amazon_Instant_Video.json.gz',
                           r'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Amazon_Instant_Video.json.gz',
                         r'http://snap.stanford.edu/data/amazon/productGraph/image_features/categoryFiles/image_features_Amazon_Instant_Video.b')

    metadata.add_category('garden',
                            r'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Patio_Lawn_and_Garden.json.gz',
                           r'http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Patio_Lawn_and_Garden.json.gz',
                         r'http://snap.stanford.edu/data/amazon/productGraph/image_features/categoryFiles/image_features_Patio_Lawn_and_Garden.b')

    return metadata


if __name__ == '__main__':
    dataset_desc = create_amazon_category_metadata()
    print(dataset_desc.get_categories())
    print(dataset_desc.get_review_url('clothing_shoes_jewellery'))
    print(dataset_desc.get_product_url('clothing_shoes_jewellery'))
