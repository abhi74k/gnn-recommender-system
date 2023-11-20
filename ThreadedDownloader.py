import os
from concurrent.futures import ThreadPoolExecutor

import requests
from tqdm import tqdm


class ThreadedDownloader:
    def __init__(self, url_list, output_dir):
        self.url_list = url_list
        self.out_dir = output_dir

    def download(self, num_workers=10):
        if not os.path.exists(self.out_dir):
            os.makedirs(self.out_dir)

        idx = len(self.url_list) // num_workers if len(self.url_list) > 9 else len(self.url_list)
        param = []
        for i in range(num_workers):
            param.append((self.url_list[((i * idx)):(idx * (i + 1))], self.out_dir, i))
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            executor.map(self.saving_op, param)

    @staticmethod
    def saving_op(var):
        url_list, out_dir, idx = var

        print("Starting download: " + str(idx))

        for url in url_list:
            filename = os.path.basename(url)
            filepath = os.path.join(out_dir, filename)
            if not os.path.exists(filepath):
                r = requests.get(url)
                if r.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(r.content)
                else:
                    print('Failed to download image: ' + url)
