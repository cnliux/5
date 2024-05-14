import requests
from bs4 import BeautifulSoup
import os
from collections import defaultdict
import time
import threading
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 模拟浏览器头
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'

# 用于网络请求的会话对象
session = requests.Session()
session.headers.update({'User-Agent': USER_AGENT})

# URL列表
urls = [
    {"url": "https://www.douyu.com/directory/subCate/yqk/290", "category": "陪看"},
    {"url": "https://www.douyu.com/directory/subCate/yqk/1863", "category": "综合"},
    {"url": "https://www.douyu.com/directory/subCate/yqk/2827", "category": "喜剧"},
    {"url": "https://www.douyu.com/directory/subCate/yqk/2828", "category": "动作"},
    {"url": "https://www.douyu.com/directory/subCate/yqk/2830", "category": "科幻"},
    {"url": "https://www.douyu.com/directory/subCate/yqk/2833", "category": "剧情"},
    {"url": "https://www.douyu.com/directory/subCate/yqk/2834", "category": "古装"},
    # ...其他URL...
]

all_data = defaultdict(list)
lock = threading.Lock()

def crawl_and_parse(url, category):
    logging.info(f"开始爬取页面: {url}")
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.warning(f"请求错误: {e}, 页面: {url}")
        return

    try:
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all(class_="DyListCover-wrap")
        page_data = [(link.find(class_="DyListCover-intro")['title'], link['href']) for link in links]
        with lock:
            all_data[category].extend(page_data)
        logging.info(f"成功爬取页面: {url}, 收录{len(page_data)}个主播")
    except Exception as e:
        logging.error(f"解析错误: {e}, 页面: {url}")

def threaded_crawler():
    threads = []
    for url_info in urls:
        t = threading.Thread(target=crawl_and_parse, args=(url_info["url"], url_info["category"]))
        threads.append(t)
        t.start()
        time.sleep(1)  # 避免过于频繁的请求
    for t in threads:
        t.join()

def write_to_current_directory(filename='斗鱼.txt'):
    """将所有数据写入到脚本所在目录的文本文件中，保持原始顺序"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(script_dir, filename)
    
    with open(full_path, 'w', encoding='utf-8') as file:
        for category, data in all_data.items():
            for title, url in data:
                # 保持原有顺序，仅在输出格式化时替换
                modified_url = url.replace("/", "")
                content = f"{title},http://douyu.dynv6.net:81/douyu.php?id={modified_url}"
                file.write(content + '\n')
        file_position = file.tell()
    logging.info(f"数据已写入文件: {full_path}, 最后写入位置: {file_position}字节")
    if os.path.exists(full_path) and os.path.getsize(full_path) > 0:
        logging.info("文件写入成功且非空。")
    else:
        logging.warning("文件可能未被写入或为空。")

if __name__ == "__main__":
    logging.info("开始爬取斗鱼主播信息...")
    threaded_crawler()
    logging.info("所有爬取任务完成，开始写入数据...")
    write_to_current_directory()
    logging.info("所有操作完成。")