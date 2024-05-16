import requests
from bs4 import BeautifulSoup
import os
from collections import defaultdict
import time
import threading
import logging

# 配置日志系统，记录爬虫运行信息
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 定义浏览器用户代理字符串，模拟浏览器发送请求
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'

# 创建网络请求会话对象，用于发送请求
session = requests.Session()
session.headers.update({'User-Agent': USER_AGENT})

# 初始化待爬取的URL列表，每个URL包含对应的类别信息
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

# 使用defaultdict存储爬取到的数据，以类别为键，主播信息为值
all_data = defaultdict(list)
# 使用锁来处理多线程间的并发写入问题
lock = threading.Lock()

def crawl_and_parse(url, category):
    """
    爬取并解析指定URL的页面，提取主播信息。
    
    :param url: 需要爬取的页面URL
    :param category: 页面所属的类别
    """
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
        # 提取页面主播信息
        page_data = [(link.find(class_="DyListCover-intro")['title'], link['href']) for link in links]
        with lock:
            all_data[category].extend(page_data)
        logging.info(f"成功爬取页面: {url}, 收录{len(page_data)}个主播")
    except Exception as e:
        logging.error(f"解析错误: {e}, 页面: {url}")

def threaded_crawler():
    """
    使用多线程爬取所有URL列表中的页面。
    """
    threads = []
    for url_info in urls:
        t = threading.Thread(target=crawl_and_parse, args=(url_info["url"], url_info["category"]))
        threads.append(t)
        t.start()
        time.sleep(1)  # 控制线程启动间隔，避免请求过于集中

    # 等待所有线程完成
    for t in threads:
        t.join()

def write_to_current_directory(filename='douyu.txt'):
    """
    将爬取到的数据写入当前目录下的文本文件中。

    :param filename: 输出文件名，默认为'douyu.txt'
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))  # 获取当前脚本所在目录
    full_path = os.path.join(script_dir, filename)

    # 初始化输出文件，写入标题行
    with open(full_path, 'w', encoding='utf-8') as file:
        file.write("🐠斗鱼直播🐠,#genre#\n")

    # 追加数据到文件
    with open(full_path, 'a', encoding='utf-8') as file:
        for category, data in all_data.items():
            for title, url in data:
                # 格式化输出数据
                modified_url = url.replace("/", "")
                content = f"{title},http://192.168.1.43:81/douyu.php?id={modified_url}"
                file.write(f"{content}\n")
        file_position = file.tell()  # 获取文件写入位置
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