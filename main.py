from selenium import webdriver
from selenium.webdriver.common.by import By
import os
import re
import subprocess
import json
import time
import requests
import argparse
from datetime import datetime
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service

from constant import *

# 1. Hàm lấy channel token từ channel_url
def get_channel_token(channel_url):
    """
    Lấy token của channel từ đường dẫn, ví dụ: https://www.xxx.com/token/abcdef
    => token = 'abcdef'
    """
    pattern = r"token/([^/?]+)"
    match = re.search(pattern, channel_url)
    if match:
        return match.group(1)
    return None

options = webdriver.ChromeOptions()
# options.add_argument('--headless')


def get_channel_url_from_txt(path):
    channel_urls = []
    with open(path, "r") as f:
        for line in f:
            channel_url = line.strip()
            channel_urls.append(channel_url)

    return channel_urls


def get_all_video_from_all_channels(channel_urls):
    video_dict = {}
    print("Starting the process to scrape videos from all channels...")
    for channel_url in channel_urls:
        # Mỗi channel_url sẽ chứa một dict con
        video_dict[channel_url] = {}

        print(f"Scraping videos from channel: {channel_url}")
        get_all_video_from_channel(channel_url, video_dict[channel_url])

    return video_dict


def get_all_video_from_channel(channel_url, video_dict={}):
    # Khởi tạo driver
    driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options)
    print("Chrome driver initialized.")

    print(f"Navigating to channel URL: {channel_url}")
    driver.get(channel_url)

    print("Waiting for the page to load...")
    time.sleep(10)

    def scroll_to_bottom():
        print("Starting to scroll down the page...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        print(f"Initial page height: {last_height}")

        for i in range(MAX_PAGE):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

            # Đợi trang load
            for attempt in range(5):
                time.sleep(3)
                new_height = driver.execute_script("return document.body.scrollHeight")

                print(f"Attempt {attempt + 1} to check if page height changed...")

                if new_height != last_height:
                    print(f"New page height after scrolling: {new_height}")
                    last_height = new_height
                    break
            else:
                print("Page height did not change after 5 attempts. Stopping scrolling.")
                break

    # Thực hiện scroll để tải hết video
    scroll_to_bottom()

    print("Finding video elements on the page...")
    elements = driver.find_elements(By.CLASS_NAME, 'profile-normal-video-card')
    print(f"Number of videos found: {len(elements)}")

    # Lấy token channel
    channel_token = get_channel_token(channel_url)
    if not channel_token:
        print(f"Không tìm được token cho channel: {channel_url}, bỏ qua.")
        driver.quit()
        return

    # Tạo thư mục result/<channel_token> nếu chưa có
    result_dir = os.path.join(os.getcwd(), 'result')
    channel_folder = os.path.join(result_dir, channel_token)
    os.makedirs(channel_folder, exist_ok=True)

    # Duyệt qua từng video
    for index, element in enumerate(elements, start=1):
        # if index == 2:
        #     break
        print(f"Processing video element {index}...")
        try:
            # Lấy tên video
            video_name = element.find_element(By.TAG_NAME, "a").get_attribute("title")

            # Lấy thời gian đăng
            video_time = element.find_elements(By.CLASS_NAME, "feed-card-footer-time-cmp")[0].text
            video_time = datetime.strptime(video_time, "%Y年%m月%d日")

            # Lấy URL video
            video_url = element.find_elements(By.CLASS_NAME, "r-content")[0] \
                               .find_element(By.TAG_NAME, "a") \
                               .get_attribute("href")

            # 2. Kiểm tra file output tương ứng trong channel_folder
            output_filepath = os.path.join(channel_folder, f"{video_name}.mp4")
            if os.path.exists(output_filepath):
                print(f"Video '{video_name}' đã tồn tại trong '{channel_folder}', bỏ qua.")
                continue

            # Thêm video vào video_dict nếu chưa có file
            video_dict[video_name] = {
                'url': video_url,
                'time': video_time.strftime("%Y-%m-%d")
            }
            print(f"Added video '{video_name}' to dictionary.")
        except Exception as e:
            print(f"Error when processing video element {index}: {e}")

    print("Closing the Chrome driver.")
    driver.quit()


def get_video_and_audio_url(video_dict):
    if HEADLESS:
        options.add_argument('--headless')
    
    for channel in video_dict.values():
        for video in channel.values():
            perf_logging_prefs = {"performance": "ALL"}
            options.set_capability("goog:loggingPrefs", perf_logging_prefs)

            driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options)
            driver.execute_cdp_cmd("Network.enable", {})

            driver.get(video['url'])

            video_url = None
            audio_url = None

            while not (video_url and audio_url):
                try:
                    logs = driver.get_log("performance")
                    for log in logs:
                        message = json.loads(log["message"])["message"]
                        if message["method"] == "Network.requestWillBeSent":
                            url = message["params"]["request"]["url"]
                            if "/media-video-avc1/" in url and not video_url:
                                video_url = url
                            elif "/media-audio-und-mp4a/" in url and not audio_url:
                                audio_url = url

                            if video_url and audio_url:
                                break
                except Exception as e:
                    print(f"Error getting logs: {e}")

            print(f"url: {video['url']}")
            print(f"Video URL: {video_url}")
            print(f"Audio URL: {audio_url}")

            video['video_url'] = video_url
            video['audio_url'] = audio_url

            driver.quit()
            time.sleep(2)


def download_file(url, filename):
    """
    Download một file từ URL tương ứng và lưu về filename.
    """
    print(f"Downloading: {filename}")
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        print(f"Downloaded successfully: {filename}")
    else:
        print(f"Failed to download {url}: {response.status_code}")


def merge_video_audio(video_path, audio_path, output_path, use_gpu):
    """
    Dùng ffmpeg để ghép video + audio thành file mp4 hoàn chỉnh.
    Nếu use_gpu=True, dùng GPU (CUDA) để encoding, nếu không thì copy stream.
    """
    command_gpu = [
        "ffmpeg",
        "-hwaccel", "cuda",        
        "-i", video_path,
        "-i", audio_path,
        "-c:v", "h264_nvenc",      
        "-preset", "fast",         
        "-c:a", "aac",             # Sử dụng AAC cho audio
        "-b:a", "192k",            # Bitrate của audio
        output_path
    ]
    command_cpu = [
        "ffmpeg",
        "-i", video_path,
        "-i", audio_path,
        "-c:v", "copy",
        "-c:a", "aac",
        "-strict", "experimental",
        output_path
    ]
    command = command_gpu if use_gpu else command_cpu
    try:
        subprocess.run(command, check=True)
        print(f"Video successfully merged and saved at: {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Error merging video and audio: {e}")


def download_video_and_audio(video_dict, use_gpu=False):
    """
    3. Duyệt qua video_dict, tải video/audio về và merge lại thành file hoàn chỉnh.
    """
    pattern = r"token/([^/?]+)"

    temp_dir = os.path.join(os.getcwd(), 'temp')
    result_dir = os.path.join(os.getcwd(), 'result')
    os.makedirs(temp_dir, exist_ok=True)
    os.makedirs(result_dir, exist_ok=True)

    for channel_url, channel in video_dict.items():
        match = re.search(pattern, channel_url)
        if not match:
            print(f"Token not found in {channel_url}")
            continue

        channel_folder = os.path.join(result_dir, match.group(1))
        os.makedirs(channel_folder, exist_ok=True)

        for video_key, video_data in channel.items():
            # Đường dẫn file output (đã check khi collect url).
            output_video_path = os.path.join(channel_folder, f"{video_key}.mp4")

            # Nếu vì lý do nào đó vẫn trùng, ta có thể skip thêm lần nữa
            if os.path.exists(output_video_path):
                print(f"File {output_video_path} đã tồn tại, bỏ qua download.")
                continue

            # Tạo đường dẫn file tạm
            temp_video_path = os.path.join(temp_dir, f"{video_key}.mp4")
            temp_audio_path = os.path.join(temp_dir, f"{video_key}.m4a")

            # Download video và audio
            download_file(video_data['video_url'], temp_video_path)
            download_file(video_data['audio_url'], temp_audio_path)

            # Merge
            merge_video_audio(temp_video_path, temp_audio_path, output_video_path, use_gpu)

            # Xoá file tạm
            os.remove(temp_video_path)
            os.remove(temp_audio_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-file", help="URL file (chứa list channel) để scrape")
    parser.add_argument('--gpu', action='store_true', help='Use GPU for encoding')
    args = parser.parse_args()

    if args.url_file:
        channel_urls = get_channel_url_from_txt(args.url_file)
        # Lấy danh sách video cho tất cả channel
        video_dict = get_all_video_from_all_channels(channel_urls)
        # Lấy link video/audio thật
        get_video_and_audio_url(video_dict)
        # Tải và merge video
        download_video_and_audio(video_dict, args.gpu)
