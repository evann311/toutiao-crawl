import os
import re
import subprocess
import json
import time
import requests
import argparse
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service

from constant import DRIVER_PATH, MAX_PAGE, HEADLESS

# First-time (non-headless) options
options_first = webdriver.ChromeOptions()

# Subsequent (headless if HEADLESS=True) options
options_sub = webdriver.ChromeOptions()
if HEADLESS:
    options_sub.add_argument('--headless')


def get_channel_token(url):
    m = re.search(r"token/([^/?]+)", url)
    return m.group(1) if m else None


def get_channel_url_from_txt(path):
    print(f"Reading channel URLs from: {path}")
    urls = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                urls.append(line)
                print(f"Found channel URL: {line}")
    return urls


def download_file(url, filepath):
    print(f"Downloading to: {filepath}")
    try:
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    f.write(chunk)
            print(f"Downloaded: {filepath}")
        else:
            print(f"Failed to download {url}, status: {r.status_code}")
    except Exception as e:
        print(f"Exception while downloading {url}: {e}")


def merge_video_audio(video_path, audio_path, output_path, use_gpu=False):
    print(f"Merging video: {video_path} + audio: {audio_path} -> {output_path}")
    cmd_gpu = [
        "ffmpeg", "-hwaccel", "cuda", "-i", video_path, "-i", audio_path,
        "-c:v", "h264_nvenc", "-preset", "fast", "-c:a", "aac", "-b:a", "192k", output_path
    ]
    cmd_cpu = [
        "ffmpeg", "-i", video_path, "-i", audio_path,
        "-c:v", "copy", "-c:a", "aac", "-strict", "experimental", output_path
    ]
    cmd = cmd_gpu if use_gpu else cmd_cpu
    try:
        subprocess.run(cmd, check=True)
        print(f"Merged successfully: {output_path}")
    except subprocess.CalledProcessError as e:
        print(f"Merge error: {e}")


def crawl_and_download_from_channel(channel_url, use_gpu=False):
    """
    Hàm duy nhất để:
      1) Mở channel
      2) Scroll để lấy tất cả video
      3) Cho từng video -> mở trang video -> tìm link video/audio
      4) Tải về & merge ngay.
    """
    print(f"====> CRAWLING CHANNEL: {channel_url}")
    # Mở channel ở chế độ non-headless (theo logic ban đầu).
    driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options_first)
    driver.get(channel_url)
    time.sleep(5)  # Cho page load

    # Scroll để load hết video
    def scroll():
        print(f"Scrolling channel page up to {MAX_PAGE} times...")
        last_height = driver.execute_script("return document.body.scrollHeight")
        print(f"Initial scroll height: {last_height}")
        for page_idx in range(MAX_PAGE):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            print(f"Scroll #{page_idx+1} to bottom...")
            scrolled = False
            for attempt in range(5):
                time.sleep(3)
                new_height = driver.execute_script("return document.body.scrollHeight")
                print(f"Attempt #{attempt+1}, new height: {new_height}")
                if new_height != last_height:
                    last_height = new_height
                    scrolled = True
                    break
            if not scrolled:
                print("No further scroll progress, stopping.")
                break

    scroll()

    # Tìm các element video
    els = driver.find_elements(By.CLASS_NAME, 'profile-normal-video-card')
    print(f"Found {len(els)} video elements on channel page.")

    # Lấy token channel (nếu có)
    channel_token = get_channel_token(channel_url)
    if not channel_token:
        print(f"No token found in URL: {channel_url}, skip.")
        driver.quit()
        return

    # Tạo folder output
    result_dir = os.path.join(os.getcwd(), 'result')
    os.makedirs(result_dir, exist_ok=True)
    channel_dir = os.path.join(result_dir, channel_token)
    os.makedirs(channel_dir, exist_ok=True)

    # Tạo folder temp
    temp_dir = os.path.join(os.getcwd(), 'temp')
    os.makedirs(temp_dir, exist_ok=True)

    # Vòng lặp từng video -> tìm URL video/audio -> tải & merge
    for idx, el in enumerate(els, 1):
        print(f"\n--- Video element #{idx} ---")
        try:
            title_el = el.find_element(By.TAG_NAME, "a")
            title = title_el.get_attribute("title") or f"video_{idx}"
            time_el = el.find_element(By.CLASS_NAME, "feed-card-footer-time-cmp").text
            t = None
            try:
                # Đôi khi format thời gian có thể khác, tuỳ trang
                t = datetime.strptime(time_el, "%Y年%m月%d日")
            except:
                pass

            href = el.find_element(By.CLASS_NAME, "r-content") \
                     .find_element(By.TAG_NAME, "a") \
                     .get_attribute("href")
            publish_time = t.strftime("%Y-%m-%d") if t else "unknown_date"
            print(f"Title: {title}, URL: {href}, Time: {publish_time}")

            # Kiểm tra đã tải chưa
            out_file = os.path.join(channel_dir, f"{title}.mp4")
            if os.path.exists(out_file):
                print(f"--> {out_file} exists, skip.")
                continue

            # 1) Mở webdriver thứ 2 (có thể headless hoặc không) để tìm src
            print("Opening video page to find source URLs...")
            sub_driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options_sub)
            sub_driver.execute_cdp_cmd("Network.enable", {})
            sub_driver.get(href)
            time.sleep(5)

            # 2) Tìm thử single source (video_src)
            video_src = None
            try:
                vid_el = sub_driver.find_element(
                    By.XPATH,
                    '//*[@id="root"]/div/div[2]/div[1]/div/div[1]/ul/li[2]/div/video'
                )
                video_src = vid_el.get_attribute("src")
            except Exception as e:
                print(f"Cannot find <video> element or src: {e}")

            # Nếu có single-source và không phải blob:
            if video_src and not video_src.startswith("blob:"):
                print(f"Single-source detected: {video_src}")
                sub_driver.quit()
                # Tải trực tiếp
                download_file(video_src, out_file)
                continue

            # 3) Nếu là splitted source, duyệt log để tìm
            print("Splitted source suspected, checking logs...")
            v_url, a_url = None, None
            start_t = time.time()
            while True:
                logs = sub_driver.get_log("performance")
                for log in logs:
                    try:
                        msg = json.loads(log["message"])["message"]
                        if msg["method"] == "Network.requestWillBeSent":
                            req_url = msg["params"]["request"]["url"]
                            if "/media-video-avc1/" in req_url and not v_url:
                                v_url = req_url
                            elif "/media-audio-und-mp4a/" in req_url and not a_url:
                                a_url = req_url
                            if v_url and a_url:
                                break
                    except:
                        pass
                if v_url and a_url:
                    print(f"Video URL: {v_url}")
                    print(f"Audio URL: {a_url}")
                    break
                if time.time() - start_t > 30:
                    print("Timeout: could not find splitted source URLs.")
                    break
                time.sleep(1)

            sub_driver.quit()

            # 4) Tiến hành tải nếu đã có link
            if not v_url and not video_src:
                print("No valid video src found, skip this video.")
                continue

            if v_url and a_url:
                # Tải video và audio rồi merge
                tmp_v = os.path.join(temp_dir, f"{title}.mp4")
                tmp_a = os.path.join(temp_dir, f"{title}.m4a")
                download_file(v_url, tmp_v)
                download_file(a_url, tmp_a)

                merge_video_audio(tmp_v, tmp_a, out_file, use_gpu)

                # Xoá file tạm
                if os.path.exists(tmp_v):
                    os.remove(tmp_v)
                if os.path.exists(tmp_a):
                    os.remove(tmp_a)

        except Exception as e:
            print(f"Error on element #{idx}: {e}")

    # Đóng driver kênh
    driver.quit()
    print(f"Done crawling + downloading from channel: {channel_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-file", help="File with channel URLs")
    parser.add_argument('--gpu', action='store_true', help='Use GPU for encoding')
    args = parser.parse_args()

    if args.url_file:
        print("Starting...")
        channels = get_channel_url_from_txt(args.url_file)
        for ch_url in channels:
            crawl_and_download_from_channel(ch_url, use_gpu=args.gpu)
        print("Done.")
    else:
        print("No --url-file provided. Exiting.")
