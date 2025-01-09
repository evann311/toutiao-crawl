import os
import re
import subprocess
import json
import time
import requests
import argparse
import threading
import logging
from datetime import datetime
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from constant import DRIVER_PATH, MAX_PAGE, HEADLESS, MAX_THREADS

# Configure logging
log_formatter = logging.Formatter(
    fmt='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("Downloader")
logger.setLevel(logging.DEBUG)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# File handler
file_handler = logging.FileHandler("downloader.log", encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)

# First-time (non-headless) options
options_first = webdriver.ChromeOptions()

# Subsequent (headless if HEADLESS=True) options
options_sub = webdriver.ChromeOptions()
if HEADLESS:
    options_sub.add_argument('--headless')


def sanitize_filename(filename):
    sanitized = filename.replace("，", "")
    sanitized = sanitized.replace(" ", "")
    # Remove any other characters that are invalid in filenames
    sanitized = re.sub(r'[\\/:"*?<>|]+', '', sanitized)
    return sanitized


def get_channel_token(url):
    # Assuming the channel URL contains 'token/<token_value>'
    m = re.search(r"token/([^/?]+)", url)
    return m.group(1) if m else None


def get_channel_url_from_txt(path):
    logger.info(f"Reading channel URLs from: {path}")
    urls = []
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    urls.append(line)
                    logger.debug(f"Found channel URL: {line}")
    except Exception as e:
        logger.error(f"Error reading URL file {path}: {e}")
    return urls


def download_file(url, filepath):
    logger.info(f"Downloading to: {filepath}")
    try:
        r = requests.get(url, stream=True)
        if r.status_code == 200:
            with open(filepath, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
            logger.info(f"Downloaded: {filepath}")
        else:
            logger.warning(f"Failed to download {url}, status: {r.status_code}")
    except Exception as e:
        logger.error(f"Exception while downloading {url}: {e}")


def download_merge_cleanup(v_url, a_url, out_file, temp_v, temp_a, use_gpu=False, video_id=None, downloaded_manager=None):
    """
    Downloads video and audio, merges them, and cleans up temporary files.
    """
    try:
        download_file(v_url, temp_v)
        download_file(a_url, temp_a)
        merge_video_audio(temp_v, temp_a, out_file, use_gpu)
        # After successful download and merge, add to downloaded_manager
        if downloaded_manager and video_id:
            downloaded_manager.add_downloaded(video_id)
    except Exception as e:
        logger.error(f"Error during download and merge: {e}")
    finally:
        try:
            if os.path.exists(temp_v):
                os.remove(temp_v)
                logger.debug(f"Removed temporary video file: {temp_v}")
            if os.path.exists(temp_a):
                os.remove(temp_a)
                logger.debug(f"Removed temporary audio file: {temp_a}")
        except Exception as cleanup_error:
            logger.error(f"Error during cleanup: {cleanup_error}")


def merge_video_audio(video_path, audio_path, output_path, use_gpu=False):
    logger.info(f"Merging video: {video_path} + audio: {audio_path} -> {output_path}")
    rmd_gpu = [
        "ffmpeg", "-hwaccel", "cuda", "-i", video_path, "-i", audio_path,
        "-c:v", "h264_nvenc", "-preset", "fast", "-c:a", "aac", "-b:a", "192k", output_path
    ]
    cmd_cpu = [
        "ffmpeg", "-i", video_path, "-i", audio_path,
        "-c:v", "copy", "-c:a", "aac", "-strict", "experimental", output_path
    ]
    cmd = rmd_gpu if use_gpu else cmd_cpu
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        logger.info(f"Merged successfully: {output_path}")
    except subprocess.CalledProcessError as e:
        logger.error(f"Merge error: {e}")


class TaskQueue:
    def __init__(self, max_threads):
        self.semaphore = threading.Semaphore(max_threads)
        self.lock = threading.Lock()

    def worker(self, func, args):
        try:
            logger.debug(f"Starting task: {func.__name__} with args: {args}")
            func(*args)
            logger.debug(f"Completed task: {func.__name__} with args: {args}")
        except Exception as e:
            logger.error(f"Error in task {func.__name__} with args {args}: {e}")
        finally:
            self.semaphore.release()

    def add_task(self, func, *args):
        self.semaphore.acquire()
        thread = threading.Thread(target=self.worker, args=(func, args), name=f"Worker-{int(time.time()*1000)}")
        thread.daemon = True
        thread.start()
        logger.debug(f"Started new thread: {thread.name}")

    def wait_completion(self):
        # Wait until all semaphore permits are released
        while self.semaphore._value < self.semaphore._initial_value:
            time.sleep(1)
        logger.debug("All tasks have been completed.")


class DownloadedManager:
    def __init__(self, filepath):
        self.filepath = filepath
        self.downloaded_ids = set()
        self.lock = threading.Lock()
        self._load_downloaded()

    def _load_downloaded(self):
        if os.path.exists(self.filepath):
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    for line in f:
                        video_id = line.strip()
                        if video_id:
                            self.downloaded_ids.add(video_id)
                logger.debug(f"Loaded {len(self.downloaded_ids)} downloaded video IDs.")
            except Exception as e:
                logger.error(f"Error loading downloaded IDs from {self.filepath}: {e}")
        else:
            logger.debug(f"No existing downloaded file found at {self.filepath}.")

    def is_downloaded(self, video_id):
        with self.lock:
            return video_id in self.downloaded_ids

    def add_downloaded(self, video_id):
        with self.lock:
            if video_id not in self.downloaded_ids:
                self.downloaded_ids.add(video_id)
                try:
                    with open(self.filepath, "a", encoding="utf-8") as f:
                        f.write(f"{video_id}\n")
                    logger.debug(f"Added video ID to downloaded list: {video_id}")
                except Exception as e:
                    logger.error(f"Error writing video ID {video_id} to {self.filepath}: {e}")


def extract_video_id(url):
    try:
        parsed_url = urlparse(url)
        path_segments = parsed_url.path.strip("/").split("/")
        if len(path_segments) >= 2 and path_segments[-2] == 'video':
            video_id = path_segments[-1]
            return video_id
        else:
            # Handle URLs without trailing slash
            m = re.search(r"/video/(\d+)", parsed_url.path)
            if m:
                return m.group(1)
    except Exception as e:
        logger.error(f"Error extracting video ID from URL {url}: {e}")
    return None


def crawl_and_download_from_channel(channel_url, task_queue, downloaded_manager, use_gpu=False):
    """
    Hàm duy nhất để:
      1) Mở channel
      2) Scroll để lấy tất cả video
      3) Cho từng video -> mở trang video -> tìm link video/audio
      4) Tải về & merge ngay.
    """
    logger.info(f"====> CRAWLING CHANNEL: {channel_url}")
    try:
        # Open channel in non-headless mode (as per initial logic).
        driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options_first)
        driver.get(channel_url)
        logger.debug("Opened channel URL in browser.")
        time.sleep(5)  # Wait for page to load

        # Scroll to load all videos
        def scroll():
            logger.info(f"Scrolling channel page up to {MAX_PAGE} times...")
            last_height = driver.execute_script("return document.body.scrollHeight")
            logger.debug(f"Initial scroll height: {last_height}")
            for page_idx in range(MAX_PAGE):
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                logger.debug(f"Scroll #{page_idx+1} to bottom...")
                scrolled = False
                for attempt in range(5):
                    time.sleep(3)
                    new_height = driver.execute_script("return document.body.scrollHeight")
                    logger.debug(f"Attempt #{attempt+1}, new height: {new_height}")
                    if new_height != last_height:
                        last_height = new_height
                        scrolled = True
                        break
                if not scrolled:
                    logger.info("No further scroll progress, stopping.")
                    break

        scroll()

        # Find video elements
        els = driver.find_elements(By.CLASS_NAME, 'feed-card-video-multi-item')
        logger.info(f"Found {len(els)} video elements on channel page.")

        # Get channel token (if any)
        channel_token = get_channel_token(channel_url)
        if not channel_token:
            logger.warning(f"No token found in URL: {channel_url}, skip.")
            driver.quit()
            return

        # Create output folder
        result_dir = os.path.join(os.getcwd(), 'result')
        os.makedirs(result_dir, exist_ok=True)
        channel_dir = os.path.join(result_dir, channel_token)
        os.makedirs(channel_dir, exist_ok=True)
        logger.debug(f"Created channel directory: {channel_dir}")

        # Create temp folder
        temp_dir = os.path.join(os.getcwd(), 'temp')
        os.makedirs(temp_dir, exist_ok=True)
        logger.debug(f"Created temp directory: {temp_dir}")

        # Iterate through each video element to find video/audio URLs and download
        for idx, el in enumerate(els, 1):
            logger.info(f"\n--- Video element #{idx} ---")
            try:
                title_el = el.find_element(By.TAG_NAME, "a")
                title = title_el.get_attribute("title") or f"video_{idx}"
                # Sanitize title to remove spaces and invalid characters
                sanitized_title = sanitize_filename(title)
                time_el = el.find_element(By.CLASS_NAME, "feed-card-footer-time-cmp").text
                t = None
                try:
                    # Sometimes the time format might differ, adjust as per the site
                    t = datetime.strptime(time_el, "%Y年%m月%d日")
                except ValueError:
                    logger.debug(f"Time format not matched for '{time_el}'")
                    pass

                href = el.find_element(By.CLASS_NAME, "feed-card-cover") \
                         .find_element(By.TAG_NAME, "a") \
                         .get_attribute("href")
                publish_time = t.strftime("%Y-%m-%d") if t else "unknown_date"
                logger.info(f"Title: {title}, URL: {href}, Time: {publish_time}")

                # Extract video ID from URL (assuming it's the numeric part after /video/)
                video_id = extract_video_id(href)
                if not video_id:
                    logger.warning(f"Could not extract video ID from URL: {href}, skip.")
                    continue

                # Check if already downloaded using DownloadedManager
                if downloaded_manager.is_downloaded(video_id):
                    logger.info(f"--> Video ID {video_id} already downloaded, skip.")
                    continue

                # Check if the output file already exists
                out_file = os.path.join(channel_dir, f"{sanitized_title}.mp4")
                if os.path.exists(out_file):
                    logger.info(f"--> {out_file} exists, skipping download.")
                    # Even if the file exists, ensure the video ID is recorded
                    downloaded_manager.add_downloaded(video_id)
                    continue

                # 1) Open a second webdriver (can be headless or not) to find src
                options_sub.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

                logger.info("Opening video page to find source URLs...")
                sub_driver = webdriver.Chrome(service=Service(DRIVER_PATH), options=options_sub)
                sub_driver.execute_cdp_cmd("Network.enable", {})
                sub_driver.get(href)
                logger.debug("Opened video URL in sub-browser.")
                time.sleep(5)

                try:
                    WebDriverWait(sub_driver, 10).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="root"]/div/div[2]/div[1]/div/div[1]/ul/li[2]/div/video'))
                    )
                    logger.debug("Video element found on the page.")
                except Exception as e:
                    logger.warning(f"Video element not found: {e}")
                    sub_driver.quit()
                    continue

                # 2) Try finding single source (video_src)
                video_src = None
                try:
                    vid_el = sub_driver.find_element(
                        By.XPATH,
                        '//*[@id="root"]/div/div[2]/div[1]/div/div[1]/ul/li[2]/div/video'
                    )
                    video_src = vid_el.get_attribute("src")
                    logger.debug(f"Single video src found: {video_src}")
                except Exception as e:
                    logger.warning(f"Cannot find <video> element or src: {e}")

                # If single-source and not blob:
                if video_src and not video_src.startswith("blob:"):
                    logger.info(f"Single-source detected: {video_src}")
                    sub_driver.quit()
                    # Add to task queue for direct download
                    task_queue.add_task(download_file, video_src, out_file)
                    # After successful download, add to downloaded_manager
                    downloaded_manager.add_downloaded(video_id)
                    continue

                # 3) If splitted source, browse logs to find
                logger.info("Splitted source suspected, checking logs...")
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
                                    logger.debug(f"Found video URL in logs: {v_url}")
                                elif "/media-audio-und-mp4a/" in req_url and not a_url:
                                    a_url = req_url
                                    logger.debug(f"Found audio URL in logs: {a_url}")
                                if v_url and a_url:
                                    break
                        except Exception as e:
                            logger.debug(f"Error parsing log entry: {e}")
                    if v_url and a_url:
                        logger.info(f"Video URL: {v_url}")
                        logger.info(f"Audio URL: {a_url}")
                        break
                    if time.time() - start_t > 30:
                        logger.warning("Timeout: could not find splitted source URLs.")
                        break
                    time.sleep(1)

                sub_driver.quit()

                # 4) Proceed to download if links are found
                if not v_url and not video_src:
                    logger.warning("No valid video src found, skip this video.")
                    continue

                if v_url and a_url:
                    # Download video and audio then merge by adding to task queue
                    tmp_v = os.path.join(temp_dir, f"{sanitized_title}.mp4")
                    tmp_a = os.path.join(temp_dir, f"{sanitized_title}.m4a")
                    task_queue.add_task(
                        download_merge_cleanup,
                        v_url,
                        a_url,
                        out_file,
                        tmp_v,
                        tmp_a,
                        use_gpu,
                        video_id=video_id,
                        downloaded_manager=downloaded_manager
                    )

            except Exception as e:
                logger.error(f"Error on element #{idx}: {e}")

    except Exception as e:
        logger.error(f"Failed to crawl channel {channel_url}: {e}")
    finally:
        driver.quit()
        logger.info(f"Done crawling + downloading from channel: {channel_url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-file", help="File with channel URLs")
    parser.add_argument('--gpu', action='store_true', help='Use GPU for encoding')
    args = parser.parse_args()

    if args.url_file:
        logger.info("Starting downloader...")
        channels = get_channel_url_from_txt(args.url_file)

        # Initialize TaskQueue with desired maximum threads
        task_queue = TaskQueue(max_threads=MAX_THREADS)

        # Initialize DownloadedManager with the path to the downloaded IDs file
        downloaded_manager = DownloadedManager(filepath="downloaded.txt")

        for ch_url in channels:
            crawl_and_download_from_channel(ch_url, task_queue, downloaded_manager, use_gpu=args.gpu)

        # Wait for all tasks to complete
        task_queue.wait_completion()
        logger.info("All downloads and merges are complete.")
    else:
        logger.warning("No --url-file provided. Exiting.")
