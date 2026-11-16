# SEO Crawler using Crawlee / Playwright / MongoDB / Beanstalkd

# Dependencies - - - - - -

# Update package lists and install system prerequisites
!apt-get update -qq
!apt-get install -y gnupg curl wget beanstalkd

# Add official MongoDB 8.0 repository for Ubuntu 22.04
!curl -fsSL https://pgp.mongodb.com/server-8.0.asc | \
    gpg --dearmor -o /usr/share/keyrings/mongodb-server-8.0.gpg

!echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu jammy/mongodb-org/8.0 multiverse" \
    > /etc/apt/sources.list.d/mongodb-org-8.0.list

!apt-get update -qq
!apt-get install -y mongodb-org

# Install Crawlee with Playwright and adaptive crawler dependencies including parsel
!pip install -q "crawlee[playwright,adaptive-crawler]" beautifulsoup4 pymongo greenstalk lxml parsel

# Install Chromium browser
!playwright install chromium

# Install required system dependencies
!playwright install-deps chromium

print("All dependencies installed successfully.")

#====================================================================

# MongoDB and Beanstalkd - - - - - - -

import os
import subprocess
import time
from pathlib import Path

# MongoDB directories
Path("/content/mongodb/data").mkdir(parents=True, exist_ok=True)
Path("/content/mongodb/logs").mkdir(parents=True, exist_ok=True)
Path("/content/beanstalkd").mkdir(parents=True, exist_ok=True)

# Start MongoDB
subprocess.run([
    "mongod",
    "--dbpath", "/content/mongodb/data",
    "--logpath", "/content/mongodb/logs/mongod.log",
    "--bind_ip", "127.0.0.1",
    "--port", "27017",
    "--fork"
], check=True)

# Start Beanstalkd
beanstalk_process = subprocess.Popen([
    "beanstalkd",
    "-l", "127.0.0.1",
    "-p", "11300",
    "-b", "/content/beanstalkd"
])

time.sleep(2)

# Verify services
from pymongo import MongoClient
import greenstalk

mongo_test = MongoClient(
    "mongodb://127.0.0.1:27017",
    serverSelectionTimeoutMS=5000
)
mongo_test.admin.command("ping")

beanstalk_test = greenstalk.Client(
    ("127.0.0.1", 11300)
)

print("MongoDB: RUNNING")
print("Beanstalkd: RUNNING")

beanstalk_test.close()
mongo_test.close()

# ==============================================================

# Crawler Implementation - - - - - - -


import asyncio
import json
import logging
import random
import re
import uuid

from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit, parse_qsl, urlencode

from pymongo import MongoClient, ASCENDING, ReturnDocument
from bs4 import BeautifulSoup
import greenstalk

from crawlee.crawlers import (
    AdaptivePlaywrightCrawler,
    AdaptivePlaywrightCrawlingContext,
)
from crawlee.storages import RequestQueue
from crawlee import ConcurrencySettings


# ============================================================
# 1. CONFIGURATION
# ============================================================

class Config:

    MONGO_URI = "mongodb://127.0.0.1:27017"
    DB_NAME = "crawler_db"

    BEANSTALK_HOST = "127.0.0.1"
    BEANSTALK_PORT = 11300
    BEANSTALK_TUBE = "crawler_jobs"

    MAX_PAGES = 50
    MAX_RETRIES = 2
    CONCURRENCY = 10

    MIN_DELAY = 0.2
    MAX_DELAY = 0.5

    OUTPUT = Path("/content/crawler_output")
    HTML = OUTPUT / "html"
    LOGS = OUTPUT / "logs"

    RESPECT_ROBOTS = True


Config.HTML.mkdir(parents=True, exist_ok=True)
Config.LOGS.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(Config.LOGS / "crawler.log")
    ],
    force=True
)

logger = logging.getLogger("crawler")


# ============================================================
# 3. URL UTILITIES
# ============================================================

class URLManager:

    @staticmethod
    def normalize(url):

        try:
            url = url.strip()

            if not url:
                return None

            if "://" not in url:
                url = "https://" + url

            parts = urlsplit(url)

            if parts.scheme.lower() not in ("http", "https"):
                return None

            if not parts.hostname:
                return None

            scheme = parts.scheme.lower()
            hostname = parts.hostname.lower()

            port = parts.port

            netloc = hostname

            if port and not (
                (scheme == "http" and port == 80)
                or (scheme == "https" and port == 443)
            ):
                netloc = f"{hostname}:{port}"

            path = parts.path or "/"

            if path != "/":
                path = path.rstrip("/")

            query = urlencode(sorted(
                (k, v)
                for k, v in parse_qsl(parts.query)
                if not k.lower().startswith("utm_")
            ))

            return urlunsplit((
                scheme,
                netloc,
                path,
                query,
                ""
            ))

        except Exception:
            logger.exception("URL normalization error")
            return None

    @staticmethod
    def same_domain(url, domain):

        try:
            hostname = urlsplit(url).hostname
            return hostname and hostname.lower() == domain.lower()
        except Exception:
            return False

    @staticmethod
    def validate_input(value):

        url = URLManager.normalize(value)

        if not url:
            raise ValueError("Invalid website URL")

        domain = urlsplit(url).hostname.lower()

        return domain, url


# ============================================================
# 4. MONGODB CLASS
# ============================================================

class MongoDB:

    def __init__(self):

        self.client = MongoClient(
            Config.MONGO_URI,
            serverSelectionTimeoutMS=5000
        )

        self.client.admin.command("ping")

        self.db = self.client[Config.DB_NAME]

        self.domains = self.db["domains"]
        self.pages = self.db["pages"]
        self.jobs = self.db["crawl_jobs"]

        # Indexes
        self.domains.create_index(
            [("domain", ASCENDING)],
            unique=True
        )

        self.pages.create_index(
            [("crawl_id", ASCENDING), ("url", ASCENDING)],
            unique=True
        )

        self.jobs.create_index(
            [("job_id", ASCENDING)],
            unique=True
        )

        self.jobs.create_index([
            ("status", ASCENDING),
            ("created_at", ASCENDING)
        ])

        logger.info("MongoDB connected")

    def create_job(self, domain, start_url):

        crawl_id = uuid.uuid4().hex
        job_id = uuid.uuid4().hex

        now = datetime.now(timezone.utc)

        job = {
            "job_id": job_id,
            "crawl_id": crawl_id,
            "domain": domain,
            "start_url": start_url,
            "status": "queued",
            "created_at": now,
            "started_at": None,
            "completed_at": None,
            "pages_saved": 0,
            "failed_requests": 0,
            "retry_events": 0,
            "next_page_number": 0
        }

        self.jobs.insert_one(job)

        self.domains.update_one(
            {"domain": domain},
            {
                "$set": {
                    "domain": domain,
                    "latest_crawl_id": crawl_id,
                    "status": "queued",
                    "updated_at": now
                },
                "$setOnInsert": {
                    "created_at": now
                },
                "$inc": {
                    "crawl_count": 1
                }
            },
            upsert=True
        )

        return job

    def claim_job(self, job_id):

        return self.jobs.find_one_and_update(
            {
                "job_id": job_id,
                "status": "queued"
            },
            {
                "$set": {
                    "status": "publishing"
                }
            },
            return_document=ReturnDocument.AFTER
        )

    def mark_published(self, job_id):

        self.jobs.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "status": "published",
                    "published_at": datetime.now(timezone.utc)
                }
            }
        )

    def return_to_queue(self, job_id, error):

        self.jobs.update_one(
            {"job_id": job_id},
            {
                "$set": {
                    "status": "queued",
                    "error": str(error)
                }
            }
        )

    def mark_running(self, job_id):

        result = self.jobs.update_one(
            {
                "job_id": job_id,
                "status": "published"
            },
            {
                "$set": {
                    "status": "running",
                    "started_at": datetime.now(timezone.utc)
                }
            }
        )

        return result.modified_count == 1

    def update_job(self, job_id, values):

        self.jobs.update_one(
            {"job_id": job_id},
            {"$set": values}
        )

    def update_domain(self, domain, crawl_id, status):

        self.domains.update_one(
            {
                "domain": domain,
                "latest_crawl_id": crawl_id
            },
            {
                "$set": {
                    "status": status,
                    "updated_at": datetime.now(timezone.utc)
                }
            }
        )

    def next_page_number(self, job_id):

        result = self.jobs.find_one_and_update(
            {"job_id": job_id},
            {"$inc": {"next_page_number": 1}},
            return_document=ReturnDocument.AFTER
        )

        return result["next_page_number"]

    def save_page(self, record):

        self.pages.update_one(
            {
                "crawl_id": record["crawl_id"],
                "url": record["url"]
            },
            {"$set": record},
            upsert=True
        )

        self.jobs.update_one(
            {"job_id": record["job_id"]},
            {"$inc": {"pages_saved": 1}}
        )

    def increment(self, job_id, field):

        self.jobs.update_one(
            {"job_id": job_id},
            {"$inc": {field: 1}}
        )

    def display_results(self, crawl_id):

        print("\n========== CRAWL JOB ==========")

        job = self.jobs.find_one(
            {"crawl_id": crawl_id},
            {"_id": 0}
        )

        print(json.dumps(job, default=str, indent=2))

        print("\n========== CRAWLED PAGES ==========")

        pages = self.pages.find(
            {"crawl_id": crawl_id},
            {"_id": 0}
        ).sort("page_number", ASCENDING)

        for page in pages:
            print(json.dumps(page, default=str, indent=2))

    def clear(self):

        answer = input(
            "Type DELETE to remove all MongoDB crawler data: "
        )

        if answer != "DELETE":
            print("Cancelled.")
            return

        self.domains.delete_many({})
        self.pages.delete_many({})
        self.jobs.delete_many({})

        print("MongoDB crawler collections cleared.")

    def close(self):
        self.client.close()


# ============================================================
# 5. LISTENER CLASS
# ============================================================

class Listener:

    def __init__(self, mongo):
        self.mongo = mongo

    def submit(self, website):

        domain, start_url = URLManager.validate_input(website)

        job = self.mongo.create_job(domain, start_url)

        logger.info("Listener created job: %s", job["job_id"])

        return job


# ============================================================
# 6. BEANSTALKD CLASS
# ============================================================

class Beanstalkd:

    def __init__(self):

        self.client = greenstalk.Client(
            (Config.BEANSTALK_HOST, Config.BEANSTALK_PORT),
            use=Config.BEANSTALK_TUBE,
            watch=Config.BEANSTALK_TUBE
        )

    def publish(self, payload):

        self.client.put(json.dumps(payload))

    def receive(self, timeout=5):

        try:
            return self.client.reserve(timeout=timeout)
        except greenstalk.TimedOutError:
            return None

    def delete(self, job):
        self.client.delete(job)

    def bury(self, job):
        self.client.bury(job)

    def close(self):
        self.client.close()


# ============================================================
# 7. PUBLISHER CLASS
# ============================================================

class Publisher:

    def __init__(self, mongo, queue):
        self.mongo = mongo
        self.queue = queue

    def publish(self, job_id):

        # MongoDB is checked before queue publishing.
        job = self.mongo.claim_job(job_id)

        if not job:
            logger.warning("Job is not queued in MongoDB")
            return False

        payload = {
            "job_id": job["job_id"],
            "crawl_id": job["crawl_id"],
            "domain": job["domain"],
            "start_url": job["start_url"]
        }

        try:

            self.queue.publish(payload)

            self.mongo.mark_published(job_id)

            logger.info("Publisher sent job to Beanstalkd")

            return True

        except Exception as error:

            logger.exception("Publisher error")

            self.mongo.return_to_queue(job_id, error)

            return False


# ============================================================
# 8. SEO PARSER CLASS
# ============================================================

class SEOParser:

    @staticmethod
    def extract(html):

        soup = BeautifulSoup(html, "html.parser")

        title_tag = soup.find("title")

        title = (
            title_tag.get_text(" ", strip=True)
            if title_tag else None
        )

        description_tag = soup.find(
            "meta",
            attrs={"name": re.compile("^description$", re.I)}
        )

        description = (
            description_tag.get("content")
            if description_tag else None
        )

        canonical_tag = soup.find(
            "link",
            rel=lambda value: value and
            "canonical" in (
                value if isinstance(value, list)
                else value.split()
            )
        )

        canonical = (
            canonical_tag.get("href")
            if canonical_tag else None
        )

        headings = {}

        for level in ["h1", "h2", "h3", "h4"]:

            headings[level] = [
                tag.get_text(" ", strip=True)
                for tag in soup.find_all(level)
            ]

        page_name = (
            headings["h1"][0]
            if headings["h1"]
            else title
        )

        return {
            "page_name": page_name,
            "title": title,
            "meta_description": description,
            "canonical_url": canonical,
            "h1": headings["h1"],
            "h2": headings["h2"],
            "h3": headings["h3"],
            "h4": headings["h4"]
        }


# ============================================================
# 9. STORAGE CLASS
# ============================================================

class Storage:

    def __init__(self, mongo):
        self.mongo = mongo

    def save(self, job, url, html, status_code):

        page_number = self.mongo.next_page_number(
            job["job_id"]
        )

        safe_domain = re.sub(
            r"[^a-zA-Z0-9.-]",
            "_",
            job["domain"]
        )

        folder = (
            Config.HTML
            / safe_domain
            / job["crawl_id"]
        )

        folder.mkdir(parents=True, exist_ok=True)

        file_path = folder / f"page_{page_number:03d}.html"

        # Save original HTML string.
        file_path.write_text(html, encoding="utf-8")

        seo = SEOParser.extract(html)

        record = {
            "domain": job["domain"],
            "crawl_id": job["crawl_id"],
            "job_id": job["job_id"],
            "page_number": page_number,
            "url": url,
            **seo,
            "http_status_code": status_code,
            "html_file_path": str(file_path),
            "crawled_at": datetime.now(timezone.utc)
        }

        self.mongo.save_page(record)

        return record


# ============================================================
# 10. CRAWLER CLASS
# ============================================================

class DomainCrawler:

    def __init__(self, mongo):

        self.mongo = mongo
        self.storage = Storage(mongo)

    async def run(self, job):

        domain = job["domain"]
        job_id = job["job_id"]
        crawl_id = job["crawl_id"]

        # Separate persistent Crawlee RequestQueue per crawl version.
        request_queue = await RequestQueue.open(
            name=f"rq-{crawl_id[:24]}"
        )

        await request_queue.add_request(job["start_url"])

        lock = asyncio.Lock()

        visited = set()
        duplicates = set()
        in_progress = set()

        counters = {
            "saved": 0,
            "failed": 0,
            "retries": 0
        }

        crawler = (
            AdaptivePlaywrightCrawler
            .with_beautifulsoup_static_parser(

                request_manager=request_queue,

                max_requests_per_crawl=Config.MAX_PAGES,

                max_request_retries=Config.MAX_RETRIES,

                concurrency_settings=ConcurrencySettings(
                    desired_concurrency=Config.CONCURRENCY,
                    max_concurrency=Config.CONCURRENCY
                ),

                respect_robots_txt_file=Config.RESPECT_ROBOTS,

                abort_on_error=False,

                additional_http_error_status_codes=[
                    429, 500, 502, 503, 504
                ],

                playwright_crawler_specific_kwargs={
                    "headless": True,
                    "browser_launch_options": {
                        "chromium_sandbox": False
                        }

                }
            )
        )

        # ----------------------------------------------------
        # Delay before every request
        # ----------------------------------------------------

        @crawler.pre_navigation_hook
        async def delay_request(context):

            await asyncio.sleep(
                random.uniform(
                    Config.MIN_DELAY,
                    Config.MAX_DELAY
                )
            )

        # ----------------------------------------------------
        # Track retry attempts
        # ----------------------------------------------------

        @crawler.error_handler
        async def error_handler(context, error):

            counters["retries"] += 1

            logger.warning(
                "Retrying %s | %s",
                context.request.url,
                error
            )

            await asyncio.to_thread(
                self.mongo.increment,
                job_id,
                "retry_events"
            )

        # ----------------------------------------------------
        # Router / Main request handler
        # ----------------------------------------------------

        @crawler.router.default_handler
        async def page_handler(
            context: AdaptivePlaywrightCrawlingContext
        ):

            requested_url = URLManager.normalize(
                context.request.url
            )

            if not requested_url:
                return

            if not URLManager.same_domain(
                requested_url, domain
            ):
                logger.warning(
                    "Rejected external URL: %s",
                    requested_url
                )
                return

            # Strict limit on successfully saved pages.
            async with lock:

                if (
                    counters["saved"] + len(in_progress)
                    >= Config.MAX_PAGES
                ):
                    return

                if (
                    requested_url in visited
                    or requested_url in in_progress
                ):
                    duplicates.add(requested_url)
                    return

                in_progress.add(requested_url)

            try:

                soup = context.parsed_content

                html = str(soup)

                # Obtain HTTP status when exposed by the active
                # adaptive crawling context.
                status_code = None

                try:
                    response = context.http_response
                    status_code = getattr(
                        response, "status_code", None
                    )
                except Exception:
                    try:
                        response = context.response
                        status_code = getattr(
                            response, "status", None
                        )
                    except Exception:
                        pass

                # Use redirected URL if available.
                final_url = URLManager.normalize(
                    getattr(
                        context.request,
                        "loaded_url",
                        None
                    ) or requested_url
                )

                # Validate redirect destination.
                if (
                    not final_url
                    or not URLManager.same_domain(
                        final_url, domain
                    )
                ):
                    logger.warning(
                        "Rejected redirected URL: %s",
                        final_url
                    )
                    return

                # Save HTML and SEO data.
                await asyncio.to_thread(
                    self.storage.save,
                    job,
                    final_url,
                    html,
                    status_code
                )

                async with lock:

                    counters["saved"] += 1
                    visited.add(requested_url)

                logger.info(
                    "Saved page %s/%s: %s",
                    counters["saved"],
                    Config.MAX_PAGES,
                    final_url
                )

                # Discover links.
                new_links = []

                for anchor in soup.find_all(
                    "a",
                    href=True
                ):

                    absolute_url = URLManager.normalize(
                        urljoin(
                            final_url,
                            anchor["href"]
                        )
                    )

                    if not absolute_url:
                        continue

                    if not URLManager.same_domain(
                        absolute_url, domain
                    ):
                        continue

                    async with lock:

                        if (
                            absolute_url in visited
                            or absolute_url in in_progress
                            or absolute_url in duplicates
                        ):
                            duplicates.add(absolute_url)
                            continue

                    new_links.append(absolute_url)

                if new_links:
                    await crawler.add_requests(new_links)

            except Exception:

                logger.exception(
                    "Page processing error: %s",
                    requested_url
                )

                raise

            finally:

                async with lock:
                    in_progress.discard(requested_url)

        # ----------------------------------------------------
        # Permanently failed requests
        # ----------------------------------------------------

        @crawler.failed_request_handler
        async def failed_handler(context, error):

            counters["failed"] += 1

            logger.error(
                "Request permanently failed: %s | %s",
                context.request.url,
                error
            )

            await asyncio.to_thread(
                self.mongo.increment,
                job_id,
                "failed_requests"
            )

        # ----------------------------------------------------
        # Execute Crawlee
        # ----------------------------------------------------

        try:

            logger.info(
                "Starting crawl: %s",
                domain
            )

            await crawler.run(
                [],
                purge_request_queue=False
            )

            status = (
                "completed"
                if counters["failed"] == 0
                else "completed_with_errors"
            )

            self.mongo.update_job(
                job_id,
                {
                    "status": status,
                    "completed_at": datetime.now(timezone.utc),
                    "pages_saved": counters["saved"],
                    "failed_requests": counters["failed"],
                    "retry_events": counters["retries"],
                    "unique_urls_seen": len(visited),
                    "duplicate_urls_seen": len(duplicates)
                }
            )

            self.mongo.update_domain(
                domain,
                crawl_id,
                status
            )

            logger.info("Crawl finished: %s", domain)

        except Exception as error:

            logger.exception("Crawler failed")

            self.mongo.update_job(
                job_id,
                {
                    "status": "failed",
                    "completed_at": datetime.now(timezone.utc),
                    "error": str(error)
                }
            )

            self.mongo.update_domain(
                domain,
                crawl_id,
                "failed"
            )

        finally:

            try:
                await request_queue.drop()
            except Exception:
                logger.exception("RequestQueue cleanup failed")


# ============================================================
# 11. WORKER CLASS
# ============================================================

class Worker:

    def __init__(self, mongo, queue):

        self.mongo = mongo
        self.queue = queue
        self.crawler = DomainCrawler(mongo)

    async def process_one(self, timeout=10):

        queue_job = self.queue.receive(timeout)

        if queue_job is None:
            logger.warning("No job received")
            return False

        payload = None

        try:

            payload = json.loads(queue_job.body)

            job_id = payload["job_id"]

            # Prevent duplicate queue messages from running.
            if not self.mongo.mark_running(job_id):

                logger.warning(
                    "Job already processed or not published: %s",
                    job_id
                )

                self.queue.delete(queue_job)
                return True

            self.mongo.update_domain(
                payload["domain"],
                payload["crawl_id"],
                "running"
            )

            await self.crawler.run(payload)

            self.queue.delete(queue_job)

            self.mongo.display_results(
                payload["crawl_id"]
            )

            return True

        except Exception as error:

            logger.exception("Worker error")

            if payload:
                self.mongo.update_job(
                    payload["job_id"],
                    {
                        "status": "failed",
                        "completed_at": datetime.now(timezone.utc),
                        "error": str(error)
                    }
                )

            self.queue.bury(queue_job)

            return False


# ============================================================
# 12. MAIN APPLICATION
# ============================================================

class CrawlerApplication:

    def __init__(self):

        self.mongo = MongoDB()

        self.listener = Listener(self.mongo)

        self.queue = Beanstalkd()

        self.publisher = Publisher(
            self.mongo,
            self.queue
        )

        self.worker = Worker(
            self.mongo,
            self.queue
        )

    async def crawl_website(self, website):

        # 1. Listener -> MongoDB
        job = self.listener.submit(website)

        print("\nJob created in MongoDB:")
        print("Domain:", job["domain"])
        print("Crawl ID:", job["crawl_id"])

        # 2. Publisher -> Beanstalkd
        published = self.publisher.publish(
            job["job_id"]
        )

        if not published:
            print("Publisher failed. Job remains in MongoDB.")
            return

        # 3. Worker -> Crawlee
        await self.worker.process_one(timeout=10)

        # 4. Display results
        self.mongo.display_results(
            job["crawl_id"]
        )

    def clear_database(self):

        self.mongo.clear()

    def close(self):

        self.queue.close()
        self.mongo.close()

    async def menu(self):

        print("\n================================")
        print(" DOMAIN-RESTRICTED WEB CRAWLER")
        print("================================")

        print("1. Crawl website")
        print("2. Clear MongoDB")
        print("3. Exit")

        choice = input("\nEnter choice: ").strip()

        try:

            if choice == "1":

                website = input(
                    "\nEnter domain or URL: "
                ).strip()

                await self.crawl_website(website)

            elif choice == "2":

                self.clear_database()

            elif choice == "3":

                print("Exiting.")

            else:

                print("Invalid choice.")

        except KeyboardInterrupt:

            logger.warning("User interrupted crawler")

        except Exception:

            logger.exception("Application error")

            print(
                "An error occurred. Check the log file."
            )

        finally:

            self.close()


# ============================================================
# 13. START
# ============================================================

async def main():

    app = None

    try:

        app = CrawlerApplication()

        await app.menu()

    except Exception:

        logger.exception("Application startup error")

        print(
            "Could not start. Check MongoDB, Beanstalkd "
            "and the installation cells."
        )

        if app:
            app.close()


# Google Colab: use await main()
await main()

# ============================================= END ===========================================
