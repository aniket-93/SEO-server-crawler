# Google Colab SEO Crawler
# Run this file in a Colab cell with: %run /content/Task_1.py
# Or paste into a cell and run: await main()

# ---------------------------------------------------------------------
# 1. INSTALLATION
# ---------------------------------------------------------------------

import os
import sys
import subprocess
from pathlib import Path

def install_dependencies():
    subprocess.run(["apt-get", "update", "-qq"], check=True)
    subprocess.run(
        ["apt-get", "install", "-y", "-qq", "gnupg", "curl", "wget"],
        check=True
    )
    subprocess.run(
        ["bash", "-lc",
         "curl -fsSL https://pgp.mongodb.com/server-8.0.asc | "
         "gpg --dearmor --yes -o /usr/share/keyrings/mongodb-server-8.0.gpg"],
        check=True
    )
    subprocess.run(
        ["bash", "-lc",
         'echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/'
         'mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu '
         'jammy/mongodb-org/8.0 multiverse" '
         '> /etc/apt/sources.list.d/mongodb-org-8.0.list'],
        check=True
    )
    subprocess.run(["apt-get", "update", "-qq"], check=True)
    subprocess.run(
        ["apt-get", "install", "-y", "-qq", "mongodb-org"],
        check=True
    )
    subprocess.run(
        [sys.executable, "-m", "pip", "install", "-q",
         "pymongo", "beautifulsoup4", "playwright", "tldextract"],
        check=True
    )
    subprocess.run(
        [sys.executable, "-m", "playwright", "install", "chromium"],
        check=True
    )
    subprocess.run(
        [sys.executable, "-m", "playwright", "install-deps", "chromium"],
        check=True
    )

# Set to True only when the Colab runtime needs initial installation.
INSTALL_DEPENDENCIES = True

if INSTALL_DEPENDENCIES:
    install_dependencies()


# ---------------------------------------------------------------------
# 2. IMPORTS AND CONFIGURATION
# ---------------------------------------------------------------------

import asyncio
import hashlib
import logging
import re
import signal
import subprocess
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import (
    parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
)
from urllib.robotparser import RobotFileParser

import tldextract
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from pymongo import ASCENDING, MongoClient, ReturnDocument


class Config:
    MONGO_URI = "mongodb://127.0.0.1:27017"
    DATABASE = "crawler_db"
    MONGO_DATA = Path("/content/mongodb/data")
    MONGO_LOG = Path("/content/mongodb/mongod.log")

    HTML_ROOT = Path("/content/crawler_output/html")

    MAX_PAGES_PER_DOMAIN = 50
    REQUEST_TIMEOUT_MS = 15_000
    MAX_ATTEMPTS = 3
    RETRY_AFTER_OTHER_URLS = 2
    INGESTOR_INTERVAL_SECONDS = 45

    WORKER_COUNT = 6
    COMPUTE_SUBMIT_CAPACITY = int(4 * 1.5)

    RESPECT_ROBOTS = True
    USER_AGENT = "ColabSEOResearchCrawler/1.0"
    DEFAULT_PRIORITY = "Medium"
    PRIORITY_ORDER = {"High": 0, "Medium": 1, "Low": 2}

    LOG_COLLECTION = "crawl_events"


# ---------------------------------------------------------------------
# 3. RUNTIME SERVICES
# ---------------------------------------------------------------------

class RuntimeServices:
    def __init__(self):
        self.mongo_process = None

    def start_mongodb(self):
        Config.MONGO_DATA.mkdir(parents=True, exist_ok=True)
        Config.MONGO_LOG.parent.mkdir(parents=True, exist_ok=True)

        client = MongoClient(
            Config.MONGO_URI,
            serverSelectionTimeoutMS=1500
        )
        try:
            client.admin.command("ping")
            client.close()
            return
        except Exception:
            client.close()

        self.mongo_process = subprocess.Popen([
            "mongod",
            "--dbpath", str(Config.MONGO_DATA),
            "--logpath", str(Config.MONGO_LOG),
            "--bind_ip", "127.0.0.1",
            "--port", "27017"
        ])

        client = MongoClient(
            Config.MONGO_URI,
            serverSelectionTimeoutMS=30_000
        )
        deadline = time.time() + 30
        while time.time() < deadline:
            try:
                client.admin.command("ping")
                client.close()
                return
            except Exception:
                time.sleep(1)

        client.close()
        raise RuntimeError("MongoDB did not start within 30 seconds.")


# ---------------------------------------------------------------------
# 4. URL AND DOMAIN UTILITIES
# ---------------------------------------------------------------------

class URLManager:
    extractor = tldextract.TLDExtract(suffix_list_urls=())

    @staticmethod
    def normalize(url):
        if not url:
            return None

        url = url.strip()
        if not url:
            return None

        if "://" not in url:
            url = "https://" + url

        try:
            parts = urlsplit(url)
            scheme = parts.scheme.lower()
            if scheme not in ("http", "https") or not parts.hostname:
                return None

            host = parts.hostname.lower()
            port = parts.port
            netloc = host
            if port and not (
                (scheme == "http" and port == 80)
                or (scheme == "https" and port == 443)
            ):
                netloc = f"{host}:{port}"

            path = parts.path or "/"
            if path != "/":
                path = path.rstrip("/") or "/"

            query_items = [
                (key, value)
                for key, value in parse_qsl(parts.query, keep_blank_values=True)
                if not key.lower().startswith("utm_")
            ]
            query = urlencode(sorted(query_items), doseq=True)

            return urlunsplit((scheme, netloc, path, query, ""))
        except (ValueError, TypeError):
            return None

    @classmethod
    def registrable_domain(cls, url):
        host = urlsplit(url).hostname or ""
        extracted = cls.extractor(host)
        if extracted.domain and extracted.suffix:
            return f"{extracted.domain}.{extracted.suffix}".lower()
        return host.lower()

    @classmethod
    def belongs_to_domain(cls, url, domain_group):
        return cls.registrable_domain(url) == domain_group

    @staticmethod
    def host(url):
        return (urlsplit(url).hostname or "").lower()

    @staticmethod
    def preferred_scheme(url):
        parts = urlsplit(url)
        return urlunsplit(("https", parts.netloc, parts.path, parts.query, ""))


# ---------------------------------------------------------------------
# 5. MONGODB REPOSITORY
# ---------------------------------------------------------------------

class MongoRepository:
    URL_STATES = ("READY", "RESERVED", "PROCESSING", "COMPLETED", "FAILED", "SKIPPED")
    SESSION_STATES = ("RUNNING", "PAUSED", "STOPPED", "COMPLETED", "FAILED")

    def __init__(self):
        self.client = MongoClient(Config.MONGO_URI)
        self.client.admin.command("ping")
        self.db = self.client[Config.DATABASE]

        self.sessions = self.db["crawl_sessions"]
        self.domains = self.db["domains"]
        self.urls = self.db["crawl_urls"]
        self.pages = self.db["pages"]
        self.events = self.db[Config.LOG_COLLECTION]
        self.external = self.db["external_domains"]

        self.sessions.create_index([("session_id", ASCENDING)], unique=True)
        self.domains.create_index(
            [("session_id", ASCENDING), ("domain", ASCENDING)], unique=True
        )
        self.urls.create_index(
            [("session_id", ASCENDING), ("domain", ASCENDING),
             ("url", ASCENDING)], unique=True
        )
        self.urls.create_index([
            ("session_id", ASCENDING), ("domain", ASCENDING),
            ("status", ASCENDING), ("depth", ASCENDING),
            ("priority_rank", ASCENDING), ("created_at", ASCENDING)
        ])
        self.pages.create_index(
            [("session_id", ASCENDING), ("url", ASCENDING)], unique=True
        )
        self.events.create_index([("timestamp", ASCENDING)])
        self.external.create_index(
            [("session_id", ASCENDING), ("url", ASCENDING)], unique=True
        )

    @staticmethod
    def now():
        return datetime.now(timezone.utc)

    def log(self, session_id, event_type, url=None, domain=None,
            status=None, details=None):
        self.events.insert_one({
            "session_id": session_id,
            "event_type": event_type,
            "url": url,
            "domain": domain,
            "status": status,
            "details": details or {},
            "timestamp": self.now()
        })

    def create_session(self, seeds):
        session_id = uuid.uuid4().hex
        now = self.now()

        self.sessions.insert_one({
            "session_id": session_id,
            "status": "RUNNING",
            "created_at": now,
            "updated_at": now,
            "completed_at": None,
            "seed_count": len(seeds)
        })

        for seed in seeds:
            url = URLManager.normalize(seed["url"])
            if not url:
                raise ValueError(f"Invalid seed URL: {seed['url']}")

            domain = URLManager.registrable_domain(url)
            priority = seed["priority"]

            self.domains.update_one(
                {"session_id": session_id, "domain": domain},
                {"$setOnInsert": {
                    "session_id": session_id,
                    "domain": domain,
                    "priority": priority,
                    "priority_rank": Config.PRIORITY_ORDER[priority],
                    "status": "RUNNING",
                    "successful_pages": 0,
                    "discovered_count": 0,
                    "failed_count": 0,
                    "skipped_count": 0,
                    "created_at": now
                }},
                upsert=True
            )

            self.add_url(
                session_id, domain, url, depth=0,
                priority=priority, source="SEED"
            )

        self.log(session_id, "SESSION_CREATED", details={"seeds": seeds})
        return session_id

    def add_url(self, session_id, domain, url, depth, priority,
                source="DISCOVERED"):
        normalized = URLManager.normalize(url)
        if not normalized:
            return False

        now = self.now()
        try:
            self.urls.insert_one({
                "session_id": session_id,
                "domain": domain,
                "url": normalized,
                "depth": depth,
                "priority": priority,
                "priority_rank": Config.PRIORITY_ORDER[priority],
                "source": source,
                "status": "READY",
                "attempts": 0,
                "retry_after_successes": 0,
                "created_at": now,
                "updated_at": now,
                "worker_id": None,
                "lease_until": None,
                "last_error": None
            })
            self.db["domains"].update_one(
                {"session_id": session_id, "domain": domain},
                {"$inc": {"discovered_count": 1}}
            )
            self.log(session_id, "URL_DISCOVERED", normalized, domain,
                     "READY", {"depth": depth, "source": source})
            return True
        except Exception as exc:
            if exc.__class__.__name__ == "DuplicateKeyError":
                self.log(session_id, "URL_DUPLICATE", normalized, domain,
                         "SKIPPED")
                return False
            raise

    def recover_expired_jobs(self):
        now = self.now()
        expired = list(self.urls.find({
            "status": {"$in": ["RESERVED", "PROCESSING"]},
            "lease_until": {"$lt": now}
        }))
        for item in expired:
            self.urls.update_one(
                {"_id": item["_id"]},
                {"$set": {
                    "status": "READY",
                    "worker_id": None,
                    "lease_until": None,
                    "updated_at": now
                }}
            )
            self.log(
                item["session_id"], "WORKER_JOB_RECOVERED",
                item["url"], item["domain"], "READY"
            )

    def count_status(self, session_id, statuses):
        return self.urls.count_documents({
            "session_id": session_id,
            "status": {"$in": list(statuses)}
        })

    def reserve_next_url(self, session_id, domain, worker_id, success_count):
        now = self.now()
        candidate = self.urls.find({
            "session_id": session_id,
            "domain": domain,
            "status": "READY",
            "retry_after_successes": {"$lte": success_count}
        }).sort([
            ("depth", ASCENDING),
            ("priority_rank", ASCENDING),
            ("created_at", ASCENDING)
        ]).limit(1)

        item = next(candidate, None)
        if not item:
            return None

        return self.urls.find_one_and_update(
            {"_id": item["_id"], "status": "READY"},
            {"$set": {
                "status": "RESERVED",
                "worker_id": worker_id,
                "lease_until": now,
                "updated_at": now
            }},
            return_document=ReturnDocument.AFTER
        )

    def set_url_status(self, item_id, status, **fields):
        fields.update({"status": status, "updated_at": self.now()})
        self.urls.update_one({"_id": item_id}, {"$set": fields})

    def increment_domain(self, session_id, domain, field):
        self.domains.update_one(
            {"session_id": session_id, "domain": domain},
            {"$inc": {field: 1}}
        )

    def save_page(self, record):
        self.pages.update_one(
            {"session_id": record["session_id"], "url": record["url"]},
            {"$set": record},
            upsert=True
        )
        self.increment_domain(
            record["session_id"], record["domain"], "successful_pages"
        )

    def save_external(self, session_id, source_domain, url, status_code=None):
        self.external.update_one(
            {"session_id": session_id, "url": url},
            {"$setOnInsert": {
                "session_id": session_id,
                "source_domain": source_domain,
                "url": url,
                "status_code": status_code,
                "first_seen_at": self.now()
            }},
            upsert=True
        )
        self.log(session_id, "EXTERNAL_DOMAIN_RECORDED", url,
                 source_domain, "SKIPPED")

    def get_session(self, session_id):
        return self.sessions.find_one({"session_id": session_id})

    def get_unfinished_session(self):
        return self.sessions.find_one(
            {"status": {"$in": ["RUNNING", "PAUSED", "STOPPED"]}},
            sort=[("updated_at", -1)]
        )

    def set_session_status(self, session_id, status):
        self.sessions.update_one(
            {"session_id": session_id},
            {"$set": {"status": status, "updated_at": self.now()}}
        )
        self.log(session_id, f"SESSION_{status}")

    def close(self):
        self.client.close()


# ---------------------------------------------------------------------
# 6. HTML PARSER AND FILE STORAGE
# ---------------------------------------------------------------------

class SEOParser:
    @staticmethod
    def parse(html, page_url):
        soup = BeautifulSoup(html, "html.parser")

        def text_of(tag):
            return tag.get_text(" ", strip=True) if tag else None

        title = text_of(soup.find("title"))
        description_tag = soup.find(
            "meta", attrs={"name": re.compile("^description$", re.I)}
        )
        description = (
            description_tag.get("content", "").strip()
            if description_tag else None
        )

        canonical_tag = soup.find(
            "link",
            rel=lambda value: value and "canonical" in (
                value if isinstance(value, list) else value.split()
            )
        )
        canonical = (
            urljoin(page_url, canonical_tag.get("href"))
            if canonical_tag and canonical_tag.get("href") else None
        )

        headings = {
            f"h{level}": [
                tag.get_text(" ", strip=True)
                for tag in soup.find_all(f"h{level}")
            ]
            for level in range(1, 5)
        }

        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()
        readable_text = re.sub(
            r"\s+", " ", soup.get_text(" ", strip=True)
        ).strip()

        links = []
        for anchor in soup.find_all("a", href=True):
            absolute = URLManager.normalize(urljoin(page_url, anchor["href"]))
            if absolute and absolute not in links:
                links.append(absolute)

        return {
            "title": title,
            "description": description,
            "canonical_url": canonical,
            **headings,
            "readable_text": readable_text,
            "links": links
        }


class HTMLStorage:
    def __init__(self, repository):
        self.repository = repository
        Config.HTML_ROOT.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def filename_for(url):
        parts = urlsplit(url)
        path = parts.path.strip("/")
        if not path:
            base = "index"
        else:
            base = re.sub(r"[^a-zA-Z0-9._-]+", "_", path)
            base = base.replace("/", "_").strip("._") or "page"

        suffix = hashlib.sha256(
            (parts.query or "").encode("utf-8")
        ).hexdigest()[:10] if parts.query else ""

        if suffix:
            base = f"{base}_{suffix}"
        if not base.lower().endswith(".html"):
            base += ".html"
        return base[:180]

    def save(self, session_id, domain, url, html, status_code, metadata):
        domain_folder = Config.HTML_ROOT / domain
        domain_folder.mkdir(parents=True, exist_ok=True)
        file_path = domain_folder / self.filename_for(url)

        if not file_path.exists():
            file_path.write_text(html, encoding="utf-8")

        record = {
            "session_id": session_id,
            "domain": domain,
            "url": url,
            **metadata,
            "http_status_code": status_code,
            "html_file_path": str(file_path),
            "crawled_at": MongoRepository.now()
        }
        self.repository.save_page(record)
        return record


# ---------------------------------------------------------------------
# 7. ROBOTS POLICY
# ---------------------------------------------------------------------

class RobotsPolicy:
    def __init__(self):
        self.cache = {}

    def allowed(self, url):
        if not Config.RESPECT_ROBOTS:
            return True

        parts = urlsplit(url)
        origin = f"{parts.scheme}://{parts.netloc}"
        if origin not in self.cache:
            parser = RobotFileParser()
            parser.set_url(origin + "/robots.txt")
            try:
                parser.read()
                self.cache[origin] = parser
            except Exception:
                # If robots.txt cannot be retrieved, do not block the crawl.
                self.cache[origin] = None

        parser = self.cache[origin]
        return True if parser is None else parser.can_fetch(
            Config.USER_AGENT, url
        )


# ---------------------------------------------------------------------
# 8. DOMAIN CRAWLER
# ---------------------------------------------------------------------

class DomainCrawler:
    def __init__(self, repository, storage):
        self.repository = repository
        self.storage = storage
        self.robots = RobotsPolicy()

    async def fetch(self, page, url):
        candidates = [URLManager.preferred_scheme(url)]
        original = URLManager.normalize(url)
        if original and original not in candidates:
            candidates.append(original)

        last_error = None
        for candidate in candidates:
            try:
                response = await page.goto(
                    candidate,
                    wait_until="domcontentloaded",
                    timeout=Config.REQUEST_TIMEOUT_MS
                )
                if response is None:
                    raise RuntimeError("The browser returned no HTTP response.")
                return response, page.url, await page.content()
            except Exception as exc:
                last_error = exc
        raise last_error or RuntimeError("Unable to fetch URL.")

    async def crawl_domain(self, session_id, domain, worker_id, browser,
                           control):
        domain_doc = self.repository.domains.find_one({
            "session_id": session_id, "domain": domain
        })
        priority = domain_doc["priority"]
        success_count = domain_doc.get("successful_pages", 0)
        page = await browser.new_page(user_agent=Config.USER_AGENT)

        try:
            while not control["stop"] and not control["pause"]:
                domain_doc = self.repository.domains.find_one({
                    "session_id": session_id, "domain": domain
                })
                success_count = domain_doc.get("successful_pages", 0)

                if success_count >= Config.MAX_PAGES_PER_DOMAIN:
                    self.repository.log(
                        session_id, "DOMAIN_PAGE_LIMIT_REACHED",
                        domain=domain, status="SKIPPED",
                        details={"page_limit": Config.MAX_PAGES_PER_DOMAIN}
                    )
                    break

                item = self.repository.reserve_next_url(
                    session_id, domain, worker_id, success_count
                )
                if not item:
                    pending_retries = self.repository.urls.count_documents({
                        "session_id": session_id,
                        "domain": domain,
                        "status": "READY"
                    })
                    if pending_retries:
                        await asyncio.sleep(1)
                        continue
                    break

                self.repository.set_url_status(
                    item["_id"], "PROCESSING",
                    lease_until=MongoRepository.now()
                )
                url = item["url"]

                if not self.robots.allowed(url):
                    self.repository.set_url_status(item["_id"], "SKIPPED")
                    self.repository.increment_domain(
                        session_id, domain, "skipped_count"
                    )
                    self.repository.log(
                        session_id, "ROBOTS_BLOCKED", url, domain, "SKIPPED"
                    )
                    continue

                try:
                    response, final_url, html = await self.fetch(page, url)
                    final_url = URLManager.normalize(final_url)
                    status_code = response.status

                    if not final_url:
                        raise RuntimeError("Invalid final URL after redirect.")

                    if not URLManager.belongs_to_domain(final_url, domain):
                        self.repository.save_external(
                            session_id, domain, final_url, status_code
                        )
                        self.repository.set_url_status(
                            item["_id"], "SKIPPED",
                            final_url=final_url,
                            last_error="Redirected outside the crawl domain."
                        )
                        self.repository.increment_domain(
                            session_id, domain, "skipped_count"
                        )
                        continue

                    if status_code >= 400:
                        raise RuntimeError(f"HTTP status {status_code}")

                    metadata = SEOParser.parse(html, final_url)
                    self.storage.save(
                        session_id, domain, final_url,
                        html, status_code, metadata
                    )

                    self.repository.set_url_status(
                        item["_id"], "COMPLETED",
                        final_url=final_url,
                        http_status_code=status_code,
                        completed_at=MongoRepository.now(),
                        worker_id=None,
                        lease_until=None
                    )
                    success_count += 1
                    self.repository.log(
                        session_id, "URL_SUCCESS", final_url, domain,
                        "COMPLETED", {"http_status_code": status_code}
                    )

                    if success_count < Config.MAX_PAGES_PER_DOMAIN:
                        for discovered_url in metadata["links"]:
                            if URLManager.belongs_to_domain(
                                discovered_url, domain
                            ):
                                self.repository.add_url(
                                    session_id, domain, discovered_url,
                                    depth=item["depth"] + 1,
                                    priority=priority
                                )
                            else:
                                self.repository.save_external(
                                    session_id, domain, discovered_url
                                )

                except Exception as exc:
                    attempts = item.get("attempts", 0) + 1
                    if attempts < Config.MAX_ATTEMPTS:
                        retry_after = success_count + Config.RETRY_AFTER_OTHER_URLS
                        self.repository.set_url_status(
                            item["_id"], "READY",
                            attempts=attempts,
                            retry_after_successes=retry_after,
                            last_error=str(exc),
                            worker_id=None,
                            lease_until=None
                        )
                        self.repository.log(
                            session_id, "URL_RETRY", url, domain, "READY",
                            {"attempt": attempts + 1, "error": str(exc),
                             "retry_after_successes": retry_after}
                        )
                    else:
                        self.repository.set_url_status(
                            item["_id"], "FAILED",
                            attempts=attempts,
                            last_error=str(exc),
                            worker_id=None,
                            lease_until=None
                        )
                        self.repository.increment_domain(
                            session_id, domain, "failed_count"
                        )
                        self.repository.log(
                            session_id, "URL_FAILED", url, domain, "FAILED",
                            {"attempts": attempts, "error": str(exc)}
                        )
        finally:
            await page.close()

        if control["pause"]:
            self.repository.domains.update_one(
                {"session_id": session_id, "domain": domain},
                {"$set": {"status": "PAUSED"}}
            )
        elif control["stop"]:
            self.repository.domains.update_one(
                {"session_id": session_id, "domain": domain},
                {"$set": {"status": "STOPPED"}}
            )
        else:
            self.repository.domains.update_one(
                {"session_id": session_id, "domain": domain},
                {"$set": {"status": "COMPLETED"}}
            )


# ---------------------------------------------------------------------
# 9. INGESTOR AND WORKERS
# ---------------------------------------------------------------------

class Worker:
    def __init__(self, worker_id, repository, crawler, browser, control):
        self.worker_id = worker_id
        self.repository = repository
        self.crawler = crawler
        self.browser = browser
        self.control = control

    async def process(self, session_id, domain):
        self.repository.domains.update_one(
            {"session_id": session_id, "domain": domain},
            {"$set": {"worker_id": self.worker_id, "status": "RUNNING"}}
        )
        await self.crawler.crawl_domain(
            session_id, domain, self.worker_id, self.browser, self.control
        )


class Ingestor:
    def __init__(self, repository, workers):
        self.repository = repository
        self.workers = workers
        self.available = asyncio.Queue()
        self.active_domains = set()
        self.tasks = set()
        self.session_id = None
        self.control = {"pause": False, "stop": False}

    async def start(self, session_id):
        self.session_id = session_id
        for worker in self.workers:
            await self.available.put(worker)

        while not self.control["stop"] and not self.control["pause"]:
            self.repository.recover_expired_jobs()

            outstanding = self.repository.count_status(
                session_id, ["READY", "RESERVED"]
            )
            capacity = max(
                0, Config.COMPUTE_SUBMIT_CAPACITY - outstanding
            )

            while capacity > 0 and not self.available.empty():
                domain_doc = self.repository.domains.find_one({
                    "session_id": session_id,
                    "status": {"$in": ["RUNNING", "READY"]}
                }, sort=[("priority_rank", ASCENDING), ("created_at", ASCENDING)])

                if not domain_doc:
                    break

                domain = domain_doc["domain"]
                if domain in self.active_domains:
                    break

                worker = await self.available.get()
                self.active_domains.add(domain)
                task = asyncio.create_task(
                    self._run_worker(worker, session_id, domain)
                )
                self.tasks.add(task)
                task.add_done_callback(self.tasks.discard)
                capacity -= 1

            if not self.tasks:
                unfinished = self.repository.domains.count_documents({
                    "session_id": session_id,
                    "status": {"$in": ["RUNNING", "READY"]}
                })
                if unfinished == 0:
                    break

            await asyncio.sleep(Config.INGESTOR_INTERVAL_SECONDS)

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

    async def _run_worker(self, worker, session_id, domain):
        try:
            await worker.process(session_id, domain)
        except Exception as exc:
            self.repository.log(
                session_id, "WORKER_ERROR", domain=domain,
                status="FAILED", details={"error": str(exc)}
            )
            self.repository.domains.update_one(
                {"session_id": session_id, "domain": domain},
                {"$set": {"status": "FAILED", "last_error": str(exc)}}
            )
        finally:
            self.active_domains.discard(domain)
            await self.available.put(worker)


# ---------------------------------------------------------------------
# 10. CONSOLE CONTROL AND PROGRESS
# ---------------------------------------------------------------------

class ConsoleController:
    def __init__(self, repository, ingestor):
        self.repository = repository
        self.ingestor = ingestor

    def display_progress(self, session_id):
        print("\n--- Crawl progress ---")
        for domain in self.repository.domains.find(
            {"session_id": session_id}, {"_id": 0}
        ).sort("priority_rank", ASCENDING):
            counts = {
                status: self.repository.urls.count_documents({
                    "session_id": session_id,
                    "domain": domain["domain"],
                    "status": status
                })
                for status in MongoRepository.URL_STATES
            }
            print(
                f"{domain['domain']} | "
                f"crawled {domain.get('successful_pages', 0)}/"
                f"{Config.MAX_PAGES_PER_DOMAIN} | "
                f"discovered {domain.get('discovered_count', 0)} | "
                f"pending {counts['READY'] + counts['RESERVED'] + counts['PROCESSING']} | "
                f"failed {counts['FAILED']} | skipped {counts['SKIPPED']} | "
                f"status {domain.get('status')}"
            )

    async def command_loop(self, session_id):
        while not self.ingestor.control["stop"] and not self.ingestor.control["pause"]:
            command = await asyncio.to_thread(
                input, "\nCommand (status / pause / stop): "
            )
            command = command.strip().lower()

            if command == "status":
                self.display_progress(session_id)
            elif command == "pause":
                self.ingestor.control["pause"] = True
                self.repository.set_session_status(session_id, "PAUSED")
                break
            elif command == "stop":
                self.ingestor.control["stop"] = True
                self.repository.set_session_status(session_id, "STOPPED")
                break
            else:
                print("Enter status, pause, or stop.")


# ---------------------------------------------------------------------
# 11. APPLICATION
# ---------------------------------------------------------------------

class CrawlerApplication:
    def __init__(self):
        self.services = RuntimeServices()
        self.services.start_mongodb()
        self.repository = MongoRepository()
        self.storage = HTMLStorage(self.repository)
        self.playwright = None
        self.browser = None

    def collect_seeds(self):
        print("\nEnter seed URLs. Leave URL empty when finished.")
        seeds = []
        while True:
            url = input("Seed URL: ").strip()
            if not url:
                break
            print("Priority: 1=High, 2=Medium, 3=Low")
            choice = input("Select priority [2]: ").strip() or "2"
            priority_map = {"1": "High", "2": "Medium", "3": "Low"}
            if choice not in priority_map:
                print("Invalid priority. Please enter 1, 2, or 3.")
                continue
            normalized = URLManager.normalize(url)
            if not normalized:
                print("Invalid URL. Try again.")
                continue
            seeds.append({"url": normalized, "priority": priority_map[choice]})
        if not seeds:
            raise ValueError("At least one valid seed URL is required.")
        return seeds

    async def open_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=["--no-sandbox"]
        )

    async def close_browser(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    async def run_session(self, session_id):
        control = {"pause": False, "stop": False}
        crawler = DomainCrawler(self.repository, self.storage)

        workers = [
            Worker(
                f"worker-{number}",
                self.repository,
                crawler,
                self.browser,
                control
            )
            for number in range(1, Config.WORKER_COUNT + 1)
        ]
        ingestor = Ingestor(self.repository, workers)
        ingestor.control = control

        controller = ConsoleController(self.repository, ingestor)
        crawl_task = asyncio.create_task(ingestor.start(session_id))
        command_task = asyncio.create_task(
            controller.command_loop(session_id)
        )

        await crawl_task
        control["stop"] = True
        command_task.cancel()
        await asyncio.gather(command_task, return_exceptions=True)

        if control["pause"]:
            self.repository.set_session_status(session_id, "PAUSED")
        elif control["stop"]:
            self.repository.set_session_status(session_id, "STOPPED")
        else:
            self.repository.set_session_status(session_id, "COMPLETED")

        controller.display_progress(session_id)

    async def resume_session(self, session_id):
        self.repository.sessions.update_one(
            {"session_id": session_id},
            {"$set": {"status": "RUNNING", "updated_at": MongoRepository.now()}}
        )
        self.repository.domains.update_many(
            {"session_id": session_id, "status": {"$in": ["PAUSED", "STOPPED"]}},
            {"$set": {"status": "RUNNING"}}
        )
        await self.run_session(session_id)

    async def start_new_session(self):
        seeds = self.collect_seeds()
        session_id = self.repository.create_session(seeds)
        print(f"Created crawl session: {session_id}")
        await self.run_session(session_id)

    async def run(self):
        await self.open_browser()
        try:
            unfinished = self.repository.get_unfinished_session()
            if unfinished:
                print(
                    f"Unfinished session found: {unfinished['session_id']} "
                    f"({unfinished['status']})"
                )
                choice = input("Resume previous session? [y/n]: ").strip().lower()
                if choice == "y":
                    await self.resume_session(unfinished["session_id"])
                else:
                    await self.start_new_session()
            else:
                await self.start_new_session()
        finally:
            await self.close_browser()
            self.repository.close()


# ---------------------------------------------------------------------
# 12. ENTRY POINT
# ---------------------------------------------------------------------

async def main():
    app = CrawlerApplication()
    await app.run()


await main()
