"""
proxy_pool.py
=============
Async proxy-pool manager for web crawlers (SOCKS5 / SOCKS4 / HTTP proxies).

Features
--------
* Parses proxies from many formats:
    - Share links:   https://t.me/socks?server=1.2.3.4&port=1080[&user=u&pass=p]
                     tg://proxy?server=1.2.3.4&port=1080
    - URL form:      socks5://user:pass@1.2.3.4:1080   (socks4/http also fine)
    - Bare:          1.2.3.4:1080
    - With auth:     1.2.3.4:1080:user:pass
  MTProto links (t.me/proxy?...&secret=...) are detected and skipped — they are
  not SOCKS5 proxies.
* Health-checks every proxy with a real HTTP GET (default https://api.ipify.org)
  through the proxy, with timeout + concurrency limits.
* Keeps only verified-working proxies in an in-memory pool, optionally mirrored
  to MongoDB (Motor).
* ``get_working_proxy()`` -> random working proxy, or ``None`` when the pool is
  empty (caller should fall back to a direct connection).
* ``fetch()`` -> full auto-rotation: if a proxy fails mid-use it is evicted and
  the next one is tried; when no proxies remain, it falls back to a direct
  connection (configurable).
* Background loop re-tests dead proxies every ``retest_interval`` seconds
  (default 30 min) so recovered proxies come back into the pool automatically.

Dependencies
------------
    pip install "httpx[socks]"     # httpx + socksio (SOCKS support)
    # optional: motor              # only if persisting pool state to MongoDB

Notes
-----
* With httpx, the target hostname is resolved *through* a SOCKS proxy
  (remote DNS — i.e. rdns=True behaviour).
* For aiohttp instead of httpx, build your session with:
      aiohttp_socks.ProxyConnector.from_url(spec.url, rdns=True)
* Only route traffic through proxies you are authorised to use, and remember
  an open proxy's operator can observe unencrypted traffic passing through it.

FastAPI integration sketch
--------------------------
    from contextlib import asynccontextmanager
    from motor.motor_asyncio import AsyncIOMotorClient
    from fastapi import FastAPI
    from proxy_pool import ProxyPool, load_sources

    mongo = AsyncIOMotorClient("mongodb://localhost:27017")
    pool = ProxyPool(
        retest_interval=1800,                      # re-test dead proxies every 30 min
        mongo_collection=mongo.crawler.proxy_state # optional persistence
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool.add_proxies(load_sources("proxies.txt"))
        await pool.initialize()   # initial health-check of everything
        await pool.start()        # background retester
        yield
        await pool.stop()

    app = FastAPI(lifespan=lifespan)

    @app.get("/crawl")
    async def crawl():
        resp = await pool.fetch("GET", "https://example.com/api/data")
        return {"status": resp.status_code, "preview": resp.text[:200]}
"""

from __future__ import annotations

import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import parse_qs, unquote, urlparse

import httpx

logger = logging.getLogger("proxy_pool")

__all__ = ["ProxyPool", "ProxySpec", "ProxyState", "parse_proxy", "load_sources"]

# Exceptions that mean "the proxy itself is broken" (evict it).
# HTTP 4xx/5xx from the target site does NOT count — that means the proxy works.
PROXY_FAILURE_EXC: tuple = (
    httpx.ProxyError,
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.PoolTimeout,
    httpx.ReadTimeout,
    OSError,
)

_SCHEME_MAP = {
    "socks5": "socks5",
    "socks5h": "socks5",
    "socks4": "socks4",
    "socks4a": "socks4",
    "http": "http",
    "https": "http",
}

_TG_HOSTS = {"t.me", "telegram.me", "telegram.dog"}

_IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")


# --------------------------------------------------------------------------- #
#  Data model
# --------------------------------------------------------------------------- #
class ProxyState(str, Enum):
    UNKNOWN = "unknown"
    WORKING = "working"
    DEAD = "dead"


@dataclass(frozen=True)
class ProxySpec:
    """A single proxy definition (immutable)."""

    host: str
    port: int
    username: Optional[str] = None
    password: Optional[str] = None
    proxy_type: str = "socks5"  # socks5 | socks4 | http

    @property
    def key(self) -> str:
        """Unique pool key."""
        return f"{self.host}:{self.port}"

    @property
    def url(self) -> str:
        """URL form usable by httpx / aiohttp-socks."""
        from urllib.parse import quote

        auth = ""
        if self.username:
            auth = quote(self.username, safe="") + ":" + quote(self.password or "", safe="") + "@"
        return f"{self.proxy_type}://{auth}{self.host}:{self.port}"

    def as_dict(self, rdns: bool = True) -> dict:
        """python-socks compatible dict (proxy_type/addr/port/rdns[/username/password])."""
        d: dict[str, Any] = {
            "proxy_type": self.proxy_type,
            "addr": self.host,
            "port": self.port,
            "rdns": rdns,
        }
        if self.username:
            d["username"] = self.username
        if self.password:
            d["password"] = self.password
        return d

    def __str__(self) -> str:  # pragma: no cover
        return self.key


# --------------------------------------------------------------------------- #
#  Parsing
# --------------------------------------------------------------------------- #
def parse_proxy(raw: str) -> Optional[ProxySpec]:
    """Parse one line into a ProxySpec.

    Accepted formats (auth optional everywhere):
      https://t.me/socks?server=1.2.3.4&port=1080[&user=u&pass=p]
      tg://proxy?server=1.2.3.4&port=1080[&user=u&pass=p]
      socks5://1.2.3.4:1080   |   socks5://u:p@1.2.3.4:1080
      1.2.3.4:1080            |   1.2.3.4:1080:user:pass

    Returns None (with a log line) for garbage input or MTProto links.
    """
    line = raw.strip()
    if not line:
        return None
    try:
        if "://" in line:
            parsed = urlparse(line)
            scheme = parsed.scheme.lower()
            host: Optional[str] = parsed.hostname
            port: Optional[int] = parsed.port
            user = unquote(parsed.username) if parsed.username else None
            pword = unquote(parsed.password) if parsed.password else None

            if (host and host.lower() in _TG_HOSTS) or scheme == "tg":
                # Telegram SOCKS share link -> real data is in the query string
                qs = parse_qs(parsed.query)
                if "secret" in qs:
                    logger.warning("parse: skipping MTProto link (not SOCKS5): %s", line)
                    return None
                host = (qs.get("server") or [None])[0]
                port = int((qs.get("port") or ["0"])[0])
                user = (qs.get("user") or [None])[0]
                pword = (qs.get("pass") or [None])[0]
                ptype = "socks5"
            elif scheme in _SCHEME_MAP:
                ptype = _SCHEME_MAP[scheme]
            else:
                logger.warning("parse: unsupported scheme in %r", line)
                return None
        else:
            parts = line.split(":")
            if len(parts) == 2:
                host, port_s, user, pword = parts[0], parts[1], None, None
            elif len(parts) == 4:
                host, port_s, user, pword = parts
            else:
                logger.warning("parse: unrecognised format %r", line)
                return None
            port = int(port_s)
            ptype = "socks5"  # bare host:port assumed SOCKS5

        host = (host or "").strip("[]")
        if not host or not (1 <= int(port) <= 65535):
            raise ValueError(f"bad host/port: {host!r}:{port}")
        return ProxySpec(host=host, port=int(port), username=user, password=pword, proxy_type=ptype)
    except Exception as exc:
        logger.warning("parse: could not parse %r: %s", line, exc)
        return None


def load_sources(path: str | Path) -> list[str]:
    """Read proxy lines (URLs / host:port) from a text file, one per line."""
    return Path(path).read_text(encoding="utf-8").splitlines()


# --------------------------------------------------------------------------- #
#  Pool
# --------------------------------------------------------------------------- #
class ProxyPool:
    """Async proxy pool: health-check, rotation, eviction, revival, direct fallback.

    Safe to use from a single asyncio event loop (e.g. one FastAPI app).
    """

    def __init__(
        self,
        sources: Iterable[str] | None = None,
        *,
        check_url: str = "https://api.ipify.org/?format=json",
        expected_text: Optional[str] = None,      # optional substring expected in check response
        timeout: float = 10.0,                    # per-request timeout for checks
        max_concurrent_checks: int = 10,
        retest_interval: float = 1800.0,          # dead-proxy revival cycle (s) — 30 min
        reverify_interval: Optional[float] = None,  # optional re-check of working proxies (s)
        allow_direct_fallback: bool = True,
        mongo_collection=None,                    # optional motor collection for persistence
    ) -> None:
        self.check_url = check_url
        self.expected_text = expected_text
        self.timeout = timeout
        self.retest_interval = retest_interval
        self.reverify_interval = reverify_interval
        self.allow_direct_fallback = allow_direct_fallback
        self.mongo_collection = mongo_collection

        self._sem = asyncio.Semaphore(max_concurrent_checks)
        self._lock = asyncio.Lock()

        self._all: dict[str, ProxySpec] = {}        # key -> spec (everything ever added)
        self._working: dict[str, ProxySpec] = {}    # key -> spec (verified working)
        self._dead: dict[str, dict] = {}            # key -> {"failed_at": ts, "reason": str}
        self._last_latency: dict[str, float] = {}   # key -> seconds

        self._tasks: list[asyncio.Task] = []
        self._stopped = True

        if sources:
            self.add_proxies(sources)

    # ------------------------------------------------------------------ #
    #  Adding / loading proxies
    # ------------------------------------------------------------------ #
    def add_proxies(self, lines: Iterable[str]) -> int:
        """Parse + dedupe a batch of proxy strings. Returns number of new proxies added."""
        added = 0
        for raw in lines:
            spec = parse_proxy(raw)
            if spec and spec.key not in self._all:
                self._all[spec.key] = spec
                added += 1
        logger.info("Added %d new proxy(ies) (total known: %d).", added, len(self._all))
        return added

    async def load_from_mongo(self, include_dead: bool = True) -> int:
        """Load previously seen proxies from MongoDB (they get re-verified on initialize())."""
        if self.mongo_collection is None:
            raise RuntimeError("mongo_collection was not configured")
        added = 0
        async for doc in self.mongo_collection.find({}):
            if doc.get("state") == "dead" and not include_dead:
                continue
            try:
                spec = ProxySpec(
                    host=doc["host"],
                    port=int(doc["port"]),
                    username=doc.get("username"),
                    password=doc.get("password"),
                    proxy_type=doc.get("proxy_type", "socks5"),
                )
            except Exception:
                logger.warning("mongo-load: skipping malformed doc %r", doc.get("key"))
                continue
            if spec.key not in self._all:
                self._all[spec.key] = spec
                added += 1
        logger.info("Loaded %d proxy(ies) from MongoDB (total known: %d).", added, len(self._all))
        return added

    # ------------------------------------------------------------------ #
    #  Health checking
    # ------------------------------------------------------------------ #
    async def check(self, spec: ProxySpec) -> tuple[bool, Optional[str], float, Optional[str]]:
        """Test one proxy end-to-end. Returns (ok, exit_ip, latency_s, error)."""
        start = time.monotonic()
        try:
            async with self._new_client(spec.url) as client:
                resp = await client.get(self.check_url)
            latency = time.monotonic() - start
            if resp.status_code == 200 and (
                self.expected_text is None or self.expected_text in resp.text
            ):
                exit_ip: Optional[str] = None
                try:
                    exit_ip = resp.json().get("ip")
                except Exception:
                    m = _IP_RE.search(resp.text)
                    exit_ip = m.group(0) if m else None
                return True, exit_ip, latency, None
            return False, None, latency, f"HTTP {resp.status_code}"
        except Exception as exc:
            return False, None, time.monotonic() - start, f"{type(exc).__name__}: {exc}"

    async def initialize(self) -> None:
        """Initial health-check of every known proxy (concurrency-limited)."""
        logger.info("Initial health-check of %d proxy(ies) via %s ...", len(self._all), self.check_url)
        await self._run_checks(list(self._all.values()))
        logger.info("Initial check done: %d working, %d dead.", len(self._working), len(self._dead))

    async def _run_checks(self, specs: list[ProxySpec]) -> None:
        if not specs:
            return
        await asyncio.gather(*(self._check_one(s) for s in specs))

    async def _check_one(self, spec: ProxySpec) -> None:
        async with self._sem:
            ok, exit_ip, latency, err = await self.check(spec)

        async with self._lock:
            if ok:
                self._dead.pop(spec.key, None)
                self._working[spec.key] = spec
                self._last_latency[spec.key] = latency
            else:
                self._working.pop(spec.key, None)
                self._dead[spec.key] = {
                    "failed_at": time.time(),
                    "reason": (err or "unknown")[:200],
                }

        await self._persist(
            spec,
            ProxyState.WORKING if ok else ProxyState.DEAD,
            exit_ip=exit_ip,
            latency_ms=round(latency * 1000, 1),
            error=err,
        )
        if ok:
            logger.info(
                "[OK ] %s (%s) exit_ip=%s latency=%.2fs — working pool: %d",
                spec.key, spec.proxy_type, exit_ip or "-", latency, len(self._working),
            )
        else:
            logger.info("[BAD] %s (%s) error=%s", spec.key, spec.proxy_type, err)

    # ------------------------------------------------------------------ #
    #  Selection & rotation
    # ------------------------------------------------------------------ #
    def get_working_proxy(self) -> Optional[ProxySpec]:
        """Random working proxy, or None if the pool is empty (=> go direct)."""
        if not self._working:
            return None
        return random.choice(list(self._working.values()))

    async def report_failure(self, spec: ProxySpec, reason: str = "unknown") -> None:
        """Evict a proxy that failed during real use. It goes to the dead set
        and will be re-tested on the next revival cycle."""
        async with self._lock:
            removed = self._working.pop(spec.key, None)
            if removed is not None:
                self._dead[spec.key] = {"failed_at": time.time(), "reason": reason[:200]}
        if removed is not None:
            logger.warning(
                "Proxy %s marked DEAD (%s). Working pool: %d left.",
                spec.key, reason, len(self._working),
            )
            await self._persist(spec, ProxyState.DEAD, error=reason)

    async def fetch(
        self,
        method: str,
        url: str,
        *,
        max_proxy_tries: int = 3,
        request_timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> httpx.Response:
        """Perform an HTTP request with automatic proxy rotation.

        1. Picks random working proxies (up to ``max_proxy_tries`` distinct ones).
        2. On a proxy-level failure (connect/timeout/socks error/407) evicts it
           and tries the next.
        3. If no working proxies remain: falls back to a direct connection
           (when ``allow_direct_fallback``), else raises the last error.

        HTTP 4xx/5xx from the target site is *returned* as-is — the proxy itself
        worked, so it is not evicted.
        """
        last_err: Optional[Exception] = None
        tried: set[str] = set()

        for _ in range(max_proxy_tries):
            candidates = [s for s in self._working.values() if s.key not in tried]
            if not candidates:
                break
            spec = random.choice(candidates)
            tried.add(spec.key)

            try:
                async with self._new_client(spec.url, timeout=request_timeout) as client:
                    resp = await client.request(method, url, **kwargs)
                if resp.status_code == 407:  # proxy auth rejected -> treat as dead proxy
                    raise httpx.ProxyError(f"407 Proxy Authentication Required via {spec.key}")
                logger.debug("fetch %s %s via %s -> HTTP %s", method, url, spec.key, resp.status_code)
                return resp
            except PROXY_FAILURE_EXC as exc:
                last_err = exc
                logger.warning(
                    "Proxy %s failed during use (%s) — evicting and trying next.",
                    spec.key, type(exc).__name__,
                )
                await self.report_failure(spec, reason=f"{type(exc).__name__}: {exc}")

        if self.allow_direct_fallback:
            logger.warning("No working proxies available — DIRECT connection fallback for %s %s", method, url)
            async with self._new_client(None, timeout=request_timeout) as client:
                return await client.request(method, url, **kwargs)

        raise last_err or RuntimeError("No working proxies available and direct fallback is disabled")

    def _new_client(self, proxy_url: Optional[str], timeout: Optional[float] = None) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            proxy=proxy_url,
            timeout=timeout or self.timeout,
            follow_redirects=True,
        )

    # ------------------------------------------------------------------ #
    #  Background loops (revival / re-verification)
    # ------------------------------------------------------------------ #
    async def start(self) -> None:
        """Start background re-tester(s). Call once at app startup."""
        if not self._stopped:
            return
        self._stopped = False
        self._tasks = [asyncio.create_task(self._retest_loop(), name="proxy-pool-retest")]
        if self.reverify_interval:
            self._tasks.append(asyncio.create_task(self._reverify_loop(), name="proxy-pool-reverify"))
        logger.info(
            "Background retester started: dead proxies every %.0fs%s",
            self.retest_interval,
            f", working proxies every {self.reverify_interval:.0f}s" if self.reverify_interval else "",
        )

    async def stop(self) -> None:
        """Cancel background tasks. Call on app shutdown."""
        self._stopped = True
        for t in self._tasks:
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks = []
        logger.info("Proxy pool stopped. Final state: %d working, %d dead.", len(self._working), len(self._dead))

    async def recheck_dead_now(self) -> None:
        """Manually trigger a re-test of all dead proxies (also used by the loop)."""
        dead = [self._all[k] for k in list(self._dead) if k in self._all]
        if not dead:
            return
        logger.info("Re-testing %d dead proxy(ies)...", len(dead))
        await self._run_checks(dead)
        logger.info("Re-test done. Pool: %d working, %d dead.", len(self._working), len(self._dead))

    async def _retest_loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(self.retest_interval)
            try:
                await self.recheck_dead_now()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Dead-proxy retest cycle failed; retrying next interval.")

    async def _reverify_loop(self) -> None:
        while not self._stopped:
            await asyncio.sleep(self.reverify_interval)  # type: ignore[arg-type]
            try:
                working = list(self._working.values())
                if working:
                    logger.info("Re-verifying %d working proxy(ies)...", len(working))
                    await self._run_checks(working)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Working-proxy reverify cycle failed; retrying next interval.")

    # ------------------------------------------------------------------ #
    #  Persistence (optional, Motor)
    # ------------------------------------------------------------------ #
    async def _persist(self, spec: ProxySpec, state: ProxyState, **extra: Any) -> None:
        if self.mongo_collection is None:
            return
        try:
            doc: dict[str, Any] = {
                "key": spec.key,
                "host": spec.host,
                "port": spec.port,
                "username": spec.username,
                "password": spec.password,
                "proxy_type": spec.proxy_type,
                "state": state.value,
                "checked_at": time.time(),
            }
            doc.update(extra)
            await self.mongo_collection.update_one({"key": spec.key}, {"$set": doc}, upsert=True)
        except Exception:
            logger.warning("MongoDB persist failed for %s", spec.key, exc_info=True)

    # ------------------------------------------------------------------ #
    #  Introspection
    # ------------------------------------------------------------------ #
    def stats(self) -> dict:
        return {
            "total_known": len(self._all),
            "working": len(self._working),
            "dead": len(self._dead),
            "working_proxies": sorted(self._working),
            "dead_proxies": sorted(self._dead),
        }


# --------------------------------------------------------------------------- #
#  Demo
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    src_file = sys.argv[1] if len(sys.argv) > 1 else "proxies.txt"
    sources = load_sources(src_file) if Path(src_file).exists() else [
        "https://t.me/socks?server=220.158.232.118&port=1080",
        "socks5://144.91.121.61:1088",
        "1.2.3.4:1080",
    ]

    async def _demo() -> None:
        pool = ProxyPool(sources, retest_interval=1800)
        await pool.initialize()
        await pool.start()
        print("stats:", pool.stats())

        proxy = pool.get_working_proxy()
        print("random working proxy:", proxy)

        try:
            resp = await pool.fetch("GET", "https://api.ipify.org/?format=json")
            print("fetch via pool ->", resp.status_code, resp.text)
        except Exception as exc:
            print("fetch failed:", exc)
        finally:
            await pool.stop()

    asyncio.run(_demo())
