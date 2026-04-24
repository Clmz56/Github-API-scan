#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GitHub Secret Scanner Pro v3.0 - Pure Asyncio Refactor

Architecture:
- Single asyncio event loop (no threading for core logic)
- PyGithub calls run in thread executor (it's sync internally)
- aiohttp for async downloads and validation
- asyncio.Queue for producer-consumer pipeline
- Rich TUI dashboard
"""

import sys
import signal
import asyncio
import argparse
import csv
from datetime import datetime
from typing import Optional
from concurrent.futures import ThreadPoolExecutor

from loguru import logger

from config import config
from database import Database, KeyStatus
from scanner import GitHubScanner, ScanResult
from validator import AsyncValidator, ValidationResult, mask_key
from ui import Dashboard


class SecretScanner:
    """Pure asyncio secret scanner - single event loop orchestration."""

    def __init__(
        self,
        enable_pastebin: bool = False,
        enable_gist: bool = False,
        enable_searchcode: bool = False,
        enable_gitlab: bool = False,
        enable_realtime: bool = False,
        pastebin_api_key: str = "",
    ):
        self.db = Database(config.db_path)
        self.dashboard = Dashboard()
        self.queue: Optional[asyncio.Queue] = None
        self._stop: Optional[asyncio.Event] = None
        self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="scanner")

        # Source toggles
        self.enable_pastebin = enable_pastebin
        self.enable_gist = enable_gist
        self.enable_searchcode = enable_searchcode
        self.enable_gitlab = enable_gitlab
        self.enable_realtime = enable_realtime
        self.pastebin_api_key = pastebin_api_key

    async def _run_scanner(self):
        """Run the GitHub scanner in executor (PyGithub is sync)."""
        scanner = GitHubScanner(
            result_queue=self.queue,
            db=self.db,
            stop_event=self._stop,
            dashboard=self.dashboard,
        )

        loop = asyncio.get_running_loop()

        # Run the blocking scanner.run() in a thread executor
        # We wrap it to bridge the sync scanner with our async queue
        def scanner_sync_runner():
            scanner.run(resume=False)

        await loop.run_in_executor(self._executor, scanner_sync_runner)

    async def _run_validator(self, worker_id: int):
        """Async validator consumer - processes queue items."""
        validator = AsyncValidator(self.db, self.dashboard)

        try:
            while not self._stop.is_set():
                # Collect a batch
                batch = []
                try:
                    # Wait for first item
                    result = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                    batch.append(result)

                    # Drain more items without blocking
                    while len(batch) < 50:
                        try:
                            result = self.queue.get_nowait()
                            batch.append(result)
                        except asyncio.QueueEmpty:
                            break
                except asyncio.TimeoutError:
                    continue

                if batch:
                    self.dashboard.update_stats(queue_size=self.queue.qsize())
                    await validator.run_batch(batch)

        except asyncio.CancelledError:
            pass
        finally:
            await validator.close()

    async def _run_dashboard(self):
        """Dashboard refresh loop."""
        while not self._stop.is_set():
            self.dashboard.update_stats(queue_size=self.queue.qsize())
            self.dashboard.refresh()
            await asyncio.sleep(0.25)

    async def run(self):
        """Main async entry point."""
        # Initialize async objects inside the event loop
        self.queue = asyncio.Queue(maxsize=5000)
        self._stop = asyncio.Event()

        # Signal handling
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._stop.set)

        self.dashboard.update_stats(
            total_tokens=len(config.github_tokens),
            is_running=True,
        )

        # Start TUI
        with self.dashboard.start():
            # Launch tasks
            tasks = []

            # Scanner (runs in executor)
            tasks.append(asyncio.create_task(self._run_scanner()))

            # Validators (2 async workers, each with 100 concurrency via semaphore)
            for i in range(2):
                tasks.append(asyncio.create_task(self._run_validator(i)))

            # Dashboard refresh
            tasks.append(asyncio.create_task(self._run_dashboard()))

            # Optional sources (run in executor since they're sync)
            if self.enable_pastebin:
                tasks.append(asyncio.create_task(self._run_extra_source("pastebin")))
            if self.enable_gist:
                tasks.append(asyncio.create_task(self._run_extra_source("gist")))
            if self.enable_searchcode:
                tasks.append(asyncio.create_task(self._run_extra_source("searchcode")))
            if self.enable_gitlab:
                tasks.append(asyncio.create_task(self._run_extra_source("gitlab")))
            if self.enable_realtime:
                tasks.append(asyncio.create_task(self._run_extra_source("realtime")))

            # Wait for stop signal
            await self._stop.wait()

            # Cancel all tasks
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        self.dashboard.stop()
        self._executor.shutdown(wait=False)
        logger.info("Scanner stopped.")

    async def _run_extra_source(self, source_type: str):
        """Run extra scan sources in executor (they're sync/threaded)."""
        import threading

        stop_event = threading.Event()
        loop = asyncio.get_running_loop()

        # Bridge async stop to threading stop
        async def watch_stop():
            await self._stop.wait()
            stop_event.set()

        asyncio.create_task(watch_stop())

        def run_source():
            if source_type == "pastebin":
                from source_pastebin import start_pastebin_scanner
                t = start_pastebin_scanner(self.queue, stop_event, self.dashboard, self.pastebin_api_key)
                t.join()
            elif source_type == "gist":
                from source_gist import start_gist_scanner
                t = start_gist_scanner(self.queue, stop_event, self.dashboard)
                t.join()
            elif source_type == "searchcode":
                from source_searchcode import start_searchcode_scanner
                t = start_searchcode_scanner(self.queue, stop_event, self.dashboard)
                t.join()
            elif source_type == "gitlab":
                from source_gitlab import start_gitlab_scanner
                t = start_gitlab_scanner(self.queue, stop_event, self.dashboard)
                t.join()
            elif source_type == "realtime":
                from source_realtime import start_realtime_scanner
                t = start_realtime_scanner(self.queue, stop_event, self.dashboard)
                t.join()

        await loop.run_in_executor(self._executor, run_source)


# ============================================================================
#                          CLI Functions
# ============================================================================

def export_keys(db_path: str, output_file: str, status_filter: str = None):
    """Export keys to text file."""
    from rich.console import Console

    console = Console()
    db = Database(db_path)

    if status_filter:
        try:
            status = KeyStatus(status_filter)
            keys = db.get_keys_by_status(status)
        except ValueError:
            console.print(f"[red]Invalid status: {status_filter}[/]")
            return
    else:
        keys = db.get_valid_keys()

    if not keys:
        console.print("[yellow]No keys found[/]")
        return

    with open(output_file, "w", encoding="utf-8") as f:
        f.write(f"# GitHub Secret Scanner Export\n")
        f.write(f"# Time: {datetime.now().isoformat()}\n")
        f.write(f"# Count: {len(keys)}\n")
        f.write("=" * 60 + "\n\n")

        for key in keys:
            f.write(f"Platform: {key.platform}\n")
            f.write(f"Status: {key.status}\n")
            f.write(f"Key: {key.api_key}\n")
            f.write(f"URL: {key.base_url}\n")
            f.write(f"Info: {key.balance}\n")
            f.write(f"Source: {key.source_url}\n")
            f.write("-" * 40 + "\n\n")

    console.print(f"[green]Exported {len(keys)} keys to {output_file}[/]")


def export_keys_csv(db_path: str, output_file: str, status_filter: str = None):
    """Export keys to CSV."""
    from rich.console import Console

    console = Console()
    db = Database(db_path)

    if status_filter:
        try:
            status = KeyStatus(status_filter)
            keys = db.get_keys_by_status(status)
        except ValueError:
            console.print(f"[red]Invalid status: {status_filter}[/]")
            return
    else:
        keys = db.get_valid_keys()

    if not keys:
        console.print("[yellow]No keys found[/]")
        return

    fieldnames = [
        "id", "platform", "status", "api_key", "base_url", "balance",
        "source_url", "model_tier", "rpm", "is_high_value", "found_time",
    ]

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for key in keys:
            writer.writerow({
                "id": getattr(key, "id", ""),
                "platform": key.platform,
                "status": key.status,
                "api_key": key.api_key,
                "base_url": key.base_url,
                "balance": key.balance,
                "source_url": key.source_url,
                "model_tier": key.model_tier,
                "rpm": key.rpm,
                "is_high_value": int(bool(getattr(key, "is_high_value", False))),
                "found_time": key.found_time.isoformat() if getattr(key, "found_time", None) else "",
            })

    console.print(f"[green]Exported {len(keys)} keys to CSV: {output_file}[/]")


def show_stats(db_path: str):
    """Show database statistics."""
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich import box

    console = Console()
    db = Database(db_path)
    stats = db.get_stats()

    table = Table(show_header=False, box=box.ROUNDED)
    table.add_column("Item", style="cyan")
    table.add_column("Count", justify="right", style="white")

    table.add_row("Total Keys", str(stats["total"]))
    table.add_row("", "")

    statuses = stats.get("statuses", {})
    table.add_row("[green]Valid[/]", f"[green]{statuses.get('valid', 0)}[/]")
    table.add_row("[yellow]Quota Exceeded[/]", f"[yellow]{statuses.get('quota_exceeded', 0)}[/]")
    table.add_row("[red]Invalid[/]", f"[red]{statuses.get('invalid', 0)}[/]")
    table.add_row("[magenta]Connection Error[/]", f"[magenta]{statuses.get('connection_error', 0)}[/]")

    if stats.get("platforms"):
        table.add_row("", "")
        table.add_row("[bold]Platforms[/]", "")
        for platform, count in stats["platforms"].items():
            table.add_row(f"  {platform}", str(count))

    console.print(Panel(table, title="Database Stats", border_style="cyan"))


def main():
    """Entry point."""
    parser = argparse.ArgumentParser(
        description="GitHub Secret Scanner Pro v3.0",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("--export", type=str, metavar="FILE", help="Export keys to text file")
    parser.add_argument("--export-csv", type=str, metavar="CSV", help="Export keys to CSV")
    parser.add_argument("--status", type=str, help="Filter by status (valid/quota_exceeded)")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    parser.add_argument("--db", type=str, default="leaked_keys.db", help="Database path")
    parser.add_argument("--proxy", type=str, help="Proxy URL")

    # Scan sources
    parser.add_argument("--pastebin", action="store_true", help="Enable Pastebin source")
    parser.add_argument("--pastebin-key", type=str, default="", help="Pastebin Pro API Key")
    parser.add_argument("--gist", action="store_true", help="Enable GitHub Gist source")
    parser.add_argument("--searchcode", action="store_true", help="Enable SearchCode source")
    parser.add_argument("--gitlab", action="store_true", help="Enable GitLab source")
    parser.add_argument("--realtime", action="store_true", help="Enable GitHub Events realtime")
    parser.add_argument("--all-sources", action="store_true", help="Enable all scan sources")

    args = parser.parse_args()

    if args.proxy:
        config.proxy_url = args.proxy
    if args.db:
        config.db_path = args.db

    # Export mode
    if args.export or args.export_csv:
        if args.export:
            export_keys(config.db_path, args.export, args.status)
        if args.export_csv:
            export_keys_csv(config.db_path, args.export_csv, args.status)
        return

    # Stats mode
    if args.stats:
        show_stats(config.db_path)
        return

    # Validate config
    if not config.github_tokens or not any(config.github_tokens):
        logger.error("No GitHub tokens configured!")
        logger.error("Set GITHUB_TOKENS env var or create config_local.py")
        sys.exit(1)

    # Enable uvloop if available
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        logger.info("uvloop enabled")
    except ImportError:
        pass

    # Scan mode
    scanner = SecretScanner(
        enable_pastebin=args.pastebin or args.all_sources,
        enable_gist=args.gist or args.all_sources,
        enable_searchcode=args.searchcode or args.all_sources,
        enable_gitlab=args.gitlab or args.all_sources,
        enable_realtime=args.realtime or args.all_sources,
        pastebin_api_key=args.pastebin_key,
    )
    asyncio.run(scanner.run())


if __name__ == "__main__":
    main()
