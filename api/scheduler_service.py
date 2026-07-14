from __future__ import annotations

import logging
import os
import shlex
import subprocess
import threading
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import Depends, FastAPI, Header, HTTPException
from pydantic import BaseModel, Field, model_validator


logger = logging.getLogger("scheduler_api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")

BASE_DIR = Path(__file__).resolve().parent.parent
SCRIPT_PATH = BASE_DIR / "langgraph_ingestion_flow.py"
DEFAULT_OUTPUT_DIR = Path(os.getenv("DEFAULT_CRAWL_OUTPUT_DIR", str(BASE_DIR / "output_clean_1")))
DEFAULT_CONFIG_PATH = os.getenv("DEFAULT_CONFIG_PATH", str(BASE_DIR / "es_search_config.json"))
DEFAULT_ES_URL = os.getenv("DEFAULT_ES_URL", "http://elasticsearch:9200")
SCHEDULER_API_KEY = os.getenv("SCHEDULER_API_KEY", "")
SCHEDULER_JOB_ID = "langgraph_recurring_ingestion"


class RunOptions(BaseModel):
    max_depth: int = Field(default=3, ge=1)
    max_pages: int = Field(default=150, ge=1)
    crawl_output_dir: str = str(DEFAULT_OUTPUT_DIR)
    payload_output: str | None = None
    embeddings_output: str | None = None
    state_output: str | None = None
    config_path: str = DEFAULT_CONFIG_PATH
    es_url: str = DEFAULT_ES_URL
    es_username: str | None = None
    es_password: str | None = None
    db_url: str | None = None
    model_name: str | None = None
    batch_size: int | None = Field(default=None, ge=1)
    max_retry_rounds: int = Field(default=2, ge=0)
    max_retry_pages: int = Field(default=40, ge=0)
    skip_crawl: bool = False
    recreate_index: bool = False
    strict_partial_crawl: bool = False
    retry_failed_pages: bool = True
    generate_offline_bundle: bool = True
    offline_bundle_dir: str | None = None
    scrape_offers: bool = False
    scrape_rewards: bool = False
    scrape_about_us: bool = False
    max_offers: int | None = Field(default=None, ge=1, description="Cap offer detail pages (omit for full scrape)")
    max_rewards: int | None = Field(default=None, ge=1, description="Cap reward items (omit for full scrape)")
    max_llm_pages: int | None = Field(
        default=None,
        ge=1,
        description="Cap pages sent to LLM enrichment (omit for no cap; requires enable_llm_enrichment)",
    )
    chunk_size: int = Field(default=1200, ge=200)
    chunk_overlap: int = Field(default=200, ge=0)
    enable_llm_enrichment: bool = False

    @model_validator(mode="after")
    def fill_derived_outputs(self) -> "RunOptions":
        output_dir = Path(self.crawl_output_dir)
        if not self.payload_output:
            self.payload_output = str(output_dir / "langgraph_embedding_payloads.jsonl")
        if not self.embeddings_output:
            self.embeddings_output = str(output_dir / "langgraph_embedding_vectors.jsonl")
        if not self.state_output:
            self.state_output = str(output_dir / "langgraph_ingestion_state.json")
        if not self.offline_bundle_dir:
            self.offline_bundle_dir = str(output_dir / "offline_es_bundle")
        if self.chunk_overlap >= self.chunk_size:
            # Keep it safe for chunker logic.
            self.chunk_overlap = max(0, self.chunk_size // 6)
        return self


class ScheduleStartRequest(BaseModel):
    interval_seconds: int | None = Field(default=None, ge=5)
    cron: str | None = None
    run_options: RunOptions = Field(default_factory=RunOptions)

    @model_validator(mode="after")
    def validate_schedule(self) -> "ScheduleStartRequest":
        if self.interval_seconds is None and not (self.cron and self.cron.strip()):
            raise ValueError("Provide either interval_seconds or cron")
        if self.interval_seconds is not None and self.cron:
            raise ValueError("Use only one of interval_seconds or cron")
        return self


@dataclass
class ActiveRun:
    run_id: str
    started_at: str
    trigger: str
    command: list[str]
    log_path: str
    process: subprocess.Popen[Any]
    log_file: Any


class RunManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active: ActiveRun | None = None
        self._last_result: dict[str, Any] | None = None

    def start_run(self, options: RunOptions, trigger: str) -> dict[str, Any]:
        with self._lock:
            if self._active is not None:
                raise RuntimeError("An ingestion run is already in progress")

            run_id = str(uuid.uuid4())
            output_dir = Path(options.crawl_output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            log_path = output_dir / f"langgraph_run_{run_id}.log"
            command = self._build_command(options)

            log_file = log_path.open("a", encoding="utf-8")
            process = subprocess.Popen(  # noqa: S603
                command,
                cwd=str(BASE_DIR),
                stdout=log_file,
                stderr=subprocess.STDOUT,
                text=True,
            )
            active = ActiveRun(
                run_id=run_id,
                started_at=_utc_now(),
                trigger=trigger,
                command=command,
                log_path=str(log_path),
                process=process,
                log_file=log_file,
            )
            self._active = active
            self._last_result = None
            threading.Thread(target=self._watch_process, args=(active,), daemon=True).start()

            return {
                "status": "started",
                "run_id": run_id,
                "started_at": active.started_at,
                "trigger": trigger,
                "pid": process.pid,
                "log_path": str(log_path),
                "command": " ".join(shlex.quote(part) for part in command),
            }

    def status(self) -> dict[str, Any]:
        with self._lock:
            active = self._active
            active_payload: dict[str, Any] | None = None
            if active is not None:
                active_payload = {
                    "run_id": active.run_id,
                    "started_at": active.started_at,
                    "trigger": active.trigger,
                    "pid": active.process.pid,
                    "log_path": active.log_path,
                }
            return {
                "running": active is not None,
                "active_run": active_payload,
                "last_run": self._last_result,
            }

    def _watch_process(self, run: ActiveRun) -> None:
        exit_code = run.process.wait()
        run.log_file.flush()
        run.log_file.close()

        result = {
            "run_id": run.run_id,
            "trigger": run.trigger,
            "started_at": run.started_at,
            "completed_at": _utc_now(),
            "exit_code": exit_code,
            "status": "success" if exit_code == 0 else "failed",
            "log_path": run.log_path,
        }
        with self._lock:
            if self._active and self._active.run_id == run.run_id:
                self._active = None
            self._last_result = result
        logger.info("Ingestion run completed run_id=%s exit_code=%s", run.run_id, exit_code)

    @staticmethod
    def _build_command(options: RunOptions) -> list[str]:
        command = [
            "python",
            str(SCRIPT_PATH),
            "--max-depth",
            str(options.max_depth),
            "--max-pages",
            str(options.max_pages),
            "--crawl-output-dir",
            options.crawl_output_dir,
            "--payload-output",
            str(options.payload_output),
            "--embeddings-output",
            str(options.embeddings_output),
            "--config",
            options.config_path,
            "--output",
            str(options.state_output),
            "--es-url",
            options.es_url,
            "--max-retry-rounds",
            str(options.max_retry_rounds),
            "--max-retry-pages",
            str(options.max_retry_pages),
        ]
        if options.model_name:
            command.extend(["--model-name", options.model_name])
        if options.batch_size is not None:
            command.extend(["--batch-size", str(options.batch_size)])
        if options.es_username:
            command.extend(["--es-username", options.es_username])
        if options.es_password:
            command.extend(["--es-password", options.es_password])
        if options.db_url:
            command.extend(["--db-url", options.db_url])
        if options.skip_crawl:
            command.append("--skip-crawl")
        if options.recreate_index:
            command.append("--recreate-index")
        if options.strict_partial_crawl:
            command.append("--strict-partial-crawl")
        if not options.retry_failed_pages:
            command.append("--no-retry-failed-pages")
        if options.offline_bundle_dir:
            command.extend(["--offline-bundle-dir", options.offline_bundle_dir])
        if not options.generate_offline_bundle:
            command.append("--no-generate-offline-bundle")
        if bool(options.enable_llm_enrichment):
            command.append("--enable-llm-enrichment")
        if bool(options.enable_llm_enrichment) and options.max_llm_pages is not None and options.max_llm_pages > 0:
            command.extend(["--max-llm-pages", str(options.max_llm_pages)])
        if options.scrape_offers:
            command.append("--scrape-offers")
        if options.scrape_rewards:
            command.append("--scrape-rewards")
        if options.scrape_about_us:
            command.append("--scrape-about-us")
        if options.scrape_offers and options.max_offers is not None and options.max_offers > 0:
            command.extend(["--max-offers", str(options.max_offers)])
        if options.scrape_rewards and options.max_rewards is not None and options.max_rewards > 0:
            command.extend(["--max-rewards", str(options.max_rewards)])
        command.extend(["--chunk-size", str(options.chunk_size)])
        command.extend(["--chunk-overlap", str(options.chunk_overlap)])
        return command


class SchedulerManager:
    def __init__(self, run_manager: RunManager) -> None:
        self._run_manager = run_manager
        self._scheduler = BackgroundScheduler(timezone="UTC")
        self._scheduler.start()
        self._lock = threading.Lock()
        self._schedule_config: dict[str, Any] | None = None

    def start(self, request: ScheduleStartRequest) -> dict[str, Any]:
        with self._lock:
            if self._scheduler.get_job(SCHEDULER_JOB_ID) is not None:
                raise RuntimeError("Scheduler is already running")

            if request.interval_seconds is not None:
                trigger: Any = {"trigger": "interval", "seconds": request.interval_seconds}
                self._scheduler.add_job(
                    self._scheduled_run,
                    "interval",
                    seconds=request.interval_seconds,
                    id=SCHEDULER_JOB_ID,
                    replace_existing=False,
                )
                self._schedule_config = {
                    "mode": "interval",
                    "interval_seconds": request.interval_seconds,
                    "run_options": request.run_options.model_dump(),
                }
            else:
                cron_parts = request.cron.strip().split()
                if len(cron_parts) != 5:
                    raise ValueError("cron must be standard 5-part format: m h dom mon dow")
                minute, hour, day, month, day_of_week = cron_parts
                cron_trigger = CronTrigger(
                    minute=minute,
                    hour=hour,
                    day=day,
                    month=month,
                    day_of_week=day_of_week,
                    timezone="UTC",
                )
                self._scheduler.add_job(
                    self._scheduled_run,
                    trigger=cron_trigger,
                    id=SCHEDULER_JOB_ID,
                    replace_existing=False,
                )
                trigger = {"trigger": "cron", "expression": request.cron}
                self._schedule_config = {
                    "mode": "cron",
                    "cron": request.cron,
                    "run_options": request.run_options.model_dump(),
                }

            self._schedule_config["started_at"] = _utc_now()
            self._schedule_config["trigger_config"] = trigger
            self._schedule_config["next_run_at"] = self._next_run_time()
            return {
                "status": "started",
                "schedule": self._schedule_config,
            }

    def stop(self) -> dict[str, Any]:
        with self._lock:
            if self._scheduler.get_job(SCHEDULER_JOB_ID) is None:
                return {"status": "not_running"}
            self._scheduler.remove_job(SCHEDULER_JOB_ID)
            stopped = {"status": "stopped", "stopped_at": _utc_now(), "previous_schedule": self._schedule_config}
            self._schedule_config = None
            return stopped

    def status(self) -> dict[str, Any]:
        with self._lock:
            job = self._scheduler.get_job(SCHEDULER_JOB_ID)
            return {
                "running": job is not None,
                "next_run_at": self._next_run_time(),
                "schedule": self._schedule_config,
            }

    def shutdown(self) -> None:
        self._scheduler.shutdown(wait=False)

    def _scheduled_run(self) -> None:
        with self._lock:
            schedule = self._schedule_config or {}
            options_data = schedule.get("run_options") or RunOptions().model_dump()
        options = RunOptions.model_validate(options_data)
        try:
            self._run_manager.start_run(options, trigger="scheduler")
        except RuntimeError:
            logger.warning("Scheduled run skipped because another ingestion run is in progress")
        except Exception:
            logger.exception("Scheduled run failed to start")

    def _next_run_time(self) -> str | None:
        job = self._scheduler.get_job(SCHEDULER_JOB_ID)
        if not job or not job.next_run_time:
            return None
        return job.next_run_time.astimezone(timezone.utc).isoformat()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _require_api_key(x_api_key: str | None = Header(default=None)) -> None:
    if not SCHEDULER_API_KEY:
        raise HTTPException(status_code=500, detail="SCHEDULER_API_KEY is not configured")
    if x_api_key != SCHEDULER_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


app = FastAPI(title="LangGraph Scheduler API", version="1.0.0")
run_manager = RunManager()
scheduler_manager = SchedulerManager(run_manager=run_manager)


@app.on_event("shutdown")
def _on_shutdown() -> None:
    scheduler_manager.shutdown()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/run-now")
def run_now(request: RunOptions, _: None = Depends(_require_api_key)) -> dict[str, Any]:
    try:
        return run_manager.start_run(request, trigger="manual")
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@app.post("/scheduler/start")
def scheduler_start(request: ScheduleStartRequest, _: None = Depends(_require_api_key)) -> dict[str, Any]:
    try:
        return scheduler_manager.start(request)
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.post("/scheduler/stop")
def scheduler_stop(_: None = Depends(_require_api_key)) -> dict[str, Any]:
    return scheduler_manager.stop()


@app.get("/scheduler/status")
def scheduler_status(_: None = Depends(_require_api_key)) -> dict[str, Any]:
    return {
        "scheduler": scheduler_manager.status(),
        "runs": run_manager.status(),
    }
