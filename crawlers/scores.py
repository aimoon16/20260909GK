import os
import json
import time
import datetime
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .base import BaseCrawler
from utils.file_utils import safe_name, write_json_atomic, read_json


def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"true", "1", "yes", "y", "on"}


class ScoreCrawler(BaseCrawler):
    CRAWLER_NAME = "scores"
    ENTITY_TYPE = "school_year_score"
    CURSOR_TYPE = "school_year_cursor"

    REQUEST_TIMEOUT = 10
    FAILURE_PAUSE_THRESHOLD = 8

    def __init__(self):
        super().__init__()
        
        self.db_enabled = False  # 补上缺失的属性定义，默认不走数据库逻辑
        
        self._first_logged = False
        self.export_json = parse_bool(os.getenv("SCORE_EXPORT_JSON", "false"), False)
        self.skip_existing = parse_bool(os.getenv("SCORE_SKIP_EXISTING", "true"), True)

        self.school_data_dir = Path(os.getenv("SCHOOL_DATA_DIR", "data/schools"))
        self.score_data_dir = Path(os.getenv("SCORE_DATA_DIR", "data/scores"))
        self.progress_dir = Path(os.getenv("SCORE_PROGRESS_DIR", "data/scores_progress"))
        self.completed_dir = Path(os.getenv("SCORE_COMPLETED_DIR", "data/scores_progress/completed"))

        self.max_tasks_per_run = int(os.getenv("SCORE_MAX_TASKS_PER_RUN", "20"))
        self.province_workers = max(1, int(os.getenv("SCORE_PROVINCE_WORKERS", "4")))
        self.task_sleep_min = float(os.getenv("SCORE_TASK_SLEEP_MIN", "0.3"))
        self.task_sleep_max = float(os.getenv("SCORE_TASK_SLEEP_MAX", "0.8"))

        self._thread_local = threading.local()

        self.province_dict = {
            "11": "北京",
            "12": "天津",
            "13": "河北",
            "14": "山西",
            "15": "内蒙古",
            "21": "辽宁",
            "22": "吉林",
            "23": "黑龙江",
            "31": "上海",
            "32": "江苏",
            "33": "浙江",
            "34": "安徽",
            "35": "福建",
            "36": "江西",
            "37": "山东",
            "41": "河南",
            "42": "湖北",
            "43": "湖南",
            "44": "广东",
            "45": "广西",
            "46": "海南",
            "50": "重庆",
            "51": "四川",
            "52": "贵州",
            "53": "云南",
            "54": "西藏",
            "61": "陕西",
            "62": "甘肃",
            "63": "青海",
            "64": "宁夏",
            "65": "新疆",
            "71": "台湾",
            "81": "香港",
            "82": "澳门",
        }

        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=max(self.province_workers, 4),
            pool_maxsize=max(self.province_workers, 4),
        )
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        
    def now_str(self):
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
    def build_worker_session(self):
        session = requests.Session()
        session.headers.update(self.headers)

        retry_strategy = Retry(
            total=2,
            connect=2,
            read=2,
            status=2,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=max(self.province_workers, 4),
            pool_maxsize=max(self.province_workers, 4),
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def get_worker_session(self):
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = self.build_worker_session()
            self._thread_local.session = session
        return session

    def parse_years(self, years_input):
        if isinstance(years_input, list):
            return [str(y).strip() for y in years_input if str(y).strip()]

        if isinstance(years_input, str):
            raw = years_input.strip()
            if not raw:
                return []

            if "-" in raw:
                start, end = raw.split("-", 1)
                start = int(start.strip())
                end = int(end.strip())
                if start <= end:
                    return [str(y) for y in range(end, start - 1, -1)]
                return [str(y) for y in range(start, end - 1, -1)]

            if "," in raw:
                return [y.strip() for y in raw.split(",") if y.strip()]

            return [raw]

        return years_input or []

    def normalize_school_target(self, school_id, school_name=None):
        if not school_id:
            return None
        return {
            "school_id": str(school_id),
            "school_name": school_name,
        }

    def load_school_targets_from_files(self):
        sample_count = int(os.getenv("SAMPLE_SCHOOLS", "3"))

        targets = []
        seen_ids = set()

        def add_school(school, fallback_name=None):
            if not isinstance(school, dict):
                return
            school_id = school.get("school_id")
            school_name = school.get("school_name") or school.get("name") or fallback_name
            target = self.normalize_school_target(school_id, school_name)
            if not target or target["school_id"] in seen_ids:
                return
            seen_ids.add(target["school_id"])
            targets.append(target)

        # Preferred retained-repo seed: data/schools.json with {"data": [...]}.
        school_data_file = Path(os.getenv("SCHOOL_DATA_FILE", "data/schools.json"))
        if school_data_file.exists():
            payload = read_json(school_data_file, default={}) or {}
            if isinstance(payload, dict):
                schools = payload.get("data") or payload.get("schools") or []
            elif isinstance(payload, list):
                schools = payload
            else:
                schools = []
            for item in schools:
                add_school(item)
        elif self.school_data_dir.exists():
            for path in self.school_data_dir.rglob("*.json"):
                try:
                    payload = read_json(path, default={}) or {}
                    data = payload.get("data") if isinstance(payload, dict) else None
                    school = data if isinstance(data, dict) else payload
                    add_school(school, fallback_name=path.stem)
                except Exception as e:
                    print(f"⚠️  读取学校文件失败: {path} - {e}")
        else:
            print(f"⚠️  未找到学校文件 {school_data_file} 或学校目录 {self.school_data_dir}")
            return []

        def sort_key(x):
            sid = str(x["school_id"])
            return (0, int(sid)) if sid.isdigit() else (1, sid)

        targets.sort(key=sort_key)

        if sample_count > 0:
            targets = targets[:sample_count]

        if targets:
            print(f"读取到 {len(targets)} 所学校")
        else:
            print("⚠️  未读取到有效学校目标")

        return targets

    def load_school_targets(self, school_targets=None):
        if school_targets is not None:
            result = []
            for item in school_targets:
                if isinstance(item, dict):
                    target = self.normalize_school_target(
                        item.get("school_id"),
                        item.get("school_name") or item.get("name"),
                    )
                else:
                    target = self.normalize_school_target(item, None)
                if target:
                    result.append(target)
            return result

        file_targets = self.load_school_targets_from_files()
        if file_targets:
            return file_targets

        print("⚠️  没有可用学校数据")
        return []

    def get_progress_file(self, scope_key):
        custom = os.getenv("SCORE_PROGRESS_FILE", "").strip()
        if custom:
            return Path(custom)
        safe_scope = safe_name(str(scope_key))
        return self.progress_dir / f"{safe_scope}.json"

    def load_progress_state(self, scope_key, years, target_school_ids):
        path = self.get_progress_file(scope_key)
        if not path.exists():
            return set(), {}

        progress = read_json(path, default={}) or {}

        saved_years = [str(x) for x in progress.get("years", [])]
        current_years = [str(x) for x in years]
        if saved_years and saved_years != current_years:
            print("⚠️  progress 的年份集合与当前不一致，忽略旧断点")
            return set(), {}

        saved_target_ids = [str(x) for x in progress.get("target_school_ids", [])]
        current_target_ids = [str(x) for x in target_school_ids]
        if saved_target_ids and saved_target_ids != current_target_ids:
            print("⚠️  progress 的目标学校集合与当前不一致，忽略旧断点")
            return set(), {}

        completed_keys = {str(x) for x in progress.get("completed_keys", [])}
        print(f"↻ 检测到断点：已完成 {len(completed_keys)} / {len(target_school_ids) * len(years)} 个学校年份组合")
        return completed_keys, progress

    def save_progress_state(
        self,
        scope_key,
        years,
        target_school_ids,
        completed_keys,
        last_school_id=None,
        last_year=None,
        total_records=0,
        consecutive_failures=0,
        last_error=None,
    ):
        path = self.get_progress_file(scope_key)
        payload = {
            "years": [str(x) for x in years],
            "target_school_ids": [str(x) for x in target_school_ids],
            "completed_keys": sorted(str(x) for x in completed_keys),
            "school_total": len(target_school_ids),
            "year_total": len(years),
            "task_total": len(target_school_ids) * len(years),
            "task_done": len(completed_keys),
            "last_school_id": str(last_school_id) if last_school_id else None,
            "last_year": str(last_year) if last_year else None,
            "total_records": int(total_records),
            "consecutive_failures": int(consecutive_failures),
            "last_error": last_error,
            "updated_at": self.now_str(),
        }
        write_json_atomic(path, payload)

    def clear_progress_state(self, scope_key):
        path = self.get_progress_file(scope_key)
        if path.exists():
            path.unlink()

    def get_completed_marker_file(self, year_label, school_id):
        return self.completed_dir / str(year_label) / f"{school_id}.json"

    def mark_task_completed(self, school_id, school_name, year_label, province_ids, hit_province_ids, record_count):
        payload = {
            "year": str(year_label),
            "school_id": str(school_id),
            "school_name": school_name,
            "checked_province_ids": [str(x) for x in province_ids],
            "hit_province_ids": sorted(str(x) for x in hit_province_ids),
            "record_count": int(record_count),
            "completed_at": self.now_str(),
        }
        write_json_atomic(self.get_completed_marker_file(year_label, school_id), payload)

    def get_completed_keys_from_files(self, years):
        result = set()
        for year_label in years:
            root = self.completed_dir / str(year_label)
            if not root.exists():
                continue

            for path in root.rglob("*.json"):
                try:
                    payload = read_json(path, default={}) or {}
                    school_id = payload.get("school_id")
                    year = payload.get("year") or year_label
                    if school_id and year:
                        result.add(f"{str(school_id)}:{str(year)}")
                except Exception:
                    continue
        return result

    def get_primary_score_file_path(self, year_label, province_id, school_name):
        province_name = safe_name(self.province_dict.get(str(province_id), f"省份{province_id}"))
        school_file_name = safe_name(school_name or "未知学校")
        return self.score_data_dir / str(year_label) / province_name / f"{school_file_name}.json"

    def get_suffix_score_file_path(self, year_label, province_id, school_name, school_id):
        province_name = safe_name(self.province_dict.get(str(province_id), f"省份{province_id}"))
        school_file_name = f"{safe_name(school_name or school_id or '未知学校')}__{school_id}"
        return self.score_data_dir / str(year_label) / province_name / f"{school_file_name}.json"

    def find_existing_score_file(self, year_label, province_id, school_name, school_id):
        primary = self.get_primary_score_file_path(year_label, province_id, school_name)
        suffix = self.get_suffix_score_file_path(year_label, province_id, school_name, school_id)

        if primary.exists():
            try:
                payload = read_json(primary, default={}) or {}
                if str(payload.get("school_id")) == str(school_id):
                    return primary
            except Exception:
                pass

        if suffix.exists():
            try:
                payload = read_json(suffix, default={}) or {}
                if str(payload.get("school_id")) == str(school_id):
                    return suffix
            except Exception:
                pass

        return None

    def get_score_file_path(self, year_label, province_id, school_name, school_id):
        existing = self.find_existing_score_file(year_label, province_id, school_name, school_id)
        if existing:
            return existing

        primary = self.get_primary_score_file_path(year_label, province_id, school_name)
        if primary.exists():
            return self.get_suffix_score_file_path(year_label, province_id, school_name, school_id)

        return primary

    def score_file_exists(self, year_label, province_id, school_name, school_id):
        return self.find_existing_score_file(year_label, province_id, school_name, school_id) is not None

    def get_existing_province_ids_for_task(self, school_id, school_name, year_label, province_ids):
        existing = set()
        for province_id in province_ids:
            if self.score_file_exists(year_label, province_id, school_name, school_id):
                existing.add(str(province_id))
        return existing

    def get_score_data(self, school_id, year, province_id, session=None):
        url = f"https://static-data.gaokao.cn/www/2.0/schoolspecialscore/{school_id}/{year}/{province_id}.json"
        result = self.get_json_with_session(
            session=session or self.session,
            url=url,
            retry=3,
            delay=2,
            timeout=self.REQUEST_TIMEOUT,
            allow_404=True,
        )

        if result == "no_data":
            return "no_data"

        if isinstance(result, dict) and self.is_success_code(result.get("code")) and "data" in result:
            return result["data"]

        return None

    def get_json_with_session(self, session, url, retry=3, delay=2, timeout=15, allow_404=False):
        for attempt in range(retry):
            try:
                response = session.get(url, timeout=timeout)

                if response.status_code == 200:
                    try:
                        return response.json()
                    except json.JSONDecodeError as e:
                        print(f"⚠️  JSON解析失败: {str(e)}")
                        print(f"   URL: {url}")
                        print(f"   响应前200字符: {response.text[:200]}")
                        return None

                if allow_404 and response.status_code == 404:
                    return "no_data"

                if response.status_code in (429, 500, 502, 503, 504):
                    pass

            except requests.exceptions.Timeout:
                pass
            except requests.exceptions.RequestException:
                pass

            if attempt < retry - 1:
                time.sleep(delay * (attempt + 1))

        return None

    def log_first_structure(self, school_id, year, province_id, province_name, data):
        if self._first_logged or not data or data == "no_data":
            return

        print(f"\n   📡 [分数线接口] school_id={school_id}, year={year}, province={province_name}")
        print(f"      URL: https://static-data.gaokao.cn/www/2.0/schoolspecialscore/{school_id}/{year}/{province_id}.json")
        print(f"\n      {'─' * 50}")
        print("      首次响应数据结构:")
        print(f"      {'─' * 50}")
        print(f"      data类型: {type(data).__name__}")

        if isinstance(data, dict):
            print(f"      data包含键: {list(data.keys())}")

            sample_item = None
            for major_type, major_info in data.items():
                if isinstance(major_info, dict):
                    items = major_info.get("item", [])
                    if items:
                        sample_item = items[0]
                        print(f"      招生类型: {major_type}")
                        print(f"      该类型数据条数: {len(items)}")
                        break

            if sample_item and isinstance(sample_item, dict):
                fields = list(sample_item.keys())
                print(f"\n      分数线数据字段({len(fields)}个):")
                print(f"      {'─' * 50}")
                for i, field in enumerate(fields, 1):
                    value = sample_item[field]
                    value_type = type(value).__name__
                    if value is None:
                        preview = "None"
                    elif isinstance(value, str):
                        preview = f'"{value[:25]}..."' if len(value) > 25 else f'"{value}"'
                    elif isinstance(value, (list, dict)):
                        preview = f"{value_type}({len(value)}项)"
                    else:
                        preview = str(value)
                    print(f"      {i:2}. {field:25} = {preview}")
                print(f"      {'─' * 50}\n")

        self._first_logged = True

    def extract_score_records(self, school_id, year, province_id, province_name, data):
        records = []

        if not data or data == "no_data" or not isinstance(data, dict):
            return records

        for major_type, major_info in data.items():
            if not isinstance(major_info, dict):
                continue

            items = major_info.get("item", [])
            for item in items:
                if not isinstance(item, dict):
                    continue

                records.append({
                    "school_id": str(school_id),
                    "year": str(year),
                    "province_id": str(province_id),
                    "province": province_name,
                    "major_type": major_type,
                    "batch": item.get("local_batch_name"),
                    "type": item.get("type"),
                    "recruit_type": item.get("zslx_name"),
                    "major": item.get("sp_name") or item.get("spname"),
                    "major_code": item.get("spcode"),
                    "major_group": item.get("sg_name"),
                    "major_group_info": item.get("sg_info"),
                    "level1_name": item.get("level1_name"),
                    "level2_name": item.get("level2_name"),
                    "level3_name": item.get("level3_name"),
                    "min_score": item.get("min"),
                    "max_score": item.get("max"),
                    "avg_score": item.get("average") or item.get("avg"),
                    "min_rank": item.get("min_section"),
                    "proscore": item.get("proscore"),
                    "enrollment": item.get("lq_num") or item.get("sg_info"),
                })

        return records

    def build_task_payload(self, school_id, school_name, year, province_id, raw_data, records):
        province_name = self.province_dict.get(str(province_id), f"省份{province_id}")
        return {
            "update_time": self.now_str(),
            "school_id": str(school_id),
            "school_name": school_name,
            "year": str(year),
            "province_id": str(province_id),
            "province": province_name,
            "record_count": len(records),
            "records": records,
            "data": raw_data,
        }

    def save_score_json(self, year_label, province_id, school_id, school_name, payload):
        file_path = self.get_score_file_path(
            year_label=year_label,
            province_id=province_id,
            school_name=school_name,
            school_id=school_id,
        )
        write_json_atomic(file_path, payload)

    def fetch_one_province(self, school_id, school_name, year_label, province_id):
        province_id = str(province_id)
        province_name = self.province_dict.get(province_id, f"省份{province_id}")

        try:
            session = self.get_worker_session()
            data = self.get_score_data(school_id, year_label, province_id, session=session)

            if data == "no_data":
                return {
                    "province_id": province_id,
                    "province_name": province_name,
                    "status": "no_data",
                    "records": [],
                    "payload": None,
                    "raw_data": None,
                }

            if data is None:
                return {
                    "province_id": province_id,
                    "province_name": province_name,
                    "status": "error",
                    "records": [],
                    "payload": None,
                    "raw_data": None,
                }

            records = self.extract_score_records(school_id, year_label, province_id, province_name, data)
            payload = self.build_task_payload(
                school_id=school_id,
                school_name=school_name,
                year=year_label,
                province_id=province_id,
                raw_data=data,
                records=records,
            )

            return {
                "province_id": province_id,
                "province_name": province_name,
                "status": "ok",
                "records": records,
                "payload": payload,
                "raw_data": data,
            }
        except Exception:
            return {
                "province_id": province_id,
                "province_name": province_name,
                "status": "error",
                "records": [],
                "payload": None,
                "raw_data": None,
            }

    def crawl(self, school_ids=None, years=None, province_ids=None, mode=None):
        years = self.parse_years(years or os.getenv("SCORE_YEARS", "2025,2024,2023,2022,2021,2020"))
        province_ids = [str(x) for x in (province_ids or list(self.province_dict.keys()))]

        if not years:
            print("⚠️  未提供有效年份")
            return []

        school_targets = self.load_school_targets(school_ids)
        if not school_targets:
            return []

        target_school_ids = [t["school_id"] for t in school_targets]
        scope_key = f"years:{','.join(years)}"

        if self.db_enabled:
            self.job_id = self.start_job(
                crawler_name=self.CRAWLER_NAME,
                mode=mode or os.getenv("CRAWL_MODE", "full"),
                scope_key=scope_key,
                year=None,
                meta_json={
                    "target_school_count": len(target_school_ids),
                    "years": years,
                    "province_count": len(province_ids),
                },
            )

        progress_completed_keys, progress_meta = self.load_progress_state(
            scope_key=scope_key,
            years=years,
            target_school_ids=target_school_ids,
        )

        file_completed_keys = set()
        if self.skip_existing:
            file_completed_keys = self.get_completed_keys_from_files(years)
            if file_completed_keys:
                print(f"✓ 已存在 {len(file_completed_keys)} 个学校年份分数线完成标记，自动跳过")

        completed_keys = set(progress_completed_keys) | set(file_completed_keys)

        pending_tasks = []
        for target in school_targets:
            school_id = target["school_id"]
            for year in years:
                task_key = f"{school_id}:{year}"
                if task_key not in completed_keys:
                    pending_tasks.append((target, str(year), task_key))

        total_records = int(progress_meta.get("total_records", 0) or 0)
        consecutive_failures = int(progress_meta.get("consecutive_failures", 0) or 0)

        print(f"\n{'=' * 60}")
        print("开始爬取分数线")
        print(f"学校数: {len(target_school_ids)}")
        print(f"年份: {', '.join(years)}")
        print(f"省份: {len(province_ids)} 个")
        print(f"已完成任务: {len(completed_keys)}")
        print(f"待爬任务: {len(pending_tasks)}")
        print(f"学校目录: {self.school_data_dir}")
        print(f"输出目录: {self.score_data_dir}")
        print(f"进度目录: {self.progress_dir}")
        print(f"完成标记目录: {self.completed_dir}")
        print(f"数据库启用: {'✓' if self.db_enabled else '✗'}")
        print(f"省份并发数: {self.province_workers}")
        print(f"单次最多完成任务数: {self.max_tasks_per_run}")
        print(f"{'=' * 60}\n")

        if not pending_tasks:
            self.clear_progress_state(scope_key)
            if self.db_enabled:
                self.mark_job_done(
                    meta_json={
                        "completed_tasks": len(completed_keys),
                        "target_school_count": len(target_school_ids),
                        "year_count": len(years),
                        "total_records": total_records,
                    }
                )
            print(f"\n{'=' * 60}")
            print("✅ 当前年份集合已全部完成，无需继续爬取")
            print(f"   完成任务: {len(completed_keys)} / {len(target_school_ids) * len(years)}")
            print(f"{'=' * 60}\n")
            return []

        all_scores = [] if self.export_json else None
        processed_this_run = 0
        has_incomplete_task = False

        try:
            for idx, (target, year, task_key) in enumerate(pending_tasks, 1):
                school_id = target["school_id"]
                school_name = target.get("school_name")

                print(f"\n[{idx}/{len(pending_tasks)}] 学校ID: {school_id}" + (f" ({school_name})" if school_name else "") + f" | 年份: {year}")

                existing_province_ids = set()
                if self.skip_existing:
                    existing_province_ids = self.get_existing_province_ids_for_task(
                        school_id=school_id,
                        school_name=school_name,
                        year_label=year,
                        province_ids=province_ids,
                    )
                    if existing_province_ids:
                        print(f"   ↻ 已存在命中省份文件 {len(existing_province_ids)} 个，本次继续补齐")

                task_records = []
                task_record_count = 0
                province_hit = len(existing_province_ids)
                task_incomplete = False
                hit_province_ids = set(existing_province_ids)
                should_pause = False

                pending_province_ids = [
                    str(pid) for pid in province_ids
                    if not (self.skip_existing and str(pid) in existing_province_ids)
                ]

                if pending_province_ids:
                    with ThreadPoolExecutor(max_workers=self.province_workers) as executor:
                        future_map = {
                            executor.submit(
                                self.fetch_one_province,
                                school_id,
                                school_name,
                                year,
                                province_id,
                            ): province_id
                            for province_id in pending_province_ids
                        }

                        for future in as_completed(future_map):
                            result = future.result()
                            province_id = str(result["province_id"])
                            province_name = result["province_name"]
                            status = result["status"]

                            if status == "no_data":
                                consecutive_failures = 0
                                continue

                            if status == "error":
                                consecutive_failures += 1
                                task_incomplete = True
                                print(f"   ⚠️  {province_name}: 请求异常，连续异常 {consecutive_failures}/{self.FAILURE_PAUSE_THRESHOLD}")

                                self.save_progress_state(
                                    scope_key=scope_key,
                                    years=years,
                                    target_school_ids=target_school_ids,
                                    completed_keys=completed_keys,
                                    last_school_id=school_id,
                                    last_year=year,
                                    total_records=total_records,
                                    consecutive_failures=consecutive_failures,
                                    last_error=f"score request failed: school_id={school_id}, year={year}, province_id={province_id}",
                                )

                                if self.should_pause_on_rate_limit(consecutive_failures, self.FAILURE_PAUSE_THRESHOLD):
                                    should_pause = True
                                continue

                            consecutive_failures = 0
                            if not self._first_logged and result["raw_data"] and result["raw_data"] != "no_data":
                                self.log_first_structure(school_id, year, province_id, province_name, result["raw_data"])

                            self.save_score_json(
                                year_label=year,
                                province_id=province_id,
                                school_id=school_id,
                                school_name=school_name,
                                payload=result["payload"],
                            )

                            hit_province_ids.add(province_id)
                            province_hit += 1
                            task_records.extend(result["records"])
                            task_record_count += len(result["records"])

                if should_pause:
                    msg = f"连续请求异常达到阈值，暂停任务: school_id={school_id}, year={year}"
                    print(f"⏸️  {msg}")
                    self.save_progress_state(
                        scope_key=scope_key,
                        years=years,
                        target_school_ids=target_school_ids,
                        completed_keys=completed_keys,
                        last_school_id=school_id,
                        last_year=year,
                        total_records=total_records,
                        consecutive_failures=consecutive_failures,
                        last_error=msg,
                    )
                    if self.db_enabled:
                        self.mark_job_paused(
                            error_message=msg,
                            meta_json={
                                "completed_tasks": len(completed_keys),
                                "total_records": total_records,
                                "last_school_id": str(school_id),
                                "last_year": str(year),
                            },
                        )
                    return all_scores if self.export_json else []

                if not task_incomplete:
                    completed_keys.add(task_key)
                    processed_this_run += 1
                    total_records += task_record_count

                    self.mark_task_completed(
                        school_id=school_id,
                        school_name=school_name,
                        year_label=year,
                        province_ids=province_ids,
                        hit_province_ids=hit_province_ids,
                        record_count=task_record_count,
                    )

                    self.save_progress_state(
                        scope_key=scope_key,
                        years=years,
                        target_school_ids=target_school_ids,
                        completed_keys=completed_keys,
                        last_school_id=school_id,
                        last_year=year,
                        total_records=total_records,
                        consecutive_failures=consecutive_failures,
                        last_error=None,
                    )

                    if self.export_json and task_records:
                        all_scores.extend(task_records)

                    if task_records:
                        print(f"   ✅ 已保存文件：命中省份 {province_hit} 个，记录数 {task_record_count}")
                    else:
                        print("   ⚠️  该学校该年份无分数线数据，已标记为完成")

                    if self.max_tasks_per_run > 0 and processed_this_run >= self.max_tasks_per_run:
                        remaining = len(pending_tasks) - idx
                        if remaining > 0:
                            print(f"   ⏸️  达到单次运行任务上限 {self.max_tasks_per_run}，本次先退出并等待提交")
                            return all_scores if self.export_json else []
                else:
                    has_incomplete_task = True
                    self.save_progress_state(
                        scope_key=scope_key,
                        years=years,
                        target_school_ids=target_school_ids,
                        completed_keys=completed_keys,
                        last_school_id=school_id,
                        last_year=year,
                        total_records=total_records,
                        consecutive_failures=consecutive_failures,
                        last_error="task not fully completed in this run",
                    )
                    print("   ⏸️  该学校该年份本次未完全补齐，已保存阶段性进度")

                if idx < len(pending_tasks):
                    self.polite_sleep(self.task_sleep_min, self.task_sleep_max)

            if len(completed_keys) == len(target_school_ids) * len(years) and not has_incomplete_task:
                self.clear_progress_state(scope_key)
                if self.db_enabled:
                    self.mark_job_done(
                        meta_json={
                            "completed_tasks": len(completed_keys),
                            "target_school_count": len(target_school_ids),
                            "year_count": len(years),
                            "total_records": total_records,
                        }
                    )

                print(f"\n{'=' * 60}")
                print("✅ 分数线爬取完成！")
                print(f"   总计记录数: {total_records}")
                print(f"   完成任务: {len(completed_keys)} / {len(target_school_ids) * len(years)}")
                print(f"   输出目录: {self.score_data_dir}")
                print(f"{'=' * 60}\n")

                return all_scores if self.export_json else []

            print(f"\n{'=' * 60}")
            print("⏸️ 分数线本次未全部完成")
            print(f"   总计记录数: {total_records}")
            print(f"   完成任务: {len(completed_keys)} / {len(target_school_ids) * len(years)}")
            print(f"   进度文件: {self.get_progress_file(scope_key)}")
            print(f"{'=' * 60}\n")

            return all_scores if self.export_json else []

        except Exception as e:
            self.save_progress_state(
                scope_key=scope_key,
                years=years,
                target_school_ids=target_school_ids,
                completed_keys=completed_keys,
                last_school_id=None,
                last_year=None,
                total_records=total_records,
                consecutive_failures=consecutive_failures,
                last_error=str(e),
            )
            if self.db_enabled:
                self.mark_job_failed(
                    error_message=str(e),
                    meta_json={
                        "completed_tasks": len(completed_keys),
                        "target_school_count": len(target_school_ids),
                        "year_count": len(years),
                        "total_records": total_records,
                    },
                )
            raise


if __name__ == "__main__":
    import sys

    years_arg = sys.argv[1] if len(sys.argv) > 1 else None
    mode_arg = sys.argv[2] if len(sys.argv) > 2 else os.getenv("CRAWL_MODE", "full")

    crawler = ScoreCrawler()
    crawler.crawl(
        years=years_arg,
        mode=mode_arg,
    )
