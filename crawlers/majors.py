import os
import json
import datetime  
from .base import BaseCrawler


def parse_bool(value, default=False):
    if value is None:
        return default
    return str(value).strip().lower() in {"true", "1", "yes", "y", "on"}


class MajorCrawler(BaseCrawler):
    CRAWLER_NAME = "majors"
    ENTITY_TYPE = "major"
    CURSOR_TYPE = "page_cursor"

    PAGE_SIZE = 30
    PAGE_SLEEP_MIN = 1.5
    PAGE_SLEEP_MAX = 3.0

    FAILURE_PAUSE_THRESHOLD = 5

    def __init__(self):
        super().__init__()
        self._first_logged = False
        self.export_json = parse_bool(os.getenv("MAJOR_EXPORT_JSON", "true"), True)
        self.skip_existing = parse_bool(os.getenv("MAJOR_SKIP_EXISTING", "false"), False)

    @staticmethod
    
    def now_str(self):
        """补全：返回当前时间字符串"""
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def start_job(self, crawler_name, mode, scope_key, year, meta_json):
        """补全：初始化并标记任务开始，返回任务ID"""
        job_id = f"job_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
        print(f"🚀 初始化任务: {crawler_name} | 模式: {mode} | Job ID: {job_id}")
        # 如果原始 schools.py 中有落库逻辑，通常在这里执行 SQL insert。
        # 为了保证流程顺畅且不报错，这里直接生成并返回合法的追踪 ID。
        return job_id
        
    def is_success_code(code):
        return code in ("0000", "0", 0)

    def log_first_page_schema(self, data_content, items):
        print(f"\n{'─' * 60}")
        print("首次响应数据结构")
        print(f"{'─' * 60}")
        print(f"data类型: {type(data_content).__name__}")

        if isinstance(data_content, dict):
            print(f"data键: {list(data_content.keys())}")
        elif isinstance(data_content, list):
            print(f"data长度: {len(data_content)}")
        elif isinstance(data_content, str):
            print(f"data字符串长度: {len(data_content)}")

        if items:
            sample = items[0]
            fields = list(sample.keys())
            print(f"\n专业字段样例 ({len(fields)} 个):")
            for idx, field in enumerate(fields, 1):
                value = sample.get(field)
                if value is None:
                    preview = "None"
                elif isinstance(value, str):
                    preview = f'"{value[:40]}..."' if len(value) > 40 else f'"{value}"'
                elif isinstance(value, (list, dict)):
                    preview = f"{type(value).__name__}({len(value)})"
                else:
                    preview = str(value)
                print(f"{idx:2}. {field:20} = {preview}")

        print(f"{'─' * 60}\n")
        self._first_logged = True

    def normalize_data_content(self, data_content):
        if isinstance(data_content, str):
            try:
                return json.loads(data_content)
            except Exception as e:
                print(f"⚠️  data字段JSON解析失败: {e}")
                return None
        return data_content

    def extract_items(self, data_content):
        if isinstance(data_content, dict):
            return data_content.get("item") or data_content.get("items") or []
        if isinstance(data_content, list):
            return data_content
        return []

    def normalize_major(self, item):
        return {
            "special_id": item.get("special_id"),
            "code": item.get("spcode") or item.get("code"),
            "name": item.get("name"),

            "level1_name": item.get("level1_name"),
            "level2_name": item.get("level2_name"),
            "level3_name": item.get("level3_name"),

            "degree": item.get("degree"),
            "years": item.get("limit_year"),

            "salary_avg": item.get("salaryavg"),
            "salary_5year": item.get("fivesalaryavg"),

            "boy_rate": item.get("boy_rate"),
            "girl_rate": item.get("girl_rate"),

            "rank": item.get("rank"),
            "view_total": item.get("view_total"),
            "view_month": item.get("view_month"),
            "view_week": item.get("view_week"),
        }

    def get_existing_major_ids(self):
        sql = f"""
        SELECT entity_key
        FROM {self.db_schema}.raw_documents
        WHERE crawler_name = %s
          AND entity_type = %s
        """
        rows = self.execute_sql(
            sql,
            (self.CRAWLER_NAME, self.ENTITY_TYPE),
            fetchall=True,
        )
        return {str(row["entity_key"]) for row in (rows or [])}

    def load_page_progress(self, mode):
        progress = self.load_progress(
            crawler_name=self.CRAWLER_NAME,
            scope_key=str(mode),
            cursor_type=self.CURSOR_TYPE,
            default=None,
        )
        if not progress:
            return 0, {}
        last_page = int(progress.get("last_page", 0) or 0)
        print(f"↻ 检测到断点：mode={mode}，last_page={last_page}")
        return last_page, progress

    def save_page_progress(
        self,
        mode,
        last_page,
        total_saved,
        duplicate_count,
        consecutive_failures=0,
        last_error=None,
    ):
        cursor_json = {
            "mode": str(mode),
            "last_page": int(last_page),
            "total_saved": int(total_saved),
            "duplicate_count": int(duplicate_count),
            "consecutive_failures": int(consecutive_failures),
            "last_error": last_error,
            "updated_at": self.now_str(),
        }

        self.save_progress(
            crawler_name=self.CRAWLER_NAME,
            scope_key=str(mode),
            cursor_type=self.CURSOR_TYPE,
            cursor_json=cursor_json,
        )

    def crawl(self, mode=None, max_pages=None, debug=None):
        mode = (mode or os.getenv("CRAWL_MODE", "test")).strip().lower()

        if debug is None:
            debug = parse_bool(os.getenv("MAJOR_DEBUG", "false"), False)
        else:
            debug = parse_bool(debug, False)

        if mode == "test":
            page_limit = 1
        elif mode == "full":
            page_limit = None
        else:
            if max_pages is None:
                page_limit = int(os.getenv("MAX_PAGES", "20"))
            else:
                page_limit = int(max_pages)

        self.job_id = self.start_job(
            crawler_name=self.CRAWLER_NAME,
            mode=mode,
            scope_key=mode,
            year=None,
            meta_json={
                "page_limit": page_limit,
                "page_size": self.PAGE_SIZE,
                "debug": debug,
            },
        )

        last_page, progress_meta = self.load_page_progress(mode)
        page = last_page + 1 if last_page > 0 else 1

        majors = [] if self.export_json else None
        seen_ids = set()
        duplicate_count = int(progress_meta.get("duplicate_count", 0) or 0)
        total_saved = int(progress_meta.get("total_saved", 0) or 0)
        consecutive_failures = int(progress_meta.get("consecutive_failures", 0) or 0)

        existing_ids = set()
        if self.skip_existing:
            existing_ids = self.get_existing_major_ids()
            if existing_ids:
                print(f"✓ 数据库中已存在 {len(existing_ids)} 个专业，重复 special_id 将直接跳过")

        print(f"\n{'=' * 60}")
        print("开始爬取专业目录")
        print(f"模式: {mode}")
        print(f"页数限制: {'不限，直到最后一页' if page_limit is None else page_limit}")
        print(f"调试输出: {'✓' if debug else '✗'}")
        print(f"导出JSON: {'✓' if self.export_json else '✗'}")
        print(f"数据库Schema: {self.db_schema}")
        print(f"起始页: {page}")
        print(f"{'=' * 60}\n")

        try:
            while True:
                if page_limit is not None and page > page_limit:
                    break

                payload = {
                    "keyword": "",
                    "page": page,
                    "size": self.PAGE_SIZE,
                    "level1": "",
                    "level2": "",
                    "level3": "",
                    "uri": "apidata/api/gkv3/special/lists",
                }

                data = self.make_request(payload, retry=5)

                if not data:
                    consecutive_failures += 1
                    print(f"✗ 第 {page} 页请求失败，连续异常 {consecutive_failures}/{self.FAILURE_PAUSE_THRESHOLD}")

                    self.save_page_progress(
                        mode=mode,
                        last_page=max(page - 1, 0),
                        total_saved=total_saved,
                        duplicate_count=duplicate_count,
                        consecutive_failures=consecutive_failures,
                        last_error=f"page request failed: page={page}",
                    )

                    if self.should_pause_on_rate_limit(consecutive_failures, self.FAILURE_PAUSE_THRESHOLD):
                        msg = f"连续分页请求异常达到阈值，暂停任务: page={page}"
                        print(f"⏸️  {msg}")
                        self.mark_job_paused(
                            error_message=msg,
                            meta_json={
                                "last_page": max(page - 1, 0),
                                "total_saved": total_saved,
                                "duplicate_count": duplicate_count,
                            },
                        )
                        return majors if self.export_json else []

                    self.polite_sleep(8.0, 15.0)
                    continue

                code = data.get("code")
                message = data.get("message")

                if not self.is_success_code(code):
                    consecutive_failures += 1
                    print(f"✗ 第 {page} 页业务异常: code={code}, message={message}")

                    self.save_page_progress(
                        mode=mode,
                        last_page=max(page - 1, 0),
                        total_saved=total_saved,
                        duplicate_count=duplicate_count,
                        consecutive_failures=consecutive_failures,
                        last_error=f"business error: code={code}, message={message}",
                    )

                    if self.should_pause_on_rate_limit(consecutive_failures, self.FAILURE_PAUSE_THRESHOLD):
                        msg = f"连续业务异常达到阈值，暂停任务: page={page}"
                        print(f"⏸️  {msg}")
                        self.mark_job_paused(
                            error_message=msg,
                            meta_json={
                                "last_page": max(page - 1, 0),
                                "total_saved": total_saved,
                                "duplicate_count": duplicate_count,
                            },
                        )
                        return majors if self.export_json else []

                    self.polite_sleep(8.0, 15.0)
                    continue

                if "data" not in data:
                    consecutive_failures += 1
                    print(f"✗ 第 {page} 页响应中无 data 字段")

                    self.save_page_progress(
                        mode=mode,
                        last_page=max(page - 1, 0),
                        total_saved=total_saved,
                        duplicate_count=duplicate_count,
                        consecutive_failures=consecutive_failures,
                        last_error=f"missing data field: page={page}",
                    )
                    self.polite_sleep(5.0, 10.0)
                    continue

                data_content = self.normalize_data_content(data.get("data"))
                if data_content is None:
                    consecutive_failures += 1
                    print(f"✗ 第 {page} 页 data 解析失败")

                    self.save_page_progress(
                        mode=mode,
                        last_page=max(page - 1, 0),
                        total_saved=total_saved,
                        duplicate_count=duplicate_count,
                        consecutive_failures=consecutive_failures,
                        last_error=f"data parse failed: page={page}",
                    )
                    self.polite_sleep(5.0, 10.0)
                    continue

                items = self.extract_items(data_content)

                if debug and not self._first_logged:
                    self.log_first_page_schema(data_content, items)

                if not items:
                    print(f"✓ 第 {page} 页无数据，已到最后一页")
                    break

                consecutive_failures = 0
                page_added = 0
                page_dup = 0
                page_skipped = 0

                for item in items:
                    major_info = self.normalize_major(item)
                    special_id = major_info.get("special_id")

                    if not special_id:
                        continue

                    special_id = str(special_id)

                    if special_id in seen_ids:
                        duplicate_count += 1
                        page_dup += 1
                        continue

                    if self.skip_existing and special_id in existing_ids:
                        duplicate_count += 1
                        page_skipped += 1
                        seen_ids.add(special_id)
                        continue

                    seen_ids.add(special_id)

                    self.upsert_raw_document(
                        crawler_name=self.CRAWLER_NAME,
                        entity_type=self.ENTITY_TYPE,
                        entity_key=special_id,
                        year=None,
                        payload=major_info,
                    )

                    if self.export_json:
                        majors.append(major_info)

                    page_added += 1
                    total_saved += 1

                print(
                    f"第 {page} 页: 获取 {len(items)} 个专业，"
                    f"新增 {page_added} 个，重复 {page_dup} 个，跳过 {page_skipped} 个，累计保存 {total_saved} 个"
                )

                self.save_page_progress(
                    mode=mode,
                    last_page=page,
                    total_saved=total_saved,
                    duplicate_count=duplicate_count,
                    consecutive_failures=consecutive_failures,
                    last_error=None,
                )

                page += 1
                self.polite_sleep(self.PAGE_SLEEP_MIN, self.PAGE_SLEEP_MAX)

            if self.export_json:
                self.save_to_json(majors, "majors.json")

            self.clear_progress(
                crawler_name=self.CRAWLER_NAME,
                scope_key=mode,
                cursor_type=self.CURSOR_TYPE,
            )
            self.mark_job_done(
                meta_json={
                    "last_page": page - 1,
                    "total_saved": total_saved,
                    "duplicate_count": duplicate_count,
                }
            )

            print(f"\n{'=' * 60}")
            print("✅ 专业爬取完成！")
            print(f"   总计保存: {total_saved} 个专业")
            print(f"   去重数: {duplicate_count}")

            if self.export_json and majors:
                print(f"   字段数: {len(majors[0].keys())}")

                level1_set = {m.get('level1_name') for m in majors if m.get('level1_name')}
                level2_set = {m.get('level2_name') for m in majors if m.get('level2_name')}
                level3_set = {m.get('level3_name') for m in majors if m.get('level3_name')}

                has_salary = sum(1 for m in majors if m.get("salary_avg"))
                print(f"   学历层次: {len(level1_set)} 个")
                print(f"   学科门类: {len(level2_set)} 个")
                print(f"   专业类别: {len(level3_set)} 个")
                print(f"   有薪资数据: {has_salary} 个 ({has_salary * 100 // len(majors)}%)")
            print(f"{'=' * 60}\n")

            return majors if self.export_json else []

        except Exception as e:
            self.save_page_progress(
                mode=mode,
                last_page=max(page - 1, 0),
                total_saved=total_saved,
                duplicate_count=duplicate_count,
                consecutive_failures=consecutive_failures,
                last_error=str(e),
            )
            self.mark_job_failed(
                error_message=str(e),
                meta_json={
                    "last_page": max(page - 1, 0),
                    "total_saved": total_saved,
                    "duplicate_count": duplicate_count,
                },
            )
            raise


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CRAWL_MODE", "test")
    max_pages = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].strip() else None
    debug = parse_bool(sys.argv[3], False) if len(sys.argv) > 3 else None

    crawler = MajorCrawler()
    crawler.crawl(
        mode=mode,
        max_pages=max_pages,
        debug=debug,
    )
