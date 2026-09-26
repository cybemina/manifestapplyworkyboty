#!/usr/bin/env python3
from __future__ import annotations
# ================================================================
# MANIFEST APPLY — Services Bot (جداگانه از بات مهاجرت)
# فایل: manifest_services_bot.py
# توکن: جداگانه — از SERVICES_BOT_TOKEN بخوانید
#
# سرویس‌ها:
#   ✅ ارسال ایمیل به اساتید (۵۰۰ ایمیل شخصی‌سازی‌شده)
#   ✅ اپلای کار حرفه‌ای (جستجوی real-time + ارسال خودکار ایمیل به HR در صورت
#      وجود ایمیل مستقیم، وگرنه لینک پورتال رسمی برای اپلای دستی کاربر —
#      پر کردن خودکار فرم‌های ATS/کپچا هنوز پیاده نشده و به‌صورت واقع‌بینانه
#      قابل تضمین ۱۰۰٪ نیست)
#
# ویژگی‌های جدید:
#   ✅ VIP (کد manifest2027) + مشتری عادی (پرداخت)
#   ✅ تایید ادمین برای هر دو مسیر
#   ✅ دریافت نام + شماره تماس از همه کاربران
#   ✅ پیش‌نمایش ایمیل قبل از ارسال (۳۰ دقیقه timeout → ارسال خودکار)
#   ✅ پشتیبانی AI در چت (سوال/ویرایش در هر لحظه)
#   ✅ جستجوی شغل real-time (تازه‌ترین ممکن — وقتی منبع تاریخ می‌ده، هدف ۴۸
#      ساعت اخیره؛ برای منابعی که تاریخ نمی‌دن، برچسب "تاریخ نامشخص" صادقانه
#      نشون داده می‌شه، نه ادعای قطعی ۴۸ ساعت — نگاه کن به _job_freshness)
#   ✅ بدون crash، بدون lag، همه حالت‌های خطا handle شده
# ================================================================

import os, re, json, time, io, asyncio, sqlite3, smtplib, requests, socket, math
import traceback, sys, logging, secrets, base64, threading, concurrent.futures, hashlib
import imaplib, ipaddress, tempfile
from urllib.parse import urlsplit, urljoin
import email as email_pkg  # اسم مستعار تا با پارامترهای smtp_e/professor_email اشتباه نشه
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

load_dotenv()

# ── رمزنگاری داده‌های حساس session (pip install cryptography) ─────
# پسورد SMTP کاربر (App Password) قبلاً به‌صورت plaintext توی جدول
# bot_sessions ذخیره می‌شد — یعنی هر کسی که به فایل services_bot.db
# دسترسی پیدا می‌کرد (بکاپ لو رفته، هاست هک‌شده، حتی یه ادمین فضول)
# پسورد ایمیل صدها کاربر رو plaintext می‌دید. این کتابخانه برای رفع
# همین مشکل لازمه؛ برخلاف کتابخانه‌های گوگل، اینجا عمداً به‌صورت
# اختیاری/optional وارد نمی‌شه — نبودش نباید باعث بشه بات بی‌سروصدا
# به حالت ناامن (ذخیره‌ی plaintext) برگرده.
try:
    from cryptography.fernet import Fernet, InvalidToken
except ImportError:
    logging.critical(
        "❌ کتابخانه‌ی 'cryptography' نصب نیست. برای امنیت پسورد SMTP "
        "کاربران، این کتابخانه لازمه: pip install cryptography")
    sys.exit(1)

# ── LOGGING ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger("services_bot")

# ── ورود با گوگل (اختیاری) ───────────────────────────────────────
# اگر کتابخانه‌های گوگل نصب نباشند (pip install google-auth google-auth-oauthlib
# google-api-python-client) فقط گزینه‌ی «ورود با گوگل» غیرفعال می‌شود؛ روش
# App Password دقیقاً مثل قبل کار می‌کند. یعنی نبود این کتابخانه‌ها هرگز
# باعث crash بات نمی‌شود.
try:
    from google.oauth2.credentials import Credentials as GoogleCredentials
    from google_auth_oauthlib.flow import Flow as GoogleFlow
    from google.auth.transport.requests import Request as GoogleAuthRequest
    from googleapiclient.discovery import build as google_build
    GOOGLE_LIBS_OK = True
except ImportError:
    GOOGLE_LIBS_OK = False

# ── توکن‌ها و کلیدها ────────────────────────────────────────────
# هیچ Secretی نباید مقدار پیش‌فرض داشته باشد. اگر مقدار پیش‌فرض بگذاریم
# و یادمان برود Environment Variable را در Railway تنظیم کنیم، بات با
# توکن/کلید هاردکدشده (که ممکن است لو رفته یا باطل باشد) بالا می‌آید —
# این یکی از رایج‌ترین علت‌های «بات بالا می‌آید ولی درست کار نمی‌کند»ست.
def _require_env(name: str) -> str:
    val = os.getenv(name, "").strip()
    if not val:
        logger.critical(f"❌ Environment variable '{name}' تنظیم نشده. برنامه متوقف می‌شود.")
        sys.exit(1)
    return val

BOT_TOKEN      = _require_env("SERVICES_BOT_TOKEN")
ADMIN_CHAT_ID  = int(os.getenv("ADMIN_CHAT_ID", "0") or "0")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GROQ_API_KEY   = os.getenv("GROQ_API_KEY", "")
# اختیاری: بدون این‌ها فقط منبع Adzuna (که تنها منبع با فیلتر واقعی کشوریه)
# غیرفعال می‌مونه، بقیه‌ی ۸ منبع دیگه دست‌نخورده کار می‌کنن. ثبت‌نام رایگان:
# https://developer.adzuna.com/
ADZUNA_APP_ID  = os.getenv("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY", "")

# ================================================================
# کلید رمزنگاری session (برای پسورد SMTP کاربران)
# ================================================================
# اگه SESSION_ENCRYPTION_KEY توی env ست شده باشه از همون استفاده می‌کنیم
# (توصیه‌شده برای production — این‌طوری اگه دیتابیس عوض/مهاجرت بشه، کلید
# مستقل از فایل دیتابیسه). اگه ست نشده باشه، یک کلید تصادفی می‌سازیم و
# توی یک فایل محلی کنار خودِ اسکریپت نگه می‌داریم (دقیقاً مثل الگوی
# .bot.lock که همین فایل قبلاً برای single-instance lock استفاده می‌کنه)
# تا لااقل بین ری‌استارت‌های همین هاست از بین نره. این فایل هرگز نباید
# commit بشه — باید توی .gitignore باشه.
_SESSION_KEY_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".session.key")

def _load_or_create_session_key() -> bytes:
    env_key = os.getenv("SESSION_ENCRYPTION_KEY", "").strip()
    if env_key:
        return env_key.encode()
    try:
        if os.path.exists(_SESSION_KEY_FILE):
            with open(_SESSION_KEY_FILE, "rb") as f:
                existing = f.read().strip()
            if existing:
                return existing
        new_key = Fernet.generate_key()
        with open(_SESSION_KEY_FILE, "wb") as f:
            f.write(new_key)
        try:
            os.chmod(_SESSION_KEY_FILE, 0o600)
        except OSError:
            pass
        logger.warning(
            "⚠️ SESSION_ENCRYPTION_KEY تنظیم نشده — یک کلید تصادفی ساخته و توی "
            f"'{_SESSION_KEY_FILE}' ذخیره شد. برای production توصیه می‌شه این "
            "مقدار رو از همون فایل بخونید و به‌عنوان SESSION_ENCRYPTION_KEY توی "
            "env تنظیم کنید (وگرنه با جابه‌جایی/پاک‌شدن این فایل، پسوردهای SMTP "
            "ذخیره‌شده‌ی قبلی دیگه قابل بازیابی نیستن و کاربرها باید دوباره وارد کنن).")
        return new_key
    except OSError as e:
        logger.critical(f"❌ نتونستیم کلید رمزنگاری session رو بخونیم/بسازیم: {e}")
        sys.exit(1)

_SESSION_FERNET = Fernet(_load_or_create_session_key())
_ENC_PREFIX = "enc:v1:"

def _encrypt_secret(value: str) -> str:
    """یک مقدار رشته‌ای رو برای ذخیره‌ی امن روی دیسک رمزنگاری می‌کنه.
    اگه از قبل رمزنگاری شده باشه (پیشوند _ENC_PREFIX) دست‌نخورده برمی‌گردونه."""
    if not isinstance(value, str) or not value or value.startswith(_ENC_PREFIX):
        return value
    return _ENC_PREFIX + _SESSION_FERNET.encrypt(value.encode()).decode()

def _decrypt_secret(value: str, *, field_label: str = "") -> str | None:
    """مقدار رمزنگاری‌شده رو برمی‌گردونه. اگه رمزنگاری‌شده نبود (داده‌ی
    قدیمی‌تر از این فیکس، هنوز plaintext) همون‌طور که هست برگردونده می‌شه —
    یعنی این تغییر روی sessionهای موجود crash نمی‌کنه، فقط از این به بعد
    هرچی جدید نوشته بشه رمزنگاری‌شده خواهد بود."""
    if not isinstance(value, str) or not value.startswith(_ENC_PREFIX):
        return value
    try:
        return _SESSION_FERNET.decrypt(value[len(_ENC_PREFIX):].encode()).decode()
    except InvalidToken:
        logger.error(
            f"⚠️ رمزگشایی {field_label or 'یک مقدار حساس'} ناموفق بود (کلید عوض شده؟) — "
            "کاربر باید دوباره وارد کنه.")
        return None

# ================================================================
# قفل تک‌نمونه‌ای (Single-Instance Lock)
# ================================================================
# دو تا از بحرانی‌ترین خطاهایی که این بات تجربه کرده («Conflict:
# terminated by other getUpdates request» و «database disk image is
# malformed») هر دو از یک ریشه میان: دو نسخه از همین پروسه هم‌زمان روی
# همون توکن/همون فایل دیتابیس اجرا شدن (مثلاً یه Always-on Task قدیمی
# روی PythonAnywhere که فراموش شده خاموش بشه، یا هم‌زمان روی دو تا هاست
# مختلف دیپلوی شده). وقتی دو پروسه هم‌زمان به یک فایل SQLite می‌نویسن،
# روی فایل‌سیستم‌های شبکه‌ای (که PythonAnywhere ازش استفاده می‌کنه)
# قفل‌گذاری WAL می‌تونه درست کار نکنه و فایل دیتابیس corrupt بشه —
# دقیقاً همون چیزی که توی لاگ افتاد.
# راه‌حل واقعی: به‌جای فقط امیدوار بودن که کسی دوباره اشتباهی دیپلوی
# نکنه، خودِ برنامه یک قفل انحصاری (exclusive file lock) روی یک فایل
# قفل می‌گیره؛ اگه قفل قبلاً گرفته شده (یعنی یه نسخه‌ی دیگه در حال
# اجراست)، این نسخه‌ی جدید بلافاصله و با یک پیام واضح متوقف می‌شه —
# به‌جای اینکه بی‌سروصدا با نسخه‌ی قبلی روی getUpdates و دیتابیس رقابت
# کنه و باعث corruption بشه.
_LOCK_FILE_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bot.lock")
_lock_file_handle = None  # باید تا پایان عمر پروسه باز بمونه، وگرنه قفل آزاد می‌شه

def _acquire_single_instance_lock():
    global _lock_file_handle
    try:
        import fcntl
        _lock_file_handle = open(_LOCK_FILE_PATH, "w")
        fcntl.flock(_lock_file_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _lock_file_handle.write(str(os.getpid()))
        _lock_file_handle.flush()
    except ImportError:
        # ویندوز/محیطی بدون fcntl — نمی‌تونیم قفل بگیریم، فقط هشدار می‌دیم
        # و اجازه می‌دیم برنامه بالا بیاد (بهتر از crash کردن روی محیطی که
        # پشتیبانی نمی‌شه)، ولی این یعنی محافظت single-instance غیرفعاله.
        logger.warning("⚠️ fcntl در دسترس نیست (احتمالاً ویندوز) — محافظت single-instance غیرفعال است.")
    except (OSError, BlockingIOError):
        logger.critical(
            "❌ یک نسخه‌ی دیگر از این بات همین الان در حال اجراست "
            f"(قفل {_LOCK_FILE_PATH} قبلاً گرفته شده). برای جلوگیری از خطای "
            "Telegram Conflict و خراب شدن دیتابیس، این نسخه‌ی جدید اجرا نمی‌شود. "
            "اول نسخه‌ی قبلی رو پیدا و متوقف کنید: ps aux | grep manifest_services_bot")
        sys.exit(1)

_acquire_single_instance_lock()

if not ADMIN_CHAT_ID:
    logger.warning("⚠️ ADMIN_CHAT_ID تنظیم نشده — تاییدیه‌های پرداخت/VIP به هیچ‌کس اطلاع داده نمی‌شود!")
if not GEMINI_API_KEY and not GROQ_API_KEY:
    logger.warning("⚠️ نه GEMINI_API_KEY و نه GROQ_API_KEY تنظیم نشده — نوشتن ایمیل با AI کار نخواهد کرد.")

# ── ورود با گوگل (Sign in with Google) ─────────────────────────────
# این سه مقدار را باید از Google Cloud Console (OAuth Client ID از نوع
# Web application) بگیرید. GOOGLE_REDIRECT_URI باید دقیقاً همان آدرسی
# باشد که در Google Cloud به‌عنوان Authorized redirect URI ثبت کرده‌اید
# (مثلاً https://<your-railway-domain>/oauth2callback).
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "").strip()
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
GOOGLE_REDIRECT_URI  = os.getenv("GOOGLE_REDIRECT_URI", "").strip()
OAUTH_LOCAL_PORT     = int(os.getenv("PORT", "8080") or "8080")

GOOGLE_LOGIN_ENABLED = bool(GOOGLE_LIBS_OK and GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET and GOOGLE_REDIRECT_URI)
if not GOOGLE_LOGIN_ENABLED:
    logger.warning(
        "⚠️ ورود با گوگل غیرفعال است "
        f"(libs={'✅' if GOOGLE_LIBS_OK else '❌'}, "
        f"client_id={'✅' if GOOGLE_CLIENT_ID else '❌'}, "
        f"client_secret={'✅' if GOOGLE_CLIENT_SECRET else '❌'}, "
        f"redirect_uri={'✅' if GOOGLE_REDIRECT_URI else '❌'}) — "
        "فقط روش App Password در دسترس خواهد بود.")

# ── تنظیمات ──────────────────────────────────────────────────────
VIP_CODE           = _require_env("VIP_CODE")
# قبلاً اگه VIP_CODE توی env ست نمی‌شد، بات بی‌سروصدا از یک مقدار پیش‌فرض
# قابل‌حدس ("manifest2027" — دقیقاً هم‌نام همین پروژه) استفاده می‌کرد؛ یعنی
# هرکسی که اسم پروژه رو می‌دونست می‌تونست رایگان از مسیر VIP رد بشه. حالا
# مثل بقیه‌ی Secretها اجباریه — اگه ست نشده باشه، بات بالا نمی‌آد.
CARD_NUMBER        = os.getenv("CARD_NUMBER", "")
CARD_OWNER         = os.getenv("CARD_OWNER", "")
PRICE_USD          = 450
PRICE_ORIGINAL_USD = 900
# اگه این عدد رو ست کنید (مثلاً 950000)، بات دیگه هیچ‌وقت سعی نمی‌کنه به
# bon-bast.com یا navasan.tech وصل بشه — همیشه همین نرخ رو مستقیم استفاده
# می‌کنه. برای میزبانی‌هایی که دسترسی اینترنت محدودی دارن (مثل پلن رایگان
# PythonAnywhere) این تنظیم توصیه می‌شه، هم برای قابل‌اعتماد بودن قیمت هم
# برای سرعت (اتصال به این دو سایت روی هاست محدود شکست می‌خوره و هر بار
# چند ثانیه معطل می‌مونه).
USD_RATE_FIXED     = int(os.getenv("USD_RATE_FIXED", "0") or "0") or None
EMAIL_QUOTA        = 500
SEND_DELAY_SEC     = 8       # تاخیر بین ایمیل‌ها
# سقف زمانی کل فاز «پیدا کردن ایمیل استادهایی که ایمیلشون از قبل معلوم
# نیست» — کندترین بخش کل فرایند (تا ۷ تا جستجوی DuckDuckGo برای هر
# استاد). بعد از این سقف، به‌جای معطل موندن، از استادهای بدون‌ایمیل
# باقی‌مونده می‌گذریم تا ارسال هیچ‌وقت به‌خاطر کند/خراب بودن یک سرویس
# خارجی متوقف نمونه.
MAX_SCAN_SECONDS   = 900  # ۱۵ دقیقه
PREVIEW_TIMEOUT    = 900     # 15 دقیقه مهلت بررسی ایمیل (طبق درخواست)
JOB_QUOTA          = 200     # اپلای کار
JOB_MAX_AGE_HOURS  = 48      # فقط فرصت‌های ۴۸ ساعت اخیر
MONITOR_CHECK_INTERVAL_HOURS = 24   # هر چند ساعت یک‌بار مانیتور مداوم دوباره جستجو کنه
MONITOR_DURATION_DAYS        = 30   # مانیتور مداوم حداکثر تا چند روز بعد از شروع فعال بمونه

# ── ATS Resume Scan ─────────────────────────────────────────────────
# ATS_SCAN_MAX_RETRIES: تعداد تلاش مجدد برای analyze_resume_ats — هم برای
# خالی برگشتن call_ai (شکست هر دو provider) هم برای JSON خراب/ناقص AI
# (باگ نادر ~۱٪ که کاربر بهش اشاره کرد). با ۳ تلاش و backoff کوتاه بین
# هرکدوم، احتمال شکست نهایی عملاً به صفر نزدیک می‌شه بدون این‌که کاربر
# منتظر بمونه.
ATS_SCAN_MAX_RETRIES = 3


DB_FILE   = "services_bot.db"
BONBAST   = "https://www.bon-bast.com/"
REQ_TIMEOUT = 20

# ================================================================
# SSRF Guard — برای هر URL که مستقیم یا غیرمستقیم از منبع غیرقابل‌اعتماد
# می‌آد (نتیجه‌ی سرچ Google/DDG، لیست دستی که کاربر پیست می‌کنه، دامنه‌ی
# ایمیل کاربر برای autoconfig SMTP)، نه یک endpoint هاردکدشده‌ی خودمون.
# ================================================================
# چرا این لازمه: قبلاً scrape() و توابع مشابه مستقیم requests.get(url)
# می‌زدن روی هر URLای که از این منابع می‌اومد. یعنی مثلاً کاربری که توی
# «لیست خودم دارم» به‌جای لینک صفحه‌ی دانشکده، آدرس
# http://169.254.169.254/latest/meta-data/ (متادیتای سرویس‌های cloud) یا
# http://localhost:<port>/... می‌فرستاد، می‌تونست باعث بشه سرور خودمون
# (که روی Railway/هر cloud دیگه اجرا می‌شه) از سمت خودش به شبکه‌ی داخلی/
# متادیتای cloud request بزنه و نتیجه رو (تا حدی) برگردونه — یعنی بات
# به‌عنوان یک SSRF proxy علیه زیرساخت خودمون استفاده می‌شد.
class _BlockedURLError(Exception):
    pass

def _is_unsafe_ip(ip_str: str) -> bool:
    """True یعنی این IP نباید هدف یک fetch خارجی باشه: private/loopback/
    link-local (که 169.254.169.254 متادیتای AWS/GCP/Azure/Railway هم
    توش می‌گنجه)/multicast/reserved/unspecified. اگه پارس IP هم شکست
    بخوره، محافظه‌کارانه مسدودش می‌کنیم (fail closed)."""
    try:
        ip = ipaddress.ip_address(ip_str)
    except ValueError:
        return True
    return bool(
        ip.is_private or ip.is_loopback or ip.is_link_local or
        ip.is_multicast or ip.is_reserved or ip.is_unspecified
    )

def _assert_url_is_safe(url: str) -> None:
    try:
        parsed = urlsplit(url)
    except Exception:
        raise _BlockedURLError(f"URL نامعتبر: {url!r}")
    if parsed.scheme not in ("http", "https"):
        raise _BlockedURLError(f"scheme غیرمجاز ({parsed.scheme!r}) در URL: {url!r}")
    host = parsed.hostname
    if not host:
        raise _BlockedURLError(f"هاست خالی در URL: {url!r}")
    if host.lower() in ("localhost", "metadata.google.internal"):
        raise _BlockedURLError(f"هاست مسدود: {host}")
    try:
        infos = socket.getaddrinfo(host, None)
    except Exception as e:
        raise _BlockedURLError(f"resolve هاست {host} ناموفق: {e}")
    for info in infos:
        ip_str = info[4][0]
        if _is_unsafe_ip(ip_str):
            raise _BlockedURLError(f"هاست {host} به IP داخلی/مسدود ({ip_str}) resolve شد")

def _safe_requests_get(url: str, *, max_redirects: int = 5, **kwargs):
    """جایگزین امن requests.get برای هر URL غیرقابل‌اعتماد. علاوه بر چک
    اولیه، هر hop ریدایرکت رو هم قبل از دنبال‌کردن دوباره چک می‌کنه —
    چون وگرنه یک URL ظاهراً بی‌خطر می‌تونست با یک 302 به سمت IP داخلی
    ریدایرکت بشه و همون‌جا چک اولیه رو دور بزنه."""
    kwargs.pop("allow_redirects", None)
    for _ in range(max_redirects + 1):
        _assert_url_is_safe(url)
        resp = requests.get(url, allow_redirects=False, **kwargs)
        if resp.is_redirect or resp.is_permanent_redirect:
            location = resp.headers.get("Location")
            if not location:
                return resp
            url = urljoin(url, location)
            continue
        return resp
    raise _BlockedURLError(f"تعداد ریدایرکت‌ها بیش از حد مجاز برای: {url!r}")

# ================================================================
# GET با Retry + Exponential Backoff (رفع باگ «۴۲۹ = تسلیم فوری»)
# ================================================================
# قبلاً همه‌جا الگوی `if status == 429: time.sleep(2); return []` بود —
# یعنی به محض یک 429، از اون منبع کامل صرف‌نظر می‌شد (نه retry واقعی).
# زیر بار (مثلاً موقع ارسال ۵۰۰ ایمیل که خیلی درخواست پشت‌سرهم می‌ره)
# این باعث می‌شد منابع پشت‌سرهم silently خالی برگردن، حتی وقتی چند ثانیه
# صبر کردن کافی بود. این تابع مشترک به‌جای «یک صبر کوتاه و تسلیم»، تا
# max_retries بار با backoff نمایی (و کمی jitter، تا چند thread هم‌زمان
# دقیقاً روی هم retry نکنن) واقعاً دوباره امتحان می‌کنه. روی 5xx هم retry
# می‌کنه (خطای موقت سرور)، ولی روی بقیه‌ی وضعیت‌ها (404، 400، ...) فوراً
# همون response رو برمی‌گردونه چون retry کردن کمکی نمی‌کنه.
def _http_get_retry(url: str, *, params: dict | None = None, headers: dict | None = None,
                     timeout: float = REQ_TIMEOUT, max_retries: int = 3,
                     base_delay: float = 1.5):
    last_exc = None
    r = None
    for attempt in range(max_retries + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 429 or r.status_code >= 500:
                if attempt < max_retries:
                    retry_after = r.headers.get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after else base_delay * (2 ** attempt)
                    except ValueError:
                        delay = base_delay * (2 ** attempt)
                    delay += secrets.randbelow(500) / 1000.0  # jitter تا ۰.۵ ثانیه
                    logger.debug(f"_http_get_retry: {url} -> {r.status_code}, retry {attempt+1}/{max_retries} in {delay:.1f}s")
                    time.sleep(delay)
                    continue
            return r
        except requests.RequestException as e:
            last_exc = e
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt) + secrets.randbelow(500) / 1000.0
                time.sleep(delay)
                continue
            raise
    if last_exc:
        raise last_exc
    return r

GROQ_MODELS = [
    "llama-3.3-70b-versatile",   # در حال منسوخ‌شدن (Groq، ۱۷ ژوئن ۲۰۲۶) — تا خاموش نشده اول امتحانش می‌کنیم چون کیفیتش شناخته‌شده‌ست
    "openai/gpt-oss-120b",       # جایگزین رسمی Groq برای llama-3.3-70b-versatile
    "openai/gpt-oss-20b",        # نسخه‌ی سبک‌تر/سریع‌تر، fallback دوم
    "qwen/qwen3.6-27b",          # fallback سوم — طبق مستندات Groq
]

# ── لیست fallback مدل‌های Gemini — دقیقاً همون الگوی GROQ_MODELS بالا ──
# چرا لیست، نه یک مدل ثابت: "gemini-2.0-flash" (که این‌جا قبلاً hardcode
# شده بود) واقعاً توسط گوگل در ۱ ژوئن ۲۰۲۶ خاموش شد — یعنی طی این دو ماه
# هر فراخوانی Gemini با خطا مواجه می‌شد و بی‌صدا (فقط یک logger.warning)
# به Groq سقوط می‌کرد؛ خود Gemini عملاً هیچ‌وقت کار نمی‌کرد. چرخه‌ی
# خاموش‌شدن مدل‌های گوگل هم تند شده (gemini-2.5-flash هم ۱۶ اکتبر ۲۰۲۶
# خاموش می‌شه) — به‌جای اینکه دوباره یک اسم ثابت hardcode کنیم که چند
# ماه دیگه دوباره همین باگ رو تکرار کنه، یک لیست fallback می‌ذاریم: اگه
# مدل اول (یا حتی کل نسل 2.5) خاموش شد، خودکار می‌ره سراغ بعدی، به‌جای
# اینکه کل مسیر Gemini بی‌صدا و برای هفته‌ها خراب بمونه.
GEMINI_MODELS = [
    "gemini-2.5-flash",        # مسیر مهاجرت رسمی گوگل از gemini-2.0-flash (خاموش‌شده) — تا ۱۶ اکتبر ۲۰۲۶ فعاله
    "gemini-2.5-flash-lite",   # fallback دوم — همون نسل، سبک‌تر، اگه flash معمولی مشکل داشت
    "gemini-3.6-flash",        # fallback سوم — نسل جدیدتر (غیر-preview)، طول عمر بیشتر ولی گرون‌تر
]

logger.info("=" * 55)
logger.info("  MANIFEST APPLY — SERVICES BOT")
logger.info("=" * 55)
logger.info(f"  Token    : ✅ ({BOT_TOKEN[:6]}...)")
logger.info(f"  Admin ID : {ADMIN_CHAT_ID or '❌ تنظیم نشده'}")
logger.info(f"  Gemini   : {'✅' if GEMINI_API_KEY else '❌'}")
logger.info(f"  Groq     : {'✅' if GROQ_API_KEY else '❌'}")
logger.info(f"  Adzuna   : {'✅' if (ADZUNA_APP_ID and ADZUNA_APP_KEY) else '❌ (فیلتر واقعی کشوری غیرفعال — https://developer.adzuna.com/)'}")
logger.info(f"  Google   : {'✅' if GOOGLE_LOGIN_ENABLED else '❌ (فقط App Password)'}")
logger.info("=" * 55)

# ================================================================
# DATABASE
# ================================================================

_thread_local_db = threading.local()

def _db():
    """یک connection SQLite برمی‌گردونه. به‌جای باز کردن یک connection
    تازه در هر فراخوانی (که قبلاً هر بار فقط commit/rollback می‌شد ولی
    هیچ‌وقت close نمی‌شد و تا GC بعدی معلق می‌موند)، هر ترد کارگر
    (asyncio.to_thread یک pool محدود از تردها رو recycle می‌کنه) فقط یک
    connection برای خودش نگه می‌داره و در فراخوانی‌های بعدی همون رو
    استفاده می‌کنه. این کاملاً امن‌ه چون connectionها هیچ‌وقت بین تردها
    به اشتراک گذاشته نمی‌شن — هر ترد فقط مال خودش رو می‌بینه
    (threading.local)."""
    conn = getattr(_thread_local_db, "conn", None)
    if conn is None:
        conn = sqlite3.connect(DB_FILE, timeout=10, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        _thread_local_db.conn = conn
    return conn

def init_db():
    with _db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users (
            telegram_id INTEGER PRIMARY KEY,
            username TEXT, first_name TEXT,
            full_name TEXT, phone TEXT,
            user_type TEXT DEFAULT 'unknown',
            service TEXT,
            status TEXT DEFAULT 'pending',
            sub_id INTEGER,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            service TEXT,
            user_type TEXT,
            status TEXT DEFAULT 'pending',
            receipt_file_id TEXT,
            price_toman INTEGER,
            emails_sent INTEGER DEFAULT 0,
            jobs_applied INTEGER DEFAULT 0,
            created_at TEXT,
            approved_at TEXT,
            completed_at TEXT
        );
        CREATE TABLE IF NOT EXISTS sent_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            professor_name TEXT,
            professor_email TEXT,
            subject TEXT,
            status TEXT,
            sent_at TEXT,
            reply_status TEXT DEFAULT 'unknown',
            reply_note TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS applied_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            job_title TEXT,
            company TEXT,
            method TEXT,
            status TEXT,
            applied_at TEXT
        );
        CREATE TABLE IF NOT EXISTS email_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            sub_id INTEGER,
            batch_idx INTEGER,
            professor_name TEXT,
            professor_email TEXT,
            status TEXT DEFAULT 'PENDING',
            attempts INTEGER DEFAULT 0,
            last_error TEXT,
            created_at TEXT,
            updated_at TEXT,
            sent_at TEXT
        );
        CREATE TABLE IF NOT EXISTS preview_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            professor_name TEXT,
            professor_email TEXT,
            subject TEXT,
            body TEXT,
            status TEXT DEFAULT 'pending',
            created_at TEXT,
            decided_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pending_snapshots (
            sub_id INTEGER PRIMARY KEY,
            data_json TEXT,
            created_at TEXT
        );
        -- snapshot لازم برای auto-send یک preview بعد از timeout (لیست
        -- اساتید batch + پروفایل کاربر + مشخصات SMTP). قبلاً این فقط توی
        -- context.application.bot_data نگه داشته می‌شد که persistent نیست
        -- (persistence صریحاً bot_data=False داره — پایین‌تر SQLiteUserDataPersistence)
        -- یعنی با هر ری‌استارت وسط ۱۵ دقیقه‌ی مهلت preview، هم خودِ
        -- snapshot و هم تسک timeout (که خودش هم صرفاً یک asyncio task
        -- در حافظه‌ست) گم می‌شدن — نه فقط پیام خطا به کاربر می‌رفت، بلکه
        -- اصلاً هیچ auto-send یا حتی پیام خطایی رخ نمی‌داد چون خود تسک
        -- زمان‌بندی‌شده دیگه وجود نداشت. با این جدول snapshot روی دیسکه؛
        -- _on_bot_startup پایین‌تر همه‌ی previewهای pending رو با توجه به
        -- created_at دوباره زمان‌بندی می‌کنه (یا اگه مهلت‌شون گذشته، فوراً
        -- auto-send رو انجام می‌ده).
        CREATE TABLE IF NOT EXISTS preview_snapshots (
            preview_id INTEGER PRIMARY KEY,
            telegram_id INTEGER,
            chat_id INTEGER,
            data_json TEXT,
            created_at TEXT
        );
        -- Task 2: ذخیره کامل Session (کل context.user_data هر کاربر) تا
        -- ری‌استارت سرور/کرش پروسه/بسته‌شدن توسط PythonAnywhere باعث نشه
        -- کاربر از اول شروع کنه. با هر تغییر state این جدول آپدیت می‌شه و
        -- موقع بالا اومدن بات، همه‌ی sessionها از همین‌جا لود می‌شن.
        CREATE TABLE IF NOT EXISTS bot_sessions (
            telegram_id INTEGER PRIMARY KEY,
            data_json TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS career_profiles (
            telegram_id INTEGER PRIMARY KEY,
            field TEXT, education TEXT, countries TEXT, gpa TEXT, work_exp TEXT,
            publications TEXT, job_filters TEXT,
            linkedin TEXT, github TEXT, scholar TEXT, skills TEXT,
            resume TEXT, resume_is_file INTEGER DEFAULT 0, resume_file_id TEXT,
            recommendations TEXT, recommenders_info TEXT,
            disliked_jobs TEXT DEFAULT '',
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS professor_cache (
            cache_key TEXT PRIMARY KEY,
            name TEXT,
            university TEXT,
            email TEXT,
            email_source TEXT,
            accepting_students INTEGER DEFAULT 0,
            accepting_evidence TEXT,
            has_funding INTEGER DEFAULT 0,
            funding_evidence TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pending_searches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            search_type TEXT,
            field TEXT,
            country TEXT,
            active INTEGER DEFAULT 1,
            attempts INTEGER DEFAULT 0,
            last_tried_at TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS search_results_cache (
            cache_key TEXT PRIMARY KEY,
            search_type TEXT,
            field TEXT,
            country TEXT,
            results_json TEXT,
            result_count INTEGER,
            created_at TEXT
        );
        -- استعلام سفارت (Embassy Inquiry) — کاملاً مجزا از جدول‌های بالا،
        -- عمداً هیچ FK یا وابستگی‌ای به subscriptions/email_jobs نداره تا
        -- یک باگ اینجا هیچ‌وقت روی سرویس ایمیل/شغل اثر نذاره. result_json
        -- خروجی نهایی (خلاصه‌شده توسط AI از متن اسکرِیپ‌شده‌ی سایت رسمی)
        -- رو نگه می‌داره؛ source_url همیشه همراهش ذخیره می‌شه تا کاربر
        -- بتونه خودش هم مرجع رسمی رو چک کنه.
        CREATE TABLE IF NOT EXISTS embassy_cache (
            cache_key TEXT PRIMARY KEY,
            embassy_country TEXT,
            category TEXT,
            source_url TEXT,
            result_json TEXT,
            created_at TEXT
        );
        -- Security Events — رویدادهای امنیتی سبک (rate-limit خورده،
        -- temp-block شده، ورود SMTP/گوگل ناموفق). عمداً بدون FK به هیچ
        -- جدول دیگه‌ای — یه باگ یا رشد بی‌رویه‌ی این جدول نباید هیچ‌وقت
        -- روی سرویس اصلی (ایمیل/شغل) اثر بذاره. فقط برای گزارش‌گیری
        -- ادمین استفاده می‌شه، هیچ منطقی از این جدول نمی‌خونه.
        CREATE TABLE IF NOT EXISTS security_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type TEXT,
            telegram_id INTEGER,
            detail TEXT,
            created_at TEXT
        );
        -- Audit Log — ثبت کامل و غیرقابل‌حذف هر اقدام ادمین (تایید/رد
        -- پرداخت، مشاهده‌ی داشبورد، پاسخ به کاربر، و هر اقدام آینده‌ای
        -- که با db_log_audit ثبت بشه). برای یک سرویس مهاجرتی که با
        -- اطلاعات حساس کاربر (رزومه، پاسپورت، وضعیت پرداخت) سروکار داره،
        -- این جدول مرجع پاسخگویی («کدوم ادمین، کِی، چه کاری روی پرونده‌ی
        -- کدوم کاربر انجام داد») است. عمداً بدون FK به جدول دیگه‌ای —
        -- دقیقاً هم‌الگوی security_events — تا رشد این جدول یا یک باگ
        -- توش هیچ‌وقت روی سرویس اصلی اثر نذاره. فقط INSERT/SELECT، هیچ
        -- UPDATE/DELETE ای روی این جدول نداریم (سوابق audit نباید بعداً
        -- عوض بشن).
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER,
            admin_name TEXT,
            action TEXT,
            target_user_id INTEGER,
            ip_address TEXT,
            result TEXT,
            detail TEXT,
            created_at TEXT
        );
        -- مانیتورینگ مداوم: برخلاف pending_searches (که فقط تا اولین نتیجه‌ی
        -- غیرصفر تلاش می‌کنه و بعدش خاموش می‌شه)، این جدول تا وقتی مشترک
        -- فعاله (حداکثر MONITOR_DURATION_DAYS روز) هر MONITOR_CHECK_INTERVAL_HOURS
        -- ساعت دوباره جستجو می‌کنه و فقط استاد/شغل *جدید* (که قبلاً به همین
        -- کاربر ارسال/اپلای نشده) رو گزارش می‌ده. UNIQUE روی
        -- (telegram_id, sub_id, monitor_type) یعنی برای هر سرویس هر کاربر
        -- فقط یک مانیتور فعال هم‌زمان وجود داره — تکرار جستجو از دو مانیتور
        -- موازی برای همون subscription اتفاق نمی‌افته.
        CREATE TABLE IF NOT EXISTS active_monitors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            sub_id INTEGER,
            monitor_type TEXT,
            field TEXT,
            country TEXT,
            active INTEGER DEFAULT 1,
            started_at TEXT,
            expires_at TEXT,
            last_checked_at TEXT,
            checks_done INTEGER DEFAULT 0,
            new_matches_found INTEGER DEFAULT 0,
            UNIQUE(telegram_id, sub_id, monitor_type)
        );
        CREATE TABLE IF NOT EXISTS search_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            field TEXT,
            country TEXT,
            count_requested INTEGER,
            count_returned INTEGER,
            elapsed_sec REAL,
            fallback_used INTEGER DEFAULT 0,
            source_counts_json TEXT,
            created_at TEXT
        );
        -- مرحله‌ی جدا از search_metrics: search_metrics فقط «مرحله‌ی
        -- کشف» رو اندازه می‌گیره (چند استاد خام از هر منبع پیدا شد).
        -- این جدول «مرحله‌ی تحلیل» رو اندازه می‌گیره — یعنی از همون
        -- استادهای خام، دقیقاً سر کدوم gate (email/name/duplicate/
        -- invalid_email/unverified_faculty/unverified_signals/no_match)
        -- چندتا رد شدن. بدون این، عدد نهایی کم بود ولی معلوم نبود
        -- گلوگاه کجاست؛ با این، هر batch یک ردیف با شمارش دقیق هر
        -- gate ثبت می‌کنه.
        CREATE TABLE IF NOT EXISTS analysis_funnel_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            sub_id INTEGER,
            field TEXT,
            total INTEGER,
            funnel_json TEXT,
            passed_final INTEGER,
            created_at TEXT
        );
        """)
        # ── قفل سخت ضدِ ارسال تکراری ──────────────────────────────
        # اگه به هر دلیلی (مثلاً دو Task همزمان برای یک کاربر) بخوایم دوباره
        # برای همون استاد رکورد status='sent' ثبت کنیم، همین ایندکس جلوشو
        # می‌گیره — حتی اگه گارد سطح‌کد (active-task registry) رد بشه.
        # روی دیتابیس‌های قدیمی که شاید از قبل رکورد تکراری داشته باشن هم
        # try/except جلوی crash استارتاپ رو می‌گیره.
        try:
            c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_sent_emails_unique
                         ON sent_emails(telegram_id, professor_email)
                         WHERE status='sent'""")
        except Exception as e:
            logger.warning(f"⚠️ ساخت idx_sent_emails_unique ناموفق بود (احتمالاً رکورد تکراری قدیمی وجود دارد): {e}")
        try:
            c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS idx_email_jobs_batch
                         ON email_jobs(telegram_id, sub_id, batch_idx)""")
        except Exception as e:
            logger.warning(f"⚠️ ساخت idx_email_jobs_batch ناموفق بود: {e}")

        # ── ایندکس‌های کارایی ────────────────────────────────────────
        # هر جدولی که بزرگ می‌شه (خصوصاً sent_emails/applied_jobs/email_jobs
        # با هزاران ردیف بعد از چند ماه)، بدون ایندکس هر SELECT روی این
        # ستون‌ها معادل یه full table scan می‌شه. هر ایندکس جدا try/except
        # داره تا اگه یکی (مثلاً به‌خاطر یه دیتابیس خیلی قدیمی) شکست بخوره،
        # بقیه ساخته بشن و برنامه بالا بیاد.
        #
        # نکته‌ی مهم: این‌ها باید بعد از _run_migrations(c) اجرا بشن، نه
        # قبلش — چون بعضی ستون‌ها (مثل applied_jobs.job_url) فقط از طریق
        # migration اضافه می‌شن، نه CREATE TABLE اولیه؛ روی یک دیتابیس
        # تازه، ایندکس زدن روشون قبل از migration با «no such column»
        # شکست می‌خوره.
        _run_migrations(c)

        _perf_indexes = [
            ("idx_users_telegram_id",        "users",             "telegram_id"),
            ("idx_users_status",             "users",             "status"),
            ("idx_subs_telegram_id",         "subscriptions",     "telegram_id"),
            ("idx_subs_status",              "subscriptions",     "status"),
            ("idx_subs_created_at",          "subscriptions",     "created_at"),
            ("idx_sent_emails_telegram_id",  "sent_emails",       "telegram_id"),
            ("idx_sent_emails_email",        "sent_emails",       "professor_email"),
            ("idx_sent_emails_status",       "sent_emails",       "status"),
            ("idx_sent_emails_reply_status", "sent_emails",       "reply_status"),
            ("idx_sent_emails_sent_at",      "sent_emails",       "sent_at"),
            ("idx_applied_jobs_telegram_id", "applied_jobs",      "telegram_id"),
            ("idx_applied_jobs_status",      "applied_jobs",      "status"),
            ("idx_applied_jobs_job_url",     "applied_jobs",      "job_url"),
            ("idx_applied_jobs_applied_at",  "applied_jobs",      "applied_at"),
            ("idx_email_jobs_telegram_id",   "email_jobs",        "telegram_id"),
            ("idx_email_jobs_status",        "email_jobs",        "status"),
            ("idx_career_profiles_field",    "career_profiles",   "field"),
            ("idx_career_profiles_countries","career_profiles",   "countries"),
            ("idx_professor_cache_university","professor_cache",  "university"),
            ("idx_professor_cache_updated_at","professor_cache",  "updated_at"),
            ("idx_search_cache_field",       "search_results_cache", "field"),
            ("idx_search_cache_country",     "search_results_cache", "country"),
            ("idx_search_cache_created_at",  "search_results_cache", "created_at"),
            ("idx_pending_searches_active",  "pending_searches",  "active"),
            ("idx_active_monitors_active",   "active_monitors",   "active"),
            ("idx_active_monitors_last_checked","active_monitors","last_checked_at"),
            ("idx_security_events_type",     "security_events",   "event_type"),
            ("idx_security_events_created_at","security_events",  "created_at"),
            ("idx_audit_log_created_at",     "audit_log",         "created_at"),
            ("idx_audit_log_admin_id",       "audit_log",         "admin_id"),
            ("idx_audit_log_target_user_id", "audit_log",         "target_user_id"),
            ("idx_audit_log_action",         "audit_log",         "action"),
            ("idx_active_monitors_telegram_id","active_monitors","telegram_id"),
            ("idx_search_metrics_created_at","search_metrics",    "created_at"),
            ("idx_search_metrics_field",     "search_metrics",    "field"),
            ("idx_analysis_funnel_created_at","analysis_funnel_metrics","created_at"),
            ("idx_analysis_funnel_field",    "analysis_funnel_metrics","field"),
            ("idx_analysis_funnel_telegram_id","analysis_funnel_metrics","telegram_id"),
        ]
        for idx_name, table, col in _perf_indexes:
            try:
                c.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({col})")
            except Exception as e:
                logger.warning(f"⚠️ ساخت ایندکس {idx_name} ناموفق بود: {e}")
    logger.info("✅ DB ready")

# ── Migration سبک مبتنی بر PRAGMA user_version خود SQLite ──────────
# چرا این و نه یه فریم‌ورک جدا: نیازی به جدول یا کتابخانه‌ی اضافه نداره،
# خود SQLite این عدد رو نگه می‌داره. هر migration دقیقاً یک‌بار روی هر
# دیتابیس اجرا می‌شه (even اگه بات صدبار ری‌استارت بشه). وقتی بعداً یه
# ستون/جدول جدید لازم شد، فقط یه ورودی جدید به MIGRATIONS اضافه کن —
# نیازی نیست نگران دیتابیس‌های قدیمی کاربرهای فعلی باشی.
MIGRATIONS: dict[int, list[str]] = {
    # 1: نسخه‌ی پایه — همون CREATE TABLE هایی که در init_db هستن.
    2: ["ALTER TABLE applied_jobs ADD COLUMN job_url TEXT"],
    # 3: AI Memory — پاسخ اساتید (reply_status/reply_note) + مشاغل/شرکت‌های
    # ناخواسته (disliked_jobs). روی دیتابیس‌های قدیمی که این ستون‌ها رو
    # ندارن اجرا می‌شه؛ اگه از قبل وجود داشته باشن (نصب تازه) خطا فقط
    # لاگ می‌شه، crash نمی‌کنه.
    3: [
        "ALTER TABLE sent_emails ADD COLUMN reply_status TEXT DEFAULT 'unknown'",
        "ALTER TABLE sent_emails ADD COLUMN reply_note TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN disliked_jobs TEXT DEFAULT ''",
    ],
    # 4: IMAP Bounce Watcher — این ستون عمداً پیش‌فرضش ۰ (خاموش) است.
    # چک کردن صندوق ایمیل کاربر، حتی فقط برای تشخیص bounce، خوندن صندوق
    # شخصی کسیه — باید صریحاً از منوی تنظیمات توسط خودِ کاربر روشن بشه،
    # نه این‌که پیش‌فرض بات برای همه فعال باشه.
    4: ["ALTER TABLE career_profiles ADD COLUMN imap_bounce_optin INTEGER DEFAULT 0"],
    # 5: Time-Budget Rescue — وقتی یک استاد به‌خاطر تموم شدن سقف زمانی
    # کل batch (MAX_SCAN_SECONDS) رد می‌شه (نه چون منبعی نداشتیم، فقط
    # چون وقت batch اصلی تموم شد)، برای این‌که بعداً بشه pipeline
    # ۱۱مرحله‌ی ایمیل‌یابی رو خارج از فشار زمانی batch دوباره براش اجرا
    # کرد، لازمه هویت همون استاد (اسم/دانشگاه/openalex_id/url) رو همون
    # لحظه‌ی enqueue شدن نگه داریم — چون خودِ ردیف email_jobs فقط
    # professor_name/professor_email رو داشت و برای دوباره صدا زدن
    # find_professor_email_and_signals کافی نبود.
    5: ["ALTER TABLE email_jobs ADD COLUMN prof_json TEXT"],
    # 6: ATS Resume Scan — امتیاز/گزارش/هش رزومه‌ای که آخرین بار اسکن شده،
    # تا اسکن اجباری فقط یک‌بار (برای هر نسخه‌ی رزومه) انجام بشه، نه هر بار.
    # resume_ats_hash هش متن رزومه‌ی اسکن‌شده‌ست — اگه کاربر رزومه‌ی جدید
    # بفرسته که هشش فرق داره، دوباره اجباری می‌شه؛ برای همون رزومه‌ی قبلی،
    # فقط با دستور جدا (منوی «🔎 اسکن ATS رزومه») دوباره قابل اجراست.
    6: [
        "ALTER TABLE career_profiles ADD COLUMN resume_ats_score INTEGER",
        "ALTER TABLE career_profiles ADD COLUMN resume_ats_report_json TEXT",
        "ALTER TABLE career_profiles ADD COLUMN resume_ats_hash TEXT",
        "ALTER TABLE career_profiles ADD COLUMN resume_ats_scanned_at TEXT",
    ],
    # 7: پرونده‌ی مهاجرتی (Immigration Profile) — همون منطق AI Memory که
    # قبلاً فقط برای سرویس ایمیل/شغل بود (یک بار بگیر، همیشه استفاده کن)
    # حالا برای تصمیم‌های مهاجرتی/ویزا هم انجام می‌شه. عمداً همون جدول
    # career_profiles (نه جدول جدا) — چون این ستون‌ها هم دقیقاً «پرونده‌ی
    # همون کاربر»ن و باید با همون telegram_id یکتا و همون منطق ON CONFLICT
    # DO UPDATE کار کنن؛ ولی (دقیقاً مثل disliked_jobs) عمداً از
    # _CAREER_PROFILE_FIELDS جدا نگه داشته می‌شن تا هر بار ذخیره‌ی معمولی
    # پروفایل شغلی (که این ستون‌ها رو نمی‌فرسته) پاکشون نکنه.
    7: [
        "ALTER TABLE career_profiles ADD COLUMN visa_goal TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN previous_refusal TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN documents_status TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN weak_points TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN strategy TEXT DEFAULT ''",
    ],
    # 8: Personal Visa Agent — پیام هفتگیِ «۳ کار ضروری این هفته» که خودِ بات
    # بدون این‌که کاربر سوال بپرسه می‌فرسته. عمداً دو تصمیم امنیتی توش رعایت
    # شده:
    #  ۱) visa_agent_optin پیش‌فرضش ۰ (خاموش) است — دقیقاً مثل
    #     imap_bounce_optin، چون این یعنی بات بدون درخواست صریح کاربر براش
    #     پیام proactive می‌فرسته؛ نباید پیش‌فرض روی همه‌ی کاربرها فعال باشه.
    #  ۲) deadline یک تاریخ واقعیه که فقط خودِ کاربر وارد می‌کنه
    #     (visa_agent_deadline_date/label) — AI هیچ‌وقت این تاریخ رو حدس
    #     نمی‌زنه و شمارش روزهای باقی‌مونده همیشه با کد پایتون از روی همین
    #     تاریخ محاسبه می‌شه، نه از تخیل مدل. اگه کاربر ددلاینی وارد نکرده
    #     باشه، پیام هفتگی اصلاً هیچ عدد روزی نشون نمی‌ده.
    8: [
        "ALTER TABLE career_profiles ADD COLUMN visa_agent_optin INTEGER DEFAULT 0",
        "ALTER TABLE career_profiles ADD COLUMN visa_agent_deadline_date TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN visa_agent_deadline_label TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN visa_agent_last_sent TEXT DEFAULT ''",
    ],
    # 9: Funding Intelligence + Supervisor Availability Score — لیبل‌های
    # اضافه‌ی اختیاری روی professor_cache. عمداً هیچ ستون فیلترکننده‌ای
    # اینجا نیست (مثلاً چیزی که match/no-match رو عوض کنه) — این ستون‌ها
    # فقط اطلاعات بیشتر برای نمایش به کاربرن، دقیقاً مثل has_funding/
    # accepting_students که از قبل بودن. اگه migration fail بشه یا کش
    # قدیمی این ستون‌ها رو نداشته باشه، کد جدید (پایین‌تر) همه‌جا با
    # .get(...) امن می‌خونتشون — یعنی نبود این ستون‌ها هیچ‌وقت باعث crash
    # یا رفتار متفاوت پایپ‌لاین اصلی نمی‌شه.
    9: [
        "ALTER TABLE professor_cache ADD COLUMN funding_intel_json TEXT DEFAULT ''",
        "ALTER TABLE professor_cache ADD COLUMN availability_score INTEGER DEFAULT 0",
        "ALTER TABLE professor_cache ADD COLUMN availability_evidence_json TEXT DEFAULT ''",
    ],
    # 10: Job Freshness Trust — علاوه بر برچسب سن آگهی که به کاربر نشون
    # داده می‌شه، لحظه‌ی دقیقی که خودمون منبع رو چک/تایید کردیم
    # (source_checked_at) هم برای هر ردیف applied_jobs ذخیره می‌شه — تا
    # اگه بعداً لازم شد («این آگهی رو کِی چک کردیم؟») قابل audit باشه، نه
    # فقط چیزی که لحظه‌ی نمایش به کاربر گفته شده و جایی ذخیره نشده.
    10: ["ALTER TABLE applied_jobs ADD COLUMN source_checked_at TEXT DEFAULT ''"],
    # 11: Job Fingerprint + Application Pipeline + Application Brain —
    #  - fingerprint/sources_json روی applied_jobs: همان «شغل واقعی» که
    #    ممکن است روی چند منبع (LinkedIn/سایت شرکت/Indeed/برد دیگر) دیده
    #    شده باشد، زیر یک شناسه‌ی یکتا (job_fingerprint) ثبت می‌شود؛
    #    sources_json لیست منابعی که این شغل رویشان پیدا شده را نگه می‌دارد
    #    (برای نمایش «Found on N sources»).
    #  - pipeline_stage روی applied_jobs و email_jobs: مرحله‌ی صریح این
    #    آگهی/ایمیل در پایپ‌لاین (DISCOVERED..OFFER برای شغل،
    #    FOUND..MEETING برای استاد) — مستقل از status خام موجود، فقط یک
    #    لیبل نمایشی/فیلترکردنی اضافه، هیچ منطق موجودی را عوض نمی‌کند.
    #  - learned_filters_json روی career_profiles: نسخه‌ی ساختاریافته‌ی
    #    disliked_jobs — به‌جای فقط یک رشته‌ی آزاد کلیدواژه، شمارنده‌ی
    #    دسته‌بندی‌شده (work_mode/seniority/company_stage/sponsorship/...)
    #    که جستجوی بعدی می‌تواند خودکار از آن یاد بگیرد.
    11: [
        "ALTER TABLE applied_jobs ADD COLUMN fingerprint TEXT DEFAULT ''",
        "ALTER TABLE applied_jobs ADD COLUMN sources_json TEXT DEFAULT ''",
        "ALTER TABLE applied_jobs ADD COLUMN pipeline_stage TEXT DEFAULT ''",
        "ALTER TABLE email_jobs ADD COLUMN pipeline_stage TEXT DEFAULT ''",
        "ALTER TABLE career_profiles ADD COLUMN learned_filters_json TEXT DEFAULT ''",
    ],
}

def _run_migrations(c):
    """نکته‌ی امنیتی: قبلاً هر sqlite3.OperationalError (نه فقط «ستون از قبل
    وجود داره») بی‌صدا catch می‌شد و بعد user_version جلو می‌رفت — یعنی اگه یه
    ALTER TABLE به هر دلیل واقعی دیگه‌ای (تایپو، دیسک پر، جدول لاک) شکست
    می‌خورد، برنامه فکر می‌کرد migration انجام شده در حالی که ستون واقعاً
    نبود. الان فقط پیام‌هایی که واقعاً یعنی «ستون/جدول از قبل هست» رو silent
    می‌کنیم؛ هر خطای دیگه‌ای raise می‌شه و کل migration اون نسخه متوقف می‌مونه
    (user_version جلو نمی‌ره) تا مشکل واقعی دیده و فیکس بشه، نه این‌که پنهان
    بمونه."""
    cur_version = c.execute("PRAGMA user_version").fetchone()[0]
    for v in sorted(k for k in MIGRATIONS if k > cur_version):
        for sql in MIGRATIONS[v]:
            try:
                c.execute(sql)
            except sqlite3.OperationalError as e:
                msg = str(e).lower()
                if "duplicate column" in msg or "already exists" in msg:
                    # این‌جا واقعاً بی‌خطره: ستون/جدول از قبل هست (مثلاً
                    # نصب تازه، یا دیتابیسی که دستی دستکاری شده).
                    logger.warning(f"migration v{v} skipped ({sql}): {e}")
                else:
                    # هر خطای دیگه‌ای (تایپو، جدول قفل، دیسک پر و ...) باید
                    # migration رو متوقف کنه، نه این‌که silent بمونه.
                    logger.critical(
                        f"❌ migration v{v} با خطای غیرمنتظره شکست خورد ({sql}): {e} — "
                        "user_version جلو نمی‌ره تا مشکل بررسی بشه.")
                    raise
        c.execute(f"PRAGMA user_version={v}")
        logger.info(f"✅ دیتابیس به نسخه v{v} migrate شد")
    if cur_version < 1:
        c.execute("PRAGMA user_version=1")

def db_save_pending_snapshot(sid, data_json):
    with _db() as c:
        c.execute("""INSERT INTO pending_snapshots (sub_id,data_json,created_at)
            VALUES(?,?,?)
            ON CONFLICT(sub_id) DO UPDATE SET data_json=excluded.data_json""",
            (sid, data_json, str(datetime.now())))

def db_get_pending_snapshot(sid):
    with _db() as c:
        row = c.execute("SELECT data_json FROM pending_snapshots WHERE sub_id=?", (sid,)).fetchone()
    return json.loads(row[0]) if row else None

def db_delete_pending_snapshot(sid):
    with _db() as c:
        c.execute("DELETE FROM pending_snapshots WHERE sub_id=?", (sid,))

# ── Task 2: Session ذخیره‌ی کامل (Resume بعد از ری‌استارت) ────────────
# این سه تابع پایه‌ی SQLiteUserDataPersistence هستن (پایین‌تر تعریف می‌شه).
# نکته‌ی مهم: این توابع sync هستن و همیشه با asyncio.to_thread صدا زده
# می‌شن، دقیقاً مثل بقیه‌ی db_* های این فایل — تا event loop اصلی هیچ‌وقت
# روی I/O دیسک بلاک نشه.
def db_save_session(telegram_id: int, data_json: str):
    with _db() as c:
        c.execute("""INSERT INTO bot_sessions (telegram_id, data_json, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                data_json=excluded.data_json, updated_at=excluded.updated_at""",
            (telegram_id, data_json, str(datetime.now())))

def db_load_all_sessions() -> dict:
    with _db() as c:
        rows = c.execute("SELECT telegram_id, data_json FROM bot_sessions").fetchall()
    out = {}
    for tid, data_json in rows:
        try:
            out[tid] = json.loads(data_json)
        except Exception as e:
            logger.warning(f"⚠️ session خراب برای uid={tid}، نادیده گرفته شد: {e}")
    return out

def db_delete_session(telegram_id: int):
    with _db() as c:
        c.execute("DELETE FROM bot_sessions WHERE telegram_id=?", (telegram_id,))

def db_upsert_user(tid, username, first_name):
    with _db() as c:
        c.execute("""INSERT OR IGNORE INTO users
            (telegram_id,username,first_name,created_at)
            VALUES(?,?,?,?)""",
            (tid, username or "", first_name or "", str(datetime.now())))

# اسم ستون‌ها توی db_set_user/db_increment_sub_count با f-string توی کوئری
# می‌ره (چون SQLite پارامتر ? رو فقط برای VALUE قبول می‌کنه نه اسم ستون).
# همه‌ی caller های فعلی این توابع رو با kwarg های هاردکد صدا می‌زنن (نه با
# متن آزاد کاربر) پس امروز SQL injection واقعی‌ای وجود نداره؛ ولی برای
# جلوگیری از اینکه یه توسعه‌ی بعدی به‌اشتباه اسم ستون رو از ورودی کاربر
# بسازه، اینجا یک whitelist صریح می‌ذاریم — هر کلید خارج از این لیست یک
# خطای برنامه‌نویسی محسوب می‌شه و باعث ValueError می‌شه، نه اجرای کوئری.
_USERS_TABLE_COLUMNS = {
    "username", "first_name", "full_name", "phone",
    "user_type", "service", "status", "sub_id",
}

def db_set_user(tid, **kw):
    if not kw: return
    bad = set(kw) - _USERS_TABLE_COLUMNS
    if bad:
        raise ValueError(f"db_set_user: ستون‌های غیرمجاز {bad}")
    sets = ",".join(f"{k}=?" for k in kw)
    with _db() as c:
        c.execute(f"UPDATE users SET {sets} WHERE telegram_id=?",
                  (*kw.values(), tid))

def db_get_user(tid):
    with _db() as c:
        row = c.execute("SELECT * FROM users WHERE telegram_id=?", (tid,)).fetchone()
    if not row: return None
    cols = ["telegram_id","username","first_name","full_name","phone",
            "user_type","service","status","sub_id","created_at"]
    return dict(zip(cols, row))

def db_create_sub(tid, service, user_type, price_toman=0):
    """ساخت subscription + آپدیت users.sub_id، هر دو توی یک transaction واحد.
    قبلاً این دو تا کار توی دو تا `with _db() as c:` جدا (پس دو commit جدا)
    انجام می‌شد — یعنی اگه دقیقاً بین این دو خط process crash می‌کرد،
    subscription ساخته شده بود ولی users.sub_id هیچ‌وقت به سمتش اشاره
    نمی‌کرد. الان هر دو INSERT/UPDATE زیر یک connection context اجرا
    می‌شن، پس یا هر دو commit می‌شن یا (روی خطا) هیچ‌کدوم."""
    with _db() as c:
        cur = c.execute("""INSERT INTO subscriptions
            (telegram_id,service,user_type,price_toman,created_at)
            VALUES(?,?,?,?,?)""",
            (tid, service, user_type, price_toman, str(datetime.now())))
        sid = cur.lastrowid
        c.execute("UPDATE users SET sub_id=? WHERE telegram_id=?", (sid, tid))
    return sid

def db_approve_sub(sid) -> bool:
    """تایید یک subscription — atomic و idempotent. اگه قبلاً approved شده
    باشه (مثلاً ادمین دوبار روی دکمه‌ی تایید کلیک کرده، یا دو تا درخواست
    هم‌زمان اومده)، این تابع هیچ کاری نمی‌کنه و False برمی‌گردونه — طوری
    که caller بفهمه این «همون تایید اولیه» نیست و نباید workflow (پیام به
    کاربر، شروع مجدد فرایند، ارسال ایمیل) رو دوباره اجرا کنه.

    نکته‌ی فنی: WHERE status != 'approved' داخل خودِ UPDATE این atomicity
    رو تضمین می‌کنه — SQLite هر UPDATE رو در یک تراکنش ضمنی و atomic اجرا
    می‌کنه، پس حتی اگه دو تا درخواست هم‌زمان (دو تپ ادمین، یا دو بار
    فراخوانی هندلر) به این تابع برسن، فقط یکی‌شون rowcount>0 می‌گیره —
    نیازی به قفل سطح‌اپلیکیشن نیست."""
    with _db() as c:
        cur = c.execute("""UPDATE subscriptions SET status='approved', approved_at=?
            WHERE id=? AND status != 'approved'""", (str(datetime.now()), sid))
        return cur.rowcount > 0

def db_reject_sub(sid) -> bool:
    """رد یک subscription — atomic و idempotent، دقیقاً هم‌الگوی
    db_approve_sub بالا. قبل از این تابع، دکمه‌ی «❌ رد» و دستور
    /reject_<sid>_<tid> فقط یک پیام به کاربر می‌فرستادن ولی هیچ UPDATE‌ای
    روی جدول subscriptions نمی‌زدن — یعنی sub برای همیشه روی 'pending'
    می‌موند: هم توی /stats به‌عنوان «در انتظار» شمرده می‌شد، هم مانیتور
    مداوم (_check_active_monitors) چون 'pending' رو هم فعال حساب می‌کرد
    (پایین‌تر همون‌جا فیکس شده) می‌تونست بی‌نهایت براش جستجو کنه، هم اگه
    ادمین بعداً روی همون sid دکمه‌ی «✅ تایید» رو (حتی اشتباهی) می‌زد،
    db_approve_sub چون شرطش فقط `status != 'approved'` بود، یک subscription
    که ادمین صراحتاً رد کرده بود رو approve می‌کرد.

    WHERE status='pending' هم idempotency رو تضمین می‌کنه (دوبار زدن دکمه‌ی
    رد کاری نمی‌کنه) و هم جلوی رد شدنِ یک sub که از قبل approved/completed
    شده رو می‌گیره — رد فقط از حالت pending معنی داره."""
    with _db() as c:
        cur = c.execute("""UPDATE subscriptions SET status='rejected'
            WHERE id=? AND status='pending'""", (sid,))
        return cur.rowcount > 0

def db_increment_sub_count(sid, kind):
    """kind: 'email' یا 'job' — شمارنده‌ی مصرف quota رو توی دیتابیس نگه
    می‌داره (نه فقط توی متغیر لوکال حلقه‌ی ارسال) تا حتی اگه بات وسط کار
    ری‌استارت بشه، عدد واقعی مصرف‌شده گم نشه."""
    col = "emails_sent" if kind == "email" else "jobs_applied"
    with _db() as c:
        c.execute(f"UPDATE subscriptions SET {col} = {col} + 1 WHERE id=?", (sid,))
        row = c.execute(f"SELECT {col} FROM subscriptions WHERE id=?", (sid,)).fetchone()
    return row[0] if row else None

def db_mark_sub_completed(sid):
    with _db() as c:
        row = c.execute("SELECT telegram_id FROM subscriptions WHERE id=?", (sid,)).fetchone()
        c.execute("UPDATE subscriptions SET status='completed',completed_at=? WHERE id=?",
                  (str(datetime.now()), sid))
    # مشترکی که به quota رسیده دیگه لازم نیست مانیتور مداوم داشته باشه —
    # ارسال ایمیل/اپلای جدید براش امکان‌پذیر نیست، پس جستجوی جدید بی‌فایده‌ست.
    if row:
        try:
            db_deactivate_monitors_for_sub(row[0], sid)
        except Exception as e:
            logger.error(f"db_deactivate_monitors_for_sub on completion: {e}")

def db_get_sub(sid):
    with _db() as c:
        row = c.execute("SELECT * FROM subscriptions WHERE id=?", (sid,)).fetchone()
    if not row: return None
    cols = ["id","telegram_id","service","user_type","status","receipt_file_id",
            "price_toman","emails_sent","jobs_applied","created_at","approved_at","completed_at"]
    return dict(zip(cols, row))

def db_get_latest_sub_for_service(tid: int, service: str) -> dict | None:
    """آخرین subscription این کاربر برای یک سرویس *مشخص* (email یا job).
    برخلاف users.sub_id — که فقط آخرین subscription از هر نوع سرویسی رو
    یادش می‌مونه و با شروع سرویس دوم بازنویسی می‌شه — این تابع مستقیم از
    خودِ جدول subscriptions با فیلتر روی service می‌خونه. برای این لازمه
    که کاربری که هم‌زمان مشترک «ایمیل به اساتید» و «اپلای کار» است، هر
    دو subscription جدا و درست پیدا بشن، نه اینکه دومی اولی رو قایم کنه."""
    with _db() as c:
        row = c.execute("""SELECT * FROM subscriptions
            WHERE telegram_id=? AND service=? ORDER BY id DESC LIMIT 1""",
            (tid, service)).fetchone()
    if not row:
        return None
    cols = ["id","telegram_id","service","user_type","status","receipt_file_id",
            "price_toman","emails_sent","jobs_applied","created_at","approved_at","completed_at"]
    return dict(zip(cols, row))

def db_is_approved(sid) -> bool:
    """آیا subscription با این id مشخصاً approved شده یا نه.

    قبلاً این تابع تنها با telegram_id صدا زده می‌شد و خودش از
    users.sub_id (آخرین subscription از هر نوع سرویسی) subscription رو
    پیدا می‌کرد. مشکل: users.sub_id با شروع سرویس دوم (مثلاً کاربری که
    اول «ایمیل به اساتید» و بعد «اپلای کار» رو هم می‌خره) بازنویسی
    می‌شه — یعنی چک approval یک subscription می‌تونست ناخواسته وضعیت
    subscription دیگه‌ای از همون کاربر رو برگردونه.

    الان تابع مستقیم sub_id مشخصی که caller در همون لحظه در دست داره
    (ud[S_SUB] یا user['sub_id'] گرفته‌شده در همون flow) رو می‌گیره، نه
    این‌که خودش از جدول users حدس بزنه — دقیقاً همون subscriptionـی که
    caller می‌خواد چکش کنه."""
    if not sid:
        return False
    s = db_get_sub(sid)
    return bool(s and s["status"] == "approved")

def db_log_email(tid, prof_name, prof_email, subject, status):
    try:
        with _db() as c:
            cur = c.execute("""INSERT INTO sent_emails
                (telegram_id,professor_name,professor_email,subject,status,sent_at)
                VALUES(?,?,?,?,?,?)""",
                (tid, prof_name, prof_email, subject, status, str(datetime.now())))
            return cur.lastrowid
    except sqlite3.IntegrityError:
        # این ترکیب کاربر+ایمیل استاد قبلاً با status='sent' ثبت شده —
        # یعنی یه‌جا (مثلاً دو تسک ارسال همزمان) داشتیم دوباره ثبتش می‌کردیم.
        # به‌جای رکورد تکراری یا crash، فقط لاگ می‌کنیم.
        logger.warning(f"duplicate sent_emails blocked: tid={tid} email={prof_email}")
        return None

def db_log_job(tid, title, company, method, status, url="", source_checked_at="",
                fingerprint="", sources_json="", pipeline_stage=""):
    with _db() as c:
        c.execute("""INSERT INTO applied_jobs
            (telegram_id,job_title,company,method,status,applied_at,job_url,source_checked_at,
             fingerprint,sources_json,pipeline_stage)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (tid, title, company, method, status, str(datetime.now()), url, source_checked_at,
             fingerprint, sources_json, job_pipeline_stage_of(status, pipeline_stage)))

# ── Application Pipeline — مرحله‌ی صریح هر آگهی/ایمیل ────────────
# ایده‌ی کاربر: به‌جای یک status خام (SENT/FAILED/...)، یک pipeline
# قابل‌فهم برای کاربر که مسیر واقعی «از پیدا شدن تا نتیجه» را نشان
# می‌دهد. عمداً هیچ منطق ارسال/فیلتر موجود عوض نشده — این فقط یک لایه‌ی
# نمایشی/دسته‌بندی روی همان دیتای موجود است، پس هیچ‌جای دیگری از بات را
# نمی‌شکند.
JOB_PIPELINE_STAGES = [
    "DISCOVERED", "MATCHED", "SHORTLISTED", "READY",
    "APPLIED", "ACKNOWLEDGED", "INTERVIEW", "OFFER", "REJECTED",
]
PROFESSOR_PIPELINE_STAGES = [
    "FOUND", "VERIFIED", "EMAIL_READY", "SENT",
    "OPENED", "REPLIED", "INTERESTED", "MEETING",
]
JOB_PIPELINE_LABELS_FA = {
    "DISCOVERED": "🔎 پیدا شد", "MATCHED": "✅ مرتبط تشخیص داده شد",
    "SHORTLISTED": "⭐ شورت‌لیست شد", "READY": "📝 آماده‌ی ارسال",
    "APPLIED": "📤 اپلای شد", "ACKNOWLEDGED": "📬 تایید دریافت شد",
    "INTERVIEW": "🎤 مصاحبه", "OFFER": "🎉 آفر", "REJECTED": "❌ رد شد",
}
PROFESSOR_PIPELINE_LABELS_FA = {
    "FOUND": "🔎 پیدا شد", "VERIFIED": "✅ ایمیل تایید شد",
    "EMAIL_READY": "📝 ایمیل آماده", "SENT": "📤 ارسال شد",
    "OPENED": "👁 باز شد", "REPLIED": "↩️ پاسخ داد",
    "INTERESTED": "🌟 علاقه‌مند بود", "MEETING": "🤝 جلسه",
}
# نگاشت status خام فعلی → مرحله‌ی pipeline (برای backward-compat؛ ردیف‌های
# قدیمی pipeline_stage خالی دارند و از همین نگاشت محاسبه می‌شود، نه اینکه
# یک migration سنگین همه‌ی تاریخچه را بازنویسی کند).
JOB_STATUS_TO_STAGE = {
    "SENT": "APPLIED", "APPLIED": "APPLIED", "FAILED": "REJECTED",
    "SKIPPED": "MATCHED", "PENDING": "READY", "LINK_PROVIDED": "READY",
}
EMAIL_STATUS_TO_STAGE = {
    "SENT": "SENT", "FAILED": "EMAIL_READY", "SKIPPED": "FOUND",
    "PENDING": "EMAIL_READY", "UNKNOWN": "SENT",
}

def job_pipeline_stage_of(status: str, explicit_stage: str = "") -> str:
    """مرحله‌ی pipeline یک آگهی را برمی‌گرداند — اگر صریحاً ذخیره شده
    باشد همان، وگرنه از status خام محاسبه می‌شود.
    ⚠️ فیکس مهم: status واقعی که db_log_job می‌گیره تقریباً هیچ‌وقت دقیقاً
    یکی از کلیدهای JOB_STATUS_TO_STAGE نیست — مثلاً "failed:SMTP timeout"
    یا "skipped:no_match(score=45):...reason". قبلاً این تابع (و db_log_job)
    با exact-match دیکشنری رو می‌خوند، یعنی این status های واقعی هیچ‌وقت
    مچ نمی‌شدن و pipeline_stage همیشه "" ذخیره می‌شد. الان با prefix
    matching (status.upper() با هر کلید شروع بشه) چک می‌کنیم — یعنی
    "failed:..." درست به REJECTED مچ می‌شه، نه اینکه گم بشه."""
    if explicit_stage:
        return explicit_stage
    s = (status or "").strip().upper()
    for prefix, stage in JOB_STATUS_TO_STAGE.items():
        if s.startswith(prefix):
            return stage
    return "DISCOVERED"

def professor_pipeline_stage_of(status: str, reply_status: str = "", explicit_stage: str = "") -> str:
    if explicit_stage:
        return explicit_stage
    if reply_status == "positive":
        return "INTERESTED"
    if reply_status == "negative":
        return "REPLIED"
    return EMAIL_STATUS_TO_STAGE.get((status or "").upper(), "FOUND")

def db_set_job_pipeline_stage(applied_job_id: int, stage: str) -> None:
    if stage not in JOB_PIPELINE_STAGES:
        return
    with _db() as c:
        c.execute("UPDATE applied_jobs SET pipeline_stage=? WHERE id=?", (stage, applied_job_id))

def db_set_email_pipeline_stage(email_job_id: int, stage: str) -> None:
    if stage not in PROFESSOR_PIPELINE_STAGES:
        return
    with _db() as c:
        c.execute("UPDATE email_jobs SET pipeline_stage=? WHERE id=?", (stage, email_job_id))

# ── Professor Cache ──────────────────────────────────────────────
# اگه امروز ایمیل یه استاد رو پیدا کردیم (چه برای این کاربر چه کاربر
# دیگه‌ای که تصادفاً همون استاد رو جستجو کرده)، فردا و چند روز بعد دیگه
# نیازی نیست دوباره ۱۱ منبع رو جستجو کنیم — این هم فشار رو زیاد کم می‌کنه
# (روی DDG/Google خصوصاً) هم سرعت رو بالا می‌بره.
PROFESSOR_CACHE_TTL_DAYS = int(os.getenv("PROFESSOR_CACHE_TTL_DAYS", "21") or "21")

def _professor_cache_key(name: str, university: str) -> str:
    return f"{(name or '').strip().lower()}|{(university or '').strip().lower()}"

def db_get_professor_cache(name: str, university: str) -> dict | None:
    key = _professor_cache_key(name, university)
    if key == "|":
        return None
    cutoff = (datetime.now() - timedelta(days=PROFESSOR_CACHE_TTL_DAYS)).strftime("%Y-%m-%d %H:%M:%S")
    with _db() as c:
        # funding_intel_json/availability_* ممکنه روی دیتابیس‌های خیلی قدیمی
        # (قبل از migration v9) هنوز نباشن — به‌جای فرض کردن وجودشون، اول با
        # PRAGMA چک می‌کنیم و اگه نبودن، همون SELECT قدیمی (بدون این ستون‌ها)
        # اجرا می‌شه. این یعنی حتی اگه migration به هر دلیلی fail شده باشه،
        # کش استاد اصلاً crash نمی‌کنه — فقط این دو لیبل جدید خالی برمی‌گردن.
        cols = {r[1] for r in c.execute("PRAGMA table_info(professor_cache)").fetchall()}
        has_new_cols = {"funding_intel_json", "availability_score", "availability_evidence_json"} <= cols
        if has_new_cols:
            row = c.execute("""SELECT email, email_source, accepting_students, accepting_evidence,
                                       has_funding, funding_evidence,
                                       funding_intel_json, availability_score, availability_evidence_json
                FROM professor_cache WHERE cache_key=? AND updated_at >= ?""", (key, cutoff)).fetchone()
        else:
            row = c.execute("""SELECT email, email_source, accepting_students, accepting_evidence,
                                       has_funding, funding_evidence
                FROM professor_cache WHERE cache_key=? AND updated_at >= ?""", (key, cutoff)).fetchone()
    if not row:
        return None
    out = {
        "email": row[0] or "", "email_source": row[1] or "",
        "accepting_students": bool(row[2]), "accepting_evidence": row[3] or "",
        "has_funding": bool(row[4]), "funding_evidence": row[5] or "",
        "lab_page_text": "", "is_lab_page": False,  # این دوتا کش نمی‌شن (سنگین/کم‌فایده برای کش)
        "funding_intelligence": None, "availability_score": 0, "availability_evidence": [],
    }
    if has_new_cols:
        try:
            out["funding_intelligence"] = json.loads(row[6]) if row[6] else None
        except Exception:
            out["funding_intelligence"] = None
        out["availability_score"] = int(row[7] or 0)
        try:
            out["availability_evidence"] = json.loads(row[8]) if row[8] else []
        except Exception:
            out["availability_evidence"] = []
    return out

def db_save_professor_cache(name: str, university: str, result: dict):
    key = _professor_cache_key(name, university)
    if key == "|" or not result.get("email"):
        return  # نتیجه‌ی بدون ایمیل رو کش نمی‌کنیم — شاید فقط این‌بار موقتاً پیدا نشده
    fi = result.get("funding_intelligence")
    with _db() as c:
        cols = {r[1] for r in c.execute("PRAGMA table_info(professor_cache)").fetchall()}
        if {"funding_intel_json", "availability_score", "availability_evidence_json"} <= cols:
            c.execute("""INSERT OR REPLACE INTO professor_cache
                (cache_key,name,university,email,email_source,accepting_students,accepting_evidence,
                 has_funding,funding_evidence,funding_intel_json,availability_score,
                 availability_evidence_json,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (key, name, university, result.get("email",""), result.get("email_source",""),
                 int(bool(result.get("accepting_students"))), result.get("accepting_evidence",""),
                 int(bool(result.get("has_funding"))), result.get("funding_evidence",""),
                 json.dumps(fi, ensure_ascii=False) if fi else "",
                 int(result.get("availability_score") or 0),
                 json.dumps(result.get("availability_evidence") or [], ensure_ascii=False),
                 str(datetime.now())))
        else:
            c.execute("""INSERT OR REPLACE INTO professor_cache
                (cache_key,name,university,email,email_source,accepting_students,accepting_evidence,
                 has_funding,funding_evidence,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (key, name, university, result.get("email",""), result.get("email_source",""),
                 int(bool(result.get("accepting_students"))), result.get("accepting_evidence",""),
                 int(bool(result.get("has_funding"))), result.get("funding_evidence",""),
                 str(datetime.now())))

# ── Search Cache — کل نتیجه‌ی جستجو (نه فقط یک استاد) ────────────────
# اگه «کانادا + Machine Learning» یک ساعت پیش جستجو شده، نیازی نیست
# دوباره همه‌ی APIها (OpenAlex/Semantic Scholar/Crossref و برای شغل:
# Jobicy/Remotive/Arbeitnow/...) صدا زده بشن — هم سرعت بات رو بالا
# می‌بره، هم فشار روی APIهای رایگان (که معمولاً rate limit دارن) رو
# کم می‌کنه. زمان پیش‌فرض ۱ ساعته و با env var قابل‌تنظیمه.
SEARCH_CACHE_TTL_SEC = int(os.getenv("SEARCH_CACHE_TTL_SEC", "3600") or "3600")
JOB_SEARCH_CACHE_TTL_SEC = int(os.getenv("JOB_SEARCH_CACHE_TTL_SEC", "1800") or "1800")

# ── نرمال‌سازی مترادف برای کلید کش ────────────────────────────────
# قبلاً «AI در کانادا» و «Machine Learning Canada» دو کلید کاملاً جدا
# می‌شدن (چون مقایسه exact-string بود) با اینکه عملاً همپوشانی زیادی
# دارن — یعنی کش miss می‌خورد و کل pipeline دوباره از صفر اجرا می‌شد.
# این دیکشنری فقط رایج‌ترین مترادف‌های رشته/کشور رو یکی می‌کنه؛ عمداً
# محافظه‌کارانه و کوچیکه — چیزی که مطمئن نیستیم مترادفه، نرمال نمی‌شه،
# چون کش اشتباه (دو رشته‌ی واقعاً متفاوت که یکی شدن) بدتر از یک miss اضافه‌ست.
_FIELD_SYNONYMS = {
    "ai": "artificial intelligence", "artificial intelligence": "artificial intelligence",
    "ml": "machine learning", "machine learning": "machine learning",
    "dl": "deep learning", "deep learning": "deep learning",
    "nlp": "natural language processing", "natural language processing": "natural language processing",
    "cv": "computer vision", "computer vision": "computer vision",
    "cs": "computer science", "computer science": "computer science",
    "robotics": "robotics", "bioinformatics": "bioinformatics",
}
_COUNTRY_SYNONYMS = {
    "usa": "united states", "us": "united states", "u.s.": "united states",
    "u.s.a.": "united states", "america": "united states", "united states": "united states",
    "uk": "united kingdom", "u.k.": "united kingdom", "britain": "united kingdom",
    "united kingdom": "united kingdom",
    "uae": "united arab emirates", "کانادا": "canada", "canada": "canada",
    "آلمان": "germany", "germany": "germany", "فرانسه": "france", "france": "france",
    "استرالیا": "australia", "australia": "australia",
}

def _normalize_cache_term(term: str, synonyms: dict) -> str:
    t = (term or "").strip().lower()
    return synonyms.get(t, t)

def _search_cache_key(search_type: str, field: str, country: str) -> str:
    norm_field = _normalize_cache_term(field, _FIELD_SYNONYMS)
    norm_country = _normalize_cache_term(country, _COUNTRY_SYNONYMS)
    return f"{search_type}|{norm_field}|{norm_country}"

def db_get_search_cache(search_type: str, field: str, country: str, ttl_sec: int | None = None) -> list[dict] | None:
    key = _search_cache_key(search_type, field, country)
    ttl = ttl_sec if ttl_sec is not None else SEARCH_CACHE_TTL_SEC
    cutoff = (datetime.now() - timedelta(seconds=ttl)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with _db() as c:
            row = c.execute("""SELECT results_json FROM search_results_cache
                WHERE cache_key=? AND created_at >= ?""", (key, cutoff)).fetchone()
        if not row or not row[0]:
            return None
        return json.loads(row[0])
    except Exception as e:
        logger.debug(f"db_get_search_cache({key}): {e}")
        return None

def db_save_search_cache(search_type: str, field: str, country: str, results: list[dict]):
    if not results:
        return  # نتیجه‌ی خالی رو کش نمی‌کنیم — شاید فقط این‌بار موقتاً fail شده
    key = _search_cache_key(search_type, field, country)
    try:
        with _db() as c:
            c.execute("""INSERT OR REPLACE INTO search_results_cache
                (cache_key, search_type, field, country, results_json, result_count, created_at)
                VALUES (?,?,?,?,?,?,?)""",
                (key, search_type, field, country, json.dumps(results), len(results), str(datetime.now())))
    except Exception as e:
        logger.error(f"db_save_search_cache({key}): {e}")

# ── Embassy Inquiry — استعلام سفارت ──────────────────────────────────
# فیچر کاملاً جانبی (side feature): بعد از پرداخت/تایید و همچنین از منوی
# اصلی، کاربر می‌تونه بپرسه سفارت مقصدش (که براش اپلای می‌کنه) الان چه
# اطلاعات/شرایطی برای تحصیلی/کاری/سرمایه‌گذاری داره. طراحی روی سه اصل:
#   ۱. دقت: فقط از سایت رسمی خودِ سفارت/مرجع مهاجرتی اسکرِیپ می‌شه —
#      نه جستجوی آزاد وب و نه حدس AI — و اگه کشوری هنوز توی
#      EMBASSY_OFFICIAL_SITES تعریف نشده، صادقانه می‌گیم «هنوز نداریم»
#      به‌جای دادن اطلاعات نامطمئن (چون این حوزه پیامد واقعی داره).
#   ۲. مقاوم بودن: هر مرحله (اسکرِیپ، خلاصه‌سازی AI) در try/except جدا،
#      با retry و timeout؛ شکست کامل یعنی پیام «فعلاً در دسترس نیست»
#      نه کرش یا هنگ کل بات.
#   ۳. ایزوله بودن: state/کلیدهای مخصوص خودش (پیشوند embassy_ / _embassy)،
#      جدول DB مجزا، هیچ جایی از سرویس ایمیل/شغل رو فراخوانی نمی‌کنه —
#      یعنی باگ اینجا هیچ‌وقت نمی‌تونه ارسال ایمیل/جستجوی شغل رو بخوابونه.

# نکته: این دیکشنری عمداً کوچیک شروع می‌شه — فقط کشورهایی که واقعاً منبع
# رسمی‌شون تایید شده. هر کشور جدید باید دستی و بعد از چک واقعی اضافه بشه؛
# حدس زدن آدرس سفارت (که گاهی اصلاً در ایران فعال نیست، مثل کانادا که
# باید IRCC رسمی چک بشه نه یک سفارت بسته) ریسک اطلاعات غلط داره.
EMBASSY_OFFICIAL_SITES: dict[str, dict] = {
    "germany": {
        "label_fa": "🇩🇪 آلمان",
        "base_url": "https://teheran.diplo.de/ir-fa",
        "note": "سفارت آلمان در تهران — منبع رسمی صدور ویزا",
    },
    "canada": {
        "label_fa": "🇨🇦 کانادا",
        "base_url": "https://www.canada.ca/en/immigration-refugees-citizenship.html",
        "note": "سفارت کانادا در تهران فعال نیست — منبع رسمی، اداره مهاجرت کانادا (IRCC) است",
    },
    "usa": {
        "label_fa": "🇺🇸 آمریکا",
        # فیکس: آدرس قبلی (/visas/) دیگه صفحه‌ی فعلی سایت نیست — آدرس
        # درست و فعلاً پابرجای صفحه‌ی ویزا روی سایت رسمی همین Virtual
        # Embassy Iran، /visas-2/ است (بررسی/تایید شد).
        "base_url": "https://ir.usembassy.gov/visas-2/",
        "note": "سفارت آمریکا در تهران فعال نیست — منبع رسمی، وزارت امور خارجه آمریکا "
                "(travel.state.gov) است؛ صفحه‌ی بخش منافع آمریکا هم برای مسیر کنسولی می‌آید.",
    },
    "france": {
        "label_fa": "🇫🇷 فرانسه",
        "base_url": "https://ir.ambafrance.org/-فارسی-",
        "note": "سفارت فرانسه در تهران فعال است — منبع رسمی صدور ویزا",
    },
    "australia": {
        "label_fa": "🇦🇺 استرالیا",
        "base_url": "https://immi.homeaffairs.gov.au/visas",
        "note": "سفارت استرالیا در تهران فعال نیست — منبع رسمی، اداره مهاجرت استرالیا "
                "(Department of Home Affairs) است",
    },
}
EMBASSY_INQUIRY_KEYS = {
    "general":    "🌐 استعلام کلی برای اتباع ایران",
    "education":  "🎓 اطلاعات تحصیلی",
    "work":       "💼 اطلاعات کاری",
    "investment": "💰 اطلاعات سرمایه‌گذاری",
}
EMBASSY_CACHE_TTL_SEC = int(os.getenv("EMBASSY_CACHE_TTL_SEC", str(48 * 3600)) or str(48 * 3600))
# ── امتیاز اطمینان منبع (Source Confidence) ──────────────────────────
# هر منبعی که استعلام سفارت ازش استفاده می‌کنه یک عدد اطمینان ثابت داره،
# فقط برای شفافیت به کاربر (نه برای فیلتر کردن جواب AI — چون این بات فقط
# دو نوع منبع داره و هر دو همیشه نمایش داده می‌شن، صرفاً با برچسب درست).
# اگه در آینده منبع سومی (مثلاً وبلاگ/فروم) اضافه شد، همین‌جا با عدد
# پایین‌تر تعریف می‌شه تا AI/پیام به کاربر صادقانه فرق بذاره.
SOURCE_CONFIDENCE = {
    "official":   99,  # مستقیم از سایت رسمی سفارت/مرجع مهاجرتی
    "unofficial": 55,  # جمع‌شده از چند نتیجه‌ی جستجوی عمومی وب
}
EMBASSY_CATEGORY_KEYWORDS = {
    "general":    ["visa", "consular", "appointment", "کنسولی", "ویزا"],
    "education":  ["study", "student", "university", "تحصیل", "دانشجو"],
    "work":       ["work", "employment", "job", "کار", "اشتغال"],
    "investment": ["invest", "business", "startup", "سرمایه", "سرمایه‌گذاری"],
}

def _embassy_cache_key(embassy_country: str, category: str, nationality: str) -> str:
    return f"{embassy_country}|{category}|{(nationality or '').strip().lower()}"

def db_get_embassy_cache(embassy_country: str, category: str, nationality: str,
                          ttl_sec: int | None = None) -> dict | None:
    key = _embassy_cache_key(embassy_country, category, nationality)
    ttl = ttl_sec if ttl_sec is not None else EMBASSY_CACHE_TTL_SEC
    cutoff = (datetime.now() - timedelta(seconds=ttl)).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with _db() as c:
            row = c.execute("""SELECT result_json, source_url, created_at FROM embassy_cache
                WHERE cache_key=? AND created_at >= ?""", (key, cutoff)).fetchone()
        if not row or not row[0]:
            return None
        data = json.loads(row[0])
        data["source_url"] = row[1]
        # به کاربر واضح نشون بدیم این نتیجه از کش اومده، نه بررسی تازه —
        # شفافیت وضعیت استعلام (نه صرفاً «یک پاسخ داخلی» بدون توضیح).
        data["from_cache"] = True
        data["cached_at"] = row[2]
        return data
    except Exception as e:
        logger.debug(f"db_get_embassy_cache({key}): {e}")
        return None

def db_save_embassy_cache(embassy_country: str, category: str, nationality: str,
                           source_url: str, result: dict):
    if not result:
        return
    key = _embassy_cache_key(embassy_country, category, nationality)
    try:
        with _db() as c:
            c.execute("""INSERT OR REPLACE INTO embassy_cache
                (cache_key, embassy_country, category, source_url, result_json, created_at)
                VALUES (?,?,?,?,?,?)""",
                (key, embassy_country, category, source_url, json.dumps(result), str(datetime.now())))
    except Exception as e:
        logger.error(f"db_save_embassy_cache({key}): {e}")

def _scrape_embassy_official(embassy_country: str, category: str) -> tuple[str, str]:
    """فقط از سایت رسمی خودِ سفارت/مرجع (EMBASSY_OFFICIAL_SITES) اسکرِیپ
    می‌کنه — هیچ جستجوی وب آزادی در کار نیست. صفحه‌ی اصلی + حداکثر ۳
    لینک داخلی که با کلیدواژه‌های دسته‌بندی (مثلاً visa/study برای
    education) match بشن رو می‌گیره تا هم صفحه‌ی کلی هم صفحه‌ی
    اختصاصی‌تر دسته پوشش داده بشه. برمی‌گردونه: (متن ترکیبی، URL اصلی).
    اگه کشور تعریف نشده باشه یا هر خطایی بیفته، متن خالی برمی‌گردونه —
    caller این حالت رو به «هنوز نداریم» ترجمه می‌کنه، نه خطای فنی."""
    site = EMBASSY_OFFICIAL_SITES.get(embassy_country)
    if not site:
        return "", ""
    base_url = site["base_url"]
    combined = scrape(base_url)
    if combined.startswith("[error:"):
        return "", base_url
    try:
        r = _safe_requests_get(base_url, headers=HEADERS, timeout=12)
        from bs4 import BeautifulSoup
        from urllib.parse import urljoin, urlparse
        soup = BeautifulSoup(r.text, "html.parser")
        keywords = EMBASSY_CATEGORY_KEYWORDS.get(category, [])
        links_checked = 0
        for a in soup.find_all("a", href=True):
            if links_checked >= 3:
                break
            href = a["href"]
            link_text = (a.get_text() or "").lower()
            if not any(kw.lower() in href.lower() or kw.lower() in link_text for kw in keywords):
                continue
            full_url = urljoin(base_url, href)
            # فقط همون دامنه‌ی رسمی — هرگز به سایت شخص ثالث نمی‌ره
            if urlparse(full_url).netloc != urlparse(base_url).netloc:
                continue
            sub_text = scrape(full_url)
            if not sub_text.startswith("[error:"):
                combined += "\n\n" + sub_text
                links_checked += 1
    except Exception as e:
        logger.debug(f"_scrape_embassy_official link discovery ({embassy_country}/{category}): {e}")
    return combined[:6000], base_url

async def _summarize_embassy_info(embassy_country: str, category: str, nationality: str,
                                   second_passport: str, raw_text: str,
                                   source_type: str = "official") -> dict | None:
    """متن خام اسکرِیپ‌شده (یا در حالت غیررسمی: متن جمع‌آوری‌شده از چند
    نتیجه‌ی جستجوی عمومی) رو به یک خلاصه‌ی ساخت‌یافته تبدیل می‌کنه — از
    همون call_ai اصلی فایل استفاده می‌کنه (Gemini→Groq fallback، خودش
    timeout و retry مدل‌ها رو داره). اگه AI جواب معتبر ندونه یا JSON
    پارس نشه، None برمی‌گردونه (نه fallback نامطمئن) — یعنی به‌جای
    نمایش چیزی که ممکنه غلط باشه، پیام «موقتاً در دسترس نیست» می‌آد.
    source_type فقط توی prompt به AI می‌گه با چه احتیاطی خلاصه کنه؛
    خودِ تگ نهایی (official/unofficial) توی run_embassy_inquiry ست
    می‌شه، نه اینجا — تا منبع خلاصه هیچ‌وقت با تشخیص AI عوض نشه."""
    if not raw_text.strip():
        return None
    cat_label = EMBASSY_INQUIRY_KEYS.get(category, category)
    passport_line = f"پاسپورت دوم: {second_passport}" if second_passport else "پاسپورت دوم: ندارد"
    if source_type == "official":
        source_desc = f"متن زیر از سایت رسمی سفارت/مرجع مهاجرتی کشور {embassy_country} گرفته شده."
        caution = ""
    else:
        source_desc = (f"متن زیر از چند نتیجه‌ی جستجوی عمومی وب درباره‌ی مهاجرت/ویزای کشور "
                        f"{embassy_country} جمع‌آوری شده — نه لزوماً از سایت رسمی سفارت/مرجع "
                        f"(چون سایت رسمی برای این دسته/کشور در دسترس نبود).")
        caution = ("\nچون منبع رسمی نیست، محتاط‌تر خلاصه کن: فقط نکاتی رو بیار که توی متن صریح "
                   "اومده، و از قطعیت زیاد پرهیز کن.")
    prompt = f"""{source_desc}
ملیت متقاضی: {nationality} — {passport_line}
دسته‌ی درخواستی: {cat_label}

متن:
{raw_text[:5000]}

فقط بر اساس همین متن (نه دانش عمومی خودت)، یک خلاصه‌ی کاربردی فارسی بساز.{caution}
اگر متن اطلاعات مرتبطی نداشت، match را false بگذار.
فقط JSON خالص، دقیقاً همین ساختار:
{{"match": true/false,
  "summary": "خلاصه‌ی کوتاه و دقیق فارسی، فقط بر اساس متن بالا (حداکثر ۶ خط)",
  "key_points": ["نکته‌ی عملی ۱", "نکته‌ی عملی ۲", "..."] (حداکثر ۵ مورد)}}"""
    try:
        ai = await call_ai(prompt, min_length=10)
        data = _extract_json_object(ai)
        if not data:
            return None
        if not data.get("match"):
            return None
        return {
            "summary": str(data.get("summary", ""))[:800],
            "key_points": [str(p)[:150] for p in (data.get("key_points") or [])][:5],
        }
    except Exception as e:
        logger.warning(f"_summarize_embassy_info({embassy_country}/{category}): {e}")
        return None

# ── جستجوی غیررسمی (fallback) ────────────────────────────────────────
# وقتی ۳ تلاش روی سایت رسمی (EMBASSY_OFFICIAL_SITES) هیچ نتیجه‌ای نداد —
# نه اینکه کشور تعریف نشده باشه، بلکه سایت رسمی موقتاً در دسترس نبود یا
# صفحه‌ی مرتبط با اون دسته پیدا نشد — به‌جای «هیچی نداریم»، از همون
# زیرساخت جستجوی موازی که برای پیدا کردن اساتید/شرکت‌ها استفاده می‌شه
# (DDG اول، Google به‌عنوان backup دوم) چند منبع عمومی رو جمع می‌کنیم.
# نتیجه‌ی این تابع همیشه با تگ «غیررسمی» به کاربر نشون داده می‌شه —
# هیچ‌وقت جای منبع رسمی رو نمی‌گیره، فقط وقتی سایت رسمی جواب نداد
# به‌عنوان یک راهنمای کمکیِ صادقانه‌برچسب‌خورده اضافه می‌شه.
def _search_embassy_unofficial(embassy_country: str, category: str, nationality: str) -> tuple[str, str]:
    """چند نتیجه‌ی عمومی وب رو برای دسته/کشور مشخص جستجو و اسکرِیپ می‌کنه.
    برمی‌گردونه: (متن ترکیبی، لیست URLها با کاما جدا شده). اگه هیچ نتیجه‌ای
    پیدا نشه، رشته‌های خالی برمی‌گردونه — caller این حالت رو مثل شکست
    اسکرِیپ رسمی مدیریت می‌کنه، نه خطای فنی."""
    site = EMBASSY_OFFICIAL_SITES.get(embassy_country, {})
    country_label = re.sub(r"[^\w\s]", "", site.get("label_fa", embassy_country)).strip() or embassy_country
    cat_label = EMBASSY_CATEGORY_KEYWORDS.get(category, [category])[0]
    query = f"{country_label} visa {cat_label} for Iranian citizens {nationality or ''}".strip()

    urls: list[str] = []
    if not _ddg_breaker_is_open():
        try:
            from duckduckgo_search import DDGS
            with DDGS(timeout=SEARCH_TIMEOUT_SEC) as ddgs:
                results = list(ddgs.text(query, max_results=4))
            _ddg_breaker_record(success=True)
            urls = [r.get("href", "") for r in results if r.get("href")]
        except Exception as e:
            _ddg_breaker_record(success=False, hard_error=_is_hard_network_error(e))
            logger.debug(f"embassy unofficial ddg '{query[:60]}': {e}")

    if not urls:
        # DDG یا شکست خورده یا breaker باز بوده — گوگل مستقیم به‌عنوان دومین تلاش
        try:
            resp = requests.get("https://www.google.com/search",
                params={"q": query, "num": 4},
                headers={**HEADERS, "Accept-Language": "en-US,en;q=0.9"},
                timeout=SEARCH_TIMEOUT_SEC)
            urls = re.findall(r'href="(https?://(?!(?:www\.)?google)[^"&]+)"', resp.text)[:4]
        except Exception as e:
            logger.debug(f"embassy unofficial google '{query[:60]}': {e}")
            return "", ""

    combined_parts, used_urls = [], []
    for url in urls[:4]:
        try:
            page_text = scrape(url)
            if page_text and not page_text.startswith("[error:"):
                combined_parts.append(page_text)
                used_urls.append(url)
        except Exception as e:
            logger.debug(f"embassy unofficial scrape '{url[:60]}': {e}")
        if len(combined_parts) >= 3:
            break
    if not combined_parts:
        return "", ""
    return "\n\n".join(combined_parts)[:6000], ", ".join(used_urls)

async def run_embassy_inquiry(embassy_country: str, category: str, nationality: str,
                               second_passport: str | None) -> dict | None:
    """نقطه‌ی ورود عمومی — هرگز exception بالا نمی‌بره. کش (۴۸ ساعت) →
    تا ۵ تلاش کلی:
      • تلاش ۱، ۲، ۳: فقط سایت رسمی خودِ سفارت/مرجع (EMBASSY_OFFICIAL_SITES).
      • تلاش ۴، ۵ (فقط اگه ۳ تلاش رسمی هیچی نداد): جستجوی عمومی وب
        (غیررسمی) — نتیجه با source_type="unofficial" برمی‌گرده تا
        همه‌جا (پیام به کاربر، کش) واضح تگ بخوره.
    شکست کامل (هر ۵ تلاش) یعنی None، نه کرش یا هنگ."""
    if embassy_country not in EMBASSY_OFFICIAL_SITES:
        return None
    try:
        cached = await asyncio.to_thread(
            db_get_embassy_cache, embassy_country, category, nationality)
        if cached is not None:
            return cached
    except Exception as e:
        logger.debug(f"run_embassy_inquiry cache read: {e}")

    OFFICIAL_ATTEMPTS = 3
    UNOFFICIAL_ATTEMPTS = 2

    for attempt in range(OFFICIAL_ATTEMPTS):
        try:
            raw_text, source_url = await asyncio.wait_for(
                asyncio.to_thread(_scrape_embassy_official, embassy_country, category),
                timeout=25)
            if not raw_text:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            summary = await _summarize_embassy_info(
                embassy_country, category, nationality, second_passport or "", raw_text,
                source_type="official")
            if summary:
                summary["source_url"] = source_url
                summary["source_type"] = "official"
                await asyncio.to_thread(db_save_embassy_cache, embassy_country, category,
                                         nationality, source_url, summary)
                return summary
        except Exception as e:
            logger.warning(f"run_embassy_inquiry official attempt {attempt+1} "
                            f"({embassy_country}/{category}): {e}")
        await asyncio.sleep(1.5 * (attempt + 1))

    # ── هر ۳ تلاش رسمی بی‌نتیجه بود — حالا نوبت جستجوی غیررسمی ─────────
    for attempt in range(UNOFFICIAL_ATTEMPTS):
        try:
            raw_text, source_urls = await asyncio.wait_for(
                asyncio.to_thread(_search_embassy_unofficial, embassy_country, category, nationality),
                timeout=25)
            if not raw_text:
                await asyncio.sleep(1.5 * (attempt + 1))
                continue
            summary = await _summarize_embassy_info(
                embassy_country, category, nationality, second_passport or "", raw_text,
                source_type="unofficial")
            if summary:
                summary["source_url"] = source_urls
                summary["source_type"] = "unofficial"
                # کش غیررسمی رو هم ذخیره می‌کنیم (همون TTL ۴۸ ساعته) تا هر
                # بار کاربر بعدی مجبور نشه دوباره ۵ تلاش کامل رو طی کنه؛
                # چون source_type داخل خودِ result ذخیره‌ست، بار بعد که از
                # کش خونده بشه هم تگ غیررسمی درست نشون داده می‌شه.
                await asyncio.to_thread(db_save_embassy_cache, embassy_country, category,
                                         nationality, source_urls, summary)
                return summary
        except Exception as e:
            logger.warning(f"run_embassy_inquiry unofficial attempt {attempt+1} "
                            f"({embassy_country}/{category}): {e}")
        await asyncio.sleep(1.5 * (attempt + 1))
    return None

# ── Search Metrics: صرفاً برای دیدن روند در طول زمان (کدوم منبع چقدر
# نتیجه می‌ده، جستجوها چقدر طول می‌کشن، fallback چقدر فعال می‌شه) —
# کاملاً جدا از مسیر اصلی جستجو/ارسال ایمیل/دکمه‌های بات. عمداً:
#   ۱. INSERT-only (هیچ SELECT/UPDATE‌ای روی جدول‌های دیگه نمی‌زنه)
#   ۲. کل تابع تو try/except — یعنی حتی اگه دیتابیس قفل باشه یا جدول
#      به هر دلیلی نباشه، این تابع فقط لاگ می‌کنه و None برمی‌گردونه؛
#      هیچ‌وقت exception بالا نمی‌ره که بخواد جریان جستجو/بات رو بشکنه.
#   ۳. صدا زدنش هم توی caller داخل try/except جداگانه‌ست (defense in
#      depth) — یعنی حتی اگه همین تابع رو یکی دستکاری کنه و try/except
#      داخلیش رو حذف کنه، caller بازم safe می‌مونه.
def db_log_search_metrics(field: str, country: str, count_requested: int,
                           count_returned: int, elapsed_sec: float,
                           fallback_used: bool, source_counts: dict):
    try:
        with _db() as c:
            c.execute("""INSERT INTO search_metrics
                (field, country, count_requested, count_returned, elapsed_sec,
                 fallback_used, source_counts_json, created_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (field[:200], country[:100], count_requested, count_returned,
                 round(elapsed_sec, 2), 1 if fallback_used else 0,
                 json.dumps(source_counts), str(datetime.now())))
    except Exception as e:
        logger.debug(f"db_log_search_metrics: {e}")  # هیچ‌وقت raise نمی‌کنه

# ── مرحله‌ی «تحلیل» (جدا از search_metrics که فقط مرحله‌ی «کشف» رو
# اندازه می‌گیره) — دقیقاً همون شمارش funnel که سر هر gate (email/name/
# duplicate/invalid_email/unverified_faculty/unverified_signals/
# no_match) چند استاد رد شدن، به‌علاوه چندتا نهایتاً passed. کاملاً
# جانبی — هیچ‌وقت روی ارسال واقعی اثر نداره، فقط بعد از تمومِ batch
# یک INSERT سریع. ──
def db_log_analysis_funnel(tid: int, sub_id, field: str, total: int,
                            funnel: dict, passed_final: int):
    try:
        with _db() as c:
            c.execute("""INSERT INTO analysis_funnel_metrics
                (telegram_id, sub_id, field, total, funnel_json, passed_final, created_at)
                VALUES (?,?,?,?,?,?,?)""",
                (tid, sub_id, (field or "")[:200], total,
                 json.dumps(funnel), passed_final, str(datetime.now())))
    except Exception as e:
        logger.debug(f"db_log_analysis_funnel: {e}")  # هیچ‌وقت raise نمی‌کنه

def db_get_analysis_funnel_summary(limit: int = 20) -> dict:
    """جمع تجمعی funnel روی آخرین `limit` batch — برای اینکه بشه دید
    به‌طور میانگین بیشترین ریزش سر کدوم gate اتفاق می‌افته، نه فقط یک
    batch تکی. برمی‌گردونه: {"batches": N, "total": N, "gates": {gate: count}, "passed_final": N}"""
    out = {"batches": 0, "total": 0, "gates": {}, "passed_final": 0}
    try:
        with _db() as c:
            rows = c.execute("""SELECT total, funnel_json, passed_final FROM analysis_funnel_metrics
                                 ORDER BY id DESC LIMIT ?""", (limit,)).fetchall()
        for total, funnel_json, passed_final in rows:
            out["batches"] += 1
            out["total"] += total or 0
            out["passed_final"] += passed_final or 0
            try:
                f = json.loads(funnel_json or "{}")
            except Exception:
                f = {}
            for gate, cnt in f.items():
                out["gates"][gate] = out["gates"].get(gate, 0) + cnt
        return out
    except Exception as e:
        logger.debug(f"db_get_analysis_funnel_summary: {e}")
        return out

# ── جستجوی خودکار هر ۲۴ ساعت (وقتی نتیجه‌ی اولیه صفر بود) ────────────
def db_add_pending_search(tid: int, search_type: str, field: str, country: str) -> int:
    with _db() as c:
        cur = c.execute("""INSERT INTO pending_searches
            (telegram_id,search_type,field,country,active,attempts,last_tried_at,created_at)
            VALUES (?,?,?,?,1,0,?,?)""",
            (tid, search_type, field, country, str(datetime.now()), str(datetime.now())))
        return cur.lastrowid

def db_get_due_pending_searches(hours=24):
    """جستجوهایی که فعالن و حداقل ۲۴ ساعت از آخرین تلاششون گذشته."""
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    with _db() as c:
        rows = c.execute("""SELECT id, telegram_id, search_type, field, country, attempts
            FROM pending_searches WHERE active=1 AND last_tried_at <= ?""", (cutoff,)).fetchall()
    return [{"id": r[0], "telegram_id": r[1], "search_type": r[2],
             "field": r[3], "country": r[4], "attempts": r[5]} for r in rows]

def db_mark_pending_search_tried(search_id: int):
    with _db() as c:
        c.execute("""UPDATE pending_searches SET attempts=attempts+1, last_tried_at=?
            WHERE id=?""", (str(datetime.now()), search_id))

def db_deactivate_pending_search(search_id: int):
    with _db() as c:
        c.execute("UPDATE pending_searches SET active=0 WHERE id=?", (search_id,))

def db_get_sent_email_set(tid):
    """مجموعه‌ی همه‌ی ایمیل‌هایی که قبلاً واقعاً برای این کاربر با موفقیت
    ارسال شده (نه فقط ۱۵ تای آخر) — برای این‌که اگر بات وسط حلقه‌ی ارسال
    ری‌استارت شد، به همون اساتیدی که قبلاً واقعاً ایمیل گرفتن دوباره ایمیل
    نره."""
    with _db() as c:
        rows = c.execute("""SELECT DISTINCT professor_email FROM sent_emails
            WHERE telegram_id=? AND status='sent'""", (tid,)).fetchall()
    return {r[0] for r in rows if r[0]}

# ── Queue/Worker واقعی برای ارسال ایمیل ──────────────────────────────
# قبلاً وضعیت هر استاد فقط توی حافظه‌ی پروسه (لیست profs + متغیرهای
# sent_ok/skipped) وجود داشت — اگه بات وسط ارسال ۵۰۰ ایمیل (به‌خاطر
# SMTP timeout، قطع اینترنت، یا ری‌استارت سرور) می‌مرد، هیچ رکورد دقیقی
# از این‌که کدوم استاد دقیقاً در چه وضعیتی بود وجود نداشت. الان هر
# استاد یک ردیف در email_jobs داره که وضعیتش (PENDING → SENDING →
# SENT/FAILED/SKIPPED) همیشه توی دیتابیسه، نه فقط توی RAM.
def db_enqueue_email_jobs(tid, sid, profs: list[dict], total: int):
    """قبل از شروع واقعی ارسال، همه‌ی استادهای این batch رو به‌عنوان
    PENDING ثبت می‌کنه. INSERT OR IGNORE یعنی اگه این تابع دوباره صدا
    زده بشه (مثلاً چون بات ری‌استارت شده و همون batch از اول فراخوانی
    شده)، رکوردهای موجود (و وضعیت SENT/FAILED قبلی‌شون) دست‌نخورده
    می‌مونن — یعنی resume واقعاً idempotent هست."""
    if sid is None or not profs:
        return
    # فقط فیلدهای هویتی لازم برای دوباره صدا زدن pipeline ایمیل‌یابی
    # (find_professor_email_and_signals / get_professor_relevance) رو
    # نگه می‌داریم — نه کل دیکشنری prof (که ممکنه لیست مقالات/متن
    # صفحه‌ی آزمایشگاه و... داشته باشه و بی‌خودی دیتابیس رو سنگین کنه).
    # این‌ها دقیقاً همون چیزیه که بعداً برای «نجات استادهای رد‌شده به‌خاطر
    # سقف زمانی batch» لازمه.
    _PROF_JSON_KEYS = ("name", "email", "university", "country", "openalex_id",
                        "url", "department", "title", "_manual_entry")
    rows = []
    for idx, p in enumerate(profs[:total]):
        snapshot = {k: p[k] for k in _PROF_JSON_KEYS if k in p}
        try:
            prof_json = json.dumps(snapshot, ensure_ascii=False)[:4000]
        except Exception:
            prof_json = ""
        rows.append((tid, sid, idx, (p.get("name","") or "")[:120], (p.get("email","") or "")[:200],
                     prof_json, str(datetime.now()), str(datetime.now())))
    if not rows:
        return
    try:
        with _db() as c:
            c.executemany("""INSERT OR IGNORE INTO email_jobs
                (telegram_id, sub_id, batch_idx, professor_name, professor_email,
                 prof_json, status, created_at, updated_at)
                VALUES (?,?,?,?,?,?, 'PENDING', ?, ?)""", rows)
    except Exception as e:
        logger.error(f"db_enqueue_email_jobs: {e}")

def db_get_job(tid, sid, batch_idx):
    if sid is None:
        return None
    with _db() as c:
        row = c.execute("""SELECT id, status FROM email_jobs
            WHERE telegram_id=? AND sub_id=? AND batch_idx=?""", (tid, sid, batch_idx)).fetchone()
    return {"id": row[0], "status": row[1]} if row else None

def db_mark_job_sending(job_id):
    if not job_id:
        return
    with _db() as c:
        c.execute("""UPDATE email_jobs SET status='SENDING', attempts=attempts+1, updated_at=?
            WHERE id=?""", (str(datetime.now()), job_id))

def db_mark_job_result(job_id, status: str, professor_email: str = "", error: str = ""):
    """status باید یکی از 'SENT' / 'FAILED' / 'SKIPPED' باشه."""
    if not job_id:
        return
    with _db() as c:
        if status == "SENT":
            c.execute("""UPDATE email_jobs SET status=?, professor_email=?, last_error='',
                sent_at=?, updated_at=? WHERE id=?""",
                (status, professor_email, str(datetime.now()), str(datetime.now()), job_id))
        else:
            c.execute("""UPDATE email_jobs SET status=?, professor_email=?, last_error=?, updated_at=?
                WHERE id=?""", (status, professor_email, (error or "")[:300], str(datetime.now()), job_id))

def db_reconcile_stale_sending_jobs() -> int:
    """فقط از _on_bot_startup صدا زده می‌شه، یک‌بار در همون لحظه‌ی بالا اومدن.

    مشکل واقعی: ترتیب فعلی هر ارسال اینه: SMTP send → db_mark_job_sending قبلش
    اجرا شده → (SMTP موفق) → db_log_email → db_mark_job_result('SENT') → افزایش
    quota. اگه دقیقاً بین «SMTP موفق شد» و «db_mark_job_result('SENT') نوشته شد»
    کل پردازش crash کنه (قطعی برق، OOM-kill، ری‌استارت هاست)، اون job برای همیشه
    روی 'SENDING' می‌مونه — و چون این وضعیت با 'PENDING' یکسان رفتار می‌شد (پایین‌تر
    توی حلقه‌ی ارسال، هرچی «status != SENT» بود دوباره امتحان می‌شد)، دفعه‌ی بعد که
    همین batch دوباره تریگر بشه، این job (که شاید واقعاً ایمیلش رفته بود) دوباره
    فرستاده می‌شد — یعنی ریسک واقعی duplicate email، دقیقاً همون چیزی که کاربر
    گزارش داد.

    فیکس: تنها راهی که یک job می‌تونه با status='SENDING' به این نقطه (استارتاپ
    یک process تازه) برسه اینه که process قبلی وسط همون ارسال مرده باشه — یعنی
    واقعاً معلوم نیست SMTP موفق شده بود یا نه. به‌جای این‌که این ابهام رو نادیده
    بگیریم و ساده دوباره ارسال کنیم، به یک وضعیت سوم صریح می‌بریمش: 'UNKNOWN'.
    پایین‌تر (db_get_job در حلقه‌ی ارسال) با UNKNOWN دقیقاً مثل SENT رفتار می‌کنه:
    خودکار دوباره فرستاده نمی‌شه — چون خطر یک ایمیل تکراری به یک استاد (اعتبار
    کاربر پیش اون استاد) بدتر از یک ایمیل معلق و گزارش‌شده به ادمینه. ادمین توی
    پیام استارتاپ این تعداد رو جدا از pending واقعی می‌بینه و می‌تونه دستی تصمیم
    بگیره (چک صندوق Sent کاربر، یا با خیال راحت دوباره enqueue کردن همون استاد)."""
    with _db() as c:
        cur = c.execute("""UPDATE email_jobs SET status='UNKNOWN', updated_at=?
            WHERE status='SENDING'""", (str(datetime.now()),))
        return cur.rowcount

def db_get_time_budget_skipped_jobs(limit: int = 300):
    """استادهایی که رد شدن نه چون منبعی نداشتیم، فقط چون سقف زمانی کل
    batch (MAX_SCAN_SECONDS) تموم شده بود. این‌ها برای rescue job جدا
    (خارج از فشار زمانی batch اصلی) برمی‌گردیم و pipeline ۱۱مرحله‌ای رو
    دوباره براشون اجرا می‌کنیم. فقط جاب‌هایی که prof_json دارن قابل
    rescue-ان (جاب‌های قدیمی‌تر از این migration این ستون رو ندارن —
    بی‌سروصدا نادیده گرفته می‌شن، نه اینکه crash کنن)."""
    with _db() as c:
        rows = c.execute("""SELECT id, telegram_id, sub_id, batch_idx, prof_json
            FROM email_jobs
            WHERE status='SKIPPED' AND last_error='batch_time_budget_exceeded'
              AND prof_json IS NOT NULL AND prof_json != ''
            ORDER BY updated_at ASC LIMIT ?""", (limit,)).fetchall()
    out = []
    for job_id, tid, sid, batch_idx, prof_json in rows:
        try:
            prof = json.loads(prof_json)
        except Exception:
            continue
        if not prof.get("name"):
            continue
        out.append({"job_id": job_id, "telegram_id": tid, "sub_id": sid,
                    "batch_idx": batch_idx, "prof": prof})
    return out

def db_get_incomplete_batches():
    """batchهایی که هنوز job با وضعیت PENDING، SENDING یا UNKNOWN دارن (یعنی
    وسط کار موندن — مثلاً به‌خاطر ری‌استارت سرور) — برای هشدار به ادمین
    بعد از بالا اومدن دوباره‌ی بات. نکته: به‌خاطر db_reconcile_stale_sending_jobs
    (که همیشه قبل از این تابع، توی _on_bot_startup صدا زده می‌شه)، عملاً
    sending همیشه ۰ می‌مونه و هر چیزی که واقعاً وسط SMTP مونده بود الان زیر
    unknown می‌افته — ولی شرط SENDING رو هم دفاعی نگه می‌داریم، برای زمانی
    که این تابع (مثلاً برای دیباگ دستی) بدون ری‌استارت واقعی صدا زده بشه."""
    with _db() as c:
        rows = c.execute("""SELECT telegram_id, sub_id,
                SUM(CASE WHEN status='PENDING' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN status='SENDING' THEN 1 ELSE 0 END) AS sending,
                SUM(CASE WHEN status='UNKNOWN' THEN 1 ELSE 0 END) AS unknown,
                SUM(CASE WHEN status='SENT'    THEN 1 ELSE 0 END) AS sent,
                SUM(CASE WHEN status='FAILED'  THEN 1 ELSE 0 END) AS failed
            FROM email_jobs GROUP BY telegram_id, sub_id
            HAVING pending > 0 OR sending > 0 OR unknown > 0""").fetchall()
    return [{"telegram_id": r[0], "sub_id": r[1], "pending": r[2], "sending": r[3],
             "unknown": r[4], "sent": r[5], "failed": r[6]} for r in rows]

def db_get_recent_emails(tid, limit=15):
    with _db() as c:
        rows = c.execute("""SELECT professor_name,professor_email,subject,status,sent_at
            FROM sent_emails WHERE telegram_id=? AND status='sent'
            ORDER BY id DESC LIMIT ?""", (tid, limit)).fetchall()
    return [{"name": r[0], "email": r[1], "subject": r[2], "status": r[3], "sent_at": r[4]} for r in rows]

def db_set_reply_status(tid, email_id, status: str, note: str = ""):
    """وضعیت پاسخ یک استاد رو ثبت می‌کنه: 'positive' / 'negative' / 'none'
    (بدون پاسخ). AND telegram_id=? هم توی WHERE هست تا یک کاربر نتونه
    وضعیت رکورد کاربر دیگه رو دستکاری کنه (حتی با callback_data دستکاری‌شده)."""
    if status not in ("positive", "negative", "none"):
        return
    with _db() as c:
        c.execute("""UPDATE sent_emails SET reply_status=?, reply_note=?
            WHERE id=? AND telegram_id=?""", (status, note, email_id, tid))

def db_get_emails_pending_reply_check(tid, limit=6):
    """آخرین ایمیل‌های واقعاً ارسال‌شده‌ای که هنوز کاربر مشخص نکرده جواب
    گرفته یا نه — برای منوی «ثبت پاسخ اساتید»."""
    with _db() as c:
        rows = c.execute("""SELECT id, professor_name, professor_email, sent_at
            FROM sent_emails
            WHERE telegram_id=? AND status='sent'
              AND (reply_status IS NULL OR reply_status='unknown')
            ORDER BY id DESC LIMIT ?""", (tid, limit)).fetchall()
    return [{"id": r[0], "name": r[1], "email": r[2], "sent_at": r[3]} for r in rows]

def db_get_reply_stats(tid) -> dict:
    with _db() as c:
        rows = c.execute("""SELECT COALESCE(reply_status,'unknown'), COUNT(*)
            FROM sent_emails WHERE telegram_id=? AND status='sent'
            GROUP BY COALESCE(reply_status,'unknown')""", (tid,)).fetchall()
    return {r[0]: r[1] for r in rows}

def db_get_applied_job_keys(tid):
    """مشابه db_get_sent_email_set ولی برای اپلای کار — تا اگر بات وسط
    اپلای ری‌استارت شد، همون فرصت‌های قبلی دوباره اپلای نشن.
    خروجی دو مجموعه است: urlها (شناسه‌ی دقیق‌تر هر آگهی، چون دو آگهی
    متفاوت می‌تونن title+company یکسان داشته باشن) و (title,company) به
    عنوان fallback برای آگهی‌های قدیمی‌تری که url ثبت نشده."""
    with _db() as c:
        rows = c.execute("""SELECT job_title, company, job_url FROM applied_jobs
            WHERE telegram_id=? AND status IN ('sent','link_provided')""", (tid,)).fetchall()
    urls  = {r[2] for r in rows if r[2]}
    pairs = {(r[0] or "", r[1] or "") for r in rows}
    return urls, pairs

# ================================================================
# مانیتورینگ مداوم (Active Monitors)
# ================================================================
# فرق این با pending_searches: pending_searches فقط برای «جستجوی اول ۰
# نتیجه داد، دوباره امتحان کن تا یه چیزی پیدا بشه» است — همین که یک بار
# نتیجه پیدا شد، خاموش می‌شه. اینجا برعکسه: حتی اگه جستجوی اول ده‌ها
# نتیجه داشته باشه، تا وقتی مشترک فعاله (حداکثر MONITOR_DURATION_DAYS
# روز) هر MONITOR_CHECK_INTERVAL_HOURS ساعت دوباره جستجو می‌کنیم، چون
# فرصت شغلی/استاد جدید هر روز اضافه می‌شه — یک اسکن یک‌باره کافی نیست.
def db_create_or_refresh_monitor(tid: int, sid: int | None, monitor_type: str,
                                  field: str, country: str) -> int | None:
    """بعد از هر جستجوی موفق (اولیه یا ویرایش‌شده) صدا زده می‌شه. اگه
    مانیتور فعالی برای همین (tid, sid, monitor_type) از قبل هست، فقط
    field/country رو به‌روز می‌کنه (کاربر ممکنه رشته/کشور رو عوض کرده
    باشه) — رکورد تکراری نمی‌سازه و expires_at/started_at رو ریست
    نمی‌کنه. اگه نیست، یکی جدید با انقضای MONITOR_DURATION_DAYS روز
    بعد می‌سازه."""
    if sid is None:
        return None
    now = datetime.now()
    expires = now + timedelta(days=MONITOR_DURATION_DAYS)
    try:
        with _db() as c:
            row = c.execute("""SELECT id FROM active_monitors
                WHERE telegram_id=? AND sub_id=? AND monitor_type=? AND active=1""",
                (tid, sid, monitor_type)).fetchone()
            if row:
                c.execute("UPDATE active_monitors SET field=?, country=? WHERE id=?",
                          (field, country, row[0]))
                return row[0]
            cur = c.execute("""INSERT INTO active_monitors
                (telegram_id, sub_id, monitor_type, field, country,
                 started_at, expires_at, last_checked_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (tid, sid, monitor_type, field, country,
                 str(now), str(expires), str(now)))
            logger.info(f"🟢 مانیتور مداوم فعال شد: tid={tid} sid={sid} "
                        f"type={monitor_type} field={field!r} country={country!r}")
            return cur.lastrowid
    except Exception as e:
        # هیچ‌وقت نباید ساخت مانیتور کل جریان جستجو رو خراب کنه — فقط لاگ.
        logger.error(f"db_create_or_refresh_monitor: {e}")
        return None

def db_get_due_monitors(hours: int = MONITOR_CHECK_INTERVAL_HOURS) -> list[dict]:
    """مانیتورهایی که فعالن، هنوز منقضی نشدن، و آخرین چکشون حداقل
    `hours` ساعت پیش بوده."""
    cutoff = str(datetime.now() - timedelta(hours=hours))
    now = str(datetime.now())
    with _db() as c:
        rows = c.execute("""SELECT id, telegram_id, sub_id, monitor_type, field, country
            FROM active_monitors
            WHERE active=1 AND last_checked_at<=? AND expires_at>=?""",
            (cutoff, now)).fetchall()
    return [{"id": r[0], "telegram_id": r[1], "sub_id": r[2],
             "monitor_type": r[3], "field": r[4], "country": r[5]} for r in rows]

def db_mark_monitor_checked(mid: int):
    """قبل از شروع جستجوی واقعی صدا زده می‌شه (نه بعدش) — دقیقاً مثل
    db_mark_pending_search_tried — تا اگه خودِ جستجو گیر کنه/کرش کنه،
    همون مانیتور تا ساعت بعد دوباره امتحان نشه (از rate limit بی‌مورد
    روی APIهای بیرونی جلوگیری می‌کنه)."""
    with _db() as c:
        c.execute("""UPDATE active_monitors SET last_checked_at=?, checks_done=checks_done+1
            WHERE id=?""", (str(datetime.now()), mid))

def db_add_monitor_new_matches(mid: int, n: int):
    """جدا از db_mark_monitor_checked — بعد از این‌که واقعاً نتیجه‌ی جدید
    پیدا و به کاربر گزارش شد صدا زده می‌شه، فقط شمارنده رو زیاد می‌کنه."""
    if n <= 0:
        return
    with _db() as c:
        c.execute("UPDATE active_monitors SET new_matches_found=new_matches_found+? WHERE id=?",
                   (n, mid))

def db_deactivate_monitor(mid: int):
    with _db() as c:
        c.execute("UPDATE active_monitors SET active=0 WHERE id=?", (mid,))

def db_deactivate_monitors_for_sub(tid: int, sid: int):
    """وقتی subscription تکمیل می‌شه (quota پر شده) یا لغو می‌شه، مانیتورهای
    مربوط به همونم دیگه لازم نیست ادامه بدن. بعد از db_mark_sub_completed
    صدا زده می‌شه."""
    with _db() as c:
        c.execute("UPDATE active_monitors SET active=0 WHERE telegram_id=? AND sub_id=?", (tid, sid))

# ── AI Career Profile — یک بار اطلاعات بگیر، همیشه استفاده کن ─────
_CAREER_PROFILE_FIELDS = [
    "field", "education", "countries", "gpa", "work_exp", "publications",
    "job_filters", "linkedin", "github", "scholar", "skills",
    "resume", "resume_is_file", "resume_file_id",
    "recommendations", "recommenders_info",
]

def db_save_career_profile(tid, client: dict):
    """پروفایل کامل کاربر (رزومه، مهارت‌ها، لینک‌ها، کشور هدف و...) رو
    ذخیره می‌کنه تا دفعه‌ی بعد — چه همین سرویس چه سرویس دیگه — نیازی به
    پرسیدن دوباره‌ی همه‌چیز نباشه. telegram_id کلید یکتاست.
    نکته‌ی مهم: عمداً از "INSERT OR REPLACE" استفاده نمی‌کنیم — اون دستور
    عملاً ردیف قدیمی رو حذف و ردیف کاملاً تازه می‌سازه، یعنی هر ستونی که
    توی _CAREER_PROFILE_FIELDS نیست (مثل disliked_jobs که جدای AI Memory
    نگه‌داری می‌شه) با هر بار ذخیره‌ی پروفایل به مقدار پیش‌فرض خالی برمی‌گرده
    و پاک می‌شه. به‌جاش با ON CONFLICT DO UPDATE فقط دقیقاً همون ستون‌هایی
    که این‌جا لیست شدن آپدیت می‌شن و بقیه‌ی ستون‌ها (فعلی و آینده) دست‌نخورده
    باقی می‌مونن."""
    vals = []
    for k in _CAREER_PROFILE_FIELDS:
        if k == "resume_is_file":
            vals.append(1 if client.get(k) else 0)
        else:
            vals.append(str(client.get(k, "") or ""))
    cols = ",".join(_CAREER_PROFILE_FIELDS)
    qs = ",".join(["?"] * len(_CAREER_PROFILE_FIELDS))
    updates = ",".join(f"{k}=excluded.{k}" for k in _CAREER_PROFILE_FIELDS)
    with _db() as c:
        c.execute(f"""INSERT INTO career_profiles (telegram_id,{cols},updated_at)
            VALUES (?,{qs},?)
            ON CONFLICT(telegram_id) DO UPDATE SET {updates}, updated_at=excluded.updated_at""",
            (tid, *vals, str(datetime.now())))

def db_get_career_profile(tid):
    with _db() as c:
        row = c.execute(f"""SELECT {",".join(_CAREER_PROFILE_FIELDS)} FROM career_profiles
            WHERE telegram_id=?""", (tid,)).fetchone()
    if not row:
        return None
    profile = dict(zip(_CAREER_PROFILE_FIELDS, row))
    profile["resume_is_file"] = bool(profile.get("resume_is_file"))
    return profile

# ── ATS Resume Scan — جدا از _CAREER_PROFILE_FIELDS نگه داشته شده،
# دقیقاً همون دلیل disliked_jobs: db_save_career_profile با ON CONFLICT
# فقط ستون‌های _CAREER_PROFILE_FIELDS رو آپدیت می‌کنه، پس این ستون‌ها با
# هر بار ذخیره‌ی پروفایل پاک نمی‌شن؛ برعکس، save کردن این ستون‌ها هم به
# بقیه‌ی پروفایل دست نمی‌زنه.
def db_save_resume_ats_scan(tid: int, score: int, report_json: str, resume_hash: str):
    with _db() as c:
        c.execute("""INSERT INTO career_profiles
                (telegram_id, resume_ats_score, resume_ats_report_json, resume_ats_hash,
                 resume_ats_scanned_at, updated_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                resume_ats_score=excluded.resume_ats_score,
                resume_ats_report_json=excluded.resume_ats_report_json,
                resume_ats_hash=excluded.resume_ats_hash,
                resume_ats_scanned_at=excluded.resume_ats_scanned_at""",
            (tid, score, report_json, resume_hash, str(datetime.now()), str(datetime.now())))

def db_get_resume_ats_scan(tid: int) -> dict | None:
    """None یعنی این کاربر هنوز هیچ‌وقت اسکن ATS نشده (باید اجباری اجرا بشه)."""
    with _db() as c:
        row = c.execute("""SELECT resume_ats_score, resume_ats_report_json, resume_ats_hash,
            resume_ats_scanned_at FROM career_profiles WHERE telegram_id=?""", (tid,)).fetchone()
    if not row or row[0] is None:
        return None
    return {"score": row[0], "report_json": row[1], "resume_hash": row[2], "scanned_at": row[3]}

# ── AI Memory — مشاغل/شرکت‌های ناخواسته ─────────────────────────────
# عمداً از _CAREER_PROFILE_FIELDS جدا نگه‌داشته شده: db_save_career_profile
# کل ردیف رو با INSERT OR REPLACE از روی جواب سوالات جایگزین می‌کنه، پس
# اگه disliked_jobs عضو همون فیلدها بود، هر بار که کاربر پروفایلش رو
# آپدیت می‌کرد (که disliked_jobs جزوش نیست) لیست ناخواسته‌ها پاک می‌شد.
# این‌جوری disliked_jobs همیشه additive و مستقل باقی می‌مونه.
def db_get_disliked_jobs(tid) -> str:
    with _db() as c:
        row = c.execute("SELECT disliked_jobs FROM career_profiles WHERE telegram_id=?",
                         (tid,)).fetchone()
    return (row[0] if row and row[0] else "")

def db_add_disliked_jobs(tid, keywords: list[str]) -> str:
    """کلیدواژه‌ها/نام شرکت‌های ناخواسته رو (بدون تکراری، بدون پاک کردن
    قبلی‌ها) اضافه می‌کنه. اگه هنوز ردیفی توی career_profiles برای این
    کاربر نباشه (هنوز هیچ سرویسی نگرفته)، خودش می‌سازدش."""
    clean = [k.strip().lower() for k in keywords if k and k.strip()]
    if not clean:
        return db_get_disliked_jobs(tid)
    existing = [x for x in db_get_disliked_jobs(tid).split("|") if x]
    merged = existing[:]
    for k in clean:
        if k not in merged:
            merged.append(k)
    merged = merged[-50:]  # سقف امن — جلوگیری از رشد بی‌نهایت رشته
    joined = "|".join(merged)
    with _db() as c:
        c.execute("""INSERT INTO career_profiles (telegram_id, disliked_jobs, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET disliked_jobs=excluded.disliked_jobs""",
            (tid, joined, str(datetime.now())))
    return joined

# ================================================================
# APPLICATION BRAIN — به‌جای این‌که فقط disliked_jobs یک لیست تخت
# کلیدواژه بمونه، هر کلیدواژه/رد کردن رو به دسته‌ی ساختاریافته
# (work_mode/seniority/company_stage/sponsorship/location) طبقه‌بندی
# می‌کنیم و شمارنده نگه می‌داریم. وقتی یک دسته به‌قدر کافی تکرار بشه
# (پیش‌فرض ۳ بار)، جستجوی بعدی خودکار همون سیگنال رو در فیلترها اعمال
# می‌کنه — دقیقاً همون «کاربر ۳۰ شغل رو reject می‌کنه → سیستم می‌فهمه
# ❌startup ❌onsite ❌junior ❌New York ❌no-sponsorship → search بعدی
# خودش تغییر می‌کنه».
# ================================================================
LEARNED_SIGNAL_THRESHOLD = int(os.getenv("LEARNED_SIGNAL_THRESHOLD", "3") or "3")

_SIGNAL_PATTERNS: dict[str, dict[str, list[str]]] = {
    "work_mode": {
        "onsite": ["onsite", "on-site", "on site", "حضوری", "غیر ریموت", "not remote"],
        "hybrid":  ["hybrid", "هیبرید"],
    },
    "seniority": {
        "junior": ["junior", "entry level", "entry-level", "intern", "internship", "کارآموز", "جونیور"],
        "senior":  ["senior", "lead", "principal", "staff", "سنیور", "ارشد"],
    },
    "company_stage": {
        "startup": ["startup", "start-up", "استارتاپ"],
    },
    "sponsorship": {
        "no_sponsorship": [
            "no sponsorship", "no visa sponsorship", "unable to sponsor",
            "sponsorship unavailable", "must be authorized", "بدون اسپانسر",
        ],
    },
}

def _infer_signals_from_text(text: str) -> list[tuple[str, str]]:
    """یک تکه متن آزاد (کلیدواژه‌ی dislike، عنوان/توضیح یک آگهی رد‌شده)
    رو به لیست (دسته, مقدار) تبدیل می‌کنه — کاملاً deterministic (regex
    ساده)، بدون نیاز به AI، پس هیچ‌وقت hallucinate نمی‌کنه و هزینه‌ی
    اضافه هم نداره."""
    t = (text or "").lower()
    out: list[tuple[str, str]] = []
    for category, values in _SIGNAL_PATTERNS.items():
        for value, patterns in values.items():
            if any(p in t for p in patterns):
                out.append((category, value))
    return out

def db_record_learned_signals(tid, texts: list[str]) -> None:
    """از یک یا چند متن آزاد (کلیدواژه‌های dislike تازه‌ی کاربر)،
    سیگنال‌های ساختاریافته استخراج و شمارنده‌شون افزایش داده می‌شه."""
    signals: list[tuple[str, str]] = []
    for t in texts or []:
        signals.extend(_infer_signals_from_text(t))
    if not signals:
        return
    with _db() as c:
        row = c.execute("SELECT learned_filters_json FROM career_profiles WHERE telegram_id=?",
                         (tid,)).fetchone()
        try:
            data = json.loads(row[0]) if row and row[0] else {}
        except Exception:
            data = {}
        for category, value in signals:
            bucket = data.setdefault(category, {})
            bucket[value] = int(bucket.get(value, 0)) + 1
        c.execute("""INSERT INTO career_profiles (telegram_id, learned_filters_json, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET learned_filters_json=excluded.learned_filters_json""",
            (tid, json.dumps(data), str(datetime.now())))

def db_get_learned_filter_summary(tid) -> dict[str, str]:
    """فقط سیگنال‌هایی که به آستانه‌ی LEARNED_SIGNAL_THRESHOLD رسیده‌اند
    رو برمی‌گردونه (dict ساده‌ی category → value)، تا یک اشاره‌ی تصادفی
    باعث فیلتر شدن نشه — باید واقعاً یک الگوی تکرارشونده باشه."""
    with _db() as c:
        row = c.execute("SELECT learned_filters_json FROM career_profiles WHERE telegram_id=?",
                         (tid,)).fetchone()
    if not row or not row[0]:
        return {}
    try:
        data = json.loads(row[0])
    except Exception:
        return {}
    out: dict[str, str] = {}
    for category, bucket in (data or {}).items():
        if not isinstance(bucket, dict):
            continue
        top_value, top_count = max(bucket.items(), key=lambda kv: kv[1], default=(None, 0))
        if top_value and top_count >= LEARNED_SIGNAL_THRESHOLD:
            out[category] = top_value
    return out

def _apply_learned_filters(user_filters: dict, learned: dict[str, str]) -> dict:
    """سیگنال‌های یادگرفته‌شده رو به همون ساختار _parse_job_filters اضافه
    می‌کنه — یعنی از همون نقطه‌ی موجود (_passes_job_filters) اعمال
    می‌شن، بدون این‌که هیچ منطق فیلترکردن جدیدی لازم باشه."""
    uf = dict(user_filters)
    if learned.get("work_mode") == "onsite":
        # کاربر مکرراً onsite رد کرده → از این به بعد فقط remote
        uf["remote_only"] = True
    if learned.get("sponsorship") == "no_sponsorship":
        uf["visa_only"] = True
    seniority_exclude = uf.get("disliked_keywords", [])
    if learned.get("seniority") == "junior" and "junior" not in seniority_exclude:
        seniority_exclude = seniority_exclude + ["junior", "entry level", "internship"]
    if learned.get("company_stage") == "startup" and "startup" not in seniority_exclude:
        seniority_exclude = seniority_exclude + ["startup"]
    if seniority_exclude:
        uf["disliked_keywords"] = seniority_exclude
    return uf

# ── پرونده‌ی مهاجرتی (Immigration Profile) — پیشرفته‌ی AI Memory ────────
# دقیقاً همون دلیل disliked_jobs بالاتر: این ستون‌ها عمداً از
# _CAREER_PROFILE_FIELDS جدا نگه داشته می‌شن تا db_save_career_profile
# (که موقع onboarding معمولی صدا زده می‌شه و این فیلدها رو نمی‌فرسته)
# هر بار مقدارشون رو با رشته‌ی خالی پاک نکنه. هر فیلد جدا و additive نیست
# (برخلاف disliked_jobs) چون این‌ها value یکتا هستن نه لیست — پس یک
# setter عمومی «فقط همین ستون رو آپدیت کن» کافیه، هم برای فیلدهای جدید
# (visa_goal/previous_refusal/...) هم برای فیلدهای قدیمی مشترک با
# _CAREER_PROFILE_FIELDS (education/countries/work_exp) وقتی کاربر
# می‌خواد فقط همون یکی رو از منوی «پرونده‌ی مهاجرتی» ویرایش کنه.
IMMIGRATION_PROFILE_FIELDS = [
    # (کلید ستون در career_profiles, برچسب فارسی برای نمایش/منو)
    ("education",         "🎓 تحصیلات"),
    ("countries",         "🌍 کشور هدف (Country Target)"),
    ("work_exp",          "💼 سابقه کار"),
    ("visa_goal",         "🎯 هدف ویزا (Visa Goal)"),
    ("previous_refusal",  "❌ ریجکت/رد قبلی (Previous Refusal)"),
    ("documents_status",  "📄 وضعیت مدارک (Documents)"),
    ("weak_points",       "⚠️ نقاط ضعف پرونده (Weak Points)"),
    ("strategy",          "🧭 استراتژی توافق‌شده (Strategy)"),
]
_IMMIGRATION_FIELD_KEYS = {k for k, _ in IMMIGRATION_PROFILE_FIELDS}
_IMMIGRATION_LABEL_TO_KEY = {label: key for key, label in IMMIGRATION_PROFILE_FIELDS}

def db_set_career_profile_field(tid: int, field_key: str, value: str) -> None:
    """فقط یک ستون از career_profiles رو آپدیت می‌کنه (یا اگه ردیفی برای
    این کاربر نبود، می‌سازدش) — بدون دست زدن به بقیه‌ی ستون‌ها. whitelist
    عمدیه (فقط IMMIGRATION_PROFILE_FIELDS)، تا این تابع عمومی هیچ‌وقت
    مسیر تزریق نام ستون دلخواه نشه."""
    if field_key not in _IMMIGRATION_FIELD_KEYS:
        logger.warning(f"db_set_career_profile_field: فیلد نامعتبر '{field_key}' نادیده گرفته شد")
        return
    value = (value or "").strip()[:500]
    with _db() as c:
        c.execute(f"""INSERT INTO career_profiles (telegram_id, {field_key}, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET {field_key}=excluded.{field_key},
                updated_at=excluded.updated_at""",
            (tid, value, str(datetime.now())))

# ── Personal Visa Agent — opt-in و ددلاین واقعی ──────────────────────
def db_set_visa_agent_optin(tid: int, enabled: bool) -> None:
    with _db() as c:
        c.execute("""INSERT INTO career_profiles (telegram_id, visa_agent_optin, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET visa_agent_optin=excluded.visa_agent_optin,
                updated_at=excluded.updated_at""",
            (tid, 1 if enabled else 0, str(datetime.now())))

def db_get_visa_agent_optin(tid: int) -> bool:
    with _db() as c:
        row = c.execute("SELECT visa_agent_optin FROM career_profiles WHERE telegram_id=?",
                         (tid,)).fetchone()
    return bool(row and row[0])

def db_set_visa_agent_deadline(tid: int, date_str: str, label: str) -> None:
    """date_str همیشه به شکل YYYY-MM-DD ذخیره می‌شه (فرمتش قبل از این تابع،
    توی هندلر state، اعتبارسنجی شده) — این تابع خودش چیزی محاسبه/حدس
    نمی‌زنه، فقط همون مقدار تاییدشده رو ذخیره می‌کنه."""
    with _db() as c:
        c.execute("""INSERT INTO career_profiles
            (telegram_id, visa_agent_deadline_date, visa_agent_deadline_label, updated_at)
            VALUES (?,?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                visa_agent_deadline_date=excluded.visa_agent_deadline_date,
                visa_agent_deadline_label=excluded.visa_agent_deadline_label,
                updated_at=excluded.updated_at""",
            (tid, date_str, label[:150], str(datetime.now())))

def db_mark_visa_agent_sent(tid: int) -> None:
    with _db() as c:
        c.execute("UPDATE career_profiles SET visa_agent_last_sent=? WHERE telegram_id=?",
                   (str(datetime.now()), tid))

def db_get_visa_agent_subscribers() -> list[dict]:
    """فقط کاربرهایی که صریحاً visa_agent_optin=1 کردن، هر فیلد این تابع
    فقط خواندنیه — تصمیم اینکه پیام واقعاً ارسال بشه یا نه (مثلاً پرونده
    خالیه یا نه) توی خودِ job هفتگی گرفته می‌شه، نه اینجا."""
    with _db() as c:
        rows = c.execute("""SELECT telegram_id, visa_agent_deadline_date, visa_agent_deadline_label
            FROM career_profiles WHERE visa_agent_optin=1""").fetchall()
    return [{"telegram_id": r[0], "deadline_date": r[1] or "", "deadline_label": r[2] or ""}
            for r in rows]

def db_get_immigration_profile(tid: int) -> dict:
    """کل پرونده‌ی مهاجرتی کاربر رو یک‌جا برمی‌گردونه (نام از جدول users +
    فیلدهای IMMIGRATION_PROFILE_FIELDS از career_profiles). فقط خواندنی و
    خطاناپذیر — اگه هنوز چیزی ثبت نشده، مقادیر خالی برمی‌گردن، نه خطا."""
    user = db_get_user(tid) or {}
    profile = db_get_career_profile(tid) or {}
    out = {"name": user.get("full_name", "") or ""}
    for key, _ in IMMIGRATION_PROFILE_FIELDS:
        out[key] = str(profile.get(key, "") or "")
    return out

def _format_immigration_profile_block(imm: dict) -> str:
    """پرونده‌ی مهاجرتی رو دقیقاً به شکل ساخت‌یافته‌ی
    'Name / Education / Country Target / Visa Goal / ...' فرمت می‌کنه —
    هم برای نمایش مستقیم به کاربر، هم به‌عنوان context ثابت و قابل‌اتکا
    برای AI (به‌جای این‌که هر بار از وسط تاریخچه‌ی آزاد حدس بزنه)."""
    lines = ["🗂 پرونده مهاجرتی کاربر (User Immigration Profile):"]
    lines.append(f"نام: {imm.get('name') or '—'}")
    for key, label in IMMIGRATION_PROFILE_FIELDS:
        clean_label = label.split(" (")[0].split(" ", 1)[-1] if " " in label else label
        lines.append(f"{clean_label}: {imm.get(key) or '—'}")
    return "\n".join(lines)

def db_get_memory_summary(tid) -> str:
    """خلاصه‌ی متنی از هرچی بات درباره‌ی این کاربر یادشه — هم برای نمایش
    مستقیم به کاربر، هم به‌عنوان context برای AI (پشتیبانی چت) تا جواب‌ها
    با تاریخچه‌ی واقعی کاربر همخوانی داشته باشه. کاملاً read-only و
    خطاناپذیر: هر بخش جدا try/except نداره چون همه‌ی توابع زیرین خودشون
    already مقاوم به رکورد خالی هستن (لیست/رشته‌ی خالی برمی‌گردونن)."""
    profile = db_get_career_profile(tid) or {}
    recent_emails = db_get_recent_emails(tid, limit=8)
    recent_jobs   = db_get_recent_jobs(tid, limit=8)
    disliked      = [x for x in db_get_disliked_jobs(tid).split("|") if x]
    reply_stats   = db_get_reply_stats(tid)

    lines = []
    if profile.get("field"):
        lines.append(f"رشته/زمینه‌ی کاربر: {profile['field']}")
    if profile.get("countries"):
        lines.append(f"کشور(های) مورد علاقه‌ی کاربر: {profile['countries']}")
    if profile.get("resume") or profile.get("resume_is_file"):
        lines.append("رزومه‌ی این کاربر قبلاً ذخیره شده و در دسترس است.")
    if recent_emails:
        profs = "، ".join(e["name"] for e in recent_emails[:5] if e.get("name"))
        if profs:
            lines.append(f"آخرین اساتیدی که برایشان ایمیل ارسال شده: {profs}")
    pos = reply_stats.get("positive", 0)
    neg = reply_stats.get("negative", 0)
    if pos or neg:
        lines.append(f"از ایمیل‌های ارسالی، پاسخ ثبت‌شده: {pos} مثبت، {neg} منفی.")
    if recent_jobs:
        titles = "، ".join(f"{j['title']}@{j['company']}" for j in recent_jobs[:5] if j.get("title"))
        if titles:
            lines.append(f"آخرین Applyهای انجام‌شده: {titles}")
    if disliked:
        lines.append(f"مشاغل/شرکت‌هایی که کاربر گفته دوست ندارد (باید در جستجوهای بعدی حذف شوند): {', '.join(disliked)}")

    # ── پرونده‌ی مهاجرتی (اگه حداقل یک فیلد ازش پر شده باشه) ──────────
    # جدا از خط‌های بالا نگه داشته می‌شه و به‌صورت بلوک ساخت‌یافته اضافه
    # می‌شه — این‌طوری AI به‌جای حدس زدن از جمله‌های پراکنده، مستقیماً
    # پرونده‌ی رسمی کاربر رو (Name/Education/Country Target/Visa Goal/...)
    # می‌بینه و می‌تونه تحلیل مشخص بده (مثلاً «مشکل اصلی مدرک زبانه»).
    try:
        imm = db_get_immigration_profile(tid)
    except Exception as e:
        logger.error(f"db_get_immigration_profile({tid}) in memory_summary: {e}")
        imm = {}
    if imm and any(imm.get(k) for k, _ in IMMIGRATION_PROFILE_FIELDS):
        lines.append("")
        lines.append(_format_immigration_profile_block(imm))
    return "\n".join(lines)

def db_get_dashboard_stats() -> dict:
    """آمار کلی برای داشبورد ادمین — یه‌جا چند query می‌زنیم تا در /stats
    سریع پاسخ بده. همه‌چیز روی دیتابیس محلی‌ه پس تأخیر شبکه ندارد."""
    with _db() as c:
        total_users      = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        pending_users    = c.execute("SELECT COUNT(*) FROM users WHERE status='pending'").fetchone()[0]
        approved_users   = c.execute("SELECT COUNT(*) FROM users WHERE status='approved'").fetchone()[0]

        email_subs       = c.execute("SELECT COUNT(*) FROM subscriptions WHERE service='email'").fetchone()[0]
        job_subs         = c.execute("SELECT COUNT(*) FROM subscriptions WHERE service='job'").fetchone()[0]
        completed_subs   = c.execute("SELECT COUNT(*) FROM subscriptions WHERE status='completed'").fetchone()[0]

        total_emails     = c.execute("SELECT COUNT(*) FROM sent_emails WHERE status='sent'").fetchone()[0]
        failed_emails    = c.execute("SELECT COUNT(*) FROM sent_emails WHERE status LIKE 'failed%'").fetchone()[0]
        skipped_emails   = c.execute("SELECT COUNT(*) FROM sent_emails WHERE status LIKE 'skipped%'").fetchone()[0]
        manual_emails    = c.execute("SELECT COUNT(*) FROM sent_emails WHERE status='manual_copy'").fetchone()[0]

        total_jobs       = c.execute("SELECT COUNT(*) FROM applied_jobs WHERE status IN ('sent','link_provided')").fetchone()[0]
        # تفکیک صریح اپلای ایمیلی واقعی از صرفاً فرصت/لینک پورتال (که کاربر
        # خودش باید دستی پر کنه) — قبلاً total_jobs این دو رو زیر یک عدد واحد
        # با برچسب «✅ اپلای» به ادمین نشون می‌داد، که برای گزارش/فروش محصول
        # گمراه‌کننده بود (نمی‌شه ادعا کرد ۲۰۰ تا application واقعاً submit شده
        # وقتی بخشیشون فقط لینک پورتاله).
        jobs_applied_email  = c.execute("SELECT COUNT(*) FROM applied_jobs WHERE status='sent'").fetchone()[0]
        jobs_portal_link    = c.execute("SELECT COUNT(*) FROM applied_jobs WHERE status='link_provided'").fetchone()[0]
        failed_jobs      = c.execute("SELECT COUNT(*) FROM applied_jobs WHERE status LIKE 'failed%'").fetchone()[0]
        skipped_jobs     = c.execute("SELECT COUNT(*) FROM applied_jobs WHERE status LIKE 'skipped%'").fetchone()[0]

        # رضایت کاربران
        likes            = c.execute("SELECT COUNT(*) FROM satisfaction_ratings WHERE rating='like'").fetchone()[0] \
                           if _table_exists(c, "satisfaction_ratings") else 0
        dislikes         = c.execute("SELECT COUNT(*) FROM satisfaction_ratings WHERE rating='dislike'").fetchone()[0] \
                           if _table_exists(c, "satisfaction_ratings") else 0

        # آخرین ۵ کاربر
        recent_users = c.execute(
            "SELECT full_name, status, service, created_at FROM users ORDER BY created_at DESC LIMIT 5"
        ).fetchall()

    email_success_rate = round(total_emails / max(total_emails+failed_emails, 1) * 100)
    job_success_rate   = round(total_jobs   / max(total_jobs+failed_jobs,   1) * 100)
    satisfaction_rate  = round(likes / max(likes+dislikes, 1) * 100) if (likes+dislikes) > 0 else None

    return {
        "total_users": total_users, "pending_users": pending_users, "approved_users": approved_users,
        "email_subs": email_subs, "job_subs": job_subs, "completed_subs": completed_subs,
        "total_emails": total_emails, "failed_emails": failed_emails,
        "skipped_emails": skipped_emails, "manual_emails": manual_emails,
        "email_success_rate": email_success_rate,
        "total_jobs": total_jobs, "jobs_applied_email": jobs_applied_email,
        "jobs_portal_link": jobs_portal_link, "failed_jobs": failed_jobs,
        "skipped_jobs": skipped_jobs, "job_success_rate": job_success_rate,
        "likes": likes, "dislikes": dislikes, "satisfaction_rate": satisfaction_rate,
        "recent_users": [{"name": r[0], "status": r[1], "svc": r[2], "at": r[3]} for r in recent_users],
    }

def _table_exists(conn, table: str) -> bool:
    return bool(conn.execute(
        f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'"
    ).fetchone())

def db_log_security_event(event_type: str, telegram_id: int | None, detail: str = "") -> None:
    """ثبت سبک یه رویداد امنیتی — هیچ‌وقت نباید خودش باعث خطای جدید توی
    مسیر اصلی بشه (برای همین caller ها این رو معمولاً fire-and-forget
    صدا می‌زنن، نه با await مستقیم توی مسیر بحرانی)."""
    try:
        with _db() as c:
            c.execute("""INSERT INTO security_events (event_type, telegram_id, detail, created_at)
                VALUES (?,?,?,?)""", (event_type, telegram_id, detail[:300], str(datetime.now())))
    except Exception as e:
        logger.error(f"db_log_security_event({event_type}): {e}")

def db_get_security_summary(hours: int = 24) -> dict:
    """شمارش رویدادهای امنیتی در N ساعت اخیر، دسته‌بندی‌شده بر اساس نوع —
    برای /security ادمین. فقط خواندنی، اگه جدول هنوز نساخته شده باشه
    (نصب خیلی قدیمی که هنوز init_db براش اجرا نشده) صفر برمی‌گردونه،
    نه خطا."""
    cutoff = (datetime.now() - timedelta(hours=hours)).strftime("%Y-%m-%d %H:%M:%S")
    out: dict[str, int] = {}
    try:
        with _db() as c:
            if not _table_exists(c, "security_events"):
                return out
            rows = c.execute("""SELECT event_type, COUNT(*) FROM security_events
                WHERE created_at >= ? GROUP BY event_type""", (cutoff,)).fetchall()
        out = {r[0]: r[1] for r in rows}
    except Exception as e:
        logger.error(f"db_get_security_summary: {e}")
    return out

# ================================================================
# AUDIT LOG — ثبت کامل هر اقدام ادمین
# ================================================================
# چرا جدا از security_events: اون جدول برای رویدادهای خودکار امنیتی
# سیستمه (rate-limit، ورود ناموفق)، این یکی مخصوص اقدامات آگاهانه‌ی
# *ادمین*ه — یعنی هرجا یه انسان (نه خود بات) روی پرونده‌ی یه کاربر
# کاری انجام می‌ده. برای سرویس مهاجرتی که با مدارک/پرداخت/اطلاعات
# حساس کاربر سروکار داره، این جدول باید بتونه به این سوال جواب بده:
# «کدوم ادمین، کِی، دقیقاً چه کاری روی پرونده‌ی کدوم کاربر انجام داد،
# و نتیجه‌اش چی شد؟» — دقیقاً فرمتی که خواسته شده.
#
# نکته‌ی مهم درباره‌ی IP: بات تلگرام از طریق Bot API با تلگرام حرف
# می‌زنه، نه مستقیم با کاربر/ادمین — یعنی وقتی ادمین یه دکمه رو توی
# تلگرام می‌زنه یا یه دستور می‌فرسته، این کد فقط یه Update از سرورهای
# تلگرام می‌گیره؛ آدرس IP واقعی دستگاه ادمین اصلاً به این کد نمی‌رسه
# (نه پی‌تلگرام‌باتی این IP رو می‌ده، نه هیچ بات تلگرام دیگه‌ای می‌تونه
# بگیرتش — این محدودیت خودِ Bot API تلگرامه، نه این کد). به‌جای ادعای
# دروغ (مثلاً ساختن یه IP جعلی)، اینجا صادقانه می‌نویسیم
# "N/A (Telegram)" — اگه یه پنل ادمین وب هم بعداً اضافه بشه (مثلاً
# روی همین PythonAnywhere با Flask)، همون‌جا IP واقعی از
# request.remote_addr در دسترسه و می‌شه به همین db_log_audit پاس داد.
def db_log_audit(admin_id: int, admin_name: str, action: str,
                  target_user_id: int | None = None, result: str = "Success",
                  detail: str = "", ip_address: str = "N/A (Telegram)") -> None:
    """ثبت یک ردیف audit log. مثل db_log_security_event عمداً هیچ‌وقت
    نباید خودش باعث خطای جدید توی مسیر اصلی بشه — فقط لاگ می‌کنه و رد
    می‌شه، هیچ exception ای رو بالا نمی‌ده."""
    try:
        with _db() as c:
            c.execute("""INSERT INTO audit_log
                (admin_id, admin_name, action, target_user_id, ip_address, result, detail, created_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (admin_id, (admin_name or "")[:150], action[:200], target_user_id,
                 (ip_address or "N/A (Telegram)")[:100], result[:50], (detail or "")[:500],
                 str(datetime.now())))
    except Exception as e:
        logger.error(f"db_log_audit({action}): {e}")

def db_get_audit_log(limit: int = 15, target_user_id: int | None = None,
                      admin_id: int | None = None) -> list[dict]:
    """آخرین ردیف‌های audit log، جدیدترین اول. فقط خواندنی — اگه جدول
    هنوز نساخته شده باشه (نصب خیلی قدیمی که init_db روش اجرا نشده)
    لیست خالی برمی‌گردونه، نه خطا."""
    out: list[dict] = []
    try:
        with _db() as c:
            if not _table_exists(c, "audit_log"):
                return out
            q = "SELECT admin_id, admin_name, action, target_user_id, ip_address, result, detail, created_at FROM audit_log"
            conds, params = [], []
            if target_user_id is not None:
                conds.append("target_user_id = ?"); params.append(target_user_id)
            if admin_id is not None:
                conds.append("admin_id = ?"); params.append(admin_id)
            if conds:
                q += " WHERE " + " AND ".join(conds)
            q += " ORDER BY id DESC LIMIT ?"
            params.append(max(1, min(limit, 500)))
            rows = c.execute(q, params).fetchall()
        for r in rows:
            out.append({
                "admin_id": r[0], "admin_name": r[1], "action": r[2],
                "target_user_id": r[3], "ip_address": r[4], "result": r[5],
                "detail": r[6], "created_at": r[7],
            })
    except Exception as e:
        logger.error(f"db_get_audit_log: {e}")
    return out

def _format_audit_entry(row: dict) -> str:
    """دقیقاً فرمت درخواست‌شده:
    تاریخ/ساعت ← Admin ← Action ← User ← IP ← Result"""
    admin_line = row["admin_name"] or f"ID:{row['admin_id']}"
    user_line  = str(row["target_user_id"]) if row["target_user_id"] else "—"
    text = (
        f"{row['created_at']}\n\n"
        f"Admin:\n{admin_line}\n\n"
        f"Action:\n{row['action']}\n\n"
        f"User:\n{user_line}\n\n"
        f"IP:\n{row['ip_address']}\n\n"
        f"Result:\n{row['result']}"
    )
    if row.get("detail"):
        text += f"\n\nDetail:\n{row['detail']}"
    return text

def _admin_display_name(update: Update) -> str:
    """اسم قابل‌نمایش ادمین برای ثبت توی audit log — ترجیح با
    username (چون یکتا و قابل‌جستجوئه)، وگرنه full_name، وگرنه فقط ID."""
    u = update.effective_user
    if not u:
        return "Unknown"
    if u.username:
        return f"@{u.username}"
    if u.full_name:
        return u.full_name
    return f"ID:{u.id}"

def db_log_satisfaction(uid: int, rating: str, service: str, sub_id=None) -> bool:
    """ثبت رضایت کاربر (👍/👎) — atomic و idempotent، دقیقاً هم‌الگوی
    db_approve_sub/db_reject_sub بالا: یک کاربر برای یک (service, sub_id)
    مشخص فقط یک‌بار می‌تونه رأی ثبت کنه. اگه دوبار تپ کنه (دوتاپ سریع)
    یا تلگرام همون callback رو دوباره دلیوری کنه، تلاش دومی هیچ ردیف
    جدیدی اضافه نمی‌کنه و False برمی‌گردونه — caller (inline_callback)
    از روی همین مقدار می‌فهمه نباید دوباره پیام تشکر/نوتیف ادمین بفرسته
    یا آمار رضایت رو دوبار بشمره.
    UNIQUE INDEX روی NULL کار نمی‌کنه (هر NULL توی SQLite با NULL دیگه
    نامساوی حساب می‌شه)، پس sub_id=None رو به 0 نرمالایز می‌کنیم."""
    sub_id_norm = sub_id if sub_id else 0
    with _db() as c:
        c.execute("""CREATE TABLE IF NOT EXISTS satisfaction_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER,
            service TEXT,
            sub_id INTEGER,
            rating TEXT,
            created_at TEXT
        )""")
        try:
            c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_satisfaction_once
                ON satisfaction_ratings(telegram_id, service, sub_id)""")
        except sqlite3.IntegrityError:
            # روی دیتابیس‌های قدیمی‌تر از این فیکس ممکنه از قبل رکورد
            # تکراری (حاصل همون باگ دوبار-کلیک) وجود داشته باشه — قبل از
            # ساختن ایندکس، فقط قدیمی‌ترین رکورد هر گروه رو نگه می‌داریم.
            c.execute("""DELETE FROM satisfaction_ratings WHERE id NOT IN (
                SELECT MIN(id) FROM satisfaction_ratings
                GROUP BY telegram_id, service, sub_id
            )""")
            c.execute("""CREATE UNIQUE INDEX IF NOT EXISTS ux_satisfaction_once
                ON satisfaction_ratings(telegram_id, service, sub_id)""")
        cur = c.execute("""INSERT OR IGNORE INTO satisfaction_ratings
            (telegram_id,service,sub_id,rating,created_at)
            VALUES(?,?,?,?,?)""", (uid, service, sub_id_norm, rating, str(datetime.now())))
        return cur.rowcount > 0

def db_get_recent_jobs(tid, limit=15):
    with _db() as c:
        rows = c.execute("""SELECT job_title,company,method,status,applied_at
            FROM applied_jobs WHERE telegram_id=? AND status='sent'
            ORDER BY id DESC LIMIT ?""", (tid, limit)).fetchall()
    return [{"title": r[0], "company": r[1], "method": r[2], "status": r[3], "applied_at": r[4]} for r in rows]

def db_add_preview(tid, pname, pemail, subj, body):
    with _db() as c:
        cur = c.execute("""INSERT INTO preview_queue
            (telegram_id,professor_name,professor_email,subject,body,created_at)
            VALUES(?,?,?,?,?,?)""",
            (tid, pname, pemail, subj, body, str(datetime.now())))
        return cur.lastrowid

def db_decide_preview(pid, status):
    with _db() as c:
        c.execute("UPDATE preview_queue SET status=?,decided_at=? WHERE id=?",
                  (status, str(datetime.now()), pid))

def db_update_preview_text(pid, subject=None, body=None):
    """آپدیت subject/body یک preview. برای جلوگیری از بلاک شدن event loop
    همیشه باید با asyncio.to_thread صدا زده شود."""
    if subject is not None and body is not None:
        with _db() as c:
            c.execute("UPDATE preview_queue SET subject=?,body=? WHERE id=?",
                      (subject, body, pid))
    elif subject is not None:
        with _db() as c:
            c.execute("UPDATE preview_queue SET subject=? WHERE id=?", (subject, pid))

def db_get_preview(pid):
    with _db() as c:
        row = c.execute("SELECT * FROM preview_queue WHERE id=?", (pid,)).fetchone()
    if not row: return None
    cols = ["id","telegram_id","professor_name","professor_email",
            "subject","body","status","created_at","decided_at"]
    return dict(zip(cols, row))

def db_get_all_pending_previews():
    """همه‌ی previewهایی که هنوز status='pending' دارن — برای reconciliation
    موقع استارتاپ (نگاه کن به db_save_preview_snapshot و _on_bot_startup)."""
    with _db() as c:
        rows = c.execute("SELECT id, telegram_id, created_at FROM preview_queue WHERE status='pending'").fetchall()
    return [{"id": r[0], "telegram_id": r[1], "created_at": r[2]} for r in rows]

def db_save_preview_snapshot(pid, tid, chat_id, snap: dict):
    """داده‌های لازم برای auto-send یک preview (لیست اساتید، پروفایل
    کاربر، مشخصات SMTP) رو روی دیسک ذخیره می‌کنه — نه فقط bot_data
    (که persistent نیست). هر خطای serialize (مثلاً یک فیلد غیرقابل
    JSON) کاملاً fail-safe مدیریت می‌شه: فقط لاگ می‌کنه، preview flow
    خودش (که از bot_data هم موازی استفاده می‌کنه) کرش نمی‌کنه — فقط
    resilience بعد از ری‌استارت رو از دست می‌ده، نه عملکرد نرمال رو.

    نکته‌ی امنیتی: snap شامل پسورد SMTP خام (plaintext، همون چیزی که
    برای لاگین واقعی استفاده می‌شه) هست — دقیقاً همون الگوی
    SQLiteUserDataPersistence.update_user_data رو اینجا هم رعایت
    می‌کنیم: قبل از نوشتن روی دیسک رمزنگاری (Fernet)، موقع خوندن
    رمزگشایی — تا این جدول جدید دوباره پسورد ایمیل کاربرها رو
    plaintext روی دیسک برنگردونه."""
    snap = dict(snap)
    if S_SMTP_P in snap and snap[S_SMTP_P]:
        snap[S_SMTP_P] = _encrypt_secret(snap[S_SMTP_P])
    try:
        data_json = json.dumps(snap, ensure_ascii=False, default=str)
    except Exception as e:
        logger.warning(f"db_save_preview_snapshot: serialize failed for pid={pid}: {e}")
        return
    try:
        with _db() as c:
            c.execute("""INSERT INTO preview_snapshots (preview_id, telegram_id, chat_id, data_json, created_at)
                VALUES(?,?,?,?,?)
                ON CONFLICT(preview_id) DO UPDATE SET data_json=excluded.data_json""",
                (pid, tid, chat_id, data_json, str(datetime.now())))
    except Exception as e:
        logger.warning(f"db_save_preview_snapshot: write failed for pid={pid}: {e}")

def db_get_preview_snapshot(pid):
    with _db() as c:
        row = c.execute("SELECT data_json, telegram_id, chat_id FROM preview_snapshots WHERE preview_id=?", (pid,)).fetchone()
    if not row:
        return None
    try:
        data = json.loads(row[0])
    except Exception as e:
        logger.warning(f"db_get_preview_snapshot: corrupt data for pid={pid}: {e}")
        return None
    if S_SMTP_P in data and data[S_SMTP_P]:
        data[S_SMTP_P] = _decrypt_secret(data[S_SMTP_P], field_label=f"preview_snapshot smtp_p (pid={pid})")
    data["_telegram_id"] = row[1]
    data["_chat_id"] = row[2]
    return data

def db_delete_preview_snapshot(pid):
    with _db() as c:
        c.execute("DELETE FROM preview_snapshots WHERE preview_id=?", (pid,))

init_db()

# ================================================================
# نرخ دلار از bon-bast.com
# ================================================================

def fetch_usd_rate() -> tuple[int, str] | tuple[None, str]:
    """نرخ دلار — چهار منبع به ترتیب.
    توصیه: روی Railway متغیر USD_RATE_FIXED=920000 رو ست کنید تا
    از همه‌ی این تلاش‌های شبکه‌ای bypass بشید (bon-bast و navasan
    معمولاً توسط proxy ریلوی block می‌شن)."""
    if USD_RATE_FIXED:
        return USD_RATE_FIXED, "نرخ ثابت (تنظیم دستی)"

    RATE_MIN, RATE_MAX = 200_000, 5_000_000
    headers = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36"}

    # ── منبع ۱: bon-bast (ممکنه روی Railway block باشه) ─────────
    try:
        r = requests.get(BONBAST, headers=headers, timeout=8)
        html = r.text
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            for pfx in ("usd1", "usd"):
                el = soup.find(id=f"{pfx}_sell")
                if el:
                    n = int(re.sub(r"[^\d]", "", el.get_text()))
                    if RATE_MIN < n < RATE_MAX:
                        return n, "bon-bast.com"
            for row in soup.find_all("tr"):
                txt = row.get_text(" ")
                if "USD" in txt or "دلار" in txt:
                    nums = [int(x.replace(",","")) for x in re.findall(r"[\d,]{4,}", txt)
                            if RATE_MIN < int(x.replace(",","")) < RATE_MAX]
                    if nums:
                        return (nums[1] if len(nums) > 1 else nums[0]), "bon-bast.com"
        except ImportError:
            pass
        m = re.search(r"usd1?_sell[^>]*>\s*([\d,]{4,})", html, re.I)
        if m:
            n = int(m.group(1).replace(",", ""))
            if RATE_MIN < n < RATE_MAX:
                return n, "bon-bast.com"
    except Exception as e:
        logger.warning(f"bon-bast error: {e}")

    # ── منبع ۲: navasan (ممکنه روی Railway block باشه) ───────────
    try:
        r = requests.get("https://api.navasan.tech/latest/?item=usd_buy", timeout=6)
        data = r.json()
        items = data if isinstance(data, list) else data.get("data", [data])
        for item in items:
            v = item.get("value") or item.get("price") or item.get("usd_buy")
            if v:
                n = int(re.sub(r"[^\d]", "", str(v)))
                if RATE_MIN < n < RATE_MAX:
                    return n, "navasan.tech"
    except Exception as e:
        logger.warning(f"navasan error: {e}")

    # ── منبع ۳: open.er-api.com — Railway-friendly، بدون key ─────
    try:
        r = requests.get("https://open.er-api.com/v6/latest/USD", timeout=8)
        irr = r.json().get("rates", {}).get("IRR", 0)
        if irr > 0:
            toman = int(irr / 10)
            if RATE_MIN < toman < RATE_MAX:
                return toman, "er-api.com"
    except Exception as e:
        logger.warning(f"er-api error: {e}")

    # ── منبع ۴: fawazahmed0 currency API — Railway-friendly ──────
    try:
        r = requests.get(
            "https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json",
            timeout=8)
        irr = r.json().get("usd", {}).get("irr", 0)
        if irr > 0:
            toman = int(irr / 10)
            if RATE_MIN < toman < RATE_MAX:
                return toman, "fawazahmed0/currency-api"
    except Exception as e:
        logger.warning(f"fawazahmed0 error: {e}")

    # همه‌ی منابع شکست خوردن — None برمی‌گردونیم تا caller
    # از کاربر بخواد خودش نرخ رو بده یا USD_RATE_FIXED ست کنه
    logger.warning("All USD rate sources failed. Set USD_RATE_FIXED env var.")
    return None, ""

def fmt_toman(t: int) -> str:
    if t >= 1_000_000: return f"{t/1_000_000:.1f} میلیون تومان"
    return f"{t:,} تومان"

# ================================================================
# AI ENGINE — Gemini → Groq → Template
# ================================================================

def _groq_sync(prompt, model):
    try:
        r = requests.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"Authorization": f"Bearer {GROQ_API_KEY}",
                     "Content-Type": "application/json"},
            json={"model": model,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.7, "max_tokens": 1500},
            timeout=REQ_TIMEOUT
        )
        d = r.json()
        if "choices" in d: return d["choices"][0]["message"]["content"]
    except Exception as e:
        logger.warning(f"Groq {model}: {e}")
    return None

def _extract_json_object(text: str) -> dict | None:
    """استخراج امن یک شیء JSON از خروجی خام مدل AI — جایگزین الگوی قبلی
    `re.search(r"\\{.*\\}", ai, re.DOTALL)` که در ۱۶ جای مختلف فایل تکرار
    شده بود.

    مشکل الگوی قبلی: `.*` با `re.DOTALL` حریصه (greedy) و از اولین `{` تا
    آخرین `}` توی کل متن رو می‌گیره. اگه مدل چیزی مثل این برگردونه:

        {"a": 1}
        توضیح اضافه...
        {"b": 2}

    اون regex هر دو JSON و متن توضیح وسطش رو یکی می‌گیره (از اولین `{` تا
    آخرین `}`) — یعنی `json.loads` روی یک رشته‌ی خراب صدا زده می‌شه و
    exception می‌خوره، درحالی‌که یک JSON کاملاً معتبر (همون اولی) همون
    اول متن وجود داشت.

    استراتژی این تابع (از سریع‌ترین/امن‌ترین به fallback):
    ۱. کل متن (بعد از strip) رو مستقیم json.loads کن — مسیر سریع برای
       خروجی تمیز (اکثر مدل‌ها وقتی صریح خواسته بشه «فقط JSON»، همینو می‌دن).
    ۲. یک brace-matching واقعی (نه regex) از اولین `{` شروع می‌کنه و با
       شمردن `{`/`}` (با احتساب رشته‌های داخل JSON، که ممکنه `{`/`}` توی
       خودشون داشته باشن) اولین شیء JSON کامل و متوازن رو پیدا می‌کنه —
       دقیقاً همون چیزی که مثال بالا نیاز داره: فقط `{"a": 1}` رو برمی‌گردونه،
       نه کل متن تا آخرین `}`.
    ۳. اگه هیچ‌کدوم جواب نداد، به‌عنوان آخرین تلاش (سازگاری با رفتار قبلی
       برای موردهای لبه‌ای عجیب)، همون regex حریص قدیمی رو امتحان می‌کنه.

    خروجی: dict در صورت موفقیت، وگرنه None — هیچ‌وقت exception بالا نمی‌بره."""
    if not text:
        return None
    s = text.strip()

    # ۱) مسیر سریع: کل متن خودش یک JSON تمیزه
    try:
        data = json.loads(s)
        if isinstance(data, dict):
            return data
    except Exception:
        pass

    # ۲) brace-matching واقعی — اولین شیء JSON متوازن رو پیدا کن
    start = s.find("{")
    while start != -1:
        depth = 0
        in_str = False
        esc = False
        for i in range(start, len(s)):
            ch = s[i]
            if in_str:
                if esc:
                    esc = False
                elif ch == "\\":
                    esc = True
                elif ch == '"':
                    in_str = False
                continue
            if ch == '"':
                in_str = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = s[start:i+1]
                    try:
                        data = json.loads(candidate)
                        if isinstance(data, dict):
                            return data
                    except Exception:
                        break  # این کاندید خراب بود، برو سراغ `{` بعدی
        start = s.find("{", start + 1)

    # ۳) fallback نهایی: همون regex حریص قدیمی (فقط برای موردهای عجیبی که
    # حتی brace-matching هم نتونست پارسشون کنه — مثلاً JSON با کامنت‌های
    # غیراستاندارد وسطش که مدل گاهی اضافه می‌کنه)
    try:
        m = re.search(r"\{.*\}", s, re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            if isinstance(data, dict):
                return data
    except Exception:
        pass
    return None

def run_ai_sync(coro):
    """اجرای امن یک coroutine (مثل call_ai(...)) از یک تابع sync معمولی —
    این توابع فعلاً همیشه از داخل ThreadPoolExecutor.submit صدا زده می‌شن
    (یعنی هیچ event loop در حال اجرایی توی همون ترد نیست)، پس asyncio.run()
    عادی کار می‌کنه. ولی اگر یه روزی (رفکتور بعدی، هندلر جدید و ...) یکی
    از این توابع مستقیماً از داخل ترد اصلی asyncio صدا زده بشه، asyncio.run()
    فوراً با RuntimeError('cannot be called from a running event loop')
    کرش می‌کنه. این helper اون حالت رو هم پوشش می‌ده: اگه در همون ترد یه
    event loop در حال اجرا تشخیص بده، coroutine رو توی یه ترد جدید (با
    event loop مستقل خودش) اجرا می‌کنه و نتیجه رو synchronous برمی‌گردونه."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as _ex:
            return _ex.submit(asyncio.run, coro).result()

async def call_ai(prompt: str, min_length: int = 50, *, _groq_fn=None, _gemini_client=None) -> str:
    """min_length=50 برای نوشتن ایمیل مناسبه (یه ایمیل واقعی همیشه بلندتر
    از این‌هاست، پس زیر این حد یعنی جواب خراب/ناقصه) — ولی judge_relevance
    و judge_job_relevance فقط یک JSON فشرده می‌خوان («{"match":false,"score":
    10,"reason":"..."}»)  که می‌تونه به‌راحتی از ۵۰ کاراکتر کمتر باشه؛
    برای اون‌ها min_length پایین‌تر (مثلاً ۱۵) پاس داده می‌شه تا یک جواب
    درستِ کوتاه به‌اشتباه به‌عنوان «خالی/خراب» دور ریخته نشه.

    circuit breaker: قبلاً وقتی Gemini کاملاً down بود (مثلاً quota تموم
    شده یا outage)، هر تک فراخوانی call_ai بازم صبر می‌کرد تا لیست کامل
    GEMINI_MODELS با timeout تا ۳۰ ثانیه fail بشه، بعد می‌رفت سراغ Groq —
    یعنی هزینه‌ی latency این وضعیت رو هر بار از نو می‌پرداختیم. الان دقیقاً
    همون الگوی _ddg_breaker (که خودِ پروژه از قبل برای DDG داره) رو برای
    هر provider AI هم پیاده کردیم: بعد از چند شکست پشت‌سرهم، برای مدتی
    (AI_BREAKER_COOLDOWN) کلاً سراغ اون provider نمی‌ریم و مستقیم می‌ریم
    سراغ fallback بعدی.

    تست‌پذیری: `_groq_fn` و `_gemini_client` seam تزریقی‌ان — فقط برای
    تست استفاده می‌شن (پیش‌فرض None یعنی رفتار واقعی بدون تغییر). با
    پاس‌دادن یه fake function/client، می‌شه کل مسیر (موفقیت هر provider،
    شکست و افتادن به fallback بعدی، ثبت شدن circuit breaker) رو بدون تماس
    واقعی با اینترنت تست کرد."""
    if GEMINI_API_KEY.startswith("AIza") and not _ai_breaker_is_open("gemini"):
        if _gemini_client is not None:
            client = _gemini_client
        else:
            try:
                from google import genai
                client = genai.Client(api_key=GEMINI_API_KEY)
            except Exception as e:
                logger.warning(f"Gemini client init: {e}")
                client = None
        if client is not None:
            from google.genai import types
            # ── فیکس باگ واقعی: امضای generate_content توی SDK فعلی
            # (google-genai) هر سه پارامتر رو keyword-only کرده:
            #   def generate_content(self, *, model, contents, config=None)
            # کد قبلی این‌ها رو positional پاس می‌داد
            # (generate_content("gemini-2.0-flash", prompt, config))، که
            # با این امضا بلافاصله TypeError می‌داد — یعنی مسیر Gemini
            # هیچ‌وقت واقعاً اجرا نمی‌شد، هر بار به except می‌افتاد و
            # بی‌صدا (فقط یک warning) به Groq سقوط می‌کرد. الان keyword
            # argument درسته. همچنین روی لیست GEMINI_MODELS (بالا) حلقه
            # می‌زنیم — دقیقاً همون الگوی GROQ_MODELS — تا اگه یک مدل
            # خاص خاموش/rate-limit شد، بقیه‌ی مدل‌ها همچنان امتحان بشن.
            for gmodel in GEMINI_MODELS:
                try:
                    r = await asyncio.wait_for(
                        asyncio.to_thread(client.models.generate_content,
                            model=gmodel, contents=prompt,
                            config=types.GenerateContentConfig(max_output_tokens=1500)),
                        timeout=30)
                    if r and r.text and len(r.text) >= min_length:
                        _ai_breaker_record("gemini", success=True)
                        return r.text
                except Exception as e:
                    logger.warning(f"Gemini {gmodel}: {e}")
            # اگه به این‌جا رسیدیم یعنی همه‌ی مدل‌های Gemini شکست خوردن
            # (موفقیت بالاتر همون‌جا return می‌کنه) — یک شکست برای کل
            # provider ثبت می‌شه، نه یکی برای هر مدل.
            _ai_breaker_record("gemini", success=False)
    if not _ai_breaker_is_open("groq"):
        groq_fn = _groq_fn if _groq_fn is not None else _groq_sync
        for model in GROQ_MODELS:
            try:
                r = await asyncio.wait_for(
                    asyncio.to_thread(groq_fn, prompt, model), timeout=28)
                if r and len(r) >= min_length:
                    _ai_breaker_record("groq", success=True)
                    return r
            except Exception:
                pass
        _ai_breaker_record("groq", success=False)
    # اگه به این‌جا رسیدیم یعنی نه Gemini نه هیچ‌کدوم از GROQ_MODELS جواب
    # قابل‌قبول ندادن — این خیلی مهم‌تر از یک warning ساکته: یعنی از این
    # لحظه به بعد، هر judge_relevance/judge_job_relevance که call_ai رو صدا
    # بزنه «could not evaluate — skipped for safety» برمی‌گردونه، یعنی
    # HEMEی نتایج جستجو (هم استاد هم شغل) رد می‌شن. این باید فوراً توی
    # لاگ قابل‌دیدن باشه، نه این‌که فقط یک رشته‌ی خالی بی‌صدا برگرده.
    logger.error("🔴 call_ai: هیچ AI providerای (نه Gemini نه هیچ مدل Groq) جواب قابل‌قبول نداد — "
                 "نتیجه‌ی این فراخوان خالیه، پس هر judge ای که از این استفاده کنه رد می‌شه "
                 "('could not evaluate — skipped for safety'). اگه این هشدار زیاد تکرار می‌شه، "
                 "یعنی الان GROQ_MODELS/GEMINI_API_KEY نیاز به بررسی دارن.")
    return ""

# ================================================================
# SCRAPER — پروفایل استاد
# ================================================================

HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36"}

def scrape(url: str) -> str:
    try:
        r = _safe_requests_get(url, headers=HEADERS, timeout=12)
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r.text, "html.parser")
            for t in soup(["script","style","nav","footer"]): t.decompose()
            return soup.get_text("\n", strip=True)[:4000]
        except ImportError:
            return re.sub(r"<[^>]+>", " ", r.text)[:4000]
    except _BlockedURLError as e:
        logger.warning(f"scrape: SSRF guard blocked '{url[:80]}': {e}")
        return f"[error: blocked url]"
    except Exception as e:
        return f"[error: {e}]"

def extract_resume_text(file_bytes: bytes, filename: str) -> str:
    """قبلاً وقتی کاربر رزومه رو به‌صورت فایل (PDF/Word) می‌فرستاد، فقط یک
    placeholder مثل "[FILE:id:name.pdf]" توی client['resume'] ذخیره می‌شد —
    یعنی هم تشخیص match استاد (judge_relevance) هم نوشتن ایمیل/کاور لتر،
    عملاً هیچ‌وقت محتوای واقعی رزومه رو نمی‌دیدن و صرفاً روی چند فیلد کوتاه
    فرم (رشته/مقطع/سابقه) کار می‌کردن. این تابع متن واقعی رزومه رو از PDF یا
    Word استخراج می‌کنه تا مچینگ و نوشتن ایمیل واقعاً بر اساس رزومه باشه."""
    ext = filename.lower().rsplit(".", 1)[-1] if "." in filename else ""
    try:
        if ext == "pdf":
            import pypdf
            reader = pypdf.PdfReader(io.BytesIO(file_bytes))
            text = "\n".join((p.extract_text() or "") for p in reader.pages)
        elif ext == "docx":
            import docx
            d = docx.Document(io.BytesIO(file_bytes))
            text = "\n".join(p.text for p in d.paragraphs)
        elif ext in ("txt", "md"):
            text = file_bytes.decode("utf-8", errors="ignore")
        else:
            # فرمت‌های قدیمی (.doc) یا ناشناخته پشتیبانی نمی‌شن — به‌جای کرش
            # یا حدس اشتباه، خالی برمی‌گردونیم تا کد صدازننده تصمیم بگیره.
            logger.warning(f"extract_resume_text: unsupported extension '{ext}' for {filename}")
            return ""
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        return text[:6000]
    except Exception as e:
        logger.warning(f"extract_resume_text({filename}): {e}")
        return ""

def extract_emails(text: str) -> list[str]:
    all_e = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", text)
    seen = set()
    all_e = [e for e in all_e if not (e in seen or seen.add(e))]
    edu = [e for e in all_e if any(d in e for d in [".edu",".ac.",".uni-"])]
    return edu if edu else all_e[:2]

_EMAIL_FORMAT_RE = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")

# دامنه‌های ایمیل موقت/یک‌بارمصرف — یک استاد واقعی هیچ‌وقت از این‌ها
# به‌عنوان ایمیل رسمی/تماس استفاده نمی‌کنه؛ همیشه رد می‌شن.
DISPOSABLE_EMAIL_DOMAINS = {
    "mailinator.com", "tempmail.com", "temp-mail.org", "guerrillamail.com",
    "guerrillamail.info", "10minutemail.com", "10minutemail.net", "yopmail.com",
    "yopmail.net", "trashmail.com", "throwawaymail.com", "fakeinbox.com",
    "sharklasers.com", "dispostable.com", "maildrop.cc", "getnada.com",
    "mintemail.com", "mohmal.com", "emailondeck.com", "tempinbox.com",
    "spamgourmet.com", "mailnesia.com", "mytemp.email", "moakt.com",
    "tempr.email", "burnermail.io", "33mail.com", "anonaddy.com",
    "mailsac.com", "inboxbear.com", "discard.email", "mailcatch.com",
}

# ایمیل‌های شخصی/عمومی — رد نمی‌شن (بعضی اساتید واقعاً از Gmail شخصی‌شون
# هم برای مکاتبه استفاده می‌کنن)، ولی برای «ایمیل رسمی استاد» معمولاً
# نشونه‌ی ضعیف‌تری نسبت به دامنه‌ی دانشگاهیه، پس امتیازش پایین میاد.
PERSONAL_EMAIL_PROVIDERS = {
    "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "icloud.com",
    "aol.com", "protonmail.com", "mail.com", "live.com", "msn.com",
    "yandex.com", "gmx.com", "zoho.com",
}

def classify_email_domain(email: str) -> dict:
    """دامنه‌ی ایمیل رو دسته‌بندی می‌کنه: university / personal / disposable
    / other. مثلاً اگه AI اشتباهاً «professor@gmail.com» بسازه، این تابع
    category='personal' برمی‌گردونه و بیرون از این تابع امتیاز match پایین
    میاد — چون این معمولاً ایمیل رسمی استاد نیست."""
    email  = (email or "").strip().lower()
    domain = email.rsplit("@", 1)[-1] if "@" in email else ""
    is_disposable = domain in DISPOSABLE_EMAIL_DOMAINS
    is_personal   = domain in PERSONAL_EMAIL_PROVIDERS
    is_university = (not is_personal) and any(d in domain for d in (".edu", ".ac.", ".uni-"))
    if is_disposable:
        category = "disposable"
    elif is_university:
        category = "university"
    elif is_personal:
        category = "personal"
    else:
        category = "other"
    return {"domain": domain, "category": category, "is_disposable": is_disposable,
            "is_personal": is_personal, "is_university": is_university}

# ── منبع مشترک برای «منبع ایمیل قابل‌اعتماد» ──────────────────────────
# قبلاً این لیست دو جای جدا (اینجا و compute_email_confidence پایین‌تر)
# با مقادیر متفاوت تعریف شده بود و از هم عقب افتاده بودن — یکی ۶ منبع
# قبول داشت، اون یکی فقط ۳ تا. الان هر دو از همین یک ثابت می‌خونن تا
# دیگه امکان drift نباشه.
STRONG_EMAIL_SOURCES = {"University Faculty Page", "Lab Website", "Department Directory",
                         "Faculty Directory", "OpenAlex", "Semantic Scholar"}

def _professor_verification_passed(prof: dict) -> tuple[bool, str]:
    """قبل از ارسال، باید حداقل یکی از این نشونه‌ها موجود باشه که این
    واقعاً یه هویت دانشگاهی فعلی و واقعیه، نه فقط یه اسم+ایمیل تنها از
    یه دیتابیس مقالات قدیمی: صفحه‌ی دانشکده/آزمایشگاه/دایرکتوری واقعاً
    پیدا و باز شده (Faculty Page/Lab/Department)، سیگنال پذیرش دانشجو
    (Open Position)، سیگنال بودجه/گرنت، یا فعالیت اخیر (مقاله‌ی نسبتاً
    جدید ≈ Last Updated). اگه هیچ‌کدوم نبود، ریسک ارسال به یه لید
    نامطمئن رو نمی‌کنیم. لیست‌های دستی کاربر (_manual_entry) از این چک
    معاف‌ن — کاربر خودش انتخاب کرده، بات نباید جلوشو بگیره."""
    if prof.get("_manual_entry"):
        return True, ""
    strong_page = prof.get("email_source") in STRONG_EMAIL_SOURCES
    # ── سیگنال مستقل: ایمیل دامنه‌ی دانشگاهی + حداقل یک مقاله‌ی واقعی
    # (نه لزوماً اخیر) توی یک دیتابیس آکادمیک پیدا شده ──────────────
    # چهارتا از پنج سیگنال بالا/پایین (faculty_page/accepting_students/
    # funding_signal/lab_page) فقط وقتی محاسبه می‌شن که pipeline
    # ایمیل‌یابی هفت‌مرحله‌ای اجرا شده باشه — و اون فقط وقتی اجرا می‌شه
    # که `if not prof.get("email")` باشه (یعنی از اول ایمیل نداشتیم).
    # برای اکثر اساتیدی که همون اول از OpenAlex/Semantic Scholar ایمیل
    # داشتن، این ۴ سیگنال هیچ‌وقت محاسبه نمی‌شن و همیشه False می‌مونن —
    # یعنی تنها راهشون برای قبول شدن recent_activity (مقاله زیر ۳ سال)
    # بود. این یعنی هر استاد باسابقه‌ای که همین اواخر منتشر نکرده، حتی
    # با ایمیل درست و تطابق پژوهشی عالی، رد می‌شد. این سیگنال جدید مستقل
    # از email_source است (که فقط توسط enrichment ست می‌شه) و به‌جاش
    # مستقیم از خودِ prof.get("email")/last_pub_year می‌خونه که برای
    # همه‌ی مسیرها (raw search یا enrichment) در دسترسه.
    email = (prof.get("email") or "").lower()
    domain = email.rsplit("@", 1)[-1] if "@" in email else ""
    academic_email = bool(domain) and (domain.endswith(".edu") or ".ac." in domain)
    has_real_publication_record = bool(prof.get("last_pub_year"))
    signals = {
        "faculty_page":       strong_page,
        "accepting_students": bool(prof.get("accepting_students")),
        "funding_signal":     bool(prof.get("has_funding")),
        "lab_page":           bool(prof.get("is_lab_page")),
        "recent_activity":    bool(prof.get("is_active")),
        "academic_email_with_pub_record": academic_email and has_real_publication_record,
    }
    if any(signals.values()):
        return True, ""
    return False, ("هیچ نشونه‌ی معتبری پیدا نشد (نه صفحه‌ی دانشکده/آزمایشگاه/"
                    "دایرکتوری، نه فعالیت اخیر، نه پذیرش دانشجو/بودجه، نه ایمیل "
                    "دانشگاهی همراه با سابقه‌ی مقاله)")

_BOUNCE_MARKERS = (
    "mailbox", "does not exist", "user unknown", "no such user",
    "550", "address not found", "recipient rejected", "undeliverable",
    "mailbox unavailable", "invalid recipient", "no mailbox",
)

def db_is_known_bad_email(email: str) -> tuple[bool, str]:
    """اگه این ایمیل قبلاً (برای هر کاربری، نه فقط همین کاربر) با خطای
    دائمی (bounce/mailbox not found/user unknown) شکست خورده، احتمالاً
    یک آدرس مرده/deprecated‌ست (مثلاً استاد جابه‌جا شده یا ایمیل عوض شده)
    — دوباره روش وقت و اعتبار SMTP تلف نکنیم."""
    email = (email or "").strip()
    if not email:
        return False, ""
    try:
        with _db() as c:
            rows = c.execute("""SELECT status FROM sent_emails
                WHERE professor_email=? AND status LIKE 'failed:%'
                ORDER BY id DESC LIMIT 5""", (email,)).fetchall()
        for r in rows:
            status_l = (r[0] or "").lower()
            if any(m in status_l for m in _BOUNCE_MARKERS):
                return True, "این ایمیل قبلاً bounce داده (احتمالاً غیرفعال/deprecated است)"
    except Exception as e:
        logger.debug(f"db_is_known_bad_email({email}): {e}")
    return False, ""

def db_mark_email_bounced(tid: int, email: str, reason: str = "imap_detected") -> int:
    """وقتی IMAP Bounce Watcher یه bounce واقعی توی صندوق کاربر پیدا کنه،
    رکورد sent_emails مربوطه رو از status='sent' به 'failed:bounced:...'
    تغییر می‌ده. از این به بعد db_is_known_bad_email بالا این آدرس رو
    برای *همه‌ی* کاربرها (نه فقط همین) به‌عنوان آدرس مرده می‌شناسه —
    چون آدرس خراب برای هرکسی که بهش ایمیل بزنه خرابه، نه فقط این کاربر.
    WHERE status='sent' یعنی idempotent است: اگه قبلاً همین رکورد
    bounced علامت خورده باشه، دوباره صدا زدن این تابع rowcount=0
    برمی‌گردونه و هیچ خطایی نمی‌ده."""
    email = (email or "").strip()
    if not email:
        return 0
    with _db() as c:
        cur = c.execute("""UPDATE sent_emails SET status=?
            WHERE telegram_id=? AND professor_email=? AND status='sent'""",
            (f"failed:bounced:{reason}", tid, email))
        return cur.rowcount

def db_get_imap_optin(tid: int) -> bool:
    with _db() as c:
        row = c.execute("SELECT imap_bounce_optin FROM career_profiles WHERE telegram_id=?",
                         (tid,)).fetchone()
    return bool(row and row[0])

def db_get_imap_optin_users() -> list[int]:
    """لیست کاربرهایی که صریحاً چک IMAP bounce رو روشن کردن — برای
    scheduler روزانه که هر کدوم رو جدا چک می‌کنه."""
    with _db() as c:
        rows = c.execute("SELECT telegram_id FROM career_profiles WHERE imap_bounce_optin=1").fetchall()
    return [r[0] for r in rows]

def db_set_imap_optin(tid: int, enabled: bool):
    """کاربر از منوی تنظیمات این رو صریحاً روشن/خاموش می‌کنه — پیش‌فرض
    همیشه خاموشه (ستون در migration v4 با DEFAULT 0 اضافه شده)."""
    with _db() as c:
        c.execute("""INSERT INTO career_profiles (telegram_id, imap_bounce_optin, updated_at)
            VALUES (?,?,?)
            ON CONFLICT(telegram_id) DO UPDATE SET
                imap_bounce_optin=excluded.imap_bounce_optin, updated_at=excluded.updated_at""",
            (tid, 1 if enabled else 0, str(datetime.now())))

# ================================================================
# IMAP BOUNCE WATCHER
# ================================================================
# نکته‌ی مهم درباره‌ی حریم خصوصی: این فقط دنبال پیام‌های *bounce*
# خودکار (Mailer Daemon/Postmaster) می‌گرده، نه جواب واقعی استادها —
# محتوای واقعی صندوق کاربر خونده/ذخیره/پردازش نمی‌شه، فقط برای شناسایی
# اینکه کدوم آدرس ایمیل قبلاً *همین کاربر* بهش فرستاده توی متن bounce
# ظاهر شده. با این‌حال چون این یعنی صندوق ایمیل شخصی کاربر باز می‌شه،
# فقط برای کاربرهایی اجرا می‌شه که db_set_imap_optin صریحاً روشنش کرده
# باشه (پیش‌فرض خاموش).
_IMAP_HOST_MAP = {
    "smtp.gmail.com":        "imap.gmail.com",
    "smtp.office365.com":    "outlook.office365.com",
    "smtp-mail.outlook.com": "outlook.office365.com",
    "smtp.mail.yahoo.com":   "imap.mail.yahoo.com",
}

def _imap_host_from_smtp(smtp_host: str) -> str:
    """اکثر ارائه‌دهنده‌ها هاست IMAP جدا از SMTP دارن (مثلاً Gmail:
    smtp.gmail.com در برابر imap.gmail.com) — چون بات فقط SMTP host رو
    از کاربر گرفته، این نگاشت رو برای ارائه‌دهنده‌های رایج نگه می‌داریم؛
    برای بقیه، حدس محافظه‌کارانه‌ی smtp.→imap. رو امتحان می‌کنیم."""
    h = (smtp_host or "").strip().lower()
    if h in _IMAP_HOST_MAP:
        return _IMAP_HOST_MAP[h]
    if h.startswith("smtp."):
        return "imap." + h[len("smtp."):]
    return h

BOUNCE_LOOKBACK_DAYS = 3

def _imap_search_bounce_uids(conn) -> list[bytes]:
    """UID پیام‌هایی که به‌احتمال زیاد یک bounce خودکارن — بر اساس
    فرستنده (mailer-daemon/postmaster) یا سابجکت‌های استاندارد bounce.
    هیچ‌جا محتوای پیام‌های دیگه خونده/فچ نمی‌شه، فقط همین لیست UID."""
    since = (datetime.now() - timedelta(days=BOUNCE_LOOKBACK_DAYS)).strftime("%d-%b-%Y")
    uids: set[bytes] = set()
    probes = [
        ("FROM", "mailer-daemon"), ("FROM", "postmaster"), ("FROM", "mail delivery"),
        ("SUBJECT", "undeliverable"), ("SUBJECT", "delivery status notification"),
        ("SUBJECT", "delivery failure"), ("SUBJECT", "returned mail"),
    ]
    for hdr, needle in probes:
        try:
            typ, data = conn.uid("search", None, f'(SINCE {since} {hdr} "{needle}")')
            if typ == "OK" and data and data[0]:
                uids.update(data[0].split())
        except Exception as e:
            logger.debug(f"IMAP bounce probe ({hdr}={needle}): {e}")
    return list(uids)

def _check_bounces_sync(tid: int, smtp_e: str, smtp_p: str, smtp_h: str,
                         already_sent: set[str]) -> int:
    """بخش sync — همیشه با asyncio.to_thread صدا زده می‌شه تا event loop
    اصلی حین I/O شبکه‌ی IMAP بلاک نشه. برمی‌گردونه: تعداد ایمیل‌هایی که
    این‌بار bounced علامت خوردن."""
    marked = 0
    imap_host = _imap_host_from_smtp(smtp_h)
    conn = imaplib.IMAP4_SSL(imap_host, timeout=REQ_TIMEOUT)
    try:
        conn.login(smtp_e, smtp_p)
        conn.select("INBOX", readonly=True)  # readonly: هیچ پرچمی رو تغییر نمی‌دیم
        for uid in _imap_search_bounce_uids(conn):
            try:
                typ, data = conn.uid("fetch", uid, "(BODY.PEEK[TEXT])")  # PEEK: پیام unread نمی‌شه
                if typ != "OK" or not data or not isinstance(data[0], tuple):
                    continue
                raw = data[0][1]
                body = raw.decode(errors="ignore") if isinstance(raw, (bytes, bytearray)) else str(raw)
                # فقط اگه *دقیقاً یک* آدرس از لیست «قبلاً براش فرستادیم» توی
                # متن bounce دیده بشه، اون رو bounced علامت می‌زنیم — اگه صفر
                # یا بیشتر از یکی پیدا شد (یعنی مطمئن نیستیم کدومه)، رد
                # می‌شیم؛ false positive (خاموش کردن یه آدرس سالم) بدتر از
                # false negative (نادیده گرفتن یه bounce) است.
                hits = [addr for addr in already_sent if addr and addr.lower() in body.lower()]
                if len(hits) == 1:
                    marked += db_mark_email_bounced(tid, hits[0], reason="imap_detected")
            except Exception as e:
                logger.debug(f"[uid={tid}] IMAP bounce fetch uid={uid}: {e}")
                continue
    finally:
        try:
            conn.logout()
        except Exception:
            pass
    return marked

async def check_bounces_for_user(tid: int, smtp_e: str, smtp_p: str, smtp_h: str) -> int:
    """نقطه‌ی ورود async. اگه کاربر SMTP وارد نکرده یا هیچ ایمیلی هنوز
    نفرستاده، چیزی چک نمی‌شه. هر خطای IMAP (پسورد عوض شده، هاست اشتباه،
    ۲FA بدون App Password و...) فقط لاگ می‌شه — هیچ‌وقت جریان اصلی بات
    یا مانیتورهای دیگه رو نمی‌شکنه."""
    if not (smtp_e and smtp_p and smtp_h):
        return 0
    already_sent = await asyncio.to_thread(db_get_sent_email_set, tid)
    if not already_sent:
        return 0
    try:
        return await asyncio.to_thread(_check_bounces_sync, tid, smtp_e, smtp_p, smtp_h, already_sent)
    except Exception as e:
        logger.warning(f"[uid={tid}] IMAP bounce check failed (host/creds/2FA؟): {e}")
        return 0

def validate_email_deliverability(email: str) -> tuple[bool, str]:
    """قبل از ارسال واقعی، سه چیز رو چک می‌کنه: فرمت صحیح، وجود دامنه، و
    در صورت امکان رکورد MX. عمداً محافظه‌کارانه‌ست: اگه dnspython نصب
    نباشه یا DNS lookup به هر دلیلی (بلاک شدن DNS خروجی روی بعضی هاست‌های
    رایگان، تایم‌اوت و...) شکست بخوره، جلوی ارسال رو نمی‌گیریم — این چک
    فقط برای رد کردن ایمیل‌های واضحاً خراب/جعلیه، نه یک ضمانت قطعی؛ یک
    false positive نباید باعث از دست رفتن یک لید واقعی بشه."""
    email = (email or "").strip()
    if not _EMAIL_FORMAT_RE.match(email):
        return False, "فرمت ایمیل نامعتبر است"
    domain = email.rsplit("@", 1)[-1].lower()

    # ── چک Disposable Email — قبل از هر چیز، این‌ها همیشه رد می‌شن ──
    if domain in DISPOSABLE_EMAIL_DOMAINS:
        return False, "دامنه‌ی disposable/موقت است — قطعاً ایمیل واقعی استاد نیست"

    # ── چک ایمیل Deprecated/Bounce شده ──────────────────────────────
    is_bad, bad_reason = db_is_known_bad_email(email)
    if is_bad:
        return False, bad_reason

    try:
        import dns.resolver
        try:
            answers = dns.resolver.resolve(domain, "MX", lifetime=6)
            if answers:
                return True, f"MX record معتبر ({len(answers)} رکورد)"
        except Exception:
            try:
                dns.resolver.resolve(domain, "A", lifetime=6)
                return True, "بدون MX ولی دامنه معتبر است (A record)"
            except Exception:
                return False, "نه MX و نه A record برای این دامنه پیدا نشد — احتمالاً دامنه واقعی نیست"
    except ImportError:
        # dnspython نصب نیست — فقط چک می‌کنیم دامنه اصلاً resolve می‌شه یا نه
        try:
            socket.getaddrinfo(domain, None)
            return True, "دامنه معتبر است (بدون بررسی دقیق MX — پکیج dnspython نصب نیست)"
        except Exception:
            return False, "دامنه resolve نشد — احتمالاً دامنه واقعی نیست"
    except Exception as e:
        logger.debug(f"MX check for {domain}: {e}")
        return True, "بررسی DNS ناموفق بود — محافظه‌کارانه رد نشد"

def fetch_abstract(title: str) -> str:
    # Semantic Scholar (رایگان)
    try:
        r = requests.get("https://api.semanticscholar.org/graph/v1/paper/search",
            params={"query": title, "fields": "abstract", "limit": 1},
            timeout=REQ_TIMEOUT)
        data = r.json().get("data", [])
        if data and data[0].get("abstract"):
            return data[0]["abstract"][:1200]
    except Exception: pass
    # arXiv
    try:
        r = requests.get(
            f"https://export.arxiv.org/api/query?search_query=all:{requests.utils.quote(title)}&max_results=1",
            timeout=REQ_TIMEOUT)
        m = re.search(r"<summary>(.*?)</summary>", r.text, re.DOTALL)
        if m:
            ab = re.sub(r"\s+", " ", m.group(1)).strip()
            if len(ab) > 80: return ab[:1200]
    except Exception: pass
    return ""

def fetch_professor_publications(name: str, university: str = "", limit: int = 5) -> list[dict]:
    """برخلاف fetch_abstract (که فقط یک مقاله با اسم مشابه رو حدس می‌زنه)،
    این تابع با Semantic Scholar Author API واقعاً پروفایل *همین* استاد رو
    پیدا می‌کنه (با تطبیق دانشگاه در صورت امکان) و جدیدترین مقالات واقعی‌اش
    رو برمی‌گردونه. این پایه‌ی اصلی تشخیص match واقعیه — بدون این، هیچ
    قضاوتی درباره‌ی مرتبط بودن قابل اعتماد نیست."""
    if not name or len(name.strip()) < 3:
        return []
    try:
        r = _http_get_retry("https://api.semanticscholar.org/graph/v1/author/search",
            params={"query": name, "fields": "name,affiliations,paperCount"},
            timeout=REQ_TIMEOUT)
        if r.status_code == 429:
            # حتی بعد از retryهای داخلی _http_get_retry هنوز 429 — این منبع
            # واقعاً الان در دسترس نیست، ولی حداقل چند بار امتحان شده
            return []
        candidates = (r.json() or {}).get("data", [])
        if not candidates:
            return []

        author = None
        if university:
            uni_l = university.lower()
            for c in candidates:
                affs = " ".join(c.get("affiliations") or []).lower()
                if uni_l and uni_l in affs:
                    author = c
                    break
        if not author:
            # بدون تطابق دانشگاه، محافظه‌کارانه‌ترین انتخاب: نویسنده‌ای که
            # بیشترین مقاله رو داره (احتمال هم‌نامی اشتباه رو کم می‌کنه چون
            # پروفایل‌های کم‌فعالیت/جعلی معمولاً paperCount پایینی دارن)
            author = max(candidates, key=lambda c: c.get("paperCount", 0) or 0)

        author_id = author.get("authorId")
        if not author_id:
            return []

        r2 = _http_get_retry(f"https://api.semanticscholar.org/graph/v1/author/{author_id}/papers",
            params={"fields": "title,abstract,year", "limit": limit},
            timeout=REQ_TIMEOUT)
        if r2.status_code == 429:
            return []
        papers = (r2.json() or {}).get("data", [])
        papers = sorted(papers, key=lambda p: p.get("year") or 0, reverse=True)
        out = []
        for p in papers[:limit]:
            if p.get("title"):
                out.append({
                    "title": p["title"],
                    "abstract": (p.get("abstract") or "")[:800],
                    "year": p.get("year"),
                })
        return out
    except Exception as e:
        logger.warning(f"fetch_professor_publications: {e}")
        return []

def _parse_relevance_json(ai_text: str) -> dict:
    """پارس مشترک خروجی JSON مدل — چه از روی مقالات ساختاریافته بیاد چه
    از روی متن صفحه‌ی وب، فرمت خروجی و منطق اعتبارسنجی یکسانه.
    فیلدهای skills_match/experience_match/visa_match/location_match/
    education_match فقط برای judge_job_relevance معنی دارن؛ اگه مدل
    برنگردونه (مسیر اساتید)، پیش‌فرض همون score کلیه — یعنی فراخوان‌های
    قبلی نمی‌شکنن.

    فیلدهای distance/thesis_overlap/topic_fit/methods_fit/publications_fit
    فقط برای مسیر اساتید (judge_relevance / judge_relevance_from_webpage)
    معنی دارن — پیاده‌سازی «Research Distance» و «Fit Matrix». برای
    judge_job_relevance مدل این فیلدها رو برنمی‌گردونه، پس پیش‌فرض‌های
    بی‌ضرر می‌گیرن و هیچ مسیر قبلی رو نمی‌شکنن."""
    try:
        data = _extract_json_object(ai_text) or {}
        score_raw = data.get("score", 0)
        score = int(score_raw) if str(score_raw).strip().lstrip("-").isdigit() else 0
        score = max(0, min(100, score))
        def _sub(key):
            v = data.get(key, score)
            return max(0, min(100, int(v))) if str(v).strip().lstrip("-").isdigit() else score
        distance_raw = str(data.get("distance", "")).strip().upper().replace(" ", "_")
        if distance_raw not in {"VERY_CLOSE", "ADJACENT", "MODERATE", "FAR"}:
            # اگه مدل distance برنگردونه (یا مسیر job relevance باشه که
            # اصلاً نمی‌خوایمش)، از روی خودِ score یک تخمین محافظه‌کارانه
            # می‌سازیم — بهتر از خالی گذاشتنش برای نمایش به کاربر.
            distance_raw = ("VERY_CLOSE" if score >= 85 else
                             "ADJACENT" if score >= 65 else
                             "MODERATE" if score >= 40 else "FAR")
        overlap_raw = str(data.get("thesis_overlap", "")).strip().capitalize()
        if overlap_raw not in {"High", "Medium", "Low"}:
            overlap_raw = {"VERY_CLOSE": "High", "ADJACENT": "Medium",
                            "MODERATE": "Medium", "FAR": "Low"}[distance_raw]
        return {
            "match": bool(data.get("match", False)),
            "score": score,
            "reason": str(data.get("reason", ""))[:200],
            "skills_match": _sub("skills_match"),
            "experience_match": _sub("experience_match"),
            "visa_match": _sub("visa_match"),
            "location_match": _sub("location_match"),
            "education_match": _sub("education_match"),
            "distance": distance_raw,
            "thesis_overlap": overlap_raw,
            "topic_fit": _sub("topic_fit"),
            "methods_fit": _sub("methods_fit"),
            "publications_fit": _sub("publications_fit"),
        }
    except Exception as e:
        logger.warning(f"relevance JSON parse error: {e}")
        return {"match": False, "score": 0, "reason": "could not evaluate — skipped for safety",
                "skills_match": 0, "experience_match": 0, "visa_match": 0, "location_match": 0,
                "education_match": 0,
                "distance": "FAR", "thesis_overlap": "Low",
                "topic_fit": 0, "methods_fit": 0, "publications_fit": 0}

async def judge_relevance(client: dict, prof: dict, papers: list[dict]) -> dict:
    """قضاوت سخت‌گیرانه‌ی AI درباره‌ی اینکه آیا پژوهش واقعی این استاد با
    رزومه/رشته‌ی کاربر واقعاً هم‌پوشانی معنادار داره یا نه — نه یه ارتباط
    سطحی و کلی. همیشه JSON ساختاریافته برمی‌گردونه (نه متن آزاد) تا
    parse کردنش قابل‌اعتماد باشه، مستقل از سبک نوشتاری مدل.

    به‌جای یک قضاوت دودویی خشک (match/no-match)، از مدل «Research
    Distance» هم می‌خوایم — VERY_CLOSE/ADJACENT/MODERATE/FAR — تا
    استادهای interdisciplinary (که موضوعشون دقیقاً یکی نیست ولی هم‌پوشانی
    واقعی داره — مثلاً یک متقاضی Medical AI با استاد Computer Vision) به
    اشتباه به‌خاطر «رشته‌ی دقیقاً یکی نیست» رد نشن. FAR یعنی واقعاً
    بی‌ربط (مثل مثال Art History که قبلاً بود)؛ MODERATE/ADJACENT یعنی
    هم‌پوشانی واقعی هست، فقط از زاویه‌ی متفاوت."""
    if not papers:
        return {"match": False, "score": 0, "reason": "no real publications found for this professor",
                "distance": "FAR", "thesis_overlap": "Low",
                "topic_fit": 0, "methods_fit": 0, "publications_fit": 0}

    papers_txt = "\n".join(
        f"- {p['title']} ({p.get('year','?')}): {p.get('abstract','')[:300]}" for p in papers)
    prompt = f"""You are an academic-fit reviewer for PhD/research-position cold emails.
Judge how closely this professor's ACTUAL research (shown below) relates to the applicant's
field/background — not just whether it's an exact topical match.

Classify the relationship as a RESEARCH DISTANCE, not a strict yes/no:
- VERY_CLOSE: same sub-field, near-identical research focus.
- ADJACENT: different sub-field but genuine, substantive overlap (e.g. a Medical AI applicant and a
  Computer Vision professor — different application domain, real methodological overlap).
- MODERATE: broader/looser overlap — shared tools, techniques, or adjacent problem space, but the
  applicant would need to bridge a real gap for a thesis to emerge.
- FAR: no genuine connection — reject. Example: a Computer Science/AI applicant matched to an Art
  History professor just because both are "academics".
Only VERY_CLOSE, ADJACENT, and MODERATE should ever produce match=true (when there's genuine
potential thesis overlap); FAR must always be match=false. Be honest about distance — don't force
FAR fields into ADJACENT, but don't reject genuinely interdisciplinary fits either.

Applicant field: {client.get('field','')}
Applicant education: {client.get('education','')}
Applicant resume/background: {(client.get('resume','') or '')[:600]}
Applicant goal: {client.get('visa_type','')}

Professor: {prof.get('name','')}
Professor's actual recent publications:
{papers_txt}

Respond with STRICT JSON only, nothing else, exactly this shape (all fields required):
{{"match": true, "score": 0, "reason": "one short sentence",
  "distance": "VERY_CLOSE|ADJACENT|MODERATE|FAR", "thesis_overlap": "High|Medium|Low",
  "topic_fit": 0, "methods_fit": 0, "publications_fit": 0}}"""

    ai = await call_ai(prompt, min_length=15)
    return _parse_relevance_json(ai)

async def analyze_cv_quality(client: dict) -> dict | None:
    """AI CV Analyzer — قبل از شروع جستجو/ارسال، رزومه رو سریع تحلیل
    می‌کنه: نقاط ضعف، مقالات کم/نداشته، مهارت‌های کلیدی که کمه، و یک
    پیشنهاد بهبود کوتاه. یک‌بار در طول کل عملیات اجرا می‌شه (نه هر ایمیل)
    چون تحلیل کل رزومه‌ست، نه مخصوص یه استاد خاص.

    کاملاً fail-safe و غیرمسدودکننده: هر خطایی (رزومه خیلی کوتاه، AI جواب
    نده، فرمت خراب، تایم‌اوت) → None برمی‌گردونه؛ caller بدون این تحلیل،
    عادی جستجو/ارسال رو شروع می‌کنه — این قابلیت هیچ‌وقت کاربر رو مجبور
    به هیچ کاری نمی‌کنه یا جلوی ارسال رو نمی‌گیره، فقط یه نکته‌ی اطلاعاتیه."""
    resume = (client.get("resume") or "").strip()
    if not resume or len(resume) < 50:
        return None
    try:
        prompt = f"""You are reviewing a CV/resume for a student applying to PhD/research positions or jobs in: {client.get('field','')}

Resume text:
{resume[:2500]}

Publications claimed: {client.get('publications','0')}
Skills listed: {client.get('skills') or '(none provided)'}

Give a quick, honest quality assessment. Respond with STRICT JSON only, nothing else, exactly this shape:
{{"strength": "weak" or "moderate" or "strong",
  "missing_publications": true/false,
  "missing_skills": ["skill or area commonly expected but missing", ...] (max 3, empty list if none),
  "improvement_tip": "one short, concrete, actionable sentence"}}"""
        ai = await call_ai(prompt, min_length=15)
        data = _extract_json_object(ai)
        if not data:
            return None
        strength = str(data.get("strength", "")).lower()
        if strength not in ("weak", "moderate", "strong"):
            strength = "moderate"
        return {
            "strength": strength,
            "missing_publications": bool(data.get("missing_publications")),
            "missing_skills": [str(s)[:40] for s in (data.get("missing_skills") or [])][:3],
            "improvement_tip": str(data.get("improvement_tip", ""))[:200],
        }
    except Exception as e:
        logger.warning(f"analyze_cv_quality: {e}")
        return None

async def suggest_related_fields(field: str, resume: str = "") -> list[str]:
    """با توجه به رشته‌ی اصلی (و رزومه، اگه باشه)، چند زیررشته/حوزه‌ی
    مرتبط پیشنهاد می‌ده تا جستجوی استاد محدود به یه عبارت باریک نمونه و
    اساتید بیشتری پیدا بشن — دقیقاً همون قابلیت «AI Recommendation».

    نکته‌ی مهم برای پایداری: این قابلیت هیچ‌وقت نباید جلوی جستجوی اصلی رو
    بگیره یا باعث کرش/توقف بشه — برای همین کاملاً در try/except پیچیده
    شده و در هر نوع خطا (AI جواب نده، فرمت خراب باشه، تایم‌اوت بخوره)
    فقط یه لیست خالی برمی‌گردونه؛ caller با همون رشته‌ی اصلی، بدون این
    پیشنهادها، به کارش ادامه می‌ده."""
    if not field or not field.strip():
        return []
    try:
        cv_instruction = (
            f"\nApplicant's CV/resume (use this to find MORE SPECIFIC, niche terms the "
            f"applicant actually works with — e.g. if the CV mentions a specific technique "
            f"like 'RWKV' or 'Federated Learning', include that and its close neighbors, not "
            f"just generic terms for '{field}'):\n{resume[:600]}"
            if resume else ""
        )
        prompt = f"""Given this academic/research field a student wants to search professors in: "{field}"
{cv_instruction}

Suggest 6 to 10 closely-related sub-fields or adjacent research specializations that a
professor search should ALSO include, to find more relevant faculty. Be specific and
genuinely adjacent (e.g. for "Machine Learning": "Reinforcement Learning", "Computer
Vision", "Explainable AI", "Time Series Forecasting", "Representation Learning" — not
generic terms like "Computer Science" or "Engineering"). Order them from most to least
specific/relevant to the applicant's actual background if a CV was given.

Respond with STRICT JSON only, nothing else, exactly this shape:
{{"related": ["field1", "field2", "field3"]}}"""
        ai = await call_ai(prompt, min_length=15)
        data = _extract_json_object(ai) or {}
        related = data.get("related", [])
        if not isinstance(related, list):
            return []
        cleaned = [str(r).strip() for r in related if str(r).strip()]
        # اگه AI دقیقاً همون رشته‌ی اصلی رو هم توی پیشنهادها تکرار کرد، حذفش کن
        cleaned = [r for r in cleaned if r.lower() != field.strip().lower()]
        return cleaned[:10]
    except Exception as e:
        logger.warning(f"suggest_related_fields: {e}")
        return []

def _reconstruct_openalex_abstract(inv_index: dict) -> str:
    """OpenAlex به‌جای متن خام abstract، یک inverted index می‌ده (کلمه ->
    لیست موقعیت‌ها) — این تابع متن اصلی رو از روش بازمی‌سازه."""
    if not inv_index:
        return ""
    try:
        positions = {}
        for word, idxs in inv_index.items():
            for i in idxs:
                positions[i] = word
        return " ".join(positions[i] for i in sorted(positions))[:800]
    except Exception:
        return ""

def fetch_professor_publications_openalex(name: str, university: str = "", limit: int = 5) -> list[dict]:
    """منبع دوم — OpenAlex. پوشش رشته‌های غیرفنی (هنر، حقوق، علوم انسانی)
    توی این سایت خیلی بهتر از Semantic Scholar ئه، برای همین وقتی منبع
    اول چیزی پیدا نکرد، سراغ این می‌ریم قبل از اینکه به صفحه‌ی وب برسیم."""
    if not name or len(name.strip()) < 3:
        return []
    try:
        r = _http_get_retry("https://api.openalex.org/authors",
            params={"search": name, "per-page": 5}, timeout=REQ_TIMEOUT)
        if r.status_code == 429:
            return []
        candidates = (r.json() or {}).get("results", [])
        if not candidates:
            return []

        author = None
        if university:
            uni_l = university.lower()
            for c in candidates:
                inst = ((c.get("last_known_institutions") or [{}])[0] or {}).get("display_name", "")
                if uni_l and uni_l in inst.lower():
                    author = c
                    break
        if not author:
            author = max(candidates, key=lambda c: c.get("works_count", 0) or 0)

        author_id = author.get("id", "")
        if not author_id:
            return []

        r2 = _http_get_retry("https://api.openalex.org/works",
            params={"filter": f"author.id:{author_id}",
                    "sort": "publication_date:desc", "per-page": limit},
            timeout=REQ_TIMEOUT)
        if r2.status_code == 429:
            return []
        works = (r2.json() or {}).get("results", [])
        out = []
        for w in works[:limit]:
            title = w.get("title") or w.get("display_name") or ""
            if not title:
                continue
            out.append({
                "title": title,
                "abstract": _reconstruct_openalex_abstract(w.get("abstract_inverted_index")),
                "year": w.get("publication_year"),
            })
        return out
    except Exception as e:
        logger.warning(f"fetch_professor_publications_openalex: {e}")
        return []

# ================================================================
# DBLP — منبع اختصاصی CS (خلأ واقعی نسبت به دیاگرام اصلی)
# ================================================================
# DBLP دقیق‌ترین و به‌روزترین دیتابیس فهرست انتشارات برای رشته‌های
# علوم کامپیوتره — پوششش برای venueهای کنفرانسی CS (که OpenAlex/
# Semantic Scholar گاهی دیر index می‌کنن) معمولاً بهتره. API عمومی و
# بدون کلید داره: https://dblp.org/faq/13501473.html
DBLP_CS_HINTS = (
    "computer", "software", "machine learning", "artificial intelligence",
    "data science", "algorithm", "networking", "cybersecurity", "security",
    "کامپیوتر", "نرم‌افزار", "هوش مصنوعی", "یادگیری ماشین", "الگوریتم",
    "شبکه", "امنیت", "داده",
)

def _looks_like_cs_field(field: str) -> bool:
    f = (field or "").lower()
    return any(h in f for h in DBLP_CS_HINTS)

def fetch_professor_publications_dblp(name: str, limit: int = 5) -> list[dict]:
    """آخرین مقالات یک استاد رو از DBLP برمی‌گردونه — مکمل
    fetch_professor_publications_openalex برای وقتی OpenAlex برای اون
    استاد ضعیف/خالیه (خیلی معمول برای CS، چون DBLP سریع‌تر venueهای
    کنفرانسی رو index می‌کنه)."""
    if not name or len(name.strip()) < 3:
        return []
    try:
        r = _http_get_retry("https://dblp.org/search/publ/api",
            params={"q": name, "format": "json", "h": limit * 3},
            timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            return []
        hits = (((r.json() or {}).get("result") or {}).get("hits") or {}).get("hit") or []
        name_l = name.lower()
        out = []
        for h in hits:
            info = h.get("info") or {}
            authors_raw = (info.get("authors") or {}).get("author")
            if authors_raw is None:
                author_names = []
            elif isinstance(authors_raw, list):
                author_names = [a.get("text", "") if isinstance(a, dict) else str(a) for a in authors_raw]
            else:
                author_names = [authors_raw.get("text", "") if isinstance(authors_raw, dict) else str(authors_raw)]
            # فقط مقالاتی که واقعاً این استاد یکی از نویسنده‌هاشه (تطبیق
            # تقریبی روی نام) — چون DBLP بر اساس متن جستجو می‌کنه، نه فیلتر
            # دقیق نویسنده، و ممکنه مقالات نامرتبط هم برگردونه.
            if not any(name_l in a.lower() or a.lower() in name_l for a in author_names):
                continue
            title = info.get("title", "").rstrip(".")
            if not title:
                continue
            out.append({
                "title": title,
                "abstract": "",  # DBLP abstract نمی‌ده، فقط متادیتا
                "year": int(info["year"]) if str(info.get("year", "")).isdigit() else None,
            })
            if len(out) >= limit:
                break
        out.sort(key=lambda p: p.get("year") or 0, reverse=True)
        return out[:limit]
    except Exception as e:
        logger.warning(f"fetch_professor_publications_dblp: {e}")
        return []

def _dblp_search(field: str, count: int, country_q: str = "") -> list[dict]:
    """منبع کشف استاد از روی DBLP — برای رشته‌های CS، به‌عنوان یکی از
    منابع موازی توی _search_professors_impl صدا زده می‌شه. مثل Crossref،
    روی مقالات جستجو می‌کنه و نویسنده‌ها رو استخراج می‌کنه (DBLP author-
    search عمومی برای کشف بر اساس موضوع مناسب نیست، فقط برای اسم دقیق
    خوبه — برای همین از publ/api با کوئری موضوعی استفاده می‌کنیم)."""
    out: list[dict] = []
    if not _looks_like_cs_field(field):
        return out
    try:
        r = _http_get_retry("https://dblp.org/search/publ/api",
            params={"q": field, "format": "json", "h": min(count * 2, 100)},
            timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            return out
        hits = (((r.json() or {}).get("result") or {}).get("hits") or {}).get("hit") or []
        seen_names: set[str] = set()
        for h in hits:
            info = h.get("info") or {}
            authors_raw = (info.get("authors") or {}).get("author")
            if authors_raw is None:
                continue
            if isinstance(authors_raw, list):
                names = [a.get("text", "") if isinstance(a, dict) else str(a) for a in authors_raw]
            else:
                names = [authors_raw.get("text", "") if isinstance(authors_raw, dict) else str(authors_raw)]
            for nm in names:
                nm = re.sub(r"\s+\d+$", "", nm).strip()  # DBLP گاهی عدد یکتاسازی به اسم می‌چسبونه ("John Smith 0001")
                if not nm or nm in seen_names or len(nm.split()) < 2:
                    continue
                seen_names.add(nm)
                out.append({"name": nm, "email": "", "university": "",
                            "country": country_q, "openalex_id": f"DBLP_{nm[:20]}",
                            "url": "", "snippet": f"DBLP: {info.get('title','')[:80]}",
                            "papers": [info.get("title", "")]})
                if len(out) >= count:
                    return out
    except Exception as e:
        logger.warning(f"_dblp_search: {e}")
    return out

async def judge_relevance_from_webpage(client: dict, prof: dict, page_text: str) -> dict:
    """آخرین لایه — وقتی نه Semantic Scholar نه OpenAlex چیزی پیدا نکردن
    (معمولاً رشته‌های خیلی غیرفنی یا استادهای کمترشناخته‌شده). به‌جای رد
    خودکار، متن خام صفحه‌ی شخصی/دانشگاهی استاد رو مستقیم به AI می‌دیم.
    کیفیتش از دو منبع ساختاریافته پایین‌تره (صفحه ممکنه شلوغ/نامرتبط
    باشه) برای همین آستانه‌ی قضاوت رو محافظه‌کارانه نگه می‌داریم."""
    page_text = (page_text or "").strip()
    if not page_text or page_text.startswith("[error"):
        return {"match": False, "score": 0, "reason": "professor webpage not accessible",
                "distance": "FAR", "thesis_overlap": "Low",
                "topic_fit": 0, "methods_fit": 0, "publications_fit": 0}

    prompt = f"""You are an academic-fit reviewer for PhD/research-position cold emails.
No structured publication database entry was found for this professor, so you must judge based on
raw text scraped from their personal/university webpage below. This text may be messy or contain
navigation clutter — focus only on research-area content.
Judge how closely the professor's research (per the page) relates to the applicant's field — a
RESEARCH DISTANCE, not a strict yes/no: VERY_CLOSE (near-identical focus), ADJACENT (different
sub-field but genuine overlap — e.g. Medical AI applicant / Computer Vision professor), MODERATE
(looser overlap, real gap to bridge), or FAR (no genuine connection — reject). Only VERY_CLOSE,
ADJACENT, and MODERATE should ever produce match=true. If the page content is unclear or unrelated
to academia at all, answer FAR/false. Because page text is noisier than a structured publication
database, stay conservative when genuinely unsure — but don't reject real interdisciplinary fits.

Applicant field: {client.get('field','')}
Applicant education: {client.get('education','')}
Applicant resume/background: {(client.get('resume','') or '')[:600]}

Professor: {prof.get('name','')}
Raw webpage text (may be messy):
{page_text[:2500]}

Respond with STRICT JSON only, nothing else, exactly this shape (all fields required):
{{"match": true, "score": 0, "reason": "one short sentence",
  "distance": "VERY_CLOSE|ADJACENT|MODERATE|FAR", "thesis_overlap": "High|Medium|Low",
  "topic_fit": 0, "methods_fit": 0, "publications_fit": 0}}"""

    ai = await call_ai(prompt, min_length=15)
    return _parse_relevance_json(ai)

# ================================================================
# Rule-based Confidence Pre-Score (قبل از AI Verification)
# ================================================================
# قبلاً تنها لایه‌ی تأیید، _ai_verify_professor بود که همیشه (وقتی
# verdict.match=True) بعد از جمع‌آوریِ کامل سیگنال‌ها صدا زده می‌شد —
# یعنی گرون و کند، حتی برای کاندیدهایی که خیلی واضح ضعیف یا خیلی واضح
# قوی بودن. این تابع سبک، rule-based، و بدون هیچ درخواست شبکه‌ای، هر
# پروفایل رو به یک امتیاز ۰ تا ۱ نگاشت می‌کنه (ایمیل edu؟ مقاله‌ی ۲ سال
# اخیر؟ صفحه‌ی دانشگاهی تأیید شده؟ ...). فقط کاندیدهای «نامطمئن» (نه
# خیلی ضعیف نه خیلی قوی) به AI (کندتر/گرون‌تر) فرستاده می‌شن.
def _prof_confidence_prescore(prof: dict) -> tuple[float, str]:
    score = 0.0
    reasons = []

    email = (prof.get("email") or "").lower()
    if email:
        domain = email.split("@")[-1]
        if domain.endswith(".edu") or ".ac." in domain:
            score += 0.35; reasons.append("edu email")
        elif domain in {"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"}:
            score += 0.05; reasons.append("personal email only")
        else:
            score += 0.2; reasons.append("institutional-looking email")

    last_year = prof.get("last_pub_year")
    if last_year:
        age = datetime.now().year - last_year
        if age <= 2:
            score += 0.3; reasons.append("paper <=2yr")
        elif age <= 5:
            score += 0.15; reasons.append("paper <=5yr")
        else:
            reasons.append("paper stale (>5yr)")

    if prof.get("university"):
        score += 0.15; reasons.append("has university")
    if prof.get("url"):
        score += 0.1; reasons.append("has verified page/url")
    if prof.get("accepting_students"):
        score += 0.1; reasons.append("accepting students signal")

    score = max(0.0, min(1.0, score))
    return score, ", ".join(reasons) or "no signals"

# آستانه‌ها: زیر این → رد سریع بدون AI. بالای این → تأیید سریع بدون AI.
# بین این دو → نامطمئنه، بفرست به AI (که context بیشتری می‌بینه).
PRESCORE_REJECT_BELOW = 0.12
PRESCORE_ACCEPT_ABOVE = 0.75

async def _verify_with_prescore(prof: dict, client: dict, verdict: dict) -> dict:
    """جایگزین فراخوانی مستقیم _ai_verify_professor بعد از verdict.match:
    اول pre-score سبک رو چک می‌کنه؛ فقط وقتی نامطمئنه AI صدا زده می‌شه."""
    if not verdict.get("match"):
        return verdict
    pre_score, pre_reason = _prof_confidence_prescore(prof)
    prof["confidence_prescore"] = round(pre_score, 2)
    if pre_score < PRESCORE_REJECT_BELOW:
        verdict["match"]  = False
        verdict["score"]  = max(0, verdict.get("score", 0) - 30)
        verdict["reason"] = f"Pre-score رد کرد ({pre_score:.2f}: {pre_reason}) — AI صدا زده نشد"
        return verdict
    if pre_score > PRESCORE_ACCEPT_ABOVE:
        verdict["reason"] = f"Pre-score تأیید کرد ({pre_score:.2f}: {pre_reason}) — AI صدا زده نشد"
        return verdict
    ai_ok, ai_reason = await _ai_verify_professor(prof, client)
    if not ai_ok:
        verdict["match"]  = False
        verdict["score"]  = max(0, verdict.get("score", 0) - 30)
        verdict["reason"] = f"AI Verification: {ai_reason}"
    return verdict

async def _ai_verify_professor(prof: dict, client: dict) -> tuple[bool, str]:
    """
    مرحله AI Verification — حتی اگر Search نتیجه اشتباه بدهد،
    AI یک‌بار دیگر هویت، فعالیت و تناسب استاد را تأیید می‌کند.

    این مرحله بعد از جمع‌آوری اطلاعات (ایمیل، مقالات، صفحه دانشگاه)
    اجرا می‌شود — نه قبل از آن. تمام سیگنال‌ها با هم به AI داده می‌شوند.

    خروجی: (تأیید؟, دلیل)
    """
    name     = prof.get("name","")
    uni      = prof.get("university","")
    email    = prof.get("email","")
    papers   = prof.get("papers",[]) or []
    active   = prof.get("is_active")
    accepting= prof.get("accepting_students")
    funding  = prof.get("has_funding")
    email_src= prof.get("email_source","")

    papers_summary = "; ".join(p.get("title","") for p in papers[:3]) if papers else "no papers found"

    prompt = f"""You are a strict academic verification agent for a cold-email service.
Your job: determine if the following professor profile is REAL, ACTIVE, and genuinely in the correct field.
Reject if: name seems fake, no university affiliation, no recent papers, or field is clearly wrong.
Be decisive. Answer in 2 sentences max.

Professor name: {name}
University: {uni or "unknown"}
Email: {email or "not found"} (source: {email_src or "unknown"})
Recent papers: {papers_summary}
Active researcher (paper <3yr): {active}
Accepting students: {accepting}
Has funding/grants: {funding}
Applicant field: {client.get("field","")}

Reply with EXACTLY this JSON (nothing else):
{{"verified": true, "reason": "one sentence"}}"""

    try:
        ai = await asyncio.wait_for(call_ai(prompt, min_length=15), timeout=15)
        m  = re.search(r'\{.*?\}', ai or "", re.DOTALL)
        if m:
            data = json.loads(m.group(0))
            return bool(data.get("verified", False)), str(data.get("reason",""))[:150]
    except Exception as e:
        logger.debug(f"AI verify for {name}: {e}")

    # Fallback rule-based: اگر AI جواب نداد
    if not name or len(name.strip()) < 4:
        return False, "name too short or missing"
    if not email and not papers:
        return False, "no email and no papers found — cannot verify"
    if email and email.split("@")[-1].lower() in {"gmail.com","yahoo.com","hotmail.com"}:
        if not papers and not uni:
            return False, "only personal email, no papers, no university"
    return True, "rule-based: basic signals present"


async def get_professor_relevance(client: dict, prof: dict) -> tuple[dict, list[dict]]:
    """
    هماهنگ‌کننده‌ی چهارلایه:
    1. Semantic Scholar → مقالات ساختاریافته
    2. OpenAlex → مقالات ساختاریافته (fallback)
    3. Crossref → مقالات (برای رشته‌های غیرفنی)
    4. صفحه وب → آخرین راه

    همزمان: ایمیل استاد از یک pipeline ۷مرحله‌ای گرفته می‌شه (صفحه‌ی رسمی
    دانشگاه → سایت آزمایشگاه → دایرکتوری دانشکده → ORCID → Google Scholar →
    جستجوی عمومی → ResearchGate) و سیگنال «پذیرش دانشجو» هم به‌عنوان
    محصول جانبی همون صفحات جمع‌آوری می‌شه. فعال بودن استاد (آخرین سال
    انتشار مقاله) هم از همون papers ای که همین‌جا می‌گیریم محاسبه می‌شه —
    بدون هیچ درخواست شبکه‌ی اضافه."""
    prof_name = prof.get("name","")
    prof_uni  = prof.get("university","")

    # ── ایمیل + سیگنال «پذیرش دانشجو» + Funding Detector (pipeline ۷مرحله‌ای) ──
    if not prof.get("email") and prof_name:
        try:
            signals = await asyncio.wait_for(
                asyncio.to_thread(find_professor_email_and_signals,
                                  prof_name, prof_uni, prof.get("openalex_id",""),
                                  prof.get("url","")),
                timeout=25)
            if signals.get("email"):
                prof["email"] = signals["email"]
                prof["email_source"] = signals.get("email_source","")
                logger.debug(f"Email found for {prof_name} via {signals.get('email_source')}: {signals['email']}")
            if signals.get("accepting_students"):
                prof["accepting_students"] = True
                prof["accepting_evidence"] = signals.get("accepting_evidence","")
            # ── Funding Detector ────────────────────────────────────
            # همون صفحه‌ای که برای ایمیل گرفتیم رو برای سیگنال گرنت/بودجه/RA
            # هم چک کردیم — بدون هیچ درخواست شبکه‌ی اضافه.
            if signals.get("has_funding"):
                prof["has_funding"] = True
                prof["funding_evidence"] = signals.get("funding_evidence","")
            # ── لیبل‌های اختیاری جدید — Availability Score + Funding
            # Intelligence — فقط کپی از signals، هیچ منطق match/filter رو
            # لمس نمی‌کنن ──────────────────────────────────────────────
            if signals.get("availability_score"):
                prof["availability_score"] = signals["availability_score"]
                prof["availability_evidence"] = signals.get("availability_evidence", [])
            if signals.get("funding_intelligence"):
                prof["funding_intelligence"] = signals["funding_intelligence"]
            # ── Lab Analyzer ─────────────────────────────────────────
            # فقط وقتی که واقعاً صفحه‌ی آزمایشگاه (نه یه نتیجه‌ی نامطمئن
            # جستجو) با موفقیت گرفته شده، تحلیل عمیق‌ترش می‌کنیم — گاهی
            # خود آزمایشگاه اعلام می‌کنه دانشجو می‌گیره حتی اگه صفحه‌ی
            # شخصی استاد چیزی ننوشته باشه.
            if signals.get("is_lab_page") and signals.get("lab_page_text"):
                lab_info = await analyze_lab_page(signals["lab_page_text"], prof_name)
                if lab_info:
                    prof["lab_analysis"] = lab_info
                    if lab_info.get("title") and not prof.get("title"):
                        prof["title"] = lab_info["title"]
                    if lab_info.get("department") and not prof.get("department"):
                        prof["department"] = lab_info["department"]
                    if lab_info.get("research_interests") and not prof.get("research_interests"):
                        prof["research_interests"] = lab_info["research_interests"]
                    if lab_info.get("open_positions"):
                        prof["accepting_students"] = True
                        if not prof.get("accepting_evidence"):
                            prof["accepting_evidence"] = "Lab Analyzer: open positions listed"
                    if lab_info.get("funding_status") and not prof.get("has_funding"):
                        prof["has_funding"] = True
                        prof["funding_evidence"] = lab_info["funding_status"]
        except Exception as e:
            logger.debug(f"email pipeline for {prof_name}: {e}")

    def _with_activity(verdict, papers):
        """آخرین سال انتشار رو از papers می‌گیره (هر سه منبع year دارن) و
        فعال/غیرفعال بودن استاد رو تخمین می‌زنه — بدون هیچ درخواست اضافه."""
        years = [p["year"] for p in papers if p.get("year")]
        if years:
            last_year = max(years)
            prof["last_pub_year"] = last_year
            prof["is_active"] = (datetime.now().year - last_year) <= 3
            verdict["last_pub_year"] = last_year
            verdict["is_active"] = prof["is_active"]
        # ── Publication Trend — لیبل اختیاری، deterministic، از همون
        # papers ای که همین‌جا داریم (بدون درخواست شبکه‌ی اضافه) ────────
        try:
            trend = analyze_publication_trend(papers)
            if trend:
                prof["publication_trend"] = trend
        except Exception as e:
            logger.debug(f"publication trend for {prof.get('name','?')}: {e}")
        return verdict

    def _with_similarity(verdict, papers):
        """Research Match — طبق درخواست صریح کاربر (Hybrid Search)، دیگه
        فقط یک ترکیب ساده‌ی ۸۰٪AI+۲۰٪شباهت نیست. الان از چهار سیگنال
        مستقل استفاده می‌کنه: Keyword(TF-IDF) 15% + Embedding 40% +
        Paper-level similarity 25% (از Paper-to-CV Matching) + LLM
        verification (judge_relevance) 20% — نگاه کن به
        compute_hybrid_research_fit. جزئیات تطابق هر موضوع CV با کدوم
        مقاله‌ی خاص هم این‌جا محاسبه و روی verdict ذخیره می‌شه تا در پیام
        نهایی به کاربر نشون داده بشه (نه فقط یک عدد کلی).

        رد سریع «شباهت نزدیک صفر» (وقتی AI گفته match=True ولی هیچ
        هم‌پوشانی کلیدواژه‌ای واقعی نیست — مثلاً Computer Vision در برابر
        Time Series Forecasting) همچنان قبل از محاسبه‌ی سنگین‌تر
        hybrid/embedding اجرا می‌شه، تا هزینه‌ی API روی کاندیدهای واضحاً
        رد‌شده هدر نره."""
        similarity = compute_research_similarity(client, prof, papers)
        verdict["similarity_score"] = round(similarity, 3)
        if verdict.get("match") and similarity < 0.02:
            verdict["match"] = False
            verdict["score"] = min(verdict.get("score", 0), 25)
            verdict["reason"] = (f"Research Match مستقل رد کرد (شباهت متنی نزدیک صفر: {similarity:.3f}) — "
                                  f"AI گفته بود مرتبطه ولی همپوشانی کلیدواژه‌ای واقعی پیدا نشد. "
                                  f"قبلی: {verdict.get('reason','')[:80]}")
            return verdict

        try:
            paper_cv_match = compute_paper_cv_matches(client, papers)
            hybrid = compute_hybrid_research_fit(
                client, prof, papers, llm_score_0_100=verdict.get("score", 0),
                paper_cv_match=paper_cv_match)
            verdict["score"] = hybrid["score"]
            verdict["hybrid_fit"] = hybrid
            verdict["paper_cv_match"] = paper_cv_match
        except Exception as e:
            # fail-safe: اگه Hybrid Search (embedding API و غیره) هر مشکلی
            # داشت، به همون ترکیب قدیمی ۸۰٪AI+۲۰٪شباهت TF-IDF برمی‌گردیم —
            # هیچ‌وقت این لایه نباید کل ارزیابی استاد رو خراب/متوقف کنه.
            logger.debug(f"compute_hybrid_research_fit failed for {prof.get('name','?')}: {e}")
            blended = round(verdict.get("score", 0) * 0.8 + similarity * 100 * 0.2)
            verdict["score"] = max(0, min(100, blended))
        return verdict

    # ── Semantic Scholar ──────────────────────────────────────────
    papers = await asyncio.to_thread(
        fetch_professor_publications, prof_name, prof_uni)
    if papers:
        verdict = await judge_relevance(client, prof, papers)
        verdict["source"] = "semantic_scholar"
        verdict = _with_activity(verdict, papers)
        verdict = _with_similarity(verdict, papers)
        # ── AI Verification (مرحله ۲ — بعد از جمع‌آوری همه سیگنال‌ها) ──
        verdict = await _verify_with_prescore(prof, client, verdict)
        return verdict, papers

    # ── OpenAlex ─────────────────────────────────────────────────
    papers = await asyncio.to_thread(
        fetch_professor_publications_openalex, prof_name, prof_uni)
    if papers:
        verdict = await judge_relevance(client, prof, papers)
        verdict["source"] = "openalex"
        verdict = _with_activity(verdict, papers)
        verdict = _with_similarity(verdict, papers)
        verdict = await _verify_with_prescore(prof, client, verdict)
        return verdict, papers

    # ── DBLP (برای CS — وقتی OpenAlex/Semantic Scholar برای این استاد
    # ضعیف/خالی بودن، که برای venueهای کنفرانسی CS خیلی معموله چون این دو
    # منبع دیرتر index می‌کنن. مکمل مستقیم نکته‌ی «آخرین مقاله برای ایمیل»). ──
    if prof_name and _looks_like_cs_field(prof.get("field", "") or client.get("field", "")):
        dblp_papers = await asyncio.to_thread(fetch_professor_publications_dblp, prof_name)
        if dblp_papers:
            verdict = await judge_relevance(client, prof, dblp_papers)
            verdict["source"] = "dblp"
            verdict = _with_activity(verdict, dblp_papers)
            verdict = _with_similarity(verdict, dblp_papers)
            verdict = await _verify_with_prescore(prof, client, verdict)
            return verdict, dblp_papers

    # ── Crossref (برای رشته‌های غیرفنی / پروفایل‌های کمتر شناخته‌شده) ──
    if prof_name:
        cr_papers = await asyncio.wait_for(
            asyncio.to_thread(_fetch_crossref_papers, prof_name, 3),
            timeout=8)
        if cr_papers and len(cr_papers) > 0:
            verdict = await judge_relevance(client, prof, cr_papers)
            verdict["source"] = "crossref"
            verdict = _with_activity(verdict, cr_papers)
            verdict = _with_similarity(verdict, cr_papers)
            verdict = await _verify_with_prescore(prof, client, verdict)
            return verdict, cr_papers

    # ── ORCID: اگه هنوز دانشگاه مطمئن نداریم، تأیید کن ──────────
    if not prof_uni and prof_name:
        try:
            orcid_info = await asyncio.wait_for(
                asyncio.to_thread(_fetch_orcid_info, prof_name, ""),
                timeout=6)
            if orcid_info.get("affiliation"):
                prof["university"] = orcid_info["affiliation"]
                logger.debug(f"ORCID affiliation for {prof_name}: {prof['university']}")
        except Exception:
            pass

    # ── صفحه‌ی وب (آخرین راه) ─────────────────────────────────────
    if prof.get("url"):
        page_text = await asyncio.to_thread(scrape, prof["url"])
        verdict = await judge_relevance_from_webpage(client, prof, page_text)
        verdict["source"] = "webpage"
        return verdict, []

    return {"match": False, "score": 0, "reason": "no publication data in any source", "source": "none"}, []

# حداقل امتیاز برای اینکه یک استاد "match واقعی" در نظر گرفته بشه
RELEVANCE_MIN_SCORE = 60

# ================================================================
# TOP-N نمایش + «استادهای نزدیک» — طبق درخواست صریح:
# «نه هر ۱۰ استادی، بلکه Top 10 Relevant» — یعنی به‌جای نمایش خام هر
# چیزی که پیدا شده، از بین همه‌ی کاندیدها فقط ۱۰ تای بالای رتبه‌بندی
# (بعد از Ranking + Availability Detection) نمایش داده می‌شه. و به‌جای
# پیام خشک «هیچ استادی پیدا نشد» وقتی هیچ‌کدوم واقعاً match=True و
# score>=RELEVANCE_MIN_SCORE نبودن، ولی کاندیدهای ضعیف‌تری («نزدیک»)
# پیدا شدن، همون‌ها رو با شفافیت به کاربر پیشنهاد می‌کنیم که ببینه —
# تصمیم نهایی (اپلای کردن یا نه) دست خودشه، نه این‌که کور کور اطلاعی
# نداشته باشه که اصلاً چیزی نزدیک وجود داشته.
# ================================================================
TOP_N_DISPLAY        = 10   # حداکثر تعداد استاد مرتبط که در University Discovery Mode نمایش داده می‌شه
CLOSE_MATCH_MIN_SCORE = 15  # زیر این حد حتی به‌عنوان «نزدیک» هم پیشنهاد نمی‌شه (صرفاً نویز)

# ================================================================
# DISCOVERY MODE BREADTH — طبق درخواست صریح: خروجی University/Company
# Discovery Mode (تلاش ۴ استاد / تلاش‌های آخر کار) قبلاً با limit=8
# دانشگاه/شرکت × نیاز ۶ نفر و سقف نمایشی TOP_N_DISPLAY=10 + close=20
# عملاً حداکثر حدود ۴۸ کاندید کراول می‌شد و فقط ۳۰ تا نمایش داده می‌شد —
# خیلی کمتر از هدف واقعی «اپلای ۱۵۰ موقعیت». الان breadth (تعداد
# دانشگاه/شرکتی که واقعاً کراول می‌شن) و سقف خروجی جدا از TOP_N_DISPLAY
# قدیمی (که برای مسیرهای دیگه هم استفاده می‌شه) بالا برده شده، و خودِ
# لوپ کراول هم موازی شده (وگرنه breadth بیشتر روی یک لوپ سری غیرقابل
# تحمل کند می‌شد).
# ================================================================
UNIVERSITY_DISCOVERY_LIMIT  = 25   # تعداد دانشگاه‌هایی که واقعاً کراول می‌شن (قبلاً ۸)
UNIVERSITY_DISCOVERY_NEEDED = 8    # حداکثر استاد استخراج‌شده به‌ازای هر دانشگاه (قبلاً ۶)
COMPANY_DISCOVERY_LIMIT     = 25   # تعداد شرکت‌هایی که واقعاً کراول می‌شن (قبلاً ۸)
COMPANY_DISCOVERY_NEEDED    = 8    # حداکثر آگهی استخراج‌شده به‌ازای هر شرکت (قبلاً ۶)
DISCOVERY_TOP_N   = 100   # سقف کاندیدهای Top (Match قوی) که نمایش داده می‌شن
DISCOVERY_CLOSE_N = 50    # سقف کاندیدهای «نزدیک» — مجموع DISCOVERY_TOP_N + DISCOVERY_CLOSE_N = ۱۵۰
DISCOVERY_CRAWL_WORKERS = 10   # حداکثر thread هم‌زمان برای کراول دانشگاه/شرکت (موازی، نه سری)
DISCOVERY_CRAWL_POOL_TIMEOUT = 45   # سقف زمانی عادی برای کل لوپ موازی کراول

# ================================================================
# سه‌حالته کردن نتیجه‌ی جستجو — طبق درخواست صریح: «۰ نتیجه» به‌تنهایی
# مبهمه (یعنی واقعاً استادی نیست؟ یا منبع‌ها الان پیدا نکردن؟). به‌جای
# یک پیام خشک، همیشه یکی از این سه حالت روشن برگردونده می‌شه:
#   Exact Match Found      → profs غیرخالی و بالاترین امتیاز >= RELEVANCE_MIN_SCORE
#   Related Match Found    → profs غیرخالی ولی امتیاز پایین‌تر، یا فقط close-candidate پیدا شده
#   Search Exhausted       → هیچ‌کدوم (نه match قوی، نه نزدیک) — واقعاً هیچی
# معیار «امتیاز» همون _rank_score (۰ تا ۱۰۰) هست که هم مسیر عادی
# (_search_professors_impl) و هم University Discovery Mode روی هر
# پروفسور تگ می‌کنن — یعنی این تابع مستقل از این‌که کدوم مرحله‌ی
# pipeline نتیجه رو پیدا کرده، فقط بر اساس واقعاً چقدر مطمئنیم تصمیم
# می‌گیره.
# ================================================================
def classify_prof_search_result(profs: list[dict], close: list[dict] | None = None) -> dict:
    """خروجی: {"state": "exact"|"related"|"exhausted", "header": "متن برای کاربر"}"""
    close = close or []
    if profs:
        top_score = profs[0].get("_rank_score")
        if top_score is None or top_score >= RELEVANCE_MIN_SCORE:
            # top_score=None یعنی هنوز از یه مسیر قدیمی‌تر بدون تگ اومده —
            # محافظه‌کارانه exact در نظر می‌گیریم (رفتار قبلی رو نمی‌شکنه)
            return {"state": "exact", "header": f"✅ Exact Match Found — {len(profs)} Professors"}
        return {"state": "related", "header": f"🔎 Related Match Found — {len(profs)} Professors"}
    if close:
        return {"state": "related", "header": f"🔎 Related Match Found — {len(close)} Professors (نزدیک)"}
    return {"state": "exhausted", "header": "🛑 Search Exhausted — No verified professors"}

# آستانه‌ی جاب کمی پایین‌تر از استاده، چون اطلاعاتی که از یک آگهی شغلی در
# دسترسه (عنوان/شرکت/گاهی توضیح کوتاه) خیلی کم‌جزئیات‌تر از مقالات علمیه —
# سخت‌گیری به همون اندازه‌ی استاد باعث می‌شه تقریباً همه‌چیز رد بشه.
JOB_RELEVANCE_MIN_SCORE = 55

# ================================================================
# Hybrid Job Scoring — قبلاً «score» نهایی هر آگهی (که هم به کاربر نشون
# داده می‌شد، هم برای فیلتر match/no-match و هم برای شانس قبولی تخمینی
# استفاده می‌شد) صرفاً یک عدد خام از AI بود. مشکل: AI Judge برای همون جفت
# «رزومه/آگهی» می‌تونه روز به روز عدد متفاوتی بده (non-deterministic) —
# یعنی دو اجرای پشت‌سرهم روی یک آگهی ثابت ممکنه دو تصمیم متفاوت (match/
# no-match) بدن. راه‌حل: جزء‌های قابل‌محاسبه‌ی دیتریمینیستیک (skills از
# روی همون bag-of-words cosine similarity که برای تطابق پژوهشی استاد هم
# استفاده می‌شه، location/visa از روی تطابق متنی ساده) رو خودمون با کد
# پایتون حساب می‌کنیم — نه AI. AI فقط یک جزء از نمره‌ی نهایی می‌مونه
# («ai_semantic»)، نه قاضی مطلق. sub-scoreهایی که واقعاً نیاز به فهم
# متنی/قضاوتی دارن (experience_match/education_match) همچنان از AI
# می‌آن، چون داده‌ی ساختاریافته‌ای برای محاسبه‌ی دیتریمینیستیک‌شون نداریم.
# همه‌ی این محاسبات صرفاً روی متن‌های کوتاهی (رزومه/آگهی) که همین الان در
# دسترسن انجام می‌شن — بدون هیچ فراخوانی شبکه/AI اضافه، پس نه سرعت بات رو
# کم می‌کنه و نه ریسک crash جدیدی اضافه می‌کنه.
JOB_SCORE_WEIGHTS = {
    "skills": 0.30, "experience": 0.20, "location": 0.15,
    "visa": 0.15, "education": 0.10, "ai_semantic": 0.10,
}

def _deterministic_skills_match(client: dict, job: dict) -> int:
    """تطابق مهارتی دیتریمینیستیک (بدون AI) بین مهارت‌ها/رزومه‌ی کاربر و
    عنوان/توضیح آگهی — با همون بردار bag-of-words + cosine similarity که
    قبلاً برای تطابق پژوهشی استاد/کاربر ساخته شده (_text_vector/
    _cosine_similarity، پایین‌تر در فایل تعریف شدن؛ چون همه در یک ماژول‌ان،
    فقط لازمه موقع صدا زدن این تابع تعریف شده باشن، نه موقع تعریفش).
    همیشه یک عدد ثابت و قابل‌تکرار برای همون ورودی می‌ده."""
    resume_text = f"{client.get('skills','')} {(client.get('resume','') or '')[:1500]} {client.get('field','')}"
    job_text = f"{job.get('title','')} {(job.get('description','') or '')[:1500]}"
    sim = _cosine_similarity(_text_vector(resume_text), _text_vector(job_text))
    return round(max(0.0, min(1.0, sim)) * 100)

def _deterministic_location_match(client: dict, job: dict) -> int:
    """تطابق موقعیت مکانی دیتریمینیستیک — هم‌پوشانی متنی کشور/شهر ترجیحی
    کاربر (career_profiles.countries) با location/توضیح آگهی. اگر آگهی
    صراحتاً Remote است یا کاربر اصلاً ترجیح کشوری ثبت نکرده، این بُعد
    محدودکننده نیست (۱۰۰) — نه اینکه بی‌دلیل نمره رو پایین بیاره."""
    loc_l   = (job.get("location","") or "").lower()
    title_l = (job.get("title","") or "").lower()
    desc_l  = (job.get("description","") or "").lower()
    if "remote" in loc_l or "remote" in title_l or job.get("source") == "RemoteOK":
        return 100
    countries = (client.get("countries","") or "").lower()
    wanted = [c.strip() for c in re.split(r"[,\|/؛;]", countries) if c.strip()]
    if not wanted:
        return 100
    if any(w in loc_l or w in desc_l for w in wanted):
        return 100
    return 40  # نه رد قطعی (location آگهی‌ها اغلب ناقص/مبهمن)، فقط سیگنال ضعیف‌تر

def _deterministic_visa_match(client: dict, job: dict, user_filters: dict) -> int:
    """اگر کاربر صراحتاً Visa Sponsorship لازم داره (job_filters.visa_only)،
    دیتریمینیستیک چک می‌کنه توضیح آگهی واقعاً به visa/sponsor/relocation
    اشاره کرده یا نه — همون کلیدواژه‌هایی که _passes_job_filters هم استفاده
    می‌کنه. اگر کاربر این الزام رو نداره، این بُعد محدودکننده نیست (۱۰۰)."""
    if not (user_filters or {}).get("visa_only"):
        return 100
    desc_l = (job.get("description","") or "").lower()
    has_visa = ("visa" in desc_l or "sponsor" in desc_l or "relocation" in desc_l)
    return 100 if has_visa else 0

async def judge_job_relevance(client: dict, job: dict, user_filters: dict | None = None) -> dict:
    """قبلاً هیچ چک AI‌ای بین رزومه‌ی کاربر و آگهی شغلی وجود نداشت — بات
    هر آگهی‌ای که جستجوی کلیدواژه‌ای پیدا می‌کرد رو مستقیم اپلای می‌کرد، بدون
    اینکه واقعاً بسنجه آیا با رزومه‌ی کاربر هم‌خوانی داره یا نه. این تابع
    دقیقاً همون منطق judge_relevance (برای اساتید) رو برای آگهی شغلی تکرار
    می‌کنه: اگه توضیح آگهی در دسترس نباشه، از صفحه‌ی لینک آگهی می‌گیریم؛
    محافظه‌کارانه قضاوت می‌کنه و در تردید واقعی، false برمی‌گردونه."""
    desc = (job.get("description") or "").strip()
    if not desc and job.get("url"):
        raw = await asyncio.to_thread(scrape, job["url"])
        if raw and not raw.startswith("[error"):
            desc = raw[:2000]

    resume_snippet = (client.get("resume", "") or "")[:1000]
    prompt = f"""You are a strict job-fit reviewer for job applications.
Decide if this job posting genuinely and substantively matches the applicant's actual field,
education and resume below — not a loose or superficial keyword overlap.
Be conservative: if in real doubt, answer false. Example of a mismatch to avoid: a Backend/Software
Engineer applicant being matched to an unrelated Sales or Marketing role just because both are "tech company jobs".

Applicant field: {client.get('field','')}
Applicant education: {client.get('education','')}
Applicant experience: {client.get('work_exp','')}
Applicant visa/work-authorization status: {client.get('visa_type','not specified')}
Applicant resume: {resume_snippet if resume_snippet else "(no resume text provided)"}

Job title: {job.get('title','')}
Company: {job.get('company','')}
Location: {job.get('location','')}
Job description (may be partial or empty): {desc[:1500] if desc else "(no description available — judge from title only, be extra conservative)"}

Respond with STRICT JSON only, nothing else, exactly this shape (all *_match fields are 0-100):
{{"match": true, "score": 0, "skills_match": 0, "experience_match": 0, "visa_match": 0, "location_match": 0, "education_match": 0, "reason": "one short sentence"}}"""

    ai = await call_ai(prompt, min_length=15)
    verdict = _parse_relevance_json(ai)

    # ── Hybrid Scoring: نمره‌ی نهایی رو خودمون از ترکیب جزء‌های دیتریمینیستیک
    # + جزء‌های AI می‌سازیم (به‌جای اینکه مستقیم verdict["score"] خام AI رو
    # قبول کنیم). هر خطایی اینجا کاملاً بی‌خطر catch می‌شه و روی همون نمره‌ی
    # خام AI fallback می‌کنه — هیچ‌وقت باعث crash یا گیر کردن batch نمی‌شه.
    try:
        skills_det   = _deterministic_skills_match(client, job)
        location_det = _deterministic_location_match(client, job)
        visa_det     = _deterministic_visa_match(client, job, user_filters or {})
        ai_semantic  = verdict["score"]
        hybrid = round(
            JOB_SCORE_WEIGHTS["skills"]      * skills_det +
            JOB_SCORE_WEIGHTS["experience"]  * verdict["experience_match"] +
            JOB_SCORE_WEIGHTS["location"]    * location_det +
            JOB_SCORE_WEIGHTS["visa"]        * visa_det +
            JOB_SCORE_WEIGHTS["education"]   * verdict["education_match"] +
            JOB_SCORE_WEIGHTS["ai_semantic"] * ai_semantic
        )
        verdict["ai_score"]       = ai_semantic  # نمره‌ی خام AI، فقط برای لاگ/دیباگ نگه داشته می‌شه
        verdict["score"]          = max(0, min(100, hybrid))
        # sub-scoreهایی که به کاربر نمایش داده می‌شن (مهارت/موقعیت/ویزا) هم با
        # همون مقادیر دیتریمینیستیکی که واقعاً در نمره‌ی نهایی استفاده شدن
        # جایگزین می‌شن — تا چیزی که کاربر می‌بینه با چیزی که تصمیم رو گرفته
        # یکی باشه، نه یک عدد AI جدا که هیچ نقشی توی تصمیم نداشته.
        verdict["skills_match"]   = skills_det
        verdict["location_match"] = location_det
        verdict["visa_match"]     = visa_det
    except Exception as e:
        logger.warning(f"hybrid job scoring failed, falling back to AI-only score: {e}")

    return verdict

# ================================================================
# ATS RESUME SCAN — رزومه رو طبق قوانین رایج ATS (سیستمی که خیلی از
# کارفرماها قبل از رسیدن به آدم واقعی خودکار رزومه‌ها رو رد می‌کنن)
# بررسی می‌کنه و نقاط ضعف/خطر رد شدن رو با پیشنهاد اصلاح گزارش می‌ده.
# ================================================================

def resume_ats_hash(resume_text: str) -> str:
    """اثرانگشت کوتاه از متن رزومه — برای تشخیص اینکه رزومه از آخرین
    اسکن تغییر کرده یا نه (اگه تغییر کرده باشه، اسکن اجباری دوباره لازمه)."""
    return hashlib.sha256((resume_text or "").encode("utf-8", errors="ignore")).hexdigest()[:16]

async def analyze_resume_ats(resume_text: str, resume_is_file: bool, field: str = "") -> dict:
    """رزومه رو طبق قوانین رایج ATS بررسی می‌کنه. خروجی:
        {"ok": True, "score": 0-100, "issues": [...], "summary": "...", "resume_is_file": bool}
    یا در صورت شکست کامل بعد از همه‌ی تلاش‌ها:
        {"ok": False, "error": "..."}

    قابلیت اطمینان: تا ATS_SCAN_MAX_RETRIES بار تلاش می‌کنه — هم برای خالی
    برگشتن call_ai (هر دو AI provider شکست خوردن) هم برای JSON خراب/ناقص
    (باگ نادر که حتی وقتی call_ai جواب می‌ده، گاهی shape جواب کامل نیست).
    این‌جوری یک شکست موقت (~۱٪) باعث نمی‌شه کاربر یک گزارش خالی/غلط بگیره."""
    if not resume_text or len(resume_text.strip()) < 30:
        return {"ok": False, "error": "resume_too_short"}

    format_note = ("این رزومه به‌صورت فایل (PDF/Word) آپلود شده — به‌خصوص دقت کن که آیا طراحی "
                   "چندستونی/جدولی/گرافیکی داره که ممکنه استخراج متن توسط ATS رو خراب کنه؛ اگه متن "
                   "استخراج‌شده‌ی زیر جابه‌جا/بریده/ناقص به‌نظر می‌رسه، خودش مدرک قوی همین مشکله."
                   if resume_is_file else
                   "این رزومه به‌صورت متن ساده وارد شده (بدون فایل PDF/Word).")

    prompt = f"""You are an ATS (Applicant Tracking System) compatibility auditor. Many employers use
automated ATS software that parses and scores resumes BEFORE any human sees them; resumes that fail
parsing or miss standard conventions get auto-rejected with no human review ever happening. Analyze the
resume below strictly for these ATS-risk categories:

1. FORMAT/PARSING RISK — multi-column layouts, tables, text boxes, headers/footers holding critical
   contact info (often skipped entirely by ATS parsers), graphics/icons replacing text, unusual fonts.
2. STRUCTURE — missing or non-standard section headers (Experience, Education, Skills, etc.), unclear
   reverse-chronological order, missing dates.
3. CONTACT INFO — is a clean, parseable email/phone present in the main body (not only header/footer)?
4. KEYWORDS — for the field "{field or 'general'}", are common industry keywords/skills present, or is
   the resume too generic to pass keyword-based ATS filtering?
5. LENGTH/DENSITY — too short (looks incomplete) or too dense (wall-of-text paragraphs instead of
   scannable bullet points).

{format_note}

Resume text (as extracted — if this extraction itself looks garbled or broken, that itself is strong
evidence of an ATS-parsing problem in the original file):
---
{resume_text[:5000]}
---

Respond with STRICT JSON only, nothing else, exactly this shape:
{{
  "overall_score": <0-100 integer, higher = more ATS-friendly>,
  "issues": [
    {{"severity": "high|medium|low", "category": "format|structure|contact|keywords|length",
      "problem": "short description of the issue found in THIS resume", "fix": "concrete short suggestion"}}
  ],
  "summary": "one or two sentence overall verdict"
}}
Only include issues that genuinely apply to THIS resume — never invent generic filler issues just to
fill the list. If the resume is genuinely solid, "issues" can be a short list or even empty."""

    last_err = "unknown"
    for attempt in range(ATS_SCAN_MAX_RETRIES):
        try:
            ai = await call_ai(prompt, min_length=20)
            if not ai:
                last_err = "empty_ai_response"
                logger.warning(f"analyze_resume_ats attempt {attempt+1}/{ATS_SCAN_MAX_RETRIES}: {last_err}")
                await asyncio.sleep(1.2 * (attempt + 1))
                continue
            data = _extract_json_object(ai)
            if not data:
                last_err = "no_json_found_in_ai_response"
                logger.warning(f"analyze_resume_ats attempt {attempt+1}/{ATS_SCAN_MAX_RETRIES}: {last_err}")
                await asyncio.sleep(1.2 * (attempt + 1))
                continue
            score = data.get("overall_score")
            issues = data.get("issues")
            if not isinstance(score, (int, float)) or not isinstance(issues, list):
                last_err = "malformed_json_shape"
                logger.warning(f"analyze_resume_ats attempt {attempt+1}/{ATS_SCAN_MAX_RETRIES}: {last_err}")
                await asyncio.sleep(1.2 * (attempt + 1))
                continue

            score = max(0, min(100, int(score)))
            clean_issues = []
            for it in issues[:12]:
                if not isinstance(it, dict):
                    continue
                sev = str(it.get("severity", "medium")).strip().lower()
                if sev not in ("high", "medium", "low"):
                    sev = "medium"
                problem = str(it.get("problem", "")).strip()
                fix = str(it.get("fix", "")).strip()
                cat = str(it.get("category", "")).strip() or "other"
                if problem:
                    clean_issues.append({"severity": sev, "category": cat, "problem": problem, "fix": fix})
            summary = str(data.get("summary", "")).strip()
            return {"ok": True, "score": score, "issues": clean_issues, "summary": summary,
                    "resume_is_file": resume_is_file}
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"
            logger.warning(f"analyze_resume_ats attempt {attempt+1}/{ATS_SCAN_MAX_RETRIES} failed: {last_err}")
            await asyncio.sleep(1.2 * (attempt + 1))
            continue

    logger.error(f"analyze_resume_ats: همه‌ی {ATS_SCAN_MAX_RETRIES} تلاش شکست خورد — last_err={last_err}")
    return {"ok": False, "error": last_err}

def job_acceptance_probability(job_score: int, ats_score: int | None) -> dict:
    """احتمال قبولی تخمینی برای یک آگهی خاص = ترکیب دو سیگنال مستقل:
      • job_score  → چقدر این آگهی با رزومه/رشته/سابقه‌ی کاربر واقعاً هم‌خوانی داره
      • ats_score  → چقدر خودِ فایل/فرمت رزومه احتمال داره از فیلتر خودکار ATS کارفرما رد بشه
    وزن ۶۰٪ روی تطابق شغلی و ۴۰٪ روی سلامت ATS رزومه — چون حتی رزومه‌ی
    کاملاً ATS-friendly هم اگه با شغل بی‌ربط باشه شانسی نداره، ولی برعکسش
    هم درسته: تطابق عالی با رزومه‌ای که ATS رد می‌کنه هیچ‌وقت حتی دیده نمی‌شه.
    اگه هنوز اسکن ATS انجام نشده (ats_score=None)، فقط روی job_score حساب
    می‌شه (بدون فرض خوش‌بینانه یا بدبینانه‌ی بی‌مورد)."""
    if ats_score is None:
        prob = job_score
    else:
        prob = round(0.6 * job_score + 0.4 * ats_score)
    prob = max(3, min(97, prob))  # هیچ‌وقت قطعیت ۰٪ یا ۱۰۰٪ ادعا نمی‌کنیم
    if prob >= 70:
        label, emoji = "بالا", "🟢"
    elif prob >= 40:
        label, emoji = "متوسط", "🟡"
    else:
        label, emoji = "پایین", "🔴"
    return {"probability": prob, "label": label, "emoji": emoji}

def format_ats_report_text(report: dict, field: str = "") -> str:
    """گزارش اسکن ATS رو برای نمایش توی چت تلگرام (متن، فارسی) فرمت می‌کنه."""
    score = report.get("score", 0)
    bar = "🟢" if score >= 75 else "🟡" if score >= 50 else "🔴"
    lines = [f"🔎 گزارش اسکن ATS رزومه{f' — {field}' if field else ''}",
              f"{bar} امتیاز سازگاری با ATS: {score}/100"]
    if report.get("resume_is_file"):
        lines.append("📄 رزومه به‌صورت فایل (PDF/Word) بررسی شده.")
    summary = report.get("summary", "")
    if summary:
        lines.append(f"\n💬 {summary}")
    issues = report.get("issues", [])
    if not issues:
        lines.append("\n✅ مشکل قابل‌توجهی برای ATS پیدا نشد.")
    else:
        sev_order = {"high": 0, "medium": 1, "low": 2}
        sev_fa = {"high": "🔴 مهم", "medium": "🟡 متوسط", "low": "⚪️ جزئی"}
        issues_sorted = sorted(issues, key=lambda i: sev_order.get(i.get("severity", "medium"), 1))
        lines.append(f"\n⚠️ {len(issues)} مورد پیدا شد:")
        for i, it in enumerate(issues_sorted[:8], 1):
            sev = sev_fa.get(it.get("severity", "medium"), "🟡 متوسط")
            lines.append(f"\n{i}. [{sev}] {it.get('problem','')}")
            if it.get("fix"):
                lines.append(f"   ✅ راه‌حل: {it['fix']}")
        if len(issues_sorted) > 8:
            lines.append(f"\n… و {len(issues_sorted) - 8} مورد دیگر (توی فایل PDF کامل هست).")
    return "\n".join(lines)

def generate_ats_report_pdf(report: dict, out_path: str, field: str = "") -> bool:
    """گزارش اسکن ATS رو به یک فایل PDF مرتب تبدیل می‌کنه. هیچ‌وقت exception
    بالا نمی‌ره (فقط True/False برمی‌گردونه) — چون این فقط یک ضمیمه‌ی کمکیه؛
    اگه تولید PDF شکست بخوره، گزارش متنی توی چت (format_ats_report_text)
    هنوز برای کاربر کاملاً کافیه، پس نباید کل فلو رو خراب کنه."""
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, HRFlowable
        from reportlab.lib.enums import TA_LEFT
        from xml.sax.saxutils import escape as _xesc

        styles = getSampleStyleSheet()
        title_style = ParagraphStyle("ATSTitle", parent=styles["Title"], alignment=TA_LEFT, fontSize=20, spaceAfter=4)
        h2_style = ParagraphStyle("ATSH2", parent=styles["Heading2"], alignment=TA_LEFT, spaceBefore=14, spaceAfter=6)
        body_style = ParagraphStyle("ATSBody", parent=styles["BodyText"], alignment=TA_LEFT, fontSize=10.5, leading=15)
        small_style = ParagraphStyle("ATSSmall", parent=styles["BodyText"], fontSize=9, textColor=colors.grey)

        score = report.get("score", 0)
        if score >= 75: score_color = colors.HexColor("#1a9850")
        elif score >= 50: score_color = colors.HexColor("#e0a800")
        else: score_color = colors.HexColor("#d32f2f")
        score_style = ParagraphStyle("ATSScore", parent=styles["Title"], fontSize=32,
                                      textColor=score_color, alignment=TA_LEFT, spaceAfter=2)

        doc = SimpleDocTemplate(out_path, pagesize=A4,
                                 leftMargin=20*mm, rightMargin=20*mm, topMargin=18*mm, bottomMargin=18*mm)
        story = []
        story.append(Paragraph("ATS Resume Compatibility Report", title_style))
        if field:
            story.append(Paragraph(f"Field: {_xesc(field)}", small_style))
        story.append(Paragraph(datetime.now().strftime("%Y-%m-%d %H:%M"), small_style))
        story.append(Spacer(1, 10))
        story.append(Paragraph(f"{score}/100", score_style))
        story.append(Paragraph("ATS-friendliness score" +
                                (" — file uploaded as PDF/Word" if report.get("resume_is_file") else " — plain text resume"),
                                small_style))
        story.append(Spacer(1, 6))
        story.append(HRFlowable(width="100%", color=colors.HexColor("#dddddd")))
        story.append(Spacer(1, 10))

        summary = report.get("summary", "")
        if summary:
            story.append(Paragraph("Summary", h2_style))
            story.append(Paragraph(_xesc(summary), body_style))

        issues = report.get("issues", [])
        story.append(Paragraph(f"Issues found ({len(issues)})", h2_style))
        if not issues:
            story.append(Paragraph("No significant ATS-compatibility issues were found.", body_style))
        else:
            sev_order = {"high": 0, "medium": 1, "low": 2}
            issues_sorted = sorted(issues, key=lambda i: sev_order.get(i.get("severity", "medium"), 1))
            sev_color = {"high": colors.HexColor("#d32f2f"), "medium": colors.HexColor("#e0a800"),
                         "low": colors.HexColor("#6c757d")}
            for it in issues_sorted:
                sev = it.get("severity", "medium")
                cat = it.get("category", "other")
                badge_style = ParagraphStyle("Badge", parent=body_style, textColor=sev_color.get(sev, colors.grey),
                                              fontName="Helvetica-Bold", fontSize=9.5)
                story.append(Paragraph(f"[{_xesc(sev.upper())}] {_xesc(cat)}", badge_style))
                story.append(Paragraph(f"<b>Issue:</b> {_xesc(it.get('problem',''))}", body_style))
                if it.get("fix"):
                    story.append(Paragraph(f"<b>Fix:</b> {_xesc(it.get('fix',''))}", body_style))
                story.append(Spacer(1, 8))

        doc.build(story)
        return True
    except Exception as e:
        logger.error(f"generate_ats_report_pdf failed: {e}")
        return False

async def run_ats_scan_flow(update, context, uid: int, client: dict, mandatory: bool = False) -> dict | None:
    """اجرای کامل و مقاوم اسکن ATS: بررسی نیاز به اسکن (رزومه عوض شده یا نه) →
    analyze_resume_ats (با retry داخلی) → ذخیره در دیتابیس → ارسال گزارش
    متنی + PDF به کاربر. هم برای اولین‌بارِ اجباری (قبل از شروع جستجوی کار)
    و هم برای دستور جدای «🔎 اسکن ATS رزومه» استفاده می‌شه.

    خروجی: دیکشنری گزارش موفق، یا None اگه رزومه‌ای نبود/اسکن شکست خورد —
    در هر دو حالت هیچ‌وقت exception بالا نمی‌ره و جریان اصلی (جستجوی کار)
    رو قفل نمی‌کنه، چون اسکن ATS کمکیه، نه پیش‌نیاز غیرقابل‌عبور."""
    resume_text = (client.get("resume", "") or "").strip()
    if not resume_text:
        if not mandatory:
            await safe(update.message,
                "📄 هنوز رزومه‌ای برای شما ثبت نشده — اول از منوی «ویرایش پروفایل» رزومه‌تون رو اضافه کنید.")
        return None

    cur_hash = resume_ats_hash(resume_text)
    prior = await asyncio.to_thread(db_get_resume_ats_scan, uid)
    if prior and prior.get("resume_hash") == cur_hash:
        # همین رزومه قبلاً اسکن شده — چیزی تغییر نکرده، دوباره AI صدا نمی‌زنیم
        if not mandatory:
            try:
                cached_report = json.loads(prior.get("report_json") or "{}")
                await safe(update.message,
                    "ℹ️ این رزومه از آخرین اسکن تغییر نکرده — نتیجه‌ی همون اسکن قبلی:")
                await safe(update.message, format_ats_report_text(cached_report, client.get("field", "")))
            except Exception:
                pass
        return {"ok": True, "score": prior["score"]}

    field = client.get("field", "")
    try:
        await safe(update.message, "🔎 در حال بررسی سازگاری رزومه‌تون با سیستم‌های ATS کارفرماها...\n⏳ چند ثانیه صبر کنید.")
    except Exception:
        pass

    report = await analyze_resume_ats(resume_text, bool(client.get("resume_is_file")), field)
    if not report.get("ok"):
        # حتی بعد از ATS_SCAN_MAX_RETRIES تلاش هم شکست خورد — این نباید کاربر
        # رو از ادامه‌ی اپلای/جستجوی کار متوقف کنه، فقط مطلع می‌کنیم.
        logger.error(f"run_ats_scan_flow: اسکن ATS برای uid={uid} شکست خورد — {report.get('error')}")
        await safe(update.message,
            "⚠️ نتوانستیم الان رزومه رو برای ATS بررسی کنیم (مشکل موقت سرویس هوش‌مصنوعی). "
            "جستجو/اپلای کار عادی ادامه پیدا می‌کنه؛ می‌تونید بعداً با «🔎 اسکن ATS رزومه» از منو دوباره امتحان کنید.")
        return None

    try:
        await asyncio.to_thread(
            db_save_resume_ats_scan, uid, report["score"], json.dumps(report, ensure_ascii=False), cur_hash)
    except Exception as e:
        logger.error(f"db_save_resume_ats_scan: {e}")

    await safe(update.message, format_ats_report_text(report, field))

    # ── تولید و ارسال PDF (best-effort — اگه شکست بخوره، متن بالا کافیه) ──
    try:
        pdf_dir = "/tmp/ats_reports"
        os.makedirs(pdf_dir, exist_ok=True)
        pdf_path = os.path.join(pdf_dir, f"ats_report_{uid}_{int(time.time())}.pdf")
        ok_pdf = await asyncio.to_thread(generate_ats_report_pdf, report, pdf_path, field)
        if ok_pdf:
            for attempt in range(NETWORK_RETRY_ATTEMPTS):
                try:
                    with open(pdf_path, "rb") as f:
                        await context.bot.send_document(
                            chat_id=update.effective_chat.id, document=f,
                            filename="ATS_Resume_Report.pdf",
                            caption="📎 نسخه‌ی کامل گزارش (PDF)")
                    break
                except RetryAfter as e:
                    await asyncio.sleep(min(e.retry_after + 1, 60))
                except _NETWORK_EXC as e:
                    await asyncio.sleep(_network_backoff(attempt))
                except Exception as e:
                    logger.warning(f"ATS PDF send failed: {e}")
                    break
            try:
                os.remove(pdf_path)
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"ATS PDF generation/send skipped: {e}")

    return report

def _build_smart_query(client: dict) -> tuple[str, list[str]]:
    """
    از پروفایل کاربر یک Query هوشمند می‌سازد.
    خروجی: (main_query, keyword_list)
    مثلاً: ('("Machine Learning" OR "Deep Learning") AND "Computer Science"', ['machine learning','deep learning','pytorch'])
    """
    field  = client.get("field", "")
    resume = client.get("resume", "") or ""
    # توجه: education/visa_type/publications عمداً اینجا استفاده نمی‌شن.
    # این‌ها دور ریخته یا فراموش‌شده نیستن — جای دیگه‌ای (judge_relevance،
    # تولید cold email/cover letter، خط subject ایمیل) مستقیماً از client
    # خونده و مصرف می‌شن؛ آنجا برای «قضاوت تناسب/شخصی‌سازی» به‌کار می‌رن.
    # (توجه: visa_type با اینکه اسمش این‌طوریه، در واقع «هدف اپلای»ه —
    # PhD position / Research collaboration / Full-time job — نه ویزای
    # مهاجرتی.) اینجا اما این تابع فقط query جستجوی *متنی* برای پیدا کردن
    # کاندیدها می‌سازه (که قراره توی صفحه‌ی دپارتمان/فکالتی/لب یا API
    # دنبالش بگرده)؛ اضافه‌کردن این فیلدها به‌عنوان keyword بی‌معنی/مخرب
    # می‌بود، چون این‌ها مشخصات خودِ کاربرن، نه چیزی که قراره توی متن
    # سایت دانشگاه یا آگهی شغلی پیدا بشه.

    # کلیدواژه‌های مستقیم از رشته
    field_parts = [p.strip() for p in re.split(r"[،,/]", field) if p.strip()]

    # استخراج کلیدواژه‌های فنی از رزومه (کلمات بزرگ‌شده یا چند کلمه‌ای)
    tech_keywords = re.findall(
        r"\b(?:[A-Z][a-z]*(?:\s+[A-Z][a-z]*)+|[A-Z]{2,}(?:\s+[A-Z]{2,})*)\b",
        resume[:1500]
    )
    # فیلتر: کلمات ناخواسته
    stop = {"I","My","The","In","At","For","And","Or","With","From","To","Of","A","An",
            "PhD","MSc","BSc","BS","MS","GPA","CV","GRE","IELTS","TOEFL"}
    tech_keywords = [k for k in tech_keywords if k not in stop and len(k) > 2][:6]

    # ساخت query با OR logic برای synonymها
    if len(field_parts) > 1:
        main_query = " OR ".join(f'"{p}"' for p in field_parts[:5])
        main_query = f"({main_query})"
    else:
        main_query = f'"{field}"' if field else "research"

    # اضافه کردن کلیدواژه‌های تکنیکال از رزومه
    if tech_keywords:
        kw_query = " OR ".join(f'"{k}"' for k in tech_keywords[:3])
        main_query = f"{main_query} AND ({kw_query})"

    all_keywords = [p.lower() for p in field_parts] + [k.lower() for k in tech_keywords]
    return main_query, all_keywords


# ── Research Match — شباهت معنایی مستقل از قضاوت AI ─────────────────
# یک embedding واقعی (مدل سنگین/API جدا) نیست، ولی یک بردار
# bag-of-words + cosine similarity واقعی و deterministic‌ست — یعنی برخلاف
# قضاوت AI، همیشه یک عدد ثابت و قابل‌تکرار می‌ده و هیچ‌وقت hallucinate
# نمی‌کنه. هدف: اگه AI به‌اشتباه یک استاد Computer Vision رو به یک کاربر
# Time Series Forecasting «مرتبط» تشخیص بده، این عدد مستقل جلوی رد شدنش
# رو می‌گیره، چون این دو حوزه عملاً هیچ کلمه‌ی مشترک معناداری ندارن.
_SIMILARITY_STOPWORDS = {
    "the","a","an","of","in","on","for","and","or","to","with","is","are","this","that",
    "we","our","study","paper","approach","method","using","based","via","new","novel",
    "results","show","propose","present","from","by","as","it","its","can","also","been",
    "was","were","have","has","research","work","paper","university","professor","department",
}

def _tokenize_for_similarity(text: str) -> list[str]:
    words = re.findall(r"[a-zA-Z]{3,}", (text or "").lower())
    return [w for w in words if w not in _SIMILARITY_STOPWORDS]

def _text_vector(text: str) -> dict:
    """بردار فرکانس کلمه (TF) خام — برای مقایسه‌ی دو متن کوتاه (نه یک
    corpus بزرگ)، cosine روی TF خام هم سیگنال معنادار کافی می‌ده، بدون
    نیاز به IDF یا هیچ مدل خارجی. وقتی IDF واقعی در دسترس نباشه (مثلاً
    موقع بررسی تک‌تک یک استاد بعد از انتخاب، جایی که «batch» معنا نداره)
    از همین استفاده می‌شه — رفتار قبلی، دست‌نخورده."""
    vec: dict = {}
    for t in _tokenize_for_similarity(text):
        vec[t] = vec.get(t, 0) + 1
    return vec

def _build_idf(documents: list[str]) -> dict:
    """IDF واقعی روی یک corpus — اینجا: متن مقالات همه‌ی کاندیدهای همون
    batch جستجو (نه یک corpus بیرونی/از قبل‌آموزش‌دیده، چون داده‌ی
    برچسب‌خورده‌ای برای train کردن نداریم؛ این کاملاً deterministic و
    بدون هیچ درخواست شبکه‌ی اضافه‌ست، فقط روی داده‌ای که همین الان جمع
    کردیم). کلمه‌ای که توی اکثر کاندیدها تکرار شده (مثلاً کلمه‌ی عمومی
    رشته، مثل «learning» توی جستجوی Machine Learning) کم‌اهمیت‌تر می‌شه،
    و کلمه‌ای که فقط توی چندتا کاندید هست (واقعاً تمایزدهنده‌ست) وزن
    بیشتری می‌گیره. Smoothing (+1 بالا و پایین کسر) از تقسیم بر صفر و
    idf منفی جلوگیری می‌کنه. اگه هیچ سندی/توکنی نباشه، دیکشنری خالی
    برمی‌گرده — که یعنی caller (compute_research_similarity) خودش
    fallback به TF خام می‌کنه، نه crash."""
    n_docs = len(documents)
    if n_docs == 0:
        return {}
    df: dict = {}
    for doc in documents:
        for t in set(_tokenize_for_similarity(doc)):
            df[t] = df.get(t, 0) + 1
    return {t: math.log((n_docs + 1) / (d + 1)) + 1.0 for t, d in df.items()}

def _text_vector_tfidf(text: str, idf: dict) -> dict:
    """بردار TF-IDF — فرکانس هر ترم توی این متن ضرب در idf همون ترم.
    اگه ترمی توی corpus نبود (یعنی فقط همین‌جا دیده شده، نه توی هیچ
    کاندید دیگه‌ای)، بیشترین idf موجود رو بهش می‌دیم — چون چنین ترمی
    از همه تمایزدهنده‌تره، نه این‌که نادیده گرفته بشه."""
    default_idf = (max(idf.values()) if idf else 1.0)
    vec: dict = {}
    for t in _tokenize_for_similarity(text):
        vec[t] = vec.get(t, 0) + 1
    return {t: freq * idf.get(t, default_idf) for t, freq in vec.items()}

def _cosine_similarity(vec1: dict, vec2: dict) -> float:
    if not vec1 or not vec2:
        return 0.0
    common = set(vec1) & set(vec2)
    dot = sum(vec1[t] * vec2[t] for t in common)
    mag1 = math.sqrt(sum(v * v for v in vec1.values()))
    mag2 = math.sqrt(sum(v * v for v in vec2.values()))
    if mag1 == 0 or mag2 == 0:
        return 0.0
    return dot / (mag1 * mag2)

def compute_research_similarity(client: dict, prof: dict, papers: list[dict] | None = None,
                                 idf: dict | None = None) -> float:
    """شباهت ۰ تا ۱ بین زمینه‌ی پژوهشی استاد و پروفایل کاربر. کاملاً
    مستقل از AI — برای cross-check قضاوت judge_relevance استفاده می‌شه.

    idf اختیاریه: وقتی رتبه‌بندی یه batch از کاندیدها در جریانه (سرچ
    اساتید)، caller یک IDF واقعی روی همون batch می‌سازه و می‌ده تا
    شباهت TF-IDF (دقیق‌تر از TF خام) محاسبه بشه. وقتی idf داده نشه
    (مثلاً موقع بررسی تک‌تک یک استاد بعد از انتخاب، جایی که «batch»
    معنا نداره) رفتار قبلی (TF خام) کاملاً دست‌نخورده می‌مونه — یعنی
    این تغییر هیچ‌کدوم از call siteهای موجود رو نمی‌شکنه."""
    user_text = " ".join([
        client.get("field","") or "", client.get("skills","") or "",
        (client.get("resume","") or "")[:2000],
    ])
    if papers:
        prof_text = " ".join(f"{p.get('title','')} {p.get('abstract','')}" for p in papers)
    else:
        prof_text = " ".join(prof.get("papers", []) or [])
    if idf:
        return _cosine_similarity(_text_vector_tfidf(user_text, idf), _text_vector_tfidf(prof_text, idf))
    return _cosine_similarity(_text_vector(user_text), _text_vector(prof_text))


# ================================================================
# EMBEDDING-BASED SIMILARITY (Hybrid Search — بخش ۱) — طبق درخواست صریح
# کاربر: TF-IDF/کلیدواژه به‌تنهایی برای Academic Search کافی نیست، چون
# دو متن هم‌معنا با کلمات متفاوت (مثلاً "AI for cancer detection" در
# برابر "deep learning based biomedical image analysis") از نظر TF-IDF
# ممکنه خیلی فاصله داشته باشن ولی از نظر علمی تقریباً یک حوزه‌ان. این
# بخش یک لایه‌ی embedding واقعی (Gemini Embedding API، همون کلید
# GEMINI_API_KEY که call_ai هم استفاده می‌کنه) اضافه می‌کنه.
#
# کاملاً optional/fail-safe به همون فلسفه‌ی بقیه‌ی فایل: بدون
# GEMINI_API_KEY یا با هر خطای شبکه/SDK، خروجی None می‌شه و caller
# (compute_hybrid_research_fit) خودکار وزن این لایه رو بین بقیه‌ی
# سیگنال‌ها بازتوزیع می‌کنه — نه اینکه صفر بمونه و امتیاز نهایی رو
# مصنوعی پایین بیاره یا کل مسیر رو crash کنه.
# ================================================================
_EMBEDDING_MODEL = "models/text-embedding-004"
_embedding_cache: dict[str, list[float]] = {}
_EMBEDDING_CACHE_MAX = 2000  # جلوگیری از رشد بی‌سقف حافظه توی یک اجرای طولانی

def _embedding_cache_key(text: str) -> str:
    return hashlib.sha256(text.strip().lower().encode("utf-8", "ignore")).hexdigest()

def get_text_embedding(text: str) -> list[float] | None:
    """بردار embedding واقعی (نه bag-of-words) برای یک متن. sync + cached
    (چون توی یک جستجو ممکنه متن رزومه‌ی همون کاربر دهها بار دوباره embed
    بشه). خروجی None یعنی «الان در دسترس نیست» (نه خطای مسدودکننده) —
    caller باید بی‌سروصدا به TF-IDF fallback کنه."""
    text = (text or "").strip()
    if not text or not GEMINI_API_KEY.startswith("AIza"):
        return None
    key = _embedding_cache_key(text)
    cached = _embedding_cache.get(key)
    if cached is not None:
        return cached
    try:
        from google import genai
        client = genai.Client(api_key=GEMINI_API_KEY)
        r = client.models.embed_content(model=_EMBEDDING_MODEL, contents=text[:8000])
        vec = None
        # نسخه‌های مختلف SDK فرمت خروجی کمی فرق دارن — هر دو حالت رایج رو پوشش می‌دیم
        if getattr(r, "embeddings", None):
            vec = list(r.embeddings[0].values)
        elif getattr(r, "embedding", None) is not None:
            vec = list(r.embedding.values) if hasattr(r.embedding, "values") else list(r.embedding)
        if not vec:
            return None
        if len(_embedding_cache) >= _EMBEDDING_CACHE_MAX:
            _embedding_cache.clear()  # ساده‌ترین راه جلوگیری از نشت حافظه — فقط کش رو ریست می‌کنه
        _embedding_cache[key] = vec
        return vec
    except Exception as e:
        logger.debug(f"get_text_embedding: {e}")
        return None

def _dense_cosine(v1: list[float], v2: list[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    dot = sum(a * b for a, b in zip(v1, v2))
    m1 = math.sqrt(sum(a * a for a in v1))
    m2 = math.sqrt(sum(b * b for b in v2))
    if m1 == 0 or m2 == 0:
        return 0.0
    return dot / (m1 * m2)

def compute_embedding_similarity(text1: str, text2: str) -> float | None:
    """شباهت embedding واقعی بین دو متن؛ None اگه embedding در دسترس نبود
    (caller باید به TF-IDF fallback کنه)."""
    v1 = get_text_embedding(text1)
    if v1 is None:
        return None
    v2 = get_text_embedding(text2)
    if v2 is None:
        return None
    return max(0.0, min(1.0, _dense_cosine(v1, v2)))


# ================================================================
# PAPER-TO-CV MATCHING (Hybrid Search — بخش ۲) — طبق درخواست صریح کاربر:
# به‌جای یک عدد کلی «شباهت» بین کل رزومه و کل کارنامه‌ی استاد، برای هر
# موضوع/مهارت کلیدی CV دانشجو مشخص می‌کنه دقیقاً کدوم مقاله‌ی استاد
# بیشترین هم‌پوشانی رو داره و چند درصد — یعنی Research Fit واقعاً از
# سطح مقاله محاسبه می‌شه («Federated Learning ↔ کدوم مقاله؟ ۹۸٪»)، نه
# فقط از برچسب کلی رشته («Machine Learning = Machine Learning»).
# ================================================================
def _extract_cv_topics(client: dict, max_topics: int = 6) -> list[str]:
    """موضوعات/مهارت‌های کلیدی رزومه — منبع اصلی فیلد skills (کاربر خودش
    با کاما/خط جداشون می‌کنه)؛ اگه خالی بود از field. عمداً کاملاً
    deterministic (بدون AI) چون ممکنه برای دهها استاد پشت‌سرهم صدا زده
    بشه و نباید هزینه/تأخیر API اضافه کنه."""
    skills = (client.get("skills") or "").strip()
    topics = [t.strip() for t in re.split(r"[،,\n/|]", skills) if t.strip()]
    if not topics:
        field = (client.get("field") or "").strip()
        topics = [t.strip() for t in re.split(r"[،,\n/|]", field) if t.strip()]
    return topics[:max_topics]

def compute_paper_cv_matches(client: dict, papers: list[dict], idf: dict | None = None) -> dict:
    """برای هر موضوع CV، بهترین مقاله‌ی منطبق استاد (و درصد تطابقش) رو
    پیدا می‌کنه. اول embedding واقعی رو امتحان می‌کنه (معنایی، مترادف‌ها
    رو هم می‌فهمه)؛ اگه در دسترس نبود (بدون GEMINI_API_KEY یا خطای شبکه)
    خودکار به TF-IDF/TF خام fallback می‌کنه — یعنی همیشه یک نتیجه‌ی
    معنادار برمی‌گردونه، حتی بدون هیچ AI/Embedding API.

    خروجی: {"topics": [{"topic","score"(0-100),"best_paper"}, ...],
            "research_fit": 0-100,  # میانگین موضوعات، نه شباهت خام کلی
            "best_paper_match": {"title","score"} یا None,
            "used_embedding": bool}"""
    topics = _extract_cv_topics(client)
    if not topics or not papers:
        return {"topics": [], "research_fit": 0, "best_paper_match": None, "used_embedding": False}

    paper_titles = [p.get("title", "") or "(untitled)" for p in papers]
    paper_texts  = [f"{p.get('title','')}. {p.get('abstract','')}".strip() for p in papers]

    used_embedding = False
    topic_results: list[dict] = []
    best_overall = {"title": "", "score": 0}

    for topic in topics:
        if len(topic) < 3:
            continue
        topic_emb = get_text_embedding(topic)
        best_score, best_title = 0.0, ""
        for title, ptext in zip(paper_titles, paper_texts):
            if not ptext:
                continue
            score = None
            if topic_emb is not None:
                paper_emb = get_text_embedding(ptext)
                if paper_emb is not None:
                    score = _dense_cosine(topic_emb, paper_emb)
                    used_embedding = True
            if score is None:
                score = (_cosine_similarity(_text_vector_tfidf(topic, idf), _text_vector_tfidf(ptext, idf))
                         if idf else _cosine_similarity(_text_vector(topic), _text_vector(ptext)))
            if score > best_score:
                best_score, best_title = score, title
        pct = round(max(0.0, min(1.0, best_score)) * 100)
        topic_results.append({"topic": topic, "score": pct, "best_paper": best_title})
        if pct > best_overall["score"]:
            best_overall = {"title": best_title, "score": pct}

    research_fit = round(sum(t["score"] for t in topic_results) / len(topic_results)) if topic_results else 0
    return {
        "topics": topic_results,
        "research_fit": research_fit,
        "best_paper_match": best_overall if best_overall["score"] > 0 else None,
        "used_embedding": used_embedding,
    }

def format_paper_cv_match_block(match: dict | None) -> str:
    """بلوک متنی «Student ↔ Paper» برای پیام تلگرام — دقیقاً همون فرمتی
    که کاربر مثال زد: هر موضوع CV + درصد تطابقش با بهترین مقاله."""
    if not match or not match.get("topics"):
        return ""
    lines = ["🔬 Student ↔ Paper Match:"]
    for t in match["topics"]:
        lines.append(f"   {t['topic']:<26} {t['score']:>3}%")
    lines.append(f"   {'—'*30}")
    lines.append(f"   Research Fit (paper-level)  {match['research_fit']:>3}%")
    if match.get("best_paper_match") and match["best_paper_match"].get("title"):
        lines.append(f"   📄 بهترین مقاله‌ی منطبق: \"{match['best_paper_match']['title'][:70]}\" "
                      f"({match['best_paper_match']['score']}%)")
    if not match.get("used_embedding"):
        lines.append("   ⚠️ بدون embedding واقعی (بر اساس TF-IDF) — GEMINI_API_KEY تنظیم نشده")
    return "\n".join(lines)


# ================================================================
# HYBRID SEARCH — طبق پیشنهاد صریح کاربر، چهار سیگنال مستقل با وزن‌های
# جدا ترکیب می‌شن (به‌جای یک عدد تکی TF-IDF یا فقط قضاوت AI):
#   Keyword (TF-IDF)        15%   — تطابق کلمه‌به‌کلمه، سریع/رایگان
#   Embedding                40%   — شباهت معنایی واقعی (مترادف/هم‌حوزه)
#   Paper-level similarity   25%   — بهترین تطابق در سطح یک مقاله‌ی خاص
#                                    (از Paper-to-CV Matching بالا)
#   LLM verification          20%   — قضاوت judge_relevance (از قبل موجود)
# جمع این چهار وزن = ۱۰۰٪. اگه embedding در دسترس نباشه، وزنش خودکار
# متناسب بین بقیه بازتوزیع می‌شه (نه صفر) — یعنی سیستم بدون
# GEMINI_API_KEY هم کامل کار می‌کنه، فقط دقتش کمتره؛ هیچ‌وقت crash نمی‌کنه.
#
# نکته‌ی مهم درباره‌ی جای استفاده: این تابع فقط برای «بررسی عمیق یک استاد
# انتخاب‌شده» (get_professor_relevance) صدا زده می‌شه، نه برای رتبه‌بندی
# اولیه‌ی صدها کاندید (_score_professor_relevance) — چون embedding واقعی
# یک API call واقعیه و برای صدها کاندید هم‌زمان نه لازمه نه صرفه‌داره.
# ================================================================
HYBRID_WEIGHTS = {"keyword": 0.15, "embedding": 0.40, "paper_similarity": 0.25, "llm": 0.20}

def compute_hybrid_research_fit(client: dict, prof: dict, papers: list[dict],
                                 llm_score_0_100: float | None = None,
                                 idf: dict | None = None,
                                 paper_cv_match: dict | None = None) -> dict:
    """چهار سیگنال Research Fit رو طبق وزن‌های HYBRID_WEIGHTS ترکیب می‌کنه.
    خروجی یک dict قابل‌نمایش (هر جزء ۰-۱۰۰، به‌جز embedding که اگه در
    دسترس نبوده None می‌مونه) + "score" نهایی (۰-۱۰۰)."""
    user_text = " ".join([
        client.get("field", "") or "", client.get("skills", "") or "",
        (client.get("resume", "") or "")[:2000],
    ])
    prof_text = (" ".join(f"{p.get('title','')} {p.get('abstract','')}" for p in papers) if papers
                 else " ".join(prof.get("papers", []) or []))

    keyword_sim = compute_research_similarity(client, prof, papers=papers or None, idf=idf)

    embedding_sim = compute_embedding_similarity(user_text, prof_text) if prof_text else None
    embedding_available = embedding_sim is not None

    if paper_cv_match is None:
        paper_cv_match = compute_paper_cv_matches(client, papers, idf=idf) if papers else None
    paper_sim = (paper_cv_match["research_fit"] / 100.0) if paper_cv_match and paper_cv_match.get("topics") else keyword_sim

    llm_sim = (llm_score_0_100 / 100.0) if llm_score_0_100 is not None else keyword_sim

    weights = dict(HYBRID_WEIGHTS)
    if not embedding_available:
        w_emb = weights.pop("embedding")
        remaining = sum(weights.values())
        for k in weights:
            weights[k] += w_emb * (weights[k] / remaining)
        embedding_sim = 0.0  # فقط برای نمایش؛ در جمع وزنی شرکت نمی‌کنه (وزنش صفر شده)

    final = (weights.get("keyword", 0) * keyword_sim +
             weights.get("embedding", 0) * embedding_sim +
             weights.get("paper_similarity", 0) * paper_sim +
             weights.get("llm", 0) * llm_sim)

    return {
        "keyword": round(keyword_sim * 100),
        "embedding": round(embedding_sim * 100) if embedding_available else None,
        "paper_similarity": round(paper_sim * 100),
        "llm": round(llm_sim * 100),
        "score": round(max(0.0, min(1.0, final)) * 100),
        "embedding_available": embedding_available,
    }


def _score_professor_relevance(prof: dict, keywords: list[str], client: dict,
                                idf: dict | None = None) -> float:
    """
    امتیاز اولیه (pre-AI) بر اساس کلیدواژه‌ها.
    برای مرتب‌سازی اساتید قبل از بررسی AI — سریع و بدون API.
    خروجی: 0.0 تا 1.0

    نسخه‌ی قبلی همه‌ی متن (اسم + دانشگاه + مقالات + snippet) رو یکجا
    قاطی می‌کرد و هر کلیدواژه رو یکسان می‌شمرد — یعنی یه match توی
    عنوان مقاله (سیگنال قوی: این استاد واقعاً روی این موضوع کار کرده)
    دقیقاً هم‌وزن یه match توی اسم دانشگاه بود (سیگنال ضعیف/تصادفی:
    مثلاً "Data University" برای کلیدواژه‌ی "data"). الان هر بخش
    وزن جدا داره.
    """
    papers_text   = " ".join(prof.get("papers", []) or []).lower()
    snippet_text  = (prof.get("snippet","") or "").lower()
    weak_text     = " ".join([prof.get("name",""), prof.get("university","")]).lower()

    score = 0.0
    if keywords:
        for kw in keywords:
            kwl = kw.lower()
            if not kwl:
                continue
            if kwl in papers_text:
                score += 0.55       # قوی‌ترین سیگنال: توی عنوان/متن مقاله
            elif kwl in snippet_text:
                score += 0.35       # متوسط: توی توضیح/snippet منبع
            elif kwl in weak_text:
                score += 0.15       # ضعیف: فقط توی اسم/دانشگاه (ممکنه تصادفی باشه)
        score = score / len(keywords)
        # نکته‌ی مهم: عمداً وزن‌های بالا رو کم گرفتیم (۰.۵۵ نه ۱.۰) که
        # حتی وقتی همه‌ی کلیدواژه‌ها توی عنوان مقاله match بشن، امتیاز
        # کامل ۱.۰ رو پر نکنه — وگرنه بونس‌های زیر (کشور، DBLP، works،
        # شباهت) هیچ‌وقت اثر واقعی نداشتن چون همیشه به سقف min(1.0,...)
        # می‌خوردن و نمی‌تونستن بین دو کاندیدِ «هر دو موضوعاً قوی» فرق
        # بذارن (که دقیقاً همون‌جاست که این بونس‌ها باید تصمیم بگیرن).

    # ── طبق درخواست صریح کاربر: «تعداد مقاله ≠ مناسب بودن استاد».
    # قبلاً bonusِ «مقاله دارد» (+0.08) و bonusِ works_count (تا +0.08، یعنی
    # جمعاً تا ۰.۱۶ از سقف ۱.۰ فقط از حجم/فعالیت پژوهشی می‌اومد) — یعنی یک
    # استاد با ۸۰۰ مقاله‌ی کاملاً بی‌ربط می‌تونست فقط به‌خاطر حجم، بالاتر از
    # یک استاد با ۲۵ مقاله‌ی دقیقاً منطبق بشینه. الان این‌ها Research
    # Activity محسوب می‌شن (نه Research Fit) و وزنشون به‌شدت کم شده؛ در
    # عوض سیگنال «تطابق واقعی محتوا» (similarity) که واقعاً Fit رو نشون
    # می‌ده، از ۰.۲۲ به ۰.۴۰ افزایش پیدا کرده — الان قوی‌ترین جزء امتیازه،
    # دقیقاً طبق فلسفه‌ی «Research Fit مهم‌تر از Research Activity».
    if prof.get("papers"):
        score = min(1.0, score + 0.02)  # فقط سیگنال کیفیت پروفایل («واقعاً مقاله داره»)، نه معیار رتبه‌بندی

    # bonus: works_count/paperCount — همچنان پیوسته (log-scale) ولی سقفش
    # به‌شدت کم شده (۰.۰۸ → ۰.۰۲۵) چون این «Research Activity»ه نه «Fit» —
    # نباید بین استادِ کم‌مقاله‌ی دقیقاً‌منطبق و استادِ پرمقاله‌ی نامرتبط،
    # فرق رو خودش تعیین کنه. نکته‌ی مهم: OpenAlex توی snippet می‌نویسه
    # "Works:N" ولی Semantic Scholar می‌نویسه "Papers:N" — رجکس اول فقط
    # "Works" رو تشخیص می‌داد، یعنی سیگنال Semantic Scholar نادیده گرفته می‌شد.
    snippet = prof.get("snippet","")
    works_m = re.search(r"(?:Works|Papers):\s*(\d+)", snippet)
    if works_m:
        works_count = int(works_m.group(1))
        if works_count > 0:
            score = min(1.0, score + min(0.025, math.log10(works_count + 1) * 0.01))

    # bonus: شباهت معنایی deterministic (مستقل از AI) — همون سیگنال Research
    # Fit واقعی؛ اگه idf (روی همین batch) داده شده باشه، TF-IDF واقعی؛ وگرنه
    # TF خام. توجه: این‌جا (رتبه‌بندی اولیه‌ی صدها کاندید) عمداً از embedding
    # واقعی استفاده نمی‌شه — آن لایه‌ی سنگین‌تر (Hybrid Search کامل، همراه
    # Paper-to-CV Matching) فقط بعداً برای هر استادِ انتخاب‌شده جدا در
    # get_professor_relevance اجرا می‌شه؛ نه برای همه‌ی کاندیدهای این batch.
    sim = compute_research_similarity(client, prof, idf=idf)
    score = min(1.0, score + sim * 0.40)

    # bonus: تطابق کشور — اگه کاربر کشور خاصی خواسته و این استاد دقیقاً
    # همون کشوره، یه امتیاز کوچیک اضافه؛ چون توی merge چندمنبعی ممکنه
    # استادهای کشورهای دیگه هم قاطی شده باشن (مخصوصاً وقتی فیلتر
    # OpenAlex/country match نشده) و نباید هم‌رتبه‌ی match دقیق بشن.
    wanted_country = (client.get("countries","") or "").strip().lower()
    prof_country = (prof.get("country","") or "").strip().lower()
    if wanted_country and prof_country and (wanted_country in prof_country or prof_country in wanted_country):
        score = min(1.0, score + 0.05)

    # bonus کوچیک: منبع DBLP برای رشته‌های CS معمولاً دقیق‌تر و
    # به‌روزتره (نکته‌ای که باعث اضافه شدن این منبع شد)، پس یه اعتماد
    # جزئی بهش می‌دیم — نه چیزی که به‌تنهایی رتبه رو عوض کنه.
    if str(prof.get("openalex_id","")).startswith("DBLP_") and _looks_like_cs_field(client.get("field","") or ""):
        score = min(1.0, score + 0.03)

    # جریمه‌ی «پروفایل خیلی کم‌محتوا» — قبلاً روی هر منبعی اعمال می‌شد
    # که university/papers/works نداشت، ولی این باگ واقعی بود: Web
    # Discovery (منبع اصلی و مستقیم‌ترین منبع، papers همیشه خالیه چون
    # از صفحه‌ی هیئت‌علمی میاد نه دیتابیس مقاله) و Semantic Scholar
    # (وقتی affiliation نداره) هم همیشه این شرط رو می‌گرفتن و به‌ناحق
    # جریمه می‌شدن — یعنی دقیقاً بهترین/مستقیم‌ترین کاندیدها زیر
    # کاندیدهای ضعیف‌تر ولی «پرحجم‌تر» OpenAlex دفن می‌شدن.
    # الان این جریمه فقط مخصوص همون مورد اصلی‌شه: Crossref، که چون از
    # استخراج نویسنده‌های یک مقاله میاد (نه از یک پروفایل استاد)، واقعاً
    # مستعد نویز/هم‌نامیه.
    is_crossref_extraction = str(prof.get("openalex_id","")).startswith("CR_")
    if is_crossref_extraction and not prof.get("university") and not works_m:
        score *= 0.6

    return round(min(1.0, max(0.0, score)), 4)


# عبارت‌هایی که نشون می‌دن یک استاد احتمالاً در حال پذیرش دانشجو/محقق جدیده —
# مرحله ۸ (بررسی پذیرش دانشجو). این‌ها به‌عنوان یک بونس امتیاز استفاده می‌شن،
# نه یک شرط سخت‌گیرانه (چون خیلی از صفحات این عبارت‌ها رو اصلاً ندارن ولی
# استاد بازم ممکنه در واقع دانشجو بگیره).
RECRUITING_PHRASES = [
    "open position", "open positions", "join our lab", "join my lab",
    "recruiting", "prospective student", "prospective students",
    "phd opportunit", "looking for phd", "looking for graduate",
    "we are hiring", "positions available", "accepting students",
]

# ── Funding Detector ──────────────────────────────────────────────
# اگه صفحه‌ی استاد/آزمایشگاه یکی از این عبارت‌ها رو داشته باشه، یعنی
# احتمال زیاد بودجه/گرنت فعال داره — یعنی جدی‌تر و "High Priority"ه،
# چون معمولاً وقتی گرنت فعال باشه، شانس گرفتن پاسخ و جذب دانشجو با
# بودجه بیشتره.
FUNDING_PHRASES = [
    "fully funded", "full funding", "funded position", "funded phd",
    "funded positions", "research assistantship", "research assistant position",
    "ra position", "ra positions", "teaching assistantship", "stipend",
    "grant funded", "funded by", "nsf grant", "nih grant", "erc grant",
    "dfg grant", "erc-funded", "new grant", "recently awarded",
    "awarded a grant", "external funding", "funding available",
    "scholarship available", "fully-funded",
]

# ── Professor Verification ──────────────────────────────────────────
# منابعی که ایمیل ازشون پیدا می‌شه، اعتبار متفاوتی دارن — یه ایمیل که از
# صفحه‌ی رسمی دانشکده یا آزمایشگاه اومده خیلی قابل‌اعتمادتر از یه نتیجه‌ی
# جستجوی عمومی (Google Search) یا یه پروفایل ثانویه (ResearchGate) است.
# نکته: این لیست عمداً از تاپلِ strong_page توی _professor_verification_passed
# باریک‌تره — اون یکی یک گیت دودویی («قابل‌اعتماد یا نه») است، این یکی
# یک لایه‌ی امتیازی جداگانه (۹۰ برای صفحه‌ی واقعاً پیدا/بازشده، ۷۵ برای
# متادیتای دیتابیس آکادمیک مثل OpenAlex/Semantic Scholar — که در ادامه‌ی
# همین تابع جدا محاسبه می‌شه). ادغام‌شون تفاوت این دو سطح امتیاز رو از
# بین می‌بره، پس عمداً جدا نگه داشته شدن.
HIGH_CONFIDENCE_EMAIL_SOURCES = STRONG_EMAIL_SOURCES  # قبلاً اینجا یک لیست جدا و ناقص‌تر بود؛ الان همون ثابت مشترک بالا

def compute_email_confidence(prof: dict, domain_info: dict) -> int:
    """امتیاز اطمینان به «واقعی و قابل‌استفاده بودن این ایمیل» — این با
    prof_score/Match Score (که تطابق پژوهشی رزومه‌ی کاربر با کار استاده)
    کاملاً فرق داره؛ این یکی می‌گه «چقدر مطمئنیم این آدرس واقعاً درست و
    فعاله»، نه «چقدر این استاد برای کاربر مناسبه».

    عمداً یک فرمول ساده و قابل‌توضیحه (نه خروجی یک AI که نتونیم توضیحش
    بدیم) — چون قراره دقیقاً همین عدد به کاربر نشون داده بشه؛ باید بتونیم
    بگیم چرا این عدده، نه فقط «AI همینو گفت»."""
    if prof.get("_manual_entry"):
        return 95  # کاربر خودش این ایمیل رو داده — اعتماد تقریباً کامل
    source = prof.get("email_source", "")
    if source in HIGH_CONFIDENCE_EMAIL_SOURCES:
        score = 90
    elif source in ("OpenAlex", "Semantic Scholar"):
        score = 75
    elif source:
        score = 60
    else:
        score = 40
    if domain_info.get("category") == "personal":
        score -= 20
    elif domain_info.get("category") == "university":
        score += 5
    if prof.get("accepting_students") or prof.get("has_funding"):
        score += 5
    if prof.get("is_active"):
        score += 5
    return max(0, min(100, score))

# ================================================================
# ADVISOR FIT vs RESEARCH FIT — طبق درخواست کاربر: «تناسب پژوهشی» با
# «تناسب استاد به‌عنوان راهنما» دو چیز کاملاً متفاوتن. یک استاد ممکنه
# Research Fit=98% داشته باشه (کارش دقیقاً هم‌راستای رزومه‌ی کاربره) ولی
# Advisor Fit پایین‌تر باشه چون واقعاً busy است، آزمایشگاهش بزرگ/شلوغه،
# بودجه‌ش نامشخصه، یا این حوزه الان اولویت فعلی لبش نیست.
#
# عمداً از همون سیگنال‌های واقعی/deterministic که قبلاً برای Availability
# Detection و Lab Analyzer جمع کردیم استفاده می‌کنه (نه یک عدد جدید که AI
# از خودش دربیاره) — دقیقاً طبق همون فلسفه‌ی compute_email_confidence:
# باید بتونیم توضیح بدیم چرا این عدده.
#
# این یک لیبل کمکیه، نه فیلتر — هیچ‌جا برای رد کردن/عبور دادن یک استاد
# استفاده نمی‌شه، فقط برای نمایش به کاربر (طبق درخواست صریح کاربر).
# ================================================================
def compute_advisor_fit(prof: dict) -> dict:
    """امتیاز عملی «این استاد چقدر راهنمای در دسترس/عملی‌ای خواهد بود» —
    مستقل از این‌که کارش چقدر با رزومه‌ی کاربر هم‌پوشانی داره.
    خروجی: {"score": 0-100, "factors": [توضیح کوتاه هر جزء]}"""
    score = 60  # نقطه‌ی شروع خنثی — نه سیگنال مثبت نه منفی
    factors = []

    if prof.get("accepting_students"):
        score += 20
        factors.append(f"✅ سیگنال پذیرش دانشجو ({prof.get('accepting_evidence','')[:40]})")
    else:
        factors.append("❔ سیگنال صریح پذیرش دانشجو پیدا نشد")

    if prof.get("has_funding"):
        score += 15
        factors.append(f"✅ بودجه/گرنت فعال ({prof.get('funding_evidence','')[:40]})")
    else:
        factors.append("⚠️ بودجه/گرنت فعال تأیید نشده — از خود استاد بپرسید")

    lab = prof.get("lab_analysis") or {}
    members = lab.get("members_count")
    if members:
        if members >= 15:
            score -= 12
            factors.append(f"⚠️ آزمایشگاه بزرگ ({members} عضو) — رقابت/شلوغی بیشتر برای توجه فردی")
        elif members <= 5:
            score += 8
            factors.append(f"✅ آزمایشگاه کوچیک ({members} عضو) — احتمال توجه فردی بیشتر")
        else:
            factors.append(f"➖ اندازه‌ی متوسط آزمایشگاه ({members} عضو)")

    if lab.get("industry_partners"):
        # نکته‌ی مهم: همکاری صنعتی هم می‌تونه مثبت باشه (منابع بیشتر) هم
        # منفی (وقت استاد بین چند پروژه/همکار پخش می‌شه) — عمداً امتیاز
        # رو تغییر نمی‌دیم، فقط شفاف نشون می‌دیم که این یک عامل قابل‌بررسیه.
        factors.append(f"➖ پروژه‌های collaborative با صنعت ({', '.join(lab['industry_partners'][:2])}) — "
                        f"می‌تونه یعنی منابع بیشتر یا وقت پخش‌شده‌تر، بسته به لب")

    if prof.get("is_active") is False:
        score -= 15
        factors.append("⚠️ آخرین انتشار قدیمیه — ممکنه کمتر فعال/در دسترس باشه")
    elif prof.get("is_active"):
        score += 5
        factors.append("✅ پژوهشگر فعال (انتشار اخیر)")

    avail = prof.get("availability_score")
    if avail:
        score += min(10, round(avail / 85 * 10))
        factors.append(f"✅ Supervisor Availability: {avail}/85")

    return {"score": max(0, min(100, round(score))), "factors": factors}


# ================================================================
# PROFESSOR FIT MATRIX — طبق درخواست کاربر: به‌جای یک عدد تکی («تطابق
# ۹۲٪»)، یک ماتریسِ قابل‌توضیح از ۹ معیار مجزا نشون بده. هر ردیف یا
# مستقیم از خروجی ساختاریافته‌ی AI میاد (topic_fit/methods_fit/
# publications_fit که judge_relevance حالا برمی‌گردونه) یا از سیگنال‌های
# deterministic ای که قبلاً جمع کردیم (funding/recruitment/activity/
# identity) — نه یک عدد که AI از خودش بسازه بدون این‌که بشه توضیحش داد.
# ================================================================
FIT_MATRIX_WEIGHTS = {
    "research_fit":         0.20,
    "topic_fit":             0.15,
    "methods_fit":           0.10,
    "publications_fit":      0.10,
    "academic_background":   0.10,
    "funding":                0.10,
    "recruitment":            0.10,
    "research_activity":      0.10,
    "identity_confidence":    0.05,
}

def compute_fit_matrix(prof: dict, verdict: dict, email_confidence: int,
                        similarity: float | None = None) -> dict:
    """ماتریس ۹تایی Fit + Final Recommendation (میانگین وزن‌دار توضیح‌پذیر
    از همون ۹ معیار — نه یک عدد جدا که AI مستقیم بگه). خروجی: dict با
    کلیدهای هر معیار (0-100) + "final" (0-100)."""
    research_fit = int(verdict.get("score", 0))
    topic_fit = int(verdict.get("topic_fit", research_fit))
    methods_fit = int(verdict.get("methods_fit", research_fit))
    publications_fit = int(verdict.get("publications_fit", research_fit))

    # Academic Background: شباهت معنایی deterministic بین رزومه‌ی کاربر و
    # کار استاد (همون similarity ای که get_professor_relevance حالا محاسبه
    # می‌کنه) — مستقل از قضاوت AI، به‌عنوان یک چک متقابل.
    if similarity is not None:
        academic_background = round(max(0.0, min(1.0, similarity)) * 100)
    else:
        academic_background = research_fit

    funding = 88 if prof.get("has_funding") else (45 if prof.get("has_funding") is None else 30)
    recruitment = 90 if prof.get("accepting_students") else 45
    if prof.get("is_active") is True:
        research_activity = 90
    elif prof.get("is_active") is False:
        research_activity = 40
    else:
        research_activity = 60
    identity_confidence = int(email_confidence)

    matrix = {
        "research_fit": research_fit,
        "topic_fit": topic_fit,
        "methods_fit": methods_fit,
        "publications_fit": publications_fit,
        "academic_background": academic_background,
        "funding": funding,
        "recruitment": recruitment,
        "research_activity": research_activity,
        "identity_confidence": identity_confidence,
    }
    final = sum(matrix[k] * FIT_MATRIX_WEIGHTS[k] for k in FIT_MATRIX_WEIGHTS)
    matrix["final"] = round(max(0.0, min(100.0, final)))
    return matrix


FIT_MATRIX_LABELS = {
    "research_fit": "Research Fit",
    "topic_fit": "Topic Fit",
    "methods_fit": "Methods Fit",
    "publications_fit": "Publications Fit",
    "academic_background": "Academic Background",
    "funding": "Funding",
    "recruitment": "Recruitment",
    "research_activity": "Research Activity",
    "identity_confidence": "Identity Confidence",
}

DISTANCE_LABELS = {
    "VERY_CLOSE": "VERY CLOSE",
    "ADJACENT": "ADJACENT",
    "MODERATE": "MODERATE",
    "FAR": "FAR",
}

def format_fit_matrix_block(matrix: dict) -> str:
    """جدول متنی ماتریس Fit برای پیام تلگرام — از همون سبک box-drawing
    قبلی فایل استفاده می‌کنه تا با بقیه‌ی پیام‌ها هم‌شکل بمونه.
    عرض border رو از روی خودِ محتوا محاسبه می‌کنه (نه یک عدد ثابت که با
    عوض‌شدن طول لیبل‌ها out-of-sync بشه — دقیقاً باگی که نسخه‌ی اول این
    تابع داشت: لیبل‌های بلندتر مثل «Academic Background»/«Identity
    Confidence» از عرض ثابتِ کپی‌شده از باکس قدیمی‌تر رد می‌شدن)."""
    row_fmt = "│  {label:<24}{score:>3}%  │"
    inner_width = len(row_fmt.format(label="", score=0)) - 2  # منهای دو کاراکتر border
    lines = ["┌" + "─" * inner_width + "┐"]
    for key, label in FIT_MATRIX_LABELS.items():
        lines.append(row_fmt.format(label=label, score=matrix[key]))
    lines.append("└" + "─" * inner_width + "┘")
    lines.append(f"🎯 Final Recommendation = {matrix['final']}%")
    return "\n".join(lines)


def _extract_page_signals(html: str) -> dict:
    """از یک صفحه‌ی HTML خام، هم ایمیل‌ها رو (با regex روی HTML خام — چون
    لینک‌های mailto: توی متن تمیزشده دیده نمی‌شن)، هم سیگنال «در حال
    پذیرش دانشجو»، و هم سیگنال «بودجه/گرنت فعال» (Funding Detector) رو
    استخراج می‌کنه — همه از همون یه صفحه‌ای که برای پیدا کردن ایمیل
    داشتیم می‌گرفتیم، بدون هیچ درخواست شبکه‌ی اضافه."""
    emails = extract_emails(html)
    text = html
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for t in soup(["script", "style", "nav", "footer"]):
            t.decompose()
        text = soup.get_text(" ", strip=True)
    except ImportError:
        text = re.sub(r"<[^>]+>", " ", html)
    text_l = text.lower()
    evidence = next((p for p in RECRUITING_PHRASES if p in text_l), "")
    funding_evidence = next((p for p in FUNDING_PHRASES if p in text_l), "")
    return {"emails": emails, "accepting_students": bool(evidence), "accepting_evidence": evidence,
            "has_funding": bool(funding_evidence), "funding_evidence": funding_evidence,
            "page_text": text[:4000]}

# ================================================================
# AVAILABILITY DETECTION — طبق مشخصات دقیق درخواستی، بین Ranking و
# نمایش نهایی اجرا می‌شه: با امتیازدهی افزایشی (additive) مشخص می‌کنه
# این استاد الان واقعاً «در دسترسه» یا نه — نه فقط مرتبط بودن رزومه:
#     +30  Recruiting PhD Students  (دقیق‌ترین/قوی‌ترین سیگنال)
#     +25  Join Our Lab             (دعوت مستقیم آزمایشگاه)
#     +20  Funding                  (بودجه/گرنت فعال)
#     +10  Recent Publication       (فعال بودن پژوهشی — از سال آخرین مقاله)
# جمع این چهار سیگنال می‌تونه تا ۸۵ برسه؛ به‌عنوان بونس رتبه‌بندی روی
# امتیاز تطابق پژوهشی اضافه می‌شه (نه جایگزینش) — یعنی یک استاد کاملاً
# بی‌ربط با ۴ تا سیگنال Availability هم بالای لیست نمی‌ره، ولی بین دو
# استاد که هر دو واقعاً مرتبطن، اونی که الان واقعاً دانشجو می‌گیره
# جلوتر می‌افته.
# ================================================================
_AVAIL_PHD_RECRUIT_RE = re.compile(
    r"recruiting\s+(?:phd|ph\.?d\.?|doctoral)\s+students?|"
    r"accepting\s+(?:phd|ph\.?d\.?|doctoral)\s+students?|"
    r"looking\s+for\s+(?:phd|ph\.?d\.?|doctoral)\s+students?", re.I)

_AVAIL_JOIN_LAB_RE = re.compile(
    r"join\s+(?:our|my|the)\s+lab|"
    r"join\s+(?:our|my|the)\s+(?:research\s+)?(?:group|team)", re.I)

def detect_availability_signals(page_text: str, prof: dict | None = None) -> tuple[int, list[str]]:
    """Availability Detection: از متن خام صفحه‌ی استاد/دانشکده/آزمایشگاه
    (page_text) + سیگنال‌های از قبل جمع‌شده‌ی prof (مثل last_pub_year که
    از پابلیکیشن‌ها میاد، نه از متن خام) امتیاز عددی می‌سازه.
    خروجی: (امتیاز ۰ تا ۸۵, لیست شواهد قابل‌نمایش به کاربر)."""
    score = 0
    evidence: list[str] = []
    text_l = (page_text or "").lower()

    if _AVAIL_PHD_RECRUIT_RE.search(text_l):
        score += 30
        evidence.append("🎓 Recruiting PhD Students (+30)")

    if _AVAIL_JOIN_LAB_RE.search(text_l):
        score += 25
        evidence.append("🧪 Join Our Lab (+25)")

    if any(p in text_l for p in FUNDING_PHRASES):
        score += 20
        evidence.append("💰 Funding (+20)")

    # Recent Publication: بر خلاف ۳ سیگنال بالا، از متن خام صفحه نمیاد —
    # از last_pub_year که قبلاً توسط _with_activity محاسبه شده. اگه آخرین
    # مقاله‌ی استاد ≤ ۲ سال قبل باشه، یعنی هنوز فعالانه پژوهش می‌کنه.
    prof = prof or {}
    last_pub_year = prof.get("last_pub_year")
    if last_pub_year:
        try:
            if (datetime.now().year - int(last_pub_year)) <= 2:
                score += 10
                evidence.append(f"📄 Recent Publication ({last_pub_year}) (+10)")
        except (TypeError, ValueError):
            pass

    return score, evidence

# ── Failover واقعی برای DuckDuckGo ─────────────────────────────────
# قبلاً وقتی DDG خراب/کند می‌شد، هر ۷ مرحله (که همه از DDG استفاده
# می‌کردن) به‌ترتیب تلاش می‌شدن و همه‌شون timeout می‌خوردن — یعنی توی لاگ
# «DDG Timeout» چند بار پشت‌سرهم دیده می‌شد، انگار داریم همون سرویس رو
# retry می‌زنیم، نه واقعاً منبع عوض می‌کنیم. الان دو تا اصلاح اساسی شده:
#  ۱) Circuit Breaker: اگه DDG پشت‌سرهم خراب باشه (خصوصاً خطاهای «سخت»ی
#     مثل ProxyError/ConnectionError/SSLError که Retry زدن روی همون
#     درخواست هیچ کمکی نمی‌کنه)، برای یه مدت (cooldown) اصلاً سراغ DDG
#     نمی‌ریم — مستقیم و فوری skip می‌شه، بدون حتی یک تلاش دیگه.
#  ۲) اجرای موازی: هر ۷ منبع هم‌زمان (نه یکی‌یکی) اجرا می‌شن، پس اگه DDG
#     خراب باشه، بقیه‌ی منابع (ORCID، fetch مستقیم homepage) هنوز به‌موقع
#     جواب می‌دن؛ و کل عملیات به‌جای «مجموع ۷ timeout» فقط «کندترین
#     مرحله» طول می‌کشه.
_ddg_breaker_lock = threading.Lock()
_ddg_breaker = {"failures": 0, "opened_at": 0.0}
DDG_BREAKER_THRESHOLD = int(os.getenv("DDG_BREAKER_THRESHOLD", "3") or "3")
DDG_BREAKER_COOLDOWN  = int(os.getenv("DDG_BREAKER_COOLDOWN_SEC", "300") or "300")  # ۵ دقیقه
DDG_HARD_ERROR_WEIGHT = 3  # خطای «سخت» (Proxy/Connection/SSL) بلافاصله مدار رو قطع می‌کنه

# Timeoutها قابل تنظیم با env var — بدون نیاز به تغییر کد
SEARCH_TIMEOUT_SEC     = float(os.getenv("SEARCH_TIMEOUT_SEC", "6") or "6")       # هر جستجوی DDG
PAGE_FETCH_TIMEOUT_SEC = float(os.getenv("PAGE_FETCH_TIMEOUT_SEC", "6") or "6")   # گرفتن هر صفحه‌ی نتیجه
EMAIL_PIPELINE_TIMEOUT_SEC = float(os.getenv("EMAIL_PIPELINE_TIMEOUT_SEC", "14") or "14")  # کل pipeline موازی
# سقف تعداد Threadهای هم‌زمان برای هر فراخوانی pipeline ایمیل‌یابی. قبلاً
# max_workers=len(tasks) بود (تا ۱۱ Thread برای هر استاد) — روی هاست‌های
# محدود (CPU کم) یا وقتی چند کاربر هم‌زمان در حال ارسالن، این می‌تونه CPU
# رو اشباع کنه و ریسک rate-limit روی سرویس‌های خارجی رو بالا ببره.
EMAIL_PIPELINE_MAX_WORKERS = int(os.getenv("EMAIL_PIPELINE_MAX_WORKERS", "5") or "5")

def _ddg_breaker_is_open() -> bool:
    """True یعنی DDG اخیراً پشت‌سرهم خراب بوده — فعلاً اصلاً سراغش نریم."""
    with _ddg_breaker_lock:
        if _ddg_breaker["failures"] < DDG_BREAKER_THRESHOLD:
            return False
        elapsed = time.monotonic() - _ddg_breaker["opened_at"]
        if elapsed > DDG_BREAKER_COOLDOWN:
            # half-open: بعد از cooldown یک تلاش دیگه مجازه — اگه اون هم
            # شکست بخوره، بلافاصله دوباره قطع می‌شه (چون failures هنوز
            # نزدیک threshold ئه).
            _ddg_breaker["failures"] = DDG_BREAKER_THRESHOLD - 1
            return False
        return True

def _ddg_breaker_record(success: bool, hard_error: bool = False):
    with _ddg_breaker_lock:
        if success:
            _ddg_breaker["failures"] = 0
        else:
            _ddg_breaker["failures"] += DDG_HARD_ERROR_WEIGHT if hard_error else 1
            if _ddg_breaker["failures"] >= DDG_BREAKER_THRESHOLD:
                _ddg_breaker["opened_at"] = time.monotonic()
                logger.warning(f"⚡ DDG circuit breaker باز شد — تا {DDG_BREAKER_COOLDOWN}s سراغش نمی‌ریم، مستقیم می‌ریم سراغ منابع دیگه.")

def _is_hard_network_error(e: Exception) -> bool:
    """خطاهایی که Retry زدن روی همون درخواست کمکی نمی‌کنه — باید فوراً
    عوض کنیم نه دوباره امتحان کنیم."""
    name = type(e).__name__
    return any(h in name for h in ("Proxy", "Connection", "SSL", "DNS"))

# ── Circuit Breaker عمومی برای providerهای AI (Gemini/Groq) ──────────
# دقیقاً همون الگوی _ddg_breaker بالا، فقط generalized برای هر provider
# (با کلید جدا برای هرکدوم، چون ممکنه یکی down باشه و اون یکی سالم).
_ai_breaker_lock = threading.Lock()
_ai_breakers: dict[str, dict] = {}  # provider name -> {"failures":int,"opened_at":float}
AI_BREAKER_THRESHOLD = int(os.getenv("AI_BREAKER_THRESHOLD", "5") or "5")
AI_BREAKER_COOLDOWN  = int(os.getenv("AI_BREAKER_COOLDOWN_SEC", "30") or "30")

def _ai_breaker_is_open(provider: str) -> bool:
    with _ai_breaker_lock:
        b = _ai_breakers.setdefault(provider, {"failures": 0, "opened_at": 0.0})
        if b["failures"] < AI_BREAKER_THRESHOLD:
            return False
        elapsed = time.monotonic() - b["opened_at"]
        if elapsed > AI_BREAKER_COOLDOWN:
            # half-open: یک تلاش دیگه مجازه؛ اگه اون هم شکست بخوره،
            # بلافاصله دوباره قطع می‌شه.
            b["failures"] = AI_BREAKER_THRESHOLD - 1
            return False
        return True

def _ai_breaker_record(provider: str, success: bool):
    with _ai_breaker_lock:
        b = _ai_breakers.setdefault(provider, {"failures": 0, "opened_at": 0.0})
        if success:
            b["failures"] = 0
        else:
            b["failures"] += 1
            if b["failures"] >= AI_BREAKER_THRESHOLD:
                b["opened_at"] = time.monotonic()
                logger.warning(
                    f"⚡ {provider} circuit breaker باز شد — تا {AI_BREAKER_COOLDOWN}s "
                    "سراغش نمی‌ریم، مستقیم می‌ریم سراغ fallback بعدی.")

# ================================================================
# EMAIL PATTERN ENRICHMENT — حدس + تایید SMTP برای استادهای بدون ایمیل
# ================================================================
# مشکل واقعی: توی اکثر مقالات علمی فقط ایمیل نویسنده‌ی مسئول عمومیه، نه
# همه‌ی نویسنده‌ها — پس استادی که دقیقاً رشته/کشور درستیه ممکنه فقط چون
# هم‌نویسنده بوده (نه نویسنده‌ی مسئول)، ایمیل نداشته باشه و کلاً از لیست
# نتایج حذف بشه. اینجا از استادهای *همون دانشگاه* که ایمیل دارن، الگوی
# ایمیل دانشگاه رو یاد می‌گیریم و برای بقیه‌ی استادهای همون دانشگاه که
# ایمیل ندارن حدس می‌زنیم — ولی فقط بعد از تایید واقعی با SMTP handshake
# (بدون فرستادن ایمیل واقعی). یک حدسِ تایید‌نشده هیچ‌وقت به‌عنوان ایمیل
# ثبت نمی‌شه، چون یعنی ریسک واقعی bounce موقع ارسال.
SMTP_PROBE_TIMEOUT_SEC       = float(os.getenv("SMTP_PROBE_TIMEOUT_SEC", "5") or "5")
SMTP_PROBE_MAX_WORKERS       = int(os.getenv("SMTP_PROBE_MAX_WORKERS", "6") or "6")
# سقف تعداد استاد بدون ایمیل که در هر جستجو امتحان می‌شه — حتی اگه صدها
# تا باشن، فقط همین تعداد رو امتحان می‌کنیم تا کل جستجو کند نشه. چون این
# فقط روی cache miss اجرا می‌شه (یک‌بار به‌ازای هر field+country، نه به‌ازای
# هر کاربر)، این هزینه بین همه‌ی کاربرهای بعدی همون جستجو مشترکه.
SMTP_PROBE_MAX_CANDIDATES    = int(os.getenv("SMTP_PROBE_MAX_CANDIDATES", "12") or "12")
SMTP_PROBE_BREAKER_THRESHOLD = int(os.getenv("SMTP_PROBE_BREAKER_THRESHOLD", "5") or "5")
SMTP_PROBE_BREAKER_COOLDOWN  = int(os.getenv("SMTP_PROBE_BREAKER_COOLDOWN_SEC", "3600") or "3600")  # ۱ ساعت
# سقف زمانی کل مرحله‌ی ۴ (تایید موازی همه‌ی کاندیدها) — دفاع تکمیلی: هر
# probe جدا timeout داره (SMTP_PROBE_TIMEOUT_SEC) و از نظر تئوری کل
# مرحله باید در ceil(candidates/workers)×چندتا-round-trip زمان تموم بشه،
# ولی به‌جای فرض کردن این ریاضی همیشه درسته، یک سقف مطلق هم می‌ذاریم —
# همون الگویی که برای email-pipeline waves (_run_wave) هم استفاده شد.
SMTP_PROBE_TOTAL_TIMEOUT_SEC = float(os.getenv("SMTP_PROBE_TOTAL_TIMEOUT_SEC", "25") or "25")

# ── الگوهای رایج ایمیل سازمانی — دقیقاً همون ۸ الگویی که ابزارهای
#    شناخته‌شده‌ی این حوزه (Hunter.io/Tomba و مشابه) استفاده می‌کنن ──
_EMAIL_PATTERN_TEMPLATES: list[tuple[str, "callable"]] = [
    ("first.last", lambda f, l: f"{f}.{l}"),
    ("f.last",     lambda f, l: f"{f[:1]}.{l}" if f else l),
    ("firstlast",  lambda f, l: f"{f}{l}"),
    ("flast",      lambda f, l: f"{f[:1]}{l}" if f else l),
    ("first_last", lambda f, l: f"{f}_{l}"),
    ("last.first", lambda f, l: f"{l}.{f}"),
    ("lastf",      lambda f, l: f"{l}{f[:1]}" if f else l),
    ("last",       lambda f, l: l),
]
_NAME_TITLE_RE = re.compile(r"^(dr\.?|prof\.?|professor|associate\s+prof\.?|assistant\s+prof\.?|mr\.?|mrs\.?|ms\.?)\s+",
                             re.IGNORECASE)

def _split_prof_name(name: str) -> tuple[str, str] | None:
    """اسم رو به (first, last) لاتین/lowercase می‌شکنه — برای اسم‌های
    غیرلاتین (فارسی، چینی و...) که توی این pipeline رومن‌نویسی نشدن،
    عمداً None برمی‌گردونه (نه یک حدس اشتباه) چون بهتره کلاً حدس نزنیم
    تا یک حدس بی‌پایه بزنیم."""
    n = _NAME_TITLE_RE.sub("", (name or "").strip())
    n = re.sub(r"[^\w\s\-'.]", "", n)
    parts = [p for p in n.split() if p and p.lower() not in ("phd", "md")]
    if len(parts) < 2:
        return None
    first = re.sub(r"[^a-zA-Z]", "", parts[0]).lower()
    last  = re.sub(r"[^a-zA-Z]", "", parts[-1]).lower()
    if not first or not last:
        return None
    return first, last

def _detect_email_pattern(known: list[tuple[str, str, str]]) -> str | None:
    """known: [(email, first, last), ...] همه از یک دانشگاه/یک دامنه.
    فقط وقتی یک الگو رو برمی‌گردونه که اکثریت واضحی از نمونه‌ها باهاش
    match داشته باشن — وگرنه None (یعنی بهتره اصلاً حدس نزنیم)."""
    if len(known) < 2:
        return None
    votes: dict[str, int] = {}
    for email, first, last in known:
        local = email.split("@")[0].lower()
        for tname, fn in _EMAIL_PATTERN_TEMPLATES:
            try:
                if fn(first, last) == local:
                    votes[tname] = votes.get(tname, 0) + 1
                    break  # هر ایمیل فقط به اولین template ای که match داره رای می‌ده
            except Exception:
                continue
    if not votes:
        return None
    best_name, best_count = max(votes.items(), key=lambda kv: kv[1])
    required = max(2, math.ceil(len(known) * 0.6))
    return best_name if best_count >= required else None

_smtp_probe_lock = threading.Lock()
_smtp_probe_breaker = {"failures": 0, "opened_at": 0.0}

def _smtp_probe_breaker_is_open() -> bool:
    """True یعنی SMTP probing اخیراً پشت‌سرهم بی‌نتیجه بوده (خیلی محتمله
    پورت ۲۵ روی این سرور/هاست بسته باشه — خیلی از هاست‌های ابری پیش‌فرض
    مسدودش می‌کنن) — فعلاً کلاً امتحانش نکنیم تا هر جستجو رو کند نکنه."""
    with _smtp_probe_lock:
        if _smtp_probe_breaker["failures"] < SMTP_PROBE_BREAKER_THRESHOLD:
            return False
        elapsed = time.monotonic() - _smtp_probe_breaker["opened_at"]
        if elapsed > SMTP_PROBE_BREAKER_COOLDOWN:
            _smtp_probe_breaker["failures"] = SMTP_PROBE_BREAKER_THRESHOLD - 1  # half-open
            return False
        return True

def _smtp_probe_record(result: bool | None):
    """فقط نتیجه‌ی *نامشخص* (نه رد صریح، نه قبول صریح) رو به‌عنوان
    نشونه‌ی مشکل شبکه حساب می‌کنیم — یک 550 رد صریح یعنی شبکه سالمه، فقط
    آدرس غلط بوده، این نباید breaker رو باز کنه."""
    with _smtp_probe_lock:
        if result is None:
            _smtp_probe_breaker["failures"] += 1
            if _smtp_probe_breaker["failures"] >= SMTP_PROBE_BREAKER_THRESHOLD:
                _smtp_probe_breaker["opened_at"] = time.monotonic()
                logger.warning(f"⚡ SMTP probe circuit breaker باز شد (احتمالاً پورت ۲۵ بسته‌ست) — "
                                f"تا {SMTP_PROBE_BREAKER_COOLDOWN}s سراغش نمی‌ریم.")
        else:
            _smtp_probe_breaker["failures"] = 0

def _get_mx_host(domain: str) -> str | None:
    """فقط اسم هاست MX با بالاترین اولویت رو برمی‌گردونه — DNS lookup،
    نه اتصال SMTP (پس نیازی به باز بودن پورت ۲۵ نداره، سریع و بی‌خطره)."""
    try:
        import dns.resolver
        answers = dns.resolver.resolve(domain, "MX", lifetime=6)
        best = min(answers, key=lambda r: r.preference)
        return str(best.exchange).rstrip(".")
    except Exception:
        return None

def _smtp_rcpt_probe(email: str, mx_host: str, timeout: float = SMTP_PROBE_TIMEOUT_SEC) -> bool | None:
    """بدون فرستادن هیچ ایمیل واقعی، فقط تا RCPT TO می‌ره جلو تا ببینه
    سرور آدرس رو قبول می‌کنه یا نه، بعد فوراً قطع می‌کنه. True=قبول شد،
    False=صریحاً رد شد (مثلاً ۵۵۰ user unknown)، None=نامشخص (تایم‌اوت،
    اتصال رد شد، پورت بسته، greylisting و...). هیچ‌وقت raise نمی‌کنه."""
    try:
        with smtplib.SMTP(timeout=timeout) as smtp:
            smtp.connect(mx_host, 25)
            smtp.helo("verify.local")
            smtp.mail("verify@verify.local")
            code, _ = smtp.rcpt(email)
            result = True if 200 <= code < 300 else (False if 500 <= code < 600 else None)
    except Exception as e:
        logger.debug(f"SMTP probe {email}@{mx_host}: {type(e).__name__}: {e}")
        result = None
    _smtp_probe_record(result)
    return result

def _domain_is_catchall(domain: str, mx_host: str) -> bool:
    """قبل از اعتماد به هر RCPT-accept روی این دامنه، یه local-part
    قطعاً جعلی رو امتحان می‌کنیم. اگه سرور اونم قبول کرد، یعنی این دامنه
    catch-all هست (به هر آدرسی جواب «معتبر» می‌ده) و هیچ‌کدوم از
    تاییدهای بعدیش قابل‌اعتماد نیست — کل دامنه رو رد می‌کنیم."""
    fake_local = f"doesnotexist{secrets.token_hex(4)}"
    return _smtp_rcpt_probe(f"{fake_local}@{domain}", mx_host) is True

def _enrich_missing_emails(profs: list[dict]) -> list[dict]:
    """نقطه‌ی ورود اصلی. روی همون لیست profs جای‌گذاری می‌کنه (ایمیل رو
    مستقیم روی دیکشنری‌های موجود می‌نویسه) و همون لیست رو برمی‌گردونه.
    هر خطای غیرمنتظره‌ای کاملاً بی‌صدا می‌بلعه و لیست اصلی رو دست‌نخورده
    برمی‌گردونه — این تابع هیچ‌وقت نباید کل جستجوی استاد رو خراب کنه."""
    try:
        if not profs or _smtp_probe_breaker_is_open():
            return profs

        # ── گام ۱: گروه‌بندی بر اساس دانشگاه (نه دامنه — چون استاد بدون
        # ایمیل هیچ دامنه‌ای نداره؛ دانشگاه تنها فیلد مشترکیه که همیشه
        # داریم) ──
        by_uni_known: dict[str, list[tuple[str, str, str, str]]] = {}  # uni -> [(email, domain, first, last)]
        missing: list[tuple[dict, str, str, str]] = []                  # (prof, uni, first, last)
        for p in profs:
            uni = (p.get("university") or "").strip().lower()
            if not uni:
                continue
            parsed = _split_prof_name(p.get("name", ""))
            if not parsed:
                continue
            email = (p.get("email") or "").strip()
            if email and "@" in email and _EMAIL_FORMAT_RE.match(email):
                domain = email.rsplit("@", 1)[-1].lower()
                by_uni_known.setdefault(uni, []).append((email.lower(), domain, parsed[0], parsed[1]))
            else:
                missing.append((p, uni, parsed[0], parsed[1]))
        if not missing:
            return profs

        # ── گام ۲: به‌ازای هر دانشگاه، دامنه‌ی غالب + الگوی ایمیل رو کشف کن ──
        uni_domain_pattern: dict[str, tuple[str, str]] = {}
        for uni, known in by_uni_known.items():
            if uni not in {u for _, u, _, _ in missing} or len(known) < 2:
                continue
            domain_votes: dict[str, int] = {}
            for _, domain, _, _ in known:
                domain_votes[domain] = domain_votes.get(domain, 0) + 1
            dom, cnt = max(domain_votes.items(), key=lambda kv: kv[1])
            if cnt < max(2, math.ceil(len(known) * 0.6)):
                continue  # دانشگاه از چند دامنه‌ی متفاوت استفاده می‌کنه، اطمینان کافی نیست
            same_domain = [(e, f, l) for e, d, f, l in known if d == dom]
            pattern = _detect_email_pattern(same_domain)
            if pattern:
                uni_domain_pattern[uni] = (dom, pattern)
        if not uni_domain_pattern:
            return profs

        candidates = [(p, *uni_domain_pattern[uni], first, last)
                       for (p, uni, first, last) in missing if uni in uni_domain_pattern]
        candidates = candidates[:SMTP_PROBE_MAX_CANDIDATES]
        if not candidates:
            return profs

        # ── گام ۳: MX + تشخیص catch-all — یک‌بار به‌ازای هر دامنه‌ی یکتا ──
        domains = sorted({dom for _, dom, _, _, _ in candidates})
        mx_map: dict[str, str | None] = {d: _get_mx_host(d) for d in domains}
        catchall_map: dict[str, bool] = {
            d: _domain_is_catchall(d, mx_map[d]) for d in domains if mx_map[d]
        }
        usable = [c for c in candidates if mx_map.get(c[1]) and not catchall_map.get(c[1], True)]
        if not usable:
            return profs

        # ── گام ۴: تایید موازی (محدود) هر کاندید با RCPT TO واقعی ──
        pattern_fns = dict(_EMAIL_PATTERN_TEMPLATES)
        verified = 0
        t0 = time.time()
        # همون فیکس _run_wave: به‌جای `with ... as ex:` (که موقع خروج
        # shutdown(wait=True) می‌زنه و منتظر همه‌ی probe های عقب‌مونده
        # می‌مونه)، pool رو دستی مدیریت می‌کنیم و با یک سقف زمانی مطلق
        # (SMTP_PROBE_TOTAL_TIMEOUT_SEC) + shutdown(wait=False) خارج
        # می‌شیم — حتی اگه چندتا probe به هر دلیلی کندتر از حد معمول شن.
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=SMTP_PROBE_MAX_WORKERS)
        try:
            futs = {}
            for p, domain, pattern, first, last in usable:
                try:
                    local = pattern_fns[pattern](first, last)
                except Exception:
                    continue
                if not local:
                    continue
                candidate_email = f"{local}@{domain}"
                if db_is_known_bad_email(candidate_email)[0]:
                    continue
                fut = pool.submit(_smtp_rcpt_probe, candidate_email, mx_map[domain])
                futs[fut] = (p, candidate_email)
            done, not_done = concurrent.futures.wait(futs, timeout=SMTP_PROBE_TOTAL_TIMEOUT_SEC)
            for fut in done:
                p, candidate_email = futs[fut]
                try:
                    ok = fut.result()
                except Exception:
                    ok = None
                if ok is True:
                    p["email"] = candidate_email
                    p["email_source"] = "pattern_guess_verified"
                    verified += 1
            if not_done:
                logger.debug(f"{len(not_done)} SMTP probe(s) still running in background after "
                             f"{SMTP_PROBE_TOTAL_TIMEOUT_SEC}s — نتیجه‌شون نادیده گرفته می‌شه")
        finally:
            pool.shutdown(wait=False)
        if verified:
            logger.info(f"email pattern enrichment: +{verified} verified emails "
                        f"({len(usable)} candidates checked, {time.time()-t0:.1f}s)")
        return profs
    except Exception as e:
        logger.error(f"_enrich_missing_emails: {e}")
        return profs

def _fetch_homepage_direct(url: str) -> dict:
    """مسیر کاملاً مستقل از DDG — مستقیم یک URL شناخته‌شده (مثلاً از
    OpenAlex) رو می‌گیره، بدون اینکه اصلاً به هیچ موتور جستجویی نیاز
    داشته باشه. حتی اگه DDG کاملاً خراب باشه، این مسیر دست‌نخورده کار
    می‌کنه."""
    try:
        resp = _safe_requests_get(url, headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
        sig = _extract_page_signals(resp.text)
        edu = [e for e in sig["emails"] if any(d in e for d in (".edu", ".ac.", ".uni-"))]
        if not (edu or sig["emails"]):
            return {}
        return {"email": (edu or sig["emails"])[0],
                "accepting_students": sig["accepting_students"],
                "accepting_evidence": sig["accepting_evidence"],
                "has_funding": sig["has_funding"],
                "funding_evidence": sig["funding_evidence"],
                "is_lab_page": True, "lab_page_text": sig["page_text"]}
    except Exception as e:
        logger.debug(f"direct homepage fetch {url}: {e}")
        return {}

def _openalex_email_lookup(name: str, university: str, openalex_id: str = "") -> dict:
    """مسیر کاملاً مستقل از هر موتور جستجو (نه DDG، نه Google) — مستقیم از
    OpenAlex API آدرس صفحه‌ی شخصی استاد یا دانشگاهش رو می‌گیره و خودش
    fetch می‌کنه. حتی اگه همه‌ی موتورهای جستجو هم‌زمان از کار بیفتن، این
    مسیر دست‌نخورده کار می‌کنه چون فقط به OpenAlex API وابسته‌ست."""
    try:
        data = {}
        aid = openalex_id.split("/")[-1] if openalex_id and not openalex_id.startswith("SS_") else ""
        if aid:
            r = requests.get(f"https://api.openalex.org/authors/{aid}",
                headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
            data = r.json()
        elif name:
            r = requests.get("https://api.openalex.org/authors",
                params={"search": name, "per-page": 1}, headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
            results = r.json().get("results", [])
            data = results[0] if results else {}
        homepage = data.get("homepage_url", "")
        if not homepage:
            insts = data.get("last_known_institutions") or []
            if isinstance(data.get("last_known_institution"), dict):
                insts = [data["last_known_institution"]]
            if insts:
                homepage = insts[0].get("homepage_url", "")
        if homepage:
            return _fetch_homepage_direct(homepage)
    except Exception as e:
        logger.debug(f"OpenAlex direct lookup for {name}: {e}")
    return {}

def _semantic_scholar_email_lookup(name: str) -> dict:
    """مسیر مستقل دیگه — API رسمی Semantic Scholar (نه یک موتور جستجو،
    مستقیم author search) برای گرفتن homepage استاد."""
    if not name:
        return {}
    try:
        r = requests.get("https://api.semanticscholar.org/graph/v1/author/search",
            params={"query": name, "fields": "homepage"},
            headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
        data = r.json().get("data", [])
        if data and data[0].get("homepage"):
            return _fetch_homepage_direct(data[0]["homepage"])
    except Exception as e:
        logger.debug(f"Semantic Scholar direct lookup for {name}: {e}")
    return {}

def _faculty_directory_probe(university: str, name: str) -> dict:
    """به‌جای جستجو کردن (که به یه موتور جستجو وابسته‌ست)، مستقیم آدرس
    دانشگاه رو از OpenAlex می‌گیره و چند مسیر رایج دایرکتوری دانشکده
    (people/faculty/directory/search) رو مستقیماً روی دامنه‌ی خودِ
    دانشگاه امتحان می‌کنه — کاملاً مستقل از DDG یا Google."""
    if not university or not name:
        return {}
    try:
        r = requests.get("https://api.openalex.org/institutions",
            params={"search": university, "per-page": 1}, headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
        insts = r.json().get("results", [])
        homepage = insts[0].get("homepage_url", "") if insts else ""
        if not homepage:
            return {}
        base = homepage.rstrip("/")
        last_name = name.split()[-1].lower()
        for path in ("/people", "/faculty", "/directory", f"/search?q={last_name}"):
            try:
                resp = requests.get(base + path, headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
                if resp.status_code != 200:
                    continue
                sig = _extract_page_signals(resp.text)
                edu = [e for e in sig["emails"]
                       if last_name in e.lower() and any(d in e for d in (".edu", ".ac.", ".uni-"))]
                if edu:
                    return {"email": edu[0], "accepting_students": sig["accepting_students"],
                            "accepting_evidence": sig["accepting_evidence"],
                            "has_funding": sig["has_funding"], "funding_evidence": sig["funding_evidence"],
                            "is_lab_page": True, "lab_page_text": sig["page_text"]}
            except Exception:
                continue
    except Exception as e:
        logger.debug(f"faculty directory probe for {university}: {e}")
    return {}

def _google_search_and_scrape(query: str, max_pages: int = 3) -> dict:
    """یک backend کاملاً جدا از DDG — مستقیم google.com. اگه گوگل بلاکش
    کنه (که روی سرورهای ابری/دیتاسنتر محتمله)، سریع و بی‌سروصدا شکست
    می‌خوره — فقط یعنی این یکی از منابع موازی چیزی برنگردونده؛ بقیه
    (DDG، OpenAlex، Semantic Scholar، دایرکتوری مستقیم) دست‌نخورده
    باقی می‌مونن، دقیقاً همون فلسفه‌ی failover واقعی."""
    out = {"email": "", "accepting_students": False, "accepting_evidence": "",
           "has_funding": False, "funding_evidence": ""}
    try:
        resp = requests.get("https://www.google.com/search",
            params={"q": query, "num": max_pages},
            headers={**HEADERS, "Accept-Language": "en-US,en;q=0.9"},
            timeout=SEARCH_TIMEOUT_SEC)
        urls = re.findall(r'href="(https?://(?!(?:www\.)?google)[^"&]+)"', resp.text)
        seen = set()
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            if len(seen) > max_pages:
                break
            try:
                page = _safe_requests_get(url, headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
                sig = _extract_page_signals(page.text)
            except Exception:
                continue
            edu_emails = [e for e in sig["emails"] if any(d in e for d in (".edu", ".ac.", ".uni-"))]
            if edu_emails and not out["email"]:
                out["email"] = edu_emails[0]
                out["is_lab_page"] = True
                out["lab_page_text"] = sig["page_text"]
            if sig["accepting_students"] and not out["accepting_students"]:
                out["accepting_students"] = True
                out["accepting_evidence"] = sig["accepting_evidence"]
            if out["email"]:
                break
    except Exception as e:
        logger.debug(f"Google direct search '{query[:50]}': {e}")
    return out

def _ddg_search_and_scrape(query: str, max_pages: int = 3, timeout_each: float | None = None) -> dict:
    """یک query جستجو می‌کنه و صفحات نتیجه رو یکی‌یکی می‌گیره تا یک ایمیل
    دانشگاهی معتبر یا سیگنال «پذیرش دانشجو» پیدا کنه. اگه circuit breaker
    باز باشه، حتی یک تلاش هم نمی‌کنه — فوری برمی‌گرده تا منابع دیگه (که
    موازی در حال اجران) وقت تلف نکنن."""
    out = {"email": "", "accepting_students": False, "accepting_evidence": "",
           "has_funding": False, "funding_evidence": ""}
    if _ddg_breaker_is_open():
        logger.debug(f"DDG breaker OPEN — skip '{query[:50]}' بدون تلاش")
        return out
    timeout_each = timeout_each or PAGE_FETCH_TIMEOUT_SEC
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=SEARCH_TIMEOUT_SEC) as ddgs:
            results = list(ddgs.text(query, max_results=max_pages))
        _ddg_breaker_record(success=True)
    except Exception as e:
        _ddg_breaker_record(success=False, hard_error=_is_hard_network_error(e))
        logger.debug(f"DDG search '{query[:60]}' failed ({type(e).__name__}): {e}")
        return out
    for r in results:
        url = r.get("href", "")
        if not url:
            continue
        try:
            resp = _safe_requests_get(url, headers=HEADERS, timeout=timeout_each)
            sig = _extract_page_signals(resp.text)
        except Exception:
            continue
        edu_emails = [e for e in sig["emails"] if any(d in e for d in (".edu", ".ac.", ".uni-"))]
        if edu_emails and not out["email"]:
            out["email"] = edu_emails[0]
            out["is_lab_page"] = True
            out["lab_page_text"] = sig["page_text"]
        if sig["accepting_students"] and not out["accepting_students"]:
            out["accepting_students"] = True
            out["accepting_evidence"] = sig["accepting_evidence"]
        if sig["has_funding"] and not out["has_funding"]:
            out["has_funding"] = True
            out["funding_evidence"] = sig["funding_evidence"]
        if out["email"]:
            break
    return out

def find_professor_email_and_signals(name: str, university: str, openalex_id: str = "",
                                      homepage_hint: str = "") -> dict:
    """پیدا کردن ایمیل استاد از چند منبع، در دو موج:
    موج ۱ (همیشه اجرا می‌شه): منابع مستقل از DDG — Lab Website مستقیم،
    ORCID، OpenAlex، Faculty Directory، Semantic Scholar، Google Search
    مستقیم — به‌صورت موازی (نه یکی‌یکی) با سقف {EMAIL_PIPELINE_MAX_WORKERS}
    Thread هم‌زمان.
    موج ۲ (فقط اگه موج ۱ چیزی پیدا نکرد): منابع مبتنی بر DuckDuckGo —
    University Faculty Page، Lab Website (DDG)، Department Directory،
    Google Scholar، Google Search (DDG)، ResearchGate. یعنی DDG واقعاً
    fallback آخره، نه یه منبع هم‌ردیف بقیه — هم فشار کمتری روی DDG میاد،
    هم اکثر وقت‌ها اصلاً لازم نمی‌شه سراغش بریم.

    در هر موج، نتیجه بر اساس ترتیب اولویت (نه هر کدوم زودتر تموم شد)
    انتخاب می‌شه.
    برمی‌گردونه: {"email", "email_source", "accepting_students",
    "accepting_evidence", "has_funding", "funding_evidence",
    "lab_page_text", "is_lab_page"}"""
    result = {"email": "", "email_source": "", "accepting_students": False, "accepting_evidence": "",
              "has_funding": False, "funding_evidence": "", "lab_page_text": "", "is_lab_page": False,
              "availability_score": 0, "availability_evidence": [], "funding_intelligence": None}
    if not name:
        return result

    # ── Professor Cache ────────────────────────────────────────────
    # اگه امروز (یا این چند هفته‌ی اخیر) قبلاً همین استاد رو پیدا کردیم —
    # چه برای همین کاربر چه کاربر دیگه‌ای — دیگه دوباره ۱۱ منبع مختلف رو
    # جستجو نمی‌کنیم. cache miss یا خطا در خواندن کش هم به‌هیچ‌وجه جلوی
    # جستجوی عادی رو نمی‌گیره — فقط یعنی از صفر جستجو می‌کنیم.
    try:
        cached = db_get_professor_cache(name, university)
    except Exception as e:
        logger.debug(f"professor cache read for '{name}': {e}")
        cached = None
    if cached:
        logger.info(f"💾 Email pipeline for '{name}': cache hit (email_source={cached.get('email_source')})")
        return cached

    # ── موج ۱: منابع مستقل از DDG ────────────────────────────────
    # این‌ها API/فچ مستقیم‌ان (نه اسکرِیپ نتایج جستجو) — سریع‌تر، بدون
    # ریسک rate-limit موتور جستجو، و اکثر وقت‌ها همینا کافیه.
    wave1: list[tuple[str, "callable"]] = []
    if homepage_hint:
        wave1.append(("Lab Website", lambda: _fetch_homepage_direct(homepage_hint)))
    wave1.append(("ORCID", lambda:
                   ({"email": (_fetch_orcid_info(name, university) or {}).get("email", "")}
                    if (_fetch_orcid_info(name, university) or {}).get("email") else {})))
    wave1.append(("OpenAlex", lambda: _openalex_email_lookup(name, university, openalex_id)))
    wave1.append(("Faculty Directory", lambda: _faculty_directory_probe(university, name)))
    wave1.append(("Semantic Scholar", lambda: _semantic_scholar_email_lookup(name)))
    wave1.append(("Google Search (direct)", lambda:
                   _google_search_and_scrape(f'"{name}" "{university}" email contact'.strip(), max_pages=3)))

    # ── موج ۲: منابع مبتنی بر DDG — فقط اگه موج ۱ چیزی پیدا نکرد ─────
    # طبق درخواست صریح: DDG باید فقط fallback آخر باشه، نه یه منبع
    # هم‌ردیف بقیه — هم برای کاهش فشار/ریسک rate-limit روی DDG، هم چون
    # اکثر وقت‌ها اصلاً لازم نمی‌شه سراغش بریم.
    wave2: list[tuple[str, "callable"]] = [
        ("University Faculty Page", lambda:
         _ddg_search_and_scrape(f'"{name}" {university} faculty profile email'.strip(), max_pages=3)),
        ("Lab Website", lambda:
         _ddg_search_and_scrape(f'"{name}" lab website {university}'.strip(), max_pages=2)),
        ("Department Directory", lambda:
         _ddg_search_and_scrape(f'"{name}" {university} department directory contact'.strip(), max_pages=2)),
        ("Google Scholar", lambda:
         _ddg_search_and_scrape(f'"{name}" site:scholar.google.com', max_pages=2)),
        ("Google Search (DDG)", lambda:
         _ddg_search_and_scrape(f'"{name}" "{university}" email contact'.strip(), max_pages=3)),
        ("ResearchGate", lambda:
         _ddg_search_and_scrape(f'"{name}" site:researchgate.net', max_pages=2)),
    ]

    PRIORITY = ["Lab Website", "University Faculty Page", "OpenAlex", "Faculty Directory",
                "Department Directory", "ORCID", "Semantic Scholar", "Google Scholar",
                "Google Search (direct)", "Google Search (DDG)", "ResearchGate"]

    def _run_wave(wave: list) -> dict:
        out: dict[str, list[dict]] = {}
        if not wave:
            return out
        workers = min(EMAIL_PIPELINE_MAX_WORKERS, len(wave))
        # ── باگ واقعی که این‌جا بود: `with ThreadPoolExecutor(...) as ex:`
        # موقع خروج از بلوک، خودش `shutdown(wait=True)` صدا می‌زنه — یعنی
        # با وجود اینکه بالاتر با `concurrent.futures.wait(..., timeout=
        # EMAIL_PIPELINE_TIMEOUT_SEC)` می‌گفتیم «فقط ۱۴ ثانیه صبر کن»،
        # خروج از `with` دوباره منتظر *همه‌ی* threadهای not_done (حتی
        # اونایی که DDG/Google کندشون کرده و ممکنه ۳۰-۶۰+ ثانیه طول
        # بکشن) می‌موند — دقیقاً همون باگی که قبلاً توی
        # _search_professors_impl پیدا و فیکس شد، ولی این‌جا (که تقریباً
        # همون الگو رو داره) جا مونده بود. کامنت قبلی می‌گفت «دیگه منتظرشون
        # نمی‌مونیم» ولی واقعیت این بود که می‌مونه — این یعنی هر باری که
        # یکی از موج‌ها کند می‌شد، کل get_professor_relevance (و از اون‌جا
        # کل batch ارسال ایمیل، چون این تابع توسط asyncio.to_thread صدا
        # زده می‌شه) می‌تونست بی‌صدا معلق بمونه، نه فقط ۱۴ ثانیه.
        # فیکس: مدیریت دستی pool + shutdown(wait=False) — thread های عقب‌
        # مونده در پس‌زمینه به کارشون ادامه می‌دن (نتیجه‌شون دور ریخته
        # می‌شه) ولی این تابع واقعاً بعد از EMAIL_PIPELINE_TIMEOUT_SEC
        # برمی‌گرده.
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=workers)
        try:
            future_to_label = {pool.submit(fn): label for label, fn in wave}
            done, not_done = concurrent.futures.wait(future_to_label, timeout=EMAIL_PIPELINE_TIMEOUT_SEC)
            for fut in done:
                label = future_to_label[fut]
                try:
                    r = fut.result() or {}
                except Exception as e:
                    logger.debug(f"email source [{label}] for {name}: {e}")
                    r = {}
                out.setdefault(label, []).append(r)
            if not_done:
                # این threadها واقعاً kill نمی‌شن (پایتون از این کار پشتیبانی
                # نمی‌کنه)، ولی چون هر کدوم خودشون یه سقف زمانی داخلی دارن
                # (SEARCH_TIMEOUT_SEC + PAGE_FETCH_TIMEOUT_SEC)، به‌زودی خودشون
                # تموم می‌شن؛ با shutdown(wait=False) پایین، این تابع واقعاً
                # دیگه منتظرشون نمی‌مونه.
                logger.debug(f"{len(not_done)} email source(s) for '{name}' still running in background — نتیجه‌شون نادیده گرفته می‌شه")
        finally:
            pool.shutdown(wait=False)
        return out

    results_by_label = _run_wave(wave1)
    found_in_wave1 = any(r.get("email") for rs in results_by_label.values() for r in rs)
    total_sources = len(wave1)
    if not found_in_wave1:
        wave2_results = _run_wave(wave2)
        for label, rs in wave2_results.items():
            results_by_label.setdefault(label, []).extend(rs)
        total_sources += len(wave2)

    # انتخاب نهایی بر اساس اولویت (نه سرعت)
    for label in PRIORITY:
        for r in results_by_label.get(label, []):
            if r.get("email"):
                result["email"] = r["email"]
                result["email_source"] = label
                break
        if result["email"]:
            break

    # سیگنال‌های جانبی از هر نتیجه‌ای که موجوده جمع می‌شن (صرف‌نظر از اینکه
    # کدوم برای ایمیل نهایی انتخاب شد)
    for label, rs in results_by_label.items():
        for r in rs:
            if not r:
                continue
            if r.get("accepting_students") and not result["accepting_students"]:
                result["accepting_students"] = True
                result["accepting_evidence"] = r.get("accepting_evidence", "")
            if r.get("has_funding") and not result["has_funding"]:
                result["has_funding"] = True
                result["funding_evidence"] = r.get("funding_evidence", "")
            if r.get("is_lab_page") and not result["lab_page_text"]:
                result["is_lab_page"] = True
                result["lab_page_text"] = r.get("lab_page_text", "")

    # ── Supervisor Availability Score — لیبل اختیاری، برای همه‌ی مسیرها
    # (نه فقط University Discovery Mode مثل قبل) ──────────────────────
    # کاملاً rule-based، از همون lab_page_text ای که بالا جمع شد — بدون
    # هیچ درخواست شبکه‌ی اضافه. عمداً هیچ‌جا استاد رد/فیلتر نمی‌شه؛ فقط
    # یک عدد ۰-۸۵ + شواهد قابل‌نمایش تولید می‌شه که پایین‌تر (در پیام به
    # کاربر) به‌عنوان یک لیبل اضافه نشون داده می‌شه.
    try:
        if result["lab_page_text"]:
            avail_score, avail_evidence = detect_availability_signals(result["lab_page_text"], result)
            result["availability_score"] = avail_score
            result["availability_evidence"] = avail_evidence
    except Exception as e:
        logger.debug(f"availability score for '{name}': {e}")

    # ── Funding Intelligence — لیبل اختیاری، فقط وقتی سیگنال بودجه از
    # قبل true شده باشه (تا فراخوانی AI اضافه فقط برای زیرمجموعه‌ای بشه
    # که واقعاً به‌دردش می‌خوره، نه هر ۵۰۰ استاد) ──────────────────────
    if result["has_funding"] and result["lab_page_text"]:
        try:
            result["funding_intelligence"] = run_ai_sync(
                analyze_funding_intelligence(result["lab_page_text"], name))
        except Exception as e:
            logger.debug(f"funding intelligence for '{name}': {e}")
            result["funding_intelligence"] = None

    if result["email"]:
        wave_used = "wave 1, non-DDG" if found_in_wave1 else "wave 2, DDG fallback"
        logger.info(f"✅ Email pipeline for '{name}': found via [{result['email_source']}] ({wave_used}, {total_sources} sources tried)")
        try:
            db_save_professor_cache(name, university, result)
        except Exception as e:
            logger.debug(f"professor cache save for '{name}': {e}")
    else:
        logger.info(f"❌ Email pipeline for '{name}': all {total_sources} sources tried (both waves), no email found")
    return result


async def analyze_lab_page(lab_text: str, prof_name: str) -> dict | None:
    """Lab/Profile Analyzer — علاوه بر تحلیل قبلی (اعضا، پروژه‌ها، بودجه،
    موقعیت‌های باز، همکاری صنعتی)، حالا این اطلاعاتِ شخصی‌سازیِ ایمیل رو
    هم استخراج می‌کنه: عنوان دقیق (Professor / Associate Professor /
    Assistant Professor / Dr.)، دپارتمان، و علایق پژوهشی (۲-۵ مورد کوتاه).
    این‌ها دقیقاً همون چیزهاییه که قبلاً نبود و باعث می‌شد ایمیل fallback
    همیشه با یک «Dear Professor {LastName}» عمومی و فقط اشاره به یک مقاله
    شروع بشه — حالا AI مولد ایمیل (write_prof_email) و حتی fallback
    قطعی (_fallback_email) می‌تونن از این اطلاعات برای شخصی‌سازی واقعی
    استفاده کنن.

    گاهی صدا زده می‌شه که واقعاً صفحه‌ی مرتبط با استاد/آزمایشگاه (نه یه
    نتیجه‌ی جستجوی نامطمئن) با موفقیت گرفته شده باشه، پس هزینه‌ی اضافه‌ش
    فقط برای همون زیرمجموعه از اساتیده، نه همه‌ی ۵۰۰ تا. کاملاً fail-safe:
    هر خطایی (AI جواب نده، فرمت خراب، تایم‌اوت) → None برمی‌گردونه و
    caller بدون این تحلیل، عادی به کارش ادامه می‌ده."""
    if not lab_text or len(lab_text) < 200:
        return None
    try:
        prompt = f"""You are analyzing a professor/lab website to help a prospective student personalize an email.
Lab/professor: {prof_name}

Raw page text (may be messy, may include navigation remnants):
{lab_text[:3000]}

Extract what you can find. If something isn't mentioned, use null/empty — never guess or invent.

Respond with STRICT JSON only, nothing else, exactly this shape:
{{"title": "exact academic title/rank if stated, e.g. 'Associate Professor' or 'Assistant Professor' or 'Professor' or 'Dr.' or null",
  "department": "department/school name or null",
  "research_interests": ["short topic", ...] (max 5, from an explicit 'Research Interests'/'Research Areas'/bio section only),
  "members_count": null or a number, "projects": ["short project name", ...] (max 4),
  "funding_status": "short phrase or null", "open_positions": true/false,
  "industry_partners": ["name", ...] (max 4)}}"""
        ai = await call_ai(prompt, min_length=15)
        data = _extract_json_object(ai)
        if not data:
            return None
        return {
            "title": (str(data.get("title")).strip() if data.get("title") else None),
            "department": (str(data.get("department")).strip() if data.get("department") else None),
            "research_interests": [str(r)[:60] for r in (data.get("research_interests") or [])][:5],
            "members_count": data.get("members_count"),
            "projects": [str(p)[:60] for p in (data.get("projects") or [])][:4],
            "funding_status": (str(data.get("funding_status")) if data.get("funding_status") else None),
            "open_positions": bool(data.get("open_positions")),
            "industry_partners": [str(p)[:40] for p in (data.get("industry_partners") or [])][:4],
        }
    except Exception as e:
        logger.warning(f"analyze_lab_page for {prof_name}: {e}")
        return None


# ── Funding Intelligence — لیبل اختیاری، نه فیلتر ────────────────────
# قبلاً has_funding فقط true/false بود. این تابع همون lab_page_text ای که
# قبلاً برای پیدا کردن ایمیل/Lab Analyzer گرفته شده رو دوباره می‌خونه (بدون
# هیچ درخواست شبکه‌ی اضافه) و سعی می‌کنه منبع/مبلغ/اعتبار بودجه رو دربیاره.
# دو تصمیم مهم:
#   ۱) فقط وقتی صدا زده می‌شه که has_funding از قبل true باشه — یعنی یک
#      فراخوانی AI اضافه فقط برای اون زیرمجموعه‌ای از اساتید که اصلاً
#      سیگنال بودجه دارن، نه همه‌ی ۵۰۰ تا (سرعت/هزینه حفظ می‌شه).
#   ۲) هیچ عددی حدس زده نمی‌شه — اگه صفحه مبلغ/تاریخ ننوشته، خروجی برای
#      همون فیلد null/"Unknown" می‌مونه، نه یک عدد ساختگی با ظاهر دقیق.
#   ۳) این هیچ‌جا match/no-match یا ارسال/عدم‌ارسال ایمیل رو عوض نمی‌کنه —
#      صرفاً یک لیبل نمایشی اضافه‌ست، دقیقاً طبق درخواست کاربر.
async def analyze_funding_intelligence(lab_text: str, prof_name: str) -> dict | None:
    """خروجی: {"source": str|None, "amount": str|None, "valid_until": str|None,
    "probability": "High"|"Medium"|"Low", "evidence_snippet": str|None} یا None
    اگه چیزی پیدا نشد/AI جواب نداد (fail-safe کامل، هیچ‌وقت exception بیرون نمی‌ره)."""
    if not lab_text or len(lab_text) < 150:
        return None
    try:
        prompt = f"""You are extracting funding details from a professor/lab webpage for a prospective student.
Professor: {prof_name}

Raw page text:
{lab_text[:3000]}

Look specifically for: named grants/funding sources (e.g. NSERC, NIH, ERC, NSF, industry sponsor name),
funding amount if explicitly stated, and any stated validity/expiry date or grant period.
If something is not explicitly stated, use null — never guess or invent a source, amount, or date.

Respond with STRICT JSON only, exactly this shape:
{{"source": "exact grant/funding name if stated, or null",
  "amount": "exact amount if stated (with currency), or null",
  "valid_until": "exact year/date if stated, or null",
  "probability": "High" if a named/specific funding source or amount is stated, "Medium" if funding is mentioned only in general terms (e.g. 'funded positions available') without specifics, "Low" if funding is only implied indirectly,
  "evidence_snippet": "the short phrase from the page text that supports this, max 100 chars, or null"}}"""
        ai = await call_ai(prompt, min_length=10)
        data = _extract_json_object(ai)
        if not data:
            return None
        prob = str(data.get("probability") or "").strip().title()
        if prob not in ("High", "Medium", "Low"):
            prob = "Medium"
        result = {
            "source": (str(data.get("source")).strip() if data.get("source") else None),
            "amount": (str(data.get("amount")).strip() if data.get("amount") else None),
            "valid_until": (str(data.get("valid_until")).strip() if data.get("valid_until") else None),
            "probability": prob,
            "evidence_snippet": (str(data.get("evidence_snippet"))[:100] if data.get("evidence_snippet") else None),
        }
        # اگه AI هیچ سیگنال مشخصی پیدا نکرد (همه فیلدها null و probability
        # فقط حدسی)، بهتره اصلاً چیزی نمایش ندیم تا کاربر یک باکس خالی
        # با «Unknown» همه‌جا نبینه.
        if not result["source"] and not result["amount"] and not result["valid_until"]:
            return None
        return result
    except Exception as e:
        logger.debug(f"analyze_funding_intelligence for {prof_name}: {e}")
        return None


# ── Publication Trend — کاملاً deterministic، بدون AI و بدون درخواست
# شبکه‌ی اضافه ─────────────────────────────────────────────────────
# از همون papers ای که قبلاً از Semantic Scholar/OpenAlex/Crossref/DBLP
# جمع شدن استفاده می‌کنه (fetch_professor_publications و توابع مشابه).
# نکته‌ی صادقانه: منابعی مثل Google Scholar/PubMed/IEEE/ACM که در
# پیام کاربر اومده بود عمداً اینجا اضافه نشدن — هرکدوم یا API رسمی رایگان
# ندارن (Google Scholar) یا نیازمند اسکرِیپ شکننده‌ی جدیدن که ریسک باگ/
# rate-limit اضافه می‌کنن؛ این تابع فقط روی داده‌ای که همین الان از قبل
# قابل‌اعتماد جمع می‌شه کار می‌کنه.
def analyze_publication_trend(papers: list[dict] | None) -> dict | None:
    """خروجی: {"recent": [{"year": int, "title": str}, ...] (حداکثر ۵ تای
    جدیدترین), "trend": "Growing"|"Stable"|"Declining"|"Unknown"} یا None
    اگه papers خالی/نامعتبر باشه."""
    if not papers:
        return None
    valid = [p for p in papers if p.get("year") and p.get("title")]
    if not valid:
        return None
    valid.sort(key=lambda p: p["year"], reverse=True)
    recent = [{"year": p["year"], "title": p["title"][:80]} for p in valid[:5]]

    # روند: مقایسه‌ی تعداد مقالات ۲ سال اخیر با ۲ سال قبل از اون — کاملاً
    # عددی و قابل توضیح، نه یک برچسب AI مبهم.
    this_year = datetime.now().year
    recent_count = sum(1 for p in valid if p["year"] >= this_year - 1)
    prior_count = sum(1 for p in valid if this_year - 3 <= p["year"] < this_year - 1)
    if recent_count == 0 and prior_count == 0:
        trend = "Unknown"
    elif recent_count > prior_count:
        trend = "Growing"
    elif recent_count < prior_count:
        trend = "Declining"
    else:
        trend = "Stable"
    return {"recent": recent, "trend": trend}


def _fetch_orcid_info(name: str, university: str = "") -> dict:
    """
    ORCID API برای تأیید هویت + گرفتن employment/affiliation + (در صورت
    عمومی بودن) ایمیل. رایگان، بدون API key برای جستجوی عمومی.
    """
    try:
        params = {"q": f'family-name:{name.split()[-1]} AND given-names:{name.split()[0]}',
                  "rows": "3"}
        r = requests.get("https://pub.orcid.org/v3.0/search/",
            params=params,
            headers={"Accept": "application/json",
                     "User-Agent": "ManifestApplyBot/1.0"},
            timeout=8)
        data = r.json()
        results = data.get("result", [])
        if not results:
            return {}
        # اولین نتیجه
        orcid_id = (results[0].get("orcid-identifier") or {}).get("path","")
        if not orcid_id:
            return {}
        # گرفتن اطلاعات کامل‌تر
        r2 = requests.get(f"https://pub.orcid.org/v3.0/{orcid_id}/record",
            headers={"Accept": "application/json",
                     "User-Agent": "ManifestApplyBot/1.0"}, timeout=8)
        record = r2.json()
        # employment
        emp = ((record.get("activities-summary") or {})
                      .get("employments") or {})
        affiliation = ""
        for e in ((emp.get("affiliation-group") or [])[:1]):
            summ = (e.get("summaries") or [{}])[0].get("employment-summary",{})
            org  = (summ.get("organization") or {}).get("name","")
            if org:
                affiliation = org
                break
        # ایمیل — ORCID اکثر مواقع این رو private نگه می‌داره، ولی بعضی
        # پروفایل‌ها صریحاً public می‌ذارنش؛ اگه بود مفته، چرا استفاده نکنیم.
        email = ""
        for e in ((record.get("person") or {}).get("emails") or {}).get("email", []):
            if e.get("email"):
                email = e["email"]
                break
        return {
            "orcid_id":    orcid_id,
            "affiliation": affiliation,
            "email":       email,
        }
    except Exception as e:
        logger.debug(f"ORCID lookup for {name}: {e}")
    return {}


def _orcid_record_by_id(orcid_id: str) -> dict:
    """رکورد کامل ORCID رو مستقیم با orcid_id می‌گیره (نه با جستجوی نام،
    که کارِ _fetch_orcid_info بالاست) — برای وقتی که orcid_id از قبل از
    یک جستجوی کلیدواژه‌ای (prof_src_orcid_keyword — تلاش ۵) در دست
    داریم و فقط باید اسم/دانشگاه/کشور/ایمیل همون رکورد رو استخراج کنیم."""
    try:
        r = requests.get(f"https://pub.orcid.org/v3.0/{orcid_id}/record",
            headers={"Accept": "application/json", "User-Agent": "ManifestApplyBot/1.0"},
            timeout=8)
        record = r.json()
        person = record.get("person") or {}
        name_obj = person.get("name") or {}
        given  = ((name_obj.get("given-names") or {}) or {}).get("value", "") if name_obj else ""
        family = ((name_obj.get("family-name") or {}) or {}).get("value", "") if name_obj else ""
        full_name = f"{given} {family}".strip()

        emp = ((record.get("activities-summary") or {}).get("employments") or {})
        affiliation, country = "", ""
        for e in ((emp.get("affiliation-group") or [])[:1]):
            summ = (e.get("summaries") or [{}])[0].get("employment-summary", {})
            org  = (summ.get("organization") or {})
            affiliation = org.get("name", "")
            country = (org.get("address") or {}).get("country", "") or ""

        email = ""
        for e in ((person.get("emails") or {}).get("email", []) or []):
            if e.get("email"):
                email = e["email"]
                break

        return {"name": full_name, "affiliation": affiliation, "country": country, "email": email}
    except Exception as e:
        logger.debug(f"ORCID record fetch for {orcid_id}: {e}")
        return {}


def _fetch_crossref_papers(name: str, limit: int = 3) -> list[dict]:
    """
    Crossref API برای مقالات — پوشش مجلات غیرفنی که در SS/OpenAlex نیستند.
    بدون API key.
    """
    try:
        r = requests.get("https://api.crossref.org/works",
            params={"query.author": name, "rows": limit,
                    "sort": "published", "order": "desc",
                    "select": "title,abstract,published"},
            headers={"User-Agent": "ManifestApplyBot/1.0 (mailto:contact@manifestapply.com)"},
            timeout=10)
        # نکته‌ی مهم: روی خطای validation، Crossref زیر کلید "message" یه
        # LIST از خطاها برمی‌گردونه (نه dict نتایج معمول) — یعنی
        # message.get("items") روی یه لیست کرش می‌کنه. باید نوعش رو چک کنیم.
        message = r.json().get("message", {})
        items = message.get("items", []) if isinstance(message, dict) else []
        out = []
        for item in items:
            titles = item.get("title",[])
            if not titles: continue
            out.append({
                "title":    titles[0],
                "abstract": (item.get("abstract") or "")[:600],
                "year":     (item.get("published") or {}).get("date-parts",[None])[0][0] if item.get("published") else None,
            })
        return out
    except Exception as e:
        logger.debug(f"Crossref papers for {name}: {e}")
    return []


def _discover_professors_via_web_search(field: str, country: str, keywords: list[str],
                                         needed: int) -> list[dict]:
    """منبع اصلیِ اول جستجوی استاد (طبق درخواست صریح) — کاملاً مستقل از
    OpenAlex/Semantic Scholar/Crossref (که فقط محققانی رو پوشش می‌دن که
    توی اون دیتابیس‌ها به‌عنوان «Author» ثبت شدن). این تابع به‌جای اون،
    مستقیم از صفحات واقعی دانشگاه می‌خونه، به این ترتیب:
      ۱. University Faculty Page  (دایرکتوری هیئت‌علمی کل دانشگاه)
      ۲. Department Faculty       (دایرکتوری هیئت‌علمی همون دانشکده/گروه)
      ۳. Lab Members              (صفحه‌ی اعضای آزمایشگاه/گروه پژوهشی)
      ۴. Google (fallback عمومی، فقط اگه ۳ مرحله‌ی بالا چیزی گیر نیاوردن)
    برای گرفتن هر صفحه اول Google امتحان می‌شه (مثل بقیه‌ی pipeline
    پروژه)، بعد فقط اگه چیزی گیر نیاورد DDG — متن خام این صفحات رو
    می‌گیره و با AI اسم اساتید واقعی (نه ناوبری/متن تصادفی صفحه) رو
    استخراج می‌کنه.

    مثل بقیه‌ی pipeline: هر خطا (Google بلاک کنه، DDG قطع باشه، AI جواب
    نده) فقط یعنی این موج چیزی اضافه نمی‌کنه — هیچ‌وقت کل جستجوی استاد رو
    نمی‌شکنه یا متوقف نمی‌کنه."""
    out: list[dict] = []
    if needed <= 0:
        return out
    q_terms = " ".join(keywords[:3]) or field
    country_part = f" {country}" if country else ""
    queries = [
        f'"{q_terms}" faculty directory{country_part}',                       # ۱. University Faculty Page
        f'"{q_terms}" department faculty{country_part} site:.edu',            # ۲. Department Faculty
        f'"{q_terms}" lab members OR "research group" "our team"{country_part}',  # ۳. Lab Members
        f'"{q_terms}" professors{country_part}',                              # ۴. Google fallback عمومی
    ]

    pages_text: list[str] = []
    for q in queries:
        try:
            g = _google_search_and_scrape(q.strip(), max_pages=3)
        except Exception as e:
            g = {}
            logger.debug(f"web discovery google '{q[:60]}': {e}")
        pt = g.get("lab_page_text", "")
        if pt:
            pages_text.append(pt)
        if len(pages_text) >= 3:
            break
    if len(pages_text) < 2:
        for q in queries:
            try:
                d = _ddg_search_and_scrape(q.strip(), max_pages=3)
            except Exception as e:
                d = {}
                logger.debug(f"web discovery ddg '{q[:60]}': {e}")
            pt = d.get("lab_page_text", "")
            if pt:
                pages_text.append(pt)
            if len(pages_text) >= 3:
                break
    if not pages_text:
        return out

    combined = "\n---\n".join(pages_text)[:6000]
    try:
        prompt = f"""Below is raw scraped text from university faculty/department directory pages — it may
be messy (navigation remnants, unrelated text). Extract real individual professors/faculty members who
appear to work in or near this field: "{field}". Never invent names; only list ones that actually appear
in the text below. For each professor, also check if their email address appears as literal text near
their name (e.g. "email: jsmith@university.edu") — if so include it; if no email is visible for that
person, leave the email field as an empty string. NEVER invent or guess an email that doesn't literally
appear in the text.
Raw text:
{combined}
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"professors": [{{"name": "Full Name", "university": "University name or empty string",
"email": "email@domain.edu or empty string"}}, ...]}}
(max 15 entries, only real-looking individual person names — never navigation labels, department names,
or generic words)"""
        ai = run_ai_sync(call_ai(prompt, min_length=15))
        data = _extract_json_object(ai) or {}
        for p in (data.get("professors") or [])[:needed]:
            name = str(p.get("name", "")).strip()
            email = str(p.get("email", "")).strip()
            if email and (not _EMAIL_FORMAT_RE.match(email) or email.lower() not in combined.lower()):
                email = ""  # هالوسینیشن احتمالی AI رو رد می‌کنیم — فقط ایمیلی که عیناً توی متن هست قبول می‌شه
            if name and len(name.split()) >= 2:
                out.append({"name": name, "university": str(p.get("university", "")).strip(), "email": email})
    except Exception as e:
        logger.debug(f"web discovery AI extraction: {e}")
    return out


async def _live_search_progress_reporter(bot, chat_id, message_id, progress: dict, interval: float = 4.0,
                                          kind: str = "اساتید"):
    """هر چند ثانیه پیام پیشرفت جستجو رو edit می‌کنه (نه پیام جدید که چت
    شلوغ بشه) — تا کاربر حس نکنه بات هنگ کرده و دقیقاً بدونه چند درصد از
    منابع بررسی شده. با progress['stage']=='done' خودش متوقف می‌شه؛ اگه
    caller قبلش cancel کنه هم بی‌خطر خارج می‌شه.

    kind: برچسب فارسی چیزی که داریم جستجو می‌کنیم — «اساتید» یا «فرصت‌های
    شغلی» — تا همین یک تابع برای هر دو نوع جستجو (استاد/شغل) استفاده بشه
    بدون تکرار کد."""
    last_text = ""
    try:
        while True:
            await asyncio.sleep(interval)
            stage = progress.get("stage")
            if stage == "done":
                return
            total = progress.get("total", 0)
            done  = progress.get("done", 0)
            source_labels = progress.get("source_labels", PROF_SOURCE_LABELS_FA)
            if stage == "enrich_emails":
                text = "📧 منابع اصلی بررسی شد — الان دارم ایمیل‌های ناقص رو از منابع تکمیلی پیدا می‌کنم...\n⏳ لطفاً صبر کنید."
            elif stage == "extending_wait":
                # ⚠️ فیکس درخواستی: وقتی سقف زمانی عادی رد شده ولی اکثر
                # منابع هنوز واقعاً در حال جستجو بودن، به‌جای رها کردنشون
                # یک پنجره‌ی اضافه بهشون داده شده — کاربر باید بدونه چرا
                # داره از حد معمول بیشتر طول می‌کشه، نه فکر کنه بات هنگ کرده.
                done_now = progress.get("done", 0)
                total_now = progress.get("total", 0)
                extra_line = f"\n📊 {done_now}/{total_now} منبع تا الان بررسی شد" if total_now else ""
                text = (f"⏳ این جستجو کمی بیشتر از حد معمول طول می‌کشه — چند منبع هنوز در حال "
                        f"بررسی‌ان و داریم بهشون زمان بیشتری می‌دیم تا نتیجه‌ی کامل بگیریم.{extra_line}\n"
                        f"لطفاً کمی بیشتر صبر کنید...")
            elif total:
                pct = int(done / total * 100)
                current = source_labels.get(progress.get("current",""), progress.get("current",""))
                src_line = f"\nآخرین منبع بررسی‌شده: {current}" if current else ""
                text = (f"🔍 در حال جستجوی دقیق {kind}...\n"
                        f"📊 {done}/{total} منبع بررسی شد ({pct}%){src_line}\n"
                        f"⏳ لطفاً صبر کنید، داریم دقیق می‌گردیم نه سطحی.")
            else:
                text = f"🔍 در حال جستجوی دقیق {kind}...\n⏳ لطفاً صبر کنید."
            if text != last_text:
                try:
                    await bot.edit_message_text(chat_id=chat_id, message_id=message_id, text=text)
                    last_text = text
                except Exception:
                    pass  # مثلاً «message not modified» یا کاربر پیام رو پاک کرده — بی‌خطر رد می‌شیم
    except asyncio.CancelledError:
        return
    except Exception as e:
        # قبلاً این تابع فقط CancelledError رو می‌گرفت — یعنی هر باگ
        # واقعی دیگه‌ای این‌جا (مثلاً روی progress dict یا فرمت‌کردن متن)
        # کاملاً بی‌صدا از دست می‌رفت: چون caller این task رو معمولاً فقط
        # cancel() می‌کنه نه await، exception هیچ‌وقت "retrieved" نمی‌شد و
        # فقط به‌عنوان یک warning داخلی asyncio (نه از مسیر logger خودمون)
        # چاپ می‌شد. الان دقیقاً مثل _preview_timeout (که همین مشکل رو
        # نداشت) لاگ می‌شه.
        logger.error(f"_live_search_progress_reporter crashed: {e}")

def search_professors(field: str, country: str, count: int = 100,
                      client: dict | None = None, progress: dict | None = None) -> list[dict]:
    """Wrapper با Search Cache: اگه همین (field, country) توی بازه‌ی
    SEARCH_CACHE_TTL_SEC (پیش‌فرض ۱ ساعت) قبلاً جستجو شده، بدون صدا زدن
    دوباره‌ی OpenAlex/Semantic Scholar/Crossref، همون نتیجه‌ی کش‌شده
    برگردونده می‌شه. شخصی‌سازی واقعی (relevance judging، ایمیل‌یابی،
    شباهت معنایی) بعداً و جدا برای هر کاربر روی همین pool انجام می‌شه،
    پس کش کردن pool کاندیدها هیچ آسیبی به دقت نمی‌زنه.

    progress: دیکشنری مشترک اختیاری (thread-safe به‌اندازه‌ی کافی چون
    GIL خود پایتون از race condition روی += یک int جلوگیری می‌کنه) که
    caller (توی event loop اصلی، هم‌زمان با اجرای این تابع توی
    asyncio.to_thread) می‌تونه بخونه و پیام «X/Y منبع بررسی شد» زنده
    برای کاربر بفرسته — بدون این‌که کاربر فکر کنه بات هنگ کرده."""
    if progress is not None:
        progress["cache_hit"] = False
    cached = db_get_search_cache("professor", field, country)
    if cached is not None:
        logger.info(f"search_professors CACHE HIT | field={field!r} country={country!r} ({len(cached)} results)")
        if progress is not None:
            progress["cache_hit"] = True
            progress["done"] = progress.get("total", 1)
        return cached[:count]
    results = _search_professors_impl(field, country, count, client, progress=progress)
    # فقط روی cache miss اجرا می‌شه — یعنی هزینه‌ی این مرحله بین همه‌ی
    # کاربرهای بعدی همین (field, country) طی SEARCH_CACHE_TTL_SEC مشترکه،
    # نه اینکه هر کاربر دوباره ازش سر بخوره.
    if progress is not None:
        progress["stage"] = "enrich_emails"
    results = _enrich_missing_emails(results)
    db_save_search_cache("professor", field, country, results)
    if progress is not None:
        progress["stage"] = "done"
    return results

# ── دسته‌بندی منابع برای «تلاش مجدد» ────────────────────────────────
# جستجوی اول همیشه از همه‌ی ۶ منبع استفاده می‌کنه (بیشترین شانس موفقیت).
# اگه نتیجه‌ای نداد، به‌جای این‌که هر «تلاش مجدد» دوباره کل pipeline
# سنگین رو تکرار کنه (کند، و اگه یه باگ/مشکل سراسری بود هر بار همون رو
# می‌خوردیم)، هر تلاش مجدد فقط یک دسته‌ی کوچیک و مجزا از منابع رو با سقف
# زمانی کوتاه‌تر (۱۵s به‌جای ۴۰s) امتحان می‌کنه — سریع‌تر جواب می‌ده، و
# اگه یه منبع خاص مشکل داشته باشه (rate limit، قطعی موقت، باگ)، تلاش‌های
# بعدی که از منابع دیگه استفاده می‌کنن مستقل ازش کار می‌کنن.
PROF_SOURCE_RETRY_GROUPS: list[list[str]] = [
    ["web_discovery", "openalex_topics"],       # تلاش مجدد ۱: همون منبع اصلی + OpenAlex
    ["semantic_scholar", "dblp"],                # تلاش مجدد ۲: یه دیتابیس Author کاملاً متفاوت
    ["openalex_institution", "crossref"],        # تلاش مجدد ۳: مبتنی بر دانشگاه/مقاله
]

# مجموع تعداد «تلاش مجدد»ها که به کاربر نشون داده می‌شه (برای پیام‌های
# progress/UI؛ منطق واقعیِ اینکه هر attempt_idx چه کاری انجام بده توی
# run_prof_retry_attempt هست، نه اینجا):
#   تلاش ۱،۲،۳ → PROF_SOURCE_RETRY_GROUPS (بالا، سریع، منابع API سبک)
#   تلاش ۴     → University Discovery Mode (دانشگاه→دانشکده→لَب→استخراج)
#   تلاش ۵     → ORCID کلیدواژه‌ای + جستجوی وب گسترده (بدون محدودیت
#                 دانشگاه‌های از‌قبل‌کشف‌شده) — پایین‌تر تعریف شده
TOTAL_PROF_RETRY_ATTEMPTS = 5

def search_professors_retry_group(field: str, country: str, count: int,
                                   client: dict | None, attempt_idx: int,
                                   progress: dict | None = None) -> list[dict]:
    """صدا زده می‌شه وقتی کاربر دکمه‌ی «🔁 تلاش مجدد» رو می‌زنه (بعد از
    این‌که جستجوی کامل اول ۰ نتیجه داد). عمداً از کش رد می‌شه (چون کش
    فقط نتیجه‌ی غیرخالی رو نگه می‌داره؛ اگه چیزی توی کش بود، اصلاً به
    اینجا نمی‌رسیدیم) و مستقیم _search_professors_impl رو با یه
    زیرمجموعه‌ی کوچیک از منابع و سقف زمانی کوتاه (۱۵ ثانیه، با پنجره‌ی
    تمدید ۲۵ ثانیه‌ای اگه اکثر منابع genuinely هنوز کار می‌کنن) صدا
    می‌زنه — یعنی کاربر معمولاً حداکثر ۱۵ ثانیه منتظر هر تلاش مجدد
    می‌مونه، نه دقیقه‌ها، مگر وقتی واقعاً به زمان بیشتری نیاز باشه."""
    group = PROF_SOURCE_RETRY_GROUPS[attempt_idx % len(PROF_SOURCE_RETRY_GROUPS)]
    logger.info(f"search_professors_retry_group: attempt={attempt_idx} group={group}")
    results = _search_professors_impl(field, country, count, client,
                                       sources=group, pool_timeout=15,
                                       skip_progressive_fallback=True, progress=progress)
    if progress is not None:
        progress["stage"] = "enrich_emails"
    if results:
        results = _enrich_missing_emails(results)
        db_save_search_cache("professor", field, country, results)
    if progress is not None:
        progress["stage"] = "done"
    return results

# ================================================================
# UNIVERSITY DISCOVERY MODE — چهارمین و آخرین لایه‌ی «تلاش مجدد»
# ================================================================
# طبق دیاگرام درخواستی:
#
#   Retry 1 → Retry 2 → Retry 3 → University Discovery → Department Crawl
#   → Faculty Crawl → Lab Crawl → Professor Extraction → Ranking
#   → Availability Detection → Top 10 → Return Results
#
# سه «تلاش مجدد» بالا (PROF_SOURCE_RETRY_GROUPS) هر بار فقط یک زیرمجموعه‌ی
# کوچیک از همون ۶ منبع API عمومی (OpenAlex/Semantic Scholar/DBLP/Crossref)
# رو امتحان می‌کنن — یعنی اگه هر ۳ تا نتیجه‌ای ندادن، تکرار دوباره‌شون
# (که قبلاً با % یعنی چرخش بی‌نهایت روی همون ۳ گروه اتفاق می‌افتاد)
# چیز جدیدی کشف نمی‌کنه. University Discovery Mode یک مسیر کاملاً متفاوته:
# به‌جای زدن یک کوئری کلی و امیدوار بودن، آگاهانه یک‌به‌یک روی دانشگاه‌های
# واقعیِ مرتبط با این رشته/کشور حرکت می‌کنه — کندتره ولی دقیق‌تر، مخصوصاً
# برای رشته‌ها/کشورهایی که پوشش کمی توی OpenAlex/Semantic Scholar دارن.
# ================================================================

def _university_domain(homepage_url: str) -> str:
    """دامنه‌ی خام یک دانشگاه رو از هوم‌پیج‌اش استخراج می‌کنه — برای
    محدود کردن جستجوهای بعدی با site: به همون دانشگاه (نه یک نتیجه‌ی
    کلی که ممکنه از دانشگاه دیگه‌ای باشه)."""
    try:
        from urllib.parse import urlparse
        netloc = urlparse(homepage_url or "").netloc.lower()
        return re.sub(r"^www\.", "", netloc)
    except Exception:
        return ""

def _university_discovery(field: str, country_q: str, limit: int = 8) -> list[dict]:
    """مرحله‌ی ۱ — University Discovery: به‌جای یک لیست ثابت از
    دانشگاه‌های «معروف»، دانشگاه‌هایی که *واقعاً* توی این رشته (طبق حجم
    مقالات OpenAlex روی همون topic) و این کشور فعالن رو پیدا می‌کنه. اگه
    کشور مشخص نشده باشه، فقط بر اساس رشته (بدون فیلتر کشور) کار می‌کنه."""
    out: list[dict] = []
    HEADERS = PROF_SEARCH_HEADERS
    iso = _resolve_country_iso(country_q) if country_q else ""
    try:
        rc = _http_get_retry("https://api.openalex.org/topics",
            params={"search": field, "per-page": 3, "select": "id,display_name"},
            headers=HEADERS, timeout=8, max_retries=2)
        topics = rc.json().get("results", [])
        topic_ids = "|".join(t["id"] for t in topics[:2])
        filt_parts = []
        if topic_ids:
            filt_parts.append(f"topics.id:{topic_ids}")
        if iso:
            filt_parts.append(f"institutions.country_code:{iso}")
        if not filt_parts:
            logger.info("University Discovery: نه topic نه کشور قابل‌تشخیص — این مرحله رد شد")
            return out
        filt = ",".join(filt_parts)
        r2 = _http_get_retry("https://api.openalex.org/works",
            params={"filter": filt, "group_by": "institutions.id", "per-page": limit * 3},
            headers=HEADERS, timeout=15, max_retries=1)
        groups = (r2.json().get("group_by", []) or [])[:limit * 3]
        for g in groups:
            inst_id = g.get("key", "")
            if not inst_id or g.get("count", 0) < 2:
                continue
            try:
                ri = _http_get_retry(f"https://api.openalex.org/institutions/{inst_id}",
                    headers=HEADERS, timeout=8, max_retries=1)
                inst = ri.json()
            except Exception as e:
                logger.debug(f"University Discovery — جزئیات institution {inst_id}: {e}")
                continue
            name = inst.get("display_name", "")
            if not name:
                continue
            out.append({
                "name": name,
                "homepage_url": inst.get("homepage_url", ""),
                "country": inst.get("country_code", ""),
                "works_count": g.get("count", 0),
            })
            if len(out) >= limit:
                break
        logger.info(f"University Discovery: {len(out)} دانشگاه کاندید برای '{field}' در '{country_q}'")
    except Exception as e:
        logger.warning(f"University Discovery: {e}")
    return out

def _crawl_university_stage(uni: dict, field: str, keywords: list[str], stage: str) -> str:
    """مراحل ۲/۳/۴ (Department Crawl / Faculty Crawl / Lab Crawl) — هر
    سه از همون مکانیزم استفاده می‌کنن (جستجو با site: محدود به دامنه‌ی
    همین دانشگاه + اسکرِیپ)، فقط عبارت کوئری فرق می‌کنه؛ یعنی برخلاف
    prof_src_web_discovery (که یک جستجوی کلی می‌زنه و ممکنه صفحه‌ای از
    یک دانشگاه دیگه برگرده)، اینجا صفحه‌ی برگشتی تضمین‌شده متعلق به
    همین دانشگاهه."""
    domain = _university_domain(uni.get("homepage_url", ""))
    site_filter = f" site:{domain}" if domain else f" \"{uni.get('name','')}\""
    q_terms = " ".join(keywords[:3]) or field

    stage_queries = {
        "department": f'"{q_terms}" department{site_filter}',
        "faculty":    f'"{q_terms}" faculty directory OR "faculty members" OR "our team"{site_filter}',
        "lab":        f'"{q_terms}" lab members OR "research group" OR "join our lab"{site_filter}',
    }
    q = stage_queries.get(stage, stage_queries["faculty"])

    try:
        g = _google_search_and_scrape(q.strip(), max_pages=2)
    except Exception as e:
        g = {}
        logger.debug(f"University Discovery [{stage}] google '{q[:60]}': {e}")
    pt = g.get("lab_page_text", "")
    if not pt:
        try:
            d = _ddg_search_and_scrape(q.strip(), max_pages=2)
            pt = d.get("lab_page_text", "")
        except Exception as e:
            logger.debug(f"University Discovery [{stage}] ddg '{q[:60]}': {e}")
    return pt or ""

def _university_department_crawl(uni: dict, field: str, keywords: list[str]) -> str:
    """مرحله‌ی ۲ — Department Crawl."""
    return _crawl_university_stage(uni, field, keywords, "department")

def _university_faculty_crawl(uni: dict, field: str, keywords: list[str]) -> str:
    """مرحله‌ی ۳ — Faculty Crawl."""
    return _crawl_university_stage(uni, field, keywords, "faculty")

def _university_lab_crawl(uni: dict, field: str, keywords: list[str]) -> str:
    """مرحله‌ی ۴ — Lab Crawl."""
    return _crawl_university_stage(uni, field, keywords, "lab")

def _professor_extraction(page_text: str, field: str, needed: int = 8) -> list[dict]:
    """مرحله‌ی ۵ — Professor Extraction: از متن خام کراول‌شده (که تضمین‌شده
    مال همین دانشگاهه چون کراول با site: محدود شده)، اسم اساتید واقعی رو
    با AI استخراج می‌کنه — دقیقاً همون منطق قابل‌اعتماد
    _discover_professors_via_web_search (هیچ اسمی که توی متن نباشه ساخته
    نمی‌شه).

    ⚠️ فیکس مهم: قبلاً این تابع فقط اسم رو استخراج می‌کرد و ایمیل رو کلاً
    نادیده می‌گرفت — با اینکه صفحه‌ی دپارتمان/دانشکده/آزمایشگاه دقیقاً
    جاییه که ایمیل معمولاً به‌صورت متن ساده (نه فقط href مخفی) کنار اسم
    نوشته شده («email: jsmith@stanford.edu»). حالا از AI می‌خوایم اگه
    ایمیلی *واقعاً* توی متن کنار همون اسم دیده می‌شه، همونو هم برگردونه —
    هیچ‌وقت ایمیل نساز، فقط اگه عیناً توی متن باشه استخراجش کن."""
    out: list[dict] = []
    if not page_text:
        return out
    try:
        prompt = f"""Below is raw scraped text from a specific university's department/faculty/lab
directory page — it may be messy (navigation remnants, unrelated text). Extract real individual
professors/faculty members who appear to work in or near this field: "{field}". Never invent names;
only list ones that actually appear in the text below. For each professor, also check if their email
address appears as literal text near their name (e.g. "email: jsmith@university.edu") — if so include
it; if no email is visible for that person, leave the email field as an empty string. NEVER invent or
guess an email that doesn't literally appear in the text.
Raw text:
{page_text[:6000]}
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"professors": [{{"name": "Full Name", "email": "email@domain.edu or empty string"}}, ...]}}
(max {needed} entries, only real-looking individual person names — never navigation labels,
department names, or generic words)"""
        ai = run_ai_sync(call_ai(prompt, min_length=15))
        data = _extract_json_object(ai) or {}
        for p in (data.get("professors") or [])[:needed]:
            name = str(p.get("name", "")).strip()
            email = str(p.get("email", "")).strip()
            # اطمینان دوباره: حتی اگه AI اشتباهی چیزی ساخته باشه، فقط
            # ایمیلی که واقعاً عیناً توی متن خام هست رو قبول می‌کنیم —
            # این کار احتمال هالوسینیشن ایمیل رو عملاً صفر می‌کنه.
            if email and (not _EMAIL_FORMAT_RE.match(email) or email.lower() not in page_text.lower()):
                email = ""
            if name and len(name.split()) >= 2:
                out.append({"name": name, "email": email})
    except Exception as e:
        logger.debug(f"Professor Extraction: {e}")
    return out

def university_discovery_mode_search(field: str, country: str, count: int,
                                      client: dict | None = None,
                                      progress: dict | None = None) -> tuple[list[dict], list[dict]]:
    """Wrapper چند-کشوری روی university_discovery_mode_search_single —
    همون دلیل _search_professors_impl بالاتر: این مسیر (تلاش ۴، University
    Discovery Mode) مستقیم _university_discovery رو صدا می‌زنه که خودش
    _resolve_country_iso (تک‌کشوری) رو صدا می‌زنه — یعنی این مسیر جدا از
    wrapper اصلی بود و همون باگ «Germany, Canada» → فقط «Germany» رو
    مستقل داشت. الان با _resolve_countries_iso چک می‌کنیم و برای چند
    کشور، هر کدوم رو جدا (هم‌زمان) اجرا می‌کنیم.

    progress: دیکشنری اختیاری برای پیشرفت زنده (thread این تابع خودش
    توی asyncio.to_thread اجرا می‌شه؛ progress رو مستقیم به تک‌کشوری
    پاس می‌دیم وقتی فقط یک کشوره — چندکشوری فعلاً فقط stage/done کلی رو
    ست می‌کنه چون چند تک‌کشوری هم‌زمان روی یک progress دیکشنری مشترک
    باعث پیام‌های نامفهوم می‌شه)."""
    countries = _resolve_countries_iso(country) if country and country.strip().lower() not in ("همه", "all", "*", "") else []
    if len(countries) <= 1:
        return university_discovery_mode_search_single(field, country, count, client, progress=progress)

    if progress is not None:
        progress["stage"] = "searching"
        progress["total"] = len(countries)
        progress["done"] = 0
        progress["current"] = ""

    per_country = max(count // len(countries), 5)
    iso_to_name = {v: k for k, v in _COUNTRY_ISO.items()}
    all_top: list[dict] = []
    all_close: list[dict] = []
    seen_ids: set[str] = set()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(countries), 6))
    try:
        futs = {
            pool.submit(university_discovery_mode_search_single, field, iso_to_name.get(iso, iso),
                        per_country, client, None): iso
            for iso in countries
        }
        results_by_iso = _run_futures_with_extension(
            futs, 120, progress=progress, extended_timeout=45,
            label="university_discovery_mode_search (multi-country)")
    finally:
        pool.shutdown(wait=False)
    for iso, result in results_by_iso.items():
        top, close = result or ([], [])
        for p in top:
            uid = p.get("email") or p.get("openalex_id") or p.get("name","")[:30]
            if uid and uid not in seen_ids:
                seen_ids.add(uid)
                all_top.append(p)
        for p in close:
            uid = p.get("email") or p.get("openalex_id") or p.get("name","")[:30]
            if uid and uid not in seen_ids:
                seen_ids.add(uid)
                all_close.append(p)
    if progress is not None:
        progress["stage"] = "done"
    return all_top[:DISCOVERY_TOP_N], all_close[:DISCOVERY_CLOSE_N]

def _process_one_university_discovery(uni: dict, field: str, keywords: list[str],
                                       country: str) -> list[dict]:
    """پردازش کامل یک دانشگاه (دپارتمان→دانشکده→آزمایشگاه→استخراج
    استاد→Availability) — به یک تابع مجزا منتقل شد تا هر دانشگاه بتونه
    مستقل و موازی (توی ThreadPoolExecutor) پردازش بشه، به‌جای لوپ سری
    قبلی که با breadth بالاتر (UNIVERSITY_DISCOVERY_LIMIT=25 به‌جای ۸)
    غیرقابل‌تحمل کند می‌شد."""
    dept_text = _university_department_crawl(uni, field, keywords)
    faculty_text = _university_faculty_crawl(uni, field, keywords)
    lab_text = _university_lab_crawl(uni, field, keywords)
    combined_text = "\n---\n".join(t for t in (dept_text, faculty_text, lab_text) if t)[:8000]
    if not combined_text:
        return []

    names = _professor_extraction(combined_text, field, needed=UNIVERSITY_DISCOVERY_NEEDED)
    if not names:
        return []

    avail_score, avail_evidence = detect_availability_signals(combined_text)

    out = []
    for nm in names:
        out.append({
            "name": nm.get("name", ""), "email": nm.get("email", ""),
            "university": uni.get("name", ""), "country": country,
            "openalex_id": f"UNIDISC_{uni.get('name','')[:12]}_{nm.get('name','')[:20]}",
            "url": uni.get("homepage_url", ""),
            "snippet": "University Discovery Mode (Department/Faculty/Lab Crawl)",
            "papers": [],
            "availability_score": avail_score,
            "availability_evidence": avail_evidence,
        })
    return out

def university_discovery_mode_search_single(field: str, country: str, count: int,
                                      client: dict | None = None,
                                      progress: dict | None = None) -> tuple[list[dict], list[dict]]:
    """اجرای کامل University Discovery Mode:
        University Discovery → Department Crawl → Faculty Crawl → Lab Crawl
        → Professor Extraction → Ranking → Availability Detection → Top N

    خروجی یک tuple ه: (top_relevant, close_candidates)
      - top_relevant: حداکثر DISCOVERY_TOP_N کاندید با بالاترین رتبه (برای
        نمایش/پیش‌نمایش عادی)
      - close_candidates: بقیه‌ی کاندیدهای معتبر (با امتیاز پایین‌تر از
        RELEVANCE_MIN_SCORE ولی بالای CLOSE_MATCH_MIN_SCORE) — برای پیام
        «هیچ‌کدوم دقیق match نبود ولی N نزدیک پیدا شد».

    ⚠️ فیکس درخواستی: قبلاً فقط ۸ دانشگاه (سری، یکی‌یکی) کراول می‌شد و
    حداکثر ۱۰ نتیجه نمایش داده می‌شد — خیلی کمتر از هدف واقعی «اپلای ۱۵۰
    موقعیت». الان UNIVERSITY_DISCOVERY_LIMIT=۲۵ دانشگاه *موازی* (نه سری)
    کراول می‌شن (با _run_futures_with_extension، یعنی اگه اکثرشون هنوز
    genuinely در حال کارن، یک پنجره‌ی اضافه هم می‌گیرن به‌جای رها شدن) و
    سقف خروجی به DISCOVERY_TOP_N + DISCOVERY_CLOSE_N (=۱۵۰) رسیده."""
    if client:
        _, keywords = _build_smart_query(client)
    else:
        keywords = field.lower().split()

    universities = _university_discovery(field, country, limit=UNIVERSITY_DISCOVERY_LIMIT)
    if not universities:
        return [], []

    if progress is not None:
        progress["stage"] = "searching"
        progress["total"] = len(universities)
        progress["done"] = 0
        progress["current"] = ""
        progress["source_labels"] = {}

    all_candidates: list[dict] = []
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(universities), DISCOVERY_CRAWL_WORKERS))
    try:
        futs = {
            pool.submit(_process_one_university_discovery, uni, field, keywords, country): uni.get("name", "?")
            for uni in universities
        }
        results_by_uni = _run_futures_with_extension(
            futs, DISCOVERY_CRAWL_POOL_TIMEOUT, progress=progress,
            label="university_discovery_mode_search_single (crawl)")
    finally:
        pool.shutdown(wait=False)
    for uni_name, cands in results_by_uni.items():
        all_candidates.extend(cands or [])

    if not all_candidates:
        return [], []

    # ── Ranking + Availability Detection: امتیاز نهایی = تطابق کلیدواژه‌ای
    # (۰ تا ۱۰۰) + بونس Availability (۰ تا ۸۵، سقف مجموع ۱۰۰) ─────────
    for p in all_candidates:
        base = round(_score_professor_relevance(p, keywords, client or {}) * 100)
        p["_rank_score"] = min(100, base + p.get("availability_score", 0))

    all_candidates.sort(key=lambda p: p["_rank_score"], reverse=True)

    top = [p for p in all_candidates[:DISCOVERY_TOP_N]]
    close = [p for p in all_candidates[DISCOVERY_TOP_N:] if p["_rank_score"] >= CLOSE_MATCH_MIN_SCORE][:DISCOVERY_CLOSE_N]

    # ⚠️ فیکس مهم: قبلاً اینجا هیچ enrichment ایمیلی صدا زده نمی‌شد —
    # یعنی حتی اسم‌هایی که Professor Extraction ایمیلشون رو مستقیم از
    # متن صفحه پیدا نکرده بود (چون فقط توی href مخفی بود، نه متن ساده)
    # هیچ تلاش دومی (pattern دامنه‌ی دانشگاه + SMTP probe، دقیقاً همون
    # تکنیک اصلی Hunter.io) نمی‌گرفتن. الان مثل بقیه‌ی تلاش‌ها
    # (۱،۲،۳،۵) این مرحله رو داره.
    if progress is not None:
        progress["stage"] = "enrich_emails"
    if top:
        top = _enrich_missing_emails(top)
    if close:
        close = _enrich_missing_emails(close)
    if progress is not None:
        progress["stage"] = "done"

    logger.info(f"University Discovery Mode done: {len(universities)} دانشگاه، "
                f"{len(all_candidates)} کاندید، top={len(top)} close={len(close)}")

    if top:
        db_save_search_cache("professor", field, country, top)
    return top, close

def run_prof_retry_attempt(field: str, country: str, count: int, client: dict | None,
                            attempt_idx: int, progress: dict | None = None) -> dict:
    """لایه‌ی یکپارچه‌ی «تلاش مجدد» — پنج مرحله:

        Retry 1 → Retry 2 → Retry 3 → University Discovery Mode
        → ORCID کلیدواژه‌ای + جستجوی وب گسترده

    attempt_idx صفرشمار (اولین کلیک روی «🔁 تلاش مجدد» = ۰).
      • attempt_idx 0..2  → PROF_SOURCE_RETRY_GROUPS (سریع، فقط چند منبع API — تلاش ۱،۲،۳)
      • attempt_idx 3     → University Discovery Mode (تلاش ۴؛ کندتر، دقیق‌تر — کراول
                             مستقیم دانشگاه‌هایی که واقعاً توی این رشته/کشور فعالن)
      • attempt_idx >= 4  → ORCID کلیدواژه‌ای + جستجوی وب گسترده و بدون محدودیت به
                             دانشگاه‌های از‌قبل‌کشف‌شده (تلاش ۵؛ گسترده‌ترین و آخرین لایه —
                             روی کلیک‌های بعدی هم همینو دوباره تازه اجرا می‌کنه، چون
                             تصادفاً بودنِ نتایج وب‌سرچ ممکنه بار بعد نتیجه‌ی متفاوتی بده)

    progress: دیکشنری پیشرفت زنده‌ی اختیاری — قبلاً هیچ‌کدوم از این پنج
    مرحله پیام زنده‌ی پیشرفت نداشتن (کاربر فقط یک پیام ثابت «در حال تلاش
    مجدد» می‌دید و تا اتمام کامل هیچ آپدیتی نبود)؛ الان به هر سه مسیر
    پاس داده می‌شه.

    خروجی: {"profs": [...], "close": [...], "mode": "retry"|"university_discovery"|"orcid_broadweb",
             "label": "متن فارسی مرحله برای پیام به کاربر"}"""
    if attempt_idx < len(PROF_SOURCE_RETRY_GROUPS):
        profs = search_professors_retry_group(field, country, count, client, attempt_idx, progress=progress)
        return {"profs": profs, "close": [], "mode": "retry",
                "label": f"روش {attempt_idx + 1} از {TOTAL_PROF_RETRY_ATTEMPTS}"}
    if attempt_idx == len(PROF_SOURCE_RETRY_GROUPS):
        top, close = university_discovery_mode_search(field, country, count, client, progress=progress)
        return {"profs": top, "close": close, "mode": "university_discovery",
                "label": f"روش {attempt_idx + 1} از {TOTAL_PROF_RETRY_ATTEMPTS} — "
                         f"🎓 University Discovery Mode (دانشگاه → دانشکده → آزمایشگاه → استخراج استاد)"}
    profs = orcid_and_broadweb_search(field, country, count, client)
    if progress is not None:
        progress["stage"] = "done"
    return {"profs": profs, "close": [], "mode": "orcid_broadweb",
            "label": f"روش {min(attempt_idx + 1, TOTAL_PROF_RETRY_ATTEMPTS)} از {TOTAL_PROF_RETRY_ATTEMPTS} — "
                     f"🌐 ORCID کلیدواژه‌ای + جستجوی وب گسترده (بدون محدودیت دانشگاه)"}

# ================================================================
# نگاشت اسم کشور (فارسی/انگلیسی) → ISO code، برای فیلتر OpenAlex.
# قبلاً این نگاشت فقط کلید انگلیسی داشت، در حالی که سوال onboarding
# خودمون به کاربر پیشنهاد می‌ده اسم کشور رو فارسی بنویسه (مثلاً «آلمان،
# کانادا»)! یعنی برای اکثر کاربرا این فیلتر هیچ‌وقت match نمی‌شد و
# بی‌صدا نادیده گرفته می‌شد. الان هم فارسی هم انگلیسی پوشش داده می‌شه.
# ================================================================
_COUNTRY_ISO = {
    "germany":"DE","آلمان":"DE",
    "canada":"CA","کانادا":"CA",
    "australia":"AU","استرالیا":"AU",
    "usa":"US","united states":"US","america":"US","آمریکا":"US",
    "uk":"GB","united kingdom":"GB","britain":"GB","انگلیس":"GB","انگلستان":"GB",
    "netherlands":"NL","holland":"NL","هلند":"NL",
    "sweden":"SE","سوئد":"SE",
    "france":"FR","فرانسه":"FR",
    "japan":"JP","ژاپن":"JP",
    "china":"CN","چین":"CN",
    "india":"IN","هند":"IN",
    "iran":"IR","ایران":"IR",
    "switzerland":"CH","سوئیس":"CH",
    "austria":"AT","اتریش":"AT",
    "denmark":"DK","دانمارک":"DK",
    "finland":"FI","فنلاند":"FI",
    "italy":"IT","ایتالیا":"IT",
    "spain":"ES","اسپانیا":"ES",
    "norway":"NO","نروژ":"NO",
    "belgium":"BE","بلژیک":"BE",
    "south korea":"KR","korea":"KR","کره جنوبی":"KR","کره":"KR",
    "singapore":"SG","سنگاپور":"SG",
    "new zealand":"NZ","نیوزیلند":"NZ",
    "ireland":"IE","ایرلند":"IE",
    "poland":"PL","لهستان":"PL",
    "turkey":"TR","ترکیه":"TR",
    "uae":"AE","united arab emirates":"AE","امارات":"AE",
    "qatar":"QA","قطر":"QA",
    "saudi arabia":"SA","عربستان":"SA",
}

def _resolve_country_iso(country_q: str) -> str:
    cq = (country_q or "").strip().lower()
    if not cq:
        return ""
    if cq in _COUNTRY_ISO:
        return _COUNTRY_ISO[cq]
    # اگه کاربر چند کشور با فاصله نوشته («Germany Canada»)، اولین کلمه‌ای
    # که می‌شناسیم رو استفاده می‌کنیم — به‌جای فقط اولین کلمه‌ی خام که
    # برای اسم‌های چندکلمه‌ای (مثل «South Korea») خراب می‌شد.
    for token in cq.split():
        if token in _COUNTRY_ISO:
            return _COUNTRY_ISO[token]
    return ""

PROF_SEARCH_HEADERS = {"User-Agent": "ManifestApplyBot/1.0 (research; contact@manifestapply.com)"}

# ================================================================
# منابع جستجوی استاد — هر کدوم تابع مستقل top-level (نه closure داخل
# _search_professors_impl) — قبلاً این ۶ منبع یکی‌یکی و سری صدا زده
# می‌شدن (اگه یه منبع کند بود/timeout می‌خورد، کل زنجیره کند می‌شد).
# الان توسط _search_professors_impl با ThreadPoolExecutor هم‌زمان صدا
# زده می‌شن. top-level بودن این توابع (به‌جای closure) عمداً هست: یعنی
# می‌شه هر منبع رو جدا، مستقل از بقیه و بدون دخالت de-dup مرحله‌ی merge،
# تست کرد — دقیقاً چیزی که برای اسکریپت تست مستقل لازمه.
# ================================================================

def prof_src_web_discovery(field: str, country_q: str, keywords: list[str], count: int) -> list[dict]:
    out = []
    try:
        discovered = _discover_professors_via_web_search(field, country_q, keywords, needed=count)
        for d in discovered:
            # ⚠️ فیکس: قبلاً اینجا ایمیلی که _discover_professors_via_web_search
            # مستقیم از متن صفحه استخراج کرده بود (اگه به‌صورت متن ساده کنار
            # اسم بود) دور ریخته می‌شد و همیشه "" می‌رفت — الان همونو نگه می‌داریم.
            out.append({"name": d.get("name", ""), "email": d.get("email", ""),
                        "university": d.get("university", ""), "country": country_q,
                        "openalex_id": f"WEB_{d.get('name','')[:20]}",
                        "url": "", "snippet": "Found via faculty/department/lab page",
                        "papers": []})
    except Exception as e:
        logger.warning(f"Web discovery search: {e}")
    return out

def prof_src_openalex_topics(field: str, country_q: str, sub_fields: list[str], count: int) -> list[dict]:
    # OpenAlex Concepts API رسماً deprecated شده و دیگه maintain نمی‌شه؛
    # جایگزینش /topics هست که دقت طبقه‌بندی رشته‌ای بهتری داره. فیلد
    # فیلتر روی Author هم از x_concepts.id به topics.id تغییر کرده.
    out = []
    HEADERS = PROF_SEARCH_HEADERS
    iso = _resolve_country_iso(country_q) if country_q else ""
    # ⚠️ فیکس: last_known_institution (تکی) توسط OpenAlex deprecated شده و
    # جایگزینش last_known_institutions (جمع، یک لیست) هست — فیلتر با اسم
    # قدیمی هیچ‌وقت match نمی‌کنه (نه خطا می‌ده نه warning، فقط ۰ نتیجه
    # برمی‌گردونه)، دقیقاً چیزی که توی لاگ‌های واقعی دیده شد.
    country_filter = f",last_known_institutions.country_code:{iso}" if iso else ""
    try:
        topic_ids_all = []
        for sub in sub_fields:
            try:
                rc = _http_get_retry("https://api.openalex.org/topics",
                    params={"search": sub, "per-page": 2, "select": "id,display_name"},
                    headers=HEADERS, timeout=8, max_retries=2)
                for c in rc.json().get("results", []):
                    if c["id"] not in topic_ids_all:
                        topic_ids_all.append(c["id"])
            except Exception as e:
                logger.debug(f"OpenAlex topic lookup for '{sub}': {e}")
        topic_ids = "|".join(topic_ids_all[:8])
        filt = (f"topics.id:{topic_ids}{country_filter}" if topic_ids
                else f"works_count:>5{country_filter}")
        r2 = _http_get_retry("https://api.openalex.org/authors",
            params={"filter": filt, "sort": "cited_by_count:desc",
                    "per-page": min(count, 50),
                    "select": "id,display_name,last_known_institutions,works_count,cited_by_count"},
            headers=HEADERS, timeout=12)
        for a in r2.json().get("results", []):
            insts = a.get("last_known_institutions") or []
            inst = insts[0] if insts else {}
            out.append({"name": a.get("display_name",""), "email": "",
                        "university": inst.get("display_name",""),
                        "country": inst.get("country_code",""),
                        "openalex_id": a.get("id",""),
                        "url": inst.get("homepage_url",""),
                        "snippet": f"Works:{a.get('works_count',0)} Citations:{a.get('cited_by_count',0)}",
                        "papers": []})
        logger.info(f"OpenAlex topics: {len(out)} candidates (from {len(sub_fields)} sub-field(s), {len(topic_ids_all)} topic(s))")
    except Exception as e:
        logger.warning(f"OpenAlex search: {e}")
    return out

def prof_src_semantic_scholar(field: str, country_q: str, keywords: list[str], count: int) -> list[dict]:
    out = []
    try:
        q = (" ".join(keywords[:3]) + (" " + country_q if country_q else "")).strip() or field
        r = _http_get_retry("https://api.semanticscholar.org/graph/v1/author/search",
            params={"query": q, "limit": min(count, 50),
                    "fields": "name,affiliations,paperCount"},
            headers=PROF_SEARCH_HEADERS, timeout=12)
        if r.status_code == 429:
            logger.info("Semantic Scholar author search: rate limited even after retries")
        else:
            for a in r.json().get("data", []):
                affs = a.get("affiliations") or []
                out.append({"name": a.get("name",""), "email": "",
                            "university": affs[0] if affs else "",
                            "country": country_q,
                            "openalex_id": f"SS_{a.get('authorId','')}",
                            "url": "", "snippet": f"Papers:{a.get('paperCount',0)}",
                            "papers": []})
    except Exception as e:
        logger.warning(f"Semantic Scholar author search: {e}")
    return out

def prof_src_openalex_institution(country_q: str) -> list[dict]:
    out = []
    if not (country_q and any(
            kw in country_q.lower() for kw in
            ["university","uni","tech","institute","tu ","lmu","mit","stanford",
             "harvard","oxford","berlin","munich","toronto","montreal"])):
        return out
    HEADERS = PROF_SEARCH_HEADERS
    try:
        r = _http_get_retry("https://api.openalex.org/institutions",
            params={"search": country_q, "per-page": 3},
            headers=HEADERS, timeout=10)
        for inst in r.json().get("results", [])[:2]:
            inst_id = inst.get("id","")
            if not inst_id: continue
            r2 = _http_get_retry("https://api.openalex.org/authors",
                params={"filter": f"last_known_institutions.id:{inst_id}",
                        "sort": "cited_by_count:desc", "per-page": 30,
                        "select": "id,display_name,last_known_institutions,works_count"},
                headers=HEADERS, timeout=12)
            for a in r2.json().get("results", []):
                if not a.get("display_name"): continue
                out.append({"name": a["display_name"], "email": "",
                            "university": inst.get("display_name",""),
                            "country": country_q, "openalex_id": a.get("id",""),
                            "url": "", "snippet": f"Works:{a.get('works_count',0)}",
                            "papers": []})
    except Exception as e:
        logger.warning(f"OpenAlex institution search: {e}")
    return out

def prof_src_crossref(field: str, country_q: str) -> list[dict]:
    out = []
    try:
        q = f"{field} {country_q}".strip()
        # ⚠️ فیکس: "institution" یک select field معتبر توی Crossref works
        # نیست (اطلاعات affiliation داخل author[].affiliation تودرتوئه، نه
        # یک فیلد top-level به اسم institution) — Crossref با select
        # نامعتبر خطای 400 برمی‌گردوند که بی‌صدا (بدون exception، چون
        # isinstance check پایین‌تر رد می‌شد) تبدیل به ۰ نتیجه می‌شد.
        r = _http_get_retry("https://api.crossref.org/works",
            params={"query": q, "rows": 50,
                    "select": "author,title",
                    "mailto": "contact@manifestapply.com"},
            headers=PROF_SEARCH_HEADERS, timeout=12)
        message = r.json().get("message", {})
        items = message.get("items", []) if isinstance(message, dict) else []
        for work in items:
            for author in (work.get("author") or []):
                name_parts = [author.get("given",""), author.get("family","")]
                name = " ".join(p for p in name_parts if p).strip()
                if not name: continue
                affs = author.get("affiliation") or []
                uni = affs[0].get("name","") if affs else ""
                out.append({"name": name, "email": "",
                            "university": uni, "country": country_q,
                            "openalex_id": f"CR_{name[:20]}",
                            "url": "", "snippet": f"Work: {work.get('title',[''])[0][:80]}",
                            "papers": [work.get("title",[""])[0]]})
    except Exception as e:
        logger.warning(f"Crossref fallback: {e}")
    return out

def prof_src_dblp(field: str, country_q: str, count: int) -> list[dict]:
    # منبع اختصاصی CS که توی دیاگرام اصلی بود ولی توی این فایل هیچ‌جا
    # پیاده‌سازی نشده بود — دقیق‌ترین/به‌روزترین منبع برای اساتید کامپیوتر
    # (به‌خصوص venueهای کنفرانسی که OpenAlex/Semantic Scholar دیرتر
    # index می‌کنن).
    try:
        return _dblp_search(field, count, country_q)
    except Exception as e:
        logger.warning(f"DBLP search: {e}")
        return []

# ================================================================
# منابع تلاش ۵ (آخرین و گسترده‌ترین «تلاش مجدد») — طبق تحقیقی که قبل از
# پیاده‌سازی انجام شد: مطمئن‌ترین منبعی که ایمیل واقعاً *همیشه* توش هست،
# صفحه‌ی دایرکتوری/هوم‌پیج شخصی خودِ استاده (mailto مستقیم) — که تلاش ۴
# (University Discovery Mode) دقیقاً همینو از دانشگاه‌های *کشف‌شده*
# می‌گیره. تلاش ۵ دو تا شکاف باقی‌مونده رو پوشش می‌ده:
#   ۱) ORCID با جستجوی کلیدواژه‌ای (نه فقط تأیید یک اسم شناخته‌شده مثل
#      جایی که ORCID الان توی pipeline ایمیل استفاده می‌شه) — کشف اسم‌های
#      جدیدی که هنوز توی OpenAlex/Semantic Scholar/Crossref/DBLP indexed
#      نشدن ولی پروفایل ORCID فعال دارن.
#   ۲) جستجوی وب کاملاً گسترده و بدون محدودیت به دانشگاه‌های از‌قبل‌
#      کشف‌شده (University Discovery Mode فقط دانشگاه‌هایی که توی
#      OpenAlex works_count بالا دارن رو می‌بینه — دانشگاه‌های کوچیک‌تر یا
#      کشورهایی با پوشش ضعیف OpenAlex همیشه جا می‌مونن).
# ================================================================

def prof_src_orcid_keyword(field: str, country_q: str, sub_fields: list[str], count: int) -> list[dict]:
    """کشف اسم استاد از ORCID با جستجوی کلیدواژه‌ای رشته+کشور (بدون
    key، رایگان). خروجی هر رکورد شامل affiliation و (در صورت public
    بودن) ایمیل مستقیم از خودِ ORCID هست."""
    out: list[dict] = []
    try:
        terms = [t for t in sub_fields[:3] if t] or [field]
        q = "text:(" + " OR ".join(f'"{t}"' for t in terms) + ")"
        if country_q:
            q += f' AND text:"{country_q}"'
        r = _http_get_retry("https://pub.orcid.org/v3.0/search/",
            params={"q": q, "rows": min(max(count, 15), 20)},
            headers={"Accept": "application/json", **PROF_SEARCH_HEADERS},
            timeout=10, max_retries=2)
        results = (r.json() or {}).get("result", []) or []
        orcid_ids = [oid for oid in (
            (res.get("orcid-identifier") or {}).get("path", "") for res in results
        ) if oid]
        if not orcid_ids:
            return out
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(8, len(orcid_ids))) as ex:
            records = list(ex.map(_orcid_record_by_id, orcid_ids))
        for oid, rec in zip(orcid_ids, records):
            if not rec or not rec.get("name"):
                continue
            out.append({
                "name": rec["name"], "email": rec.get("email", ""),
                "university": rec.get("affiliation", ""),
                "country": rec.get("country", "") or country_q,
                "openalex_id": f"ORCID_{oid}",
                "url": f"https://orcid.org/{oid}",
                "snippet": "ORCID keyword discovery", "papers": [],
            })
        logger.info(f"ORCID keyword search: {len(out)} candidates for '{field}' in '{country_q}'")
    except Exception as e:
        logger.warning(f"ORCID keyword search: {e}")
    return out

def prof_src_broad_web(field: str, country_q: str, keywords: list[str], count: int) -> list[dict]:
    """جستجوی وب کاملاً گسترده — برخلاف web_discovery (که دنبال صفحه‌ی
    دایرکتوری هیئت‌علمیِ *یک* دانشگاهه) یا University Discovery Mode (که
    فقط روی دانشگاه‌های از‌قبل‌کشف‌شده کار می‌کنه)، این یکی هیچ محدودیت
    site:/دامنه‌ای نداره و مستقیم صفحه‌ی شخصی/CV خودِ استاد رو هدف
    می‌گیره — چون خیلی از استادها ایمیل‌شون رو روی هوم‌پیج شخصی خودشون
    می‌ذارن، جایی که گاهی توی دایرکتوری رسمی دانشگاه نیست."""
    out: list[dict] = []
    q_terms = " ".join(keywords[:3]) or field
    country_part = f" {country_q}" if country_q else ""
    queries = [
        f'"{q_terms}" professor personal homepage email{country_part}',
        f'"{q_terms}" "assistant professor" OR "associate professor" OR "full professor"{country_part} contact email',
        f'"{q_terms}" CV professor{country_part} "email"',
    ]
    pages_text: list[str] = []
    for q in queries:
        try:
            g = _google_search_and_scrape(q.strip(), max_pages=3)
        except Exception as e:
            g = {}
            logger.debug(f"broad web google '{q[:60]}': {e}")
        pt = g.get("lab_page_text", "")
        if pt:
            pages_text.append(pt)
        if len(pages_text) >= 3:
            break
    if len(pages_text) < 2:
        for q in queries:
            try:
                d = _ddg_search_and_scrape(q.strip(), max_pages=3)
            except Exception as e:
                d = {}
                logger.debug(f"broad web ddg '{q[:60]}': {e}")
            pt = d.get("lab_page_text", "")
            if pt:
                pages_text.append(pt)
            if len(pages_text) >= 3:
                break
    if not pages_text:
        return out

    combined = "\n---\n".join(pages_text)[:6000]
    try:
        prompt = f"""Below is raw scraped text from professor personal homepages/CV pages found via a broad,
un-restricted web search (not limited to a specific university's directory) — it may be messy. Extract real
individual professors/faculty members who appear to work in or near this field: "{field}". Never invent
names; only list ones that actually appear in the text below.
Raw text:
{combined}
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"professors": [{{"name": "Full Name", "university": "University name or empty string"}}, ...]}}
(max 15 entries, only real-looking individual person names — never navigation labels, department names,
or generic words)"""
        ai = run_ai_sync(call_ai(prompt, min_length=15))
        data = _extract_json_object(ai) or {}
        for p in (data.get("professors") or [])[:count]:
            name = str(p.get("name", "")).strip()
            if name and len(name.split()) >= 2:
                out.append({"name": name, "email": "",
                            "university": str(p.get("university", "")).strip(),
                            "country": country_q, "openalex_id": f"BWEB_{name[:20]}",
                            "url": "", "snippet": "Broad web search (personal homepage/CV, no site restriction)",
                            "papers": []})
    except Exception as e:
        logger.debug(f"broad web AI extraction: {e}")
    return out

def orcid_and_broadweb_search(field: str, country: str, count: int, client: dict | None = None) -> list[dict]:
    """تلاش ۵ — دو منبع بالا رو موازی اجرا می‌کنه، merge/de-dup می‌کنه و
    دقیقاً مثل بقیه‌ی تلاش‌های مجدد (search_professors_retry_group) از
    همون pipeline تکمیل ایمیل (_enrich_missing_emails) عبور می‌ده تا
    اسم‌هایی که خودشون ایمیل مستقیم نداشتن (مثلاً از broad_web) هم یک
    تلاش آخر برای پیدا کردن ایمیل بگیرن."""
    country_q = "" if country.strip() in ("همه", "all", "*", "") else country
    if client:
        _, keywords = _build_smart_query(client)
    else:
        keywords = field.lower().split()
    sub_fields = [p.strip() for p in re.split(r"[،,]", field) if p.strip()][:7] or [field]

    seen_ids: set[str] = set()
    profs: list[dict] = []
    def _add(p: dict):
        uid = p.get("email") or p.get("openalex_id") or p.get("name", "")[:30]
        if uid and uid not in seen_ids:
            seen_ids.add(uid)
            profs.append(p)

    pool = concurrent.futures.ThreadPoolExecutor(max_workers=2)
    try:
        futures = {
            pool.submit(prof_src_orcid_keyword, field, country_q, sub_fields, count): "orcid_keyword",
            pool.submit(prof_src_broad_web, field, country_q, keywords, count): "broad_web",
        }
        results_by_source = _run_futures_with_extension(
            futures, 25, label="orcid_and_broadweb_search")
        for name, res in results_by_source.items():
            for p in (res or []):
                _add(p)
    finally:
        pool.shutdown(wait=False)

    if profs:
        profs = _enrich_missing_emails(profs)
        db_save_search_cache("professor", field, country, profs)
    logger.info(f"orcid_and_broadweb_search done: {len(profs)} candidates for '{field}' in '{country}'")
    return profs

# نام منابع، به همون ترتیب اولویت قبلی (برای merge/logging سازگار)
PROF_SOURCES = [
    "web_discovery", "openalex_topics", "semantic_scholar",
    "openalex_institution", "crossref", "dblp",
]

# برای پیام‌های پیشرفت زنده‌ی جستجو (progress dict توی _search_professors_impl) —
# اسم فارسی‌خوان هر منبع، فقط برای نمایش به کاربر.
PROF_SOURCE_LABELS_FA = {
    "web_discovery":        "جستجوی وب/صفحات دانشگاه",
    "openalex_topics":      "OpenAlex (بر اساس موضوع)",
    "semantic_scholar":     "Semantic Scholar",
    "openalex_institution": "OpenAlex (بر اساس دانشگاه)",
    "crossref":              "Crossref",
    "dblp":                  "DBLP",
}

# ================================================================
# EXTENDABLE POOL TIMEOUT — طبق درخواست صریح: قبلاً هر جستجویی که سقف
# زمانی pool_timeout (مثلاً ۴۰ ثانیه) رو رد می‌کرد، منابع ناتموم رو
# بی‌قید و شرط خالی درنظر می‌گرفت و رد می‌شد — حتی اگه اکثر اون منابع
# واقعاً هنوز داشتن کار می‌کردن (نه fail/hang شده، فقط یه سایت کند
# جواب می‌داد) و چند ثانیه‌ی دیگه نتیجه‌ی واقعی می‌دادن. الان به‌جای
# رها کردن فوری، یک تمدید یک‌باره‌ی محدود (EXTENDED_TIMEOUT_SEC) به
# منابعی که هنوز genuinely در حال اجرا هستن می‌دیم — فقط وقتی سهم
# قابل‌توجهی از منابع باقی‌مونده (EXTENSION_MIN_STILL_WORKING_RATIO)
# هنوز واقعاً کار می‌کنن، وگرنه (مثلاً همه fail شدن) تمدید فایده‌ای
# نداره و مثل قبل رفتار می‌کنیم. توی progress با
# stage="extending_wait" علامت زده می‌شه تا کاربر پیام «کمی بیشتر صبر
# کنید» رو ببینه، نه این‌که فکر کنه بات هنگ کرده.
# ================================================================
EXTENDED_TIMEOUT_SEC = 25                   # پنجره‌ی اضافه‌ای که در صورت نیاز داده می‌شه
EXTENSION_MIN_STILL_WORKING_RATIO = 0.5     # حداقل نسبتی از منابع باقی‌مونده که باید genuinely در حال کار باشن تا تمدید بدیم

def _run_futures_with_extension(future_to_name: dict, pool_timeout: float,
                                 progress: dict | None = None,
                                 extended_timeout: float = EXTENDED_TIMEOUT_SEC,
                                 min_still_working_ratio: float = EXTENSION_MIN_STILL_WORKING_RATIO,
                                 label: str = "search") -> dict:
    """اجرای as_completed روی future_to_name با یک پنجره‌ی زمانی
    تمدیدپذیر. اول با pool_timeout عادی امتحان می‌کنه؛ اگه timeout بخوره
    ولی حداقل min_still_working_ratio از futureهای هنوز ناتموم واقعاً در
    حال اجرا باشن (fut.done()==False، یعنی نه موفق نه fail شده)، یک
    تمدید یک‌باره‌ی extended_timeout ثانیه‌ای بهشون می‌ده. اگه بعد از
    تمدید هم چیزی تموم نشده بود، همون‌ها رو خالی درنظر می‌گیره (دقیقاً
    رفتار قبلی). خروجی: dict {name: result_list}."""
    results: dict = {}
    try:
        for fut in concurrent.futures.as_completed(future_to_name, timeout=pool_timeout):
            name = future_to_name[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                logger.warning(f"{label} source '{name}' failed/timed out: {e}")
                results[name] = []
            if progress is not None:
                progress["done"] = progress.get("done", 0) + 1
                progress["current"] = name
        return results
    except concurrent.futures.TimeoutError:
        pass

    unfinished = {f: n for f, n in future_to_name.items() if not f.done()}
    if not unfinished:
        return results

    still_working_ratio = len(unfinished) / len(future_to_name)
    if still_working_ratio < min_still_working_ratio:
        logger.warning(f"{label}: pool-wide {pool_timeout}s timeout hit — only {len(unfinished)}/"
                        f"{len(future_to_name)} sources still genuinely working (below "
                        f"{min_still_working_ratio:.0%}) — not extending, treated as empty: "
                        f"{list(unfinished.values())}")
        for name in unfinished.values():
            results.setdefault(name, [])
        return results

    logger.info(f"{label}: pool-wide {pool_timeout}s timeout hit but {len(unfinished)}/"
                f"{len(future_to_name)} sources still genuinely working — granting one "
                f"{extended_timeout}s extension: {list(unfinished.values())}")
    if progress is not None:
        progress["stage"] = "extending_wait"
    try:
        for fut in concurrent.futures.as_completed(unfinished, timeout=extended_timeout):
            name = unfinished[fut]
            try:
                results[name] = fut.result()
            except Exception as e:
                logger.warning(f"{label} source '{name}' failed/timed out (extension window): {e}")
                results[name] = []
            if progress is not None:
                progress["done"] = progress.get("done", 0) + 1
                progress["current"] = name
    except concurrent.futures.TimeoutError:
        still_unfinished = [n for f, n in unfinished.items() if not f.done()]
        logger.warning(f"{label}: extension window ({extended_timeout}s) also exhausted — "
                        f"giving up on: {still_unfinished}")
        for name in still_unfinished:
            results.setdefault(name, [])
    if progress is not None:
        # برگردوندن stage به حالت عادی — caller اگه لازم باشه خودش "done"/
        # "enrich_emails" رو بعداً ست می‌کنه.
        progress["stage"] = "searching"
    return results

def run_prof_source(source_name: str, *, field: str, country_q: str, keywords: list[str],
                     sub_fields: list[str], count: int) -> list[dict]:
    """Dispatcher — یک منبع رو با اسمش صدا می‌زنه. هم توسط
    _search_professors_impl استفاده می‌شه، هم توسط اسکریپت تست مستقل
    (test_professor_search.py) که هر منبع رو جدا و بدون دخالت de-dup
    مرحله‌ی merge امتحان می‌کنه."""
    if source_name == "web_discovery":
        return prof_src_web_discovery(field, country_q, keywords, count)
    if source_name == "openalex_topics":
        return prof_src_openalex_topics(field, country_q, sub_fields, count)
    if source_name == "semantic_scholar":
        return prof_src_semantic_scholar(field, country_q, keywords, count)
    if source_name == "openalex_institution":
        return prof_src_openalex_institution(country_q)
    if source_name == "crossref":
        return prof_src_crossref(field, country_q)
    if source_name == "dblp":
        return prof_src_dblp(field, country_q, count)
    raise ValueError(f"unknown professor source: {source_name}")


def _resolve_countries_iso(country_q: str) -> list[str]:
    """برخلاف _resolve_country_iso (که فقط اولین کشور شناخته‌شده‌ی رشته رو
    برمی‌گردونه — یعنی «Germany, Canada» عملاً می‌شد فقط «Germany»)، این
    تابع همه‌ی کشورهای شناخته‌شده‌ی داخل رشته رو جدا جدا برمی‌گردونه.

    فرم ورودی مورد انتظار (نگاه کن به COLLECT_QUESTIONS بالاتر): کاربر
    صریحاً خواسته شده «با ویرگول جدا کنید» (مثال: «آلمان، کانادا، هلند,
    استرالیا») — پس اول با جداکننده‌های صریح (کاما فارسی/انگلیسی، «and»،
    «/»، «+») می‌شکنیم. اگه یک قطعه خودش مستقیم شناخته نشد (مثلاً کاربر
    به‌جای ویرگول فقط فاصله زده: «Germany Canada»)، توکن‌به‌توکن هم چک
    می‌کنیم — همون fallback قبلی، ولی این‌بار روی *همه‌ی* توکن‌ها، نه فقط
    اولین match.

    خروجی: لیست ISO codeهای یکتا، به ترتیبی که اولین بار دیده شدن."""
    cq = (country_q or "").strip().lower()
    if not cq:
        return []
    out: list[str] = []
    seen: set[str] = set()
    def _add(iso):
        if iso and iso not in seen:
            seen.add(iso); out.append(iso)
    for part in re.split(r"[،,]|(?:\s+and\s+)|/|\+", cq):
        part = part.strip()
        if not part:
            continue
        if part in _COUNTRY_ISO:
            _add(_COUNTRY_ISO[part])
            continue
        for token in part.split():
            if token in _COUNTRY_ISO:
                _add(_COUNTRY_ISO[token])
    return out

# ================================================================
# PROFESSOR IDENTITY ENGINE — طبق درخواست صریح کاربر: DBLP/Crossref فقط
# یک اسم خام (مثل "John Smith") از روی نویسنده‌ی یک مقاله استخراج
# می‌کنن — نه یک پروفایل استاد با دانشگاه مشخص. یعنی خودِ این نتیجه به
# تنهایی نمی‌گه این "John Smith" کدوم دانشگاهه، پس دو نفر کاملاً متفاوت
# با اسم مشابه (یا حتی یک نفر که در دو منبع کمی متفاوت نوشته شده —
# "John Smith" / "J. Smith" / "John A. Smith") ممکنه به اشتباه دو رکورد
# جدا (false split) یا یک رکورد اشتباهی merge‌شده (false merge) بشن.
#
# قبلاً dedup فقط یک `uid` تکی بود:
#     uid = email یا openalex_id یا name[:30]
# که با یک اختلاف جزئی (یه initial، یه فاصله‌ی اضافه، نبود ایمیل) کاملاً
# می‌شکست — یعنی همون "John Smith" از OpenAlex و "J. Smith" از DBLP دو
# رکورد کاملاً جدا می‌شدن، هر کدوم با نصف اطلاعات واقعی.
#
# الان: امضای چندلایه — نام (نرمال‌شده/initial-aware) + دانشگاه (fuzzy
# token overlap) + شناسه‌ی قطعی (ORCID/OpenAlex/Semantic Scholar ID، اگه
# باشه) + دامنه‌ی ایمیل. فقط وقتی این‌ها به‌اندازه‌ی کافی هم‌پوشانی دارن
# merge می‌شن؛ وگرنه (مثلاً دانشگاه هیچ‌کدوم مشخص نیست) عمداً merge
# نمی‌کنیم — چون ریسک false-merge (اطلاعات دو نفر متفاوت قاطی بشه) بدتر
# از نگه‌داشتن دو رکورد جداست.
# ================================================================
_IDENTITY_TITLE_RE = re.compile(r"\b(dr|prof|professor|mr|mrs|ms|phd|ph\.d)\.?\b")
_UNIVERSITY_STOPWORDS = {
    "university", "institute", "of", "the", "college", "school", "dept",
    "department", "technology", "and", "for", "national", "state",
}

def _normalize_person_name(name: str) -> str:
    """نام رو به یک فرم نرمال برای مقایسه‌ی identity تبدیل می‌کنه: حروف
    کوچک، حذف عنوان‌ها (Dr./Prof./...)، حذف نقطه‌ی initial. عمداً ترتیب
    توکن‌ها رو تغییر نمی‌ده (چون "John Smith" != "Smith John" یه
    false-positive خطرناکه اگه بی‌ترتیب کنیم)."""
    n = _IDENTITY_TITLE_RE.sub(" ", (name or "").strip().lower())
    n = re.sub(r"[^\w\s]", " ", n)
    return re.sub(r"\s+", " ", n).strip()

def _name_identity_match(n1: str, n2: str) -> bool:
    """True اگه دو نام نرمال‌شده احتمالاً یک نفرن — یا دقیقاً یکی‌ان، یا
    فامیلی‌شون (آخرین توکن) دقیقاً یکیه و حرف اول نام کوچیک هم یکیه
    (پوشش حالت‌های "John Smith" در برابر "J. Smith")."""
    if not n1 or not n2:
        return False
    if n1 == n2:
        return True
    t1, t2 = n1.split(), n2.split()
    if not t1 or not t2 or t1[-1] != t2[-1]:
        return False
    if len(t1) < 2 or len(t2) < 2:
        return True  # یکی‌شون فقط فامیلی داره — فامیلی که یکیه، کافیه
    return t1[0][0] == t2[0][0]

def _university_tokens(u: str) -> set[str]:
    u = (u or "").lower()
    tokens = set(re.findall(r"[a-z]{3,}", u))
    return tokens - _UNIVERSITY_STOPWORDS

_ACRONYM_CONNECTORS = {"of", "the", "and", "for", "de", "la", "der", "van"}

def _university_acronym(u: str) -> str:
    words = re.findall(r"[A-Za-z]+", u or "")
    return "".join(w[0] for w in words if w.lower() not in _ACRONYM_CONNECTORS).lower()

def _same_university(u1: str, u2: str) -> bool:
    """True فقط وقتی *هر دو* دانشگاه مشخصه و واقعاً هم‌پوشانی معنادار
    دارن — عمداً محافظه‌کار: اگه یکی/هر دو خالی باشن False برمی‌گردونه
    (تصمیم merge رو نمی‌شه فقط با حدسِ «شاید یکیه» گرفت).

    دو حالت رو پوشش می‌ده: ۱) هم‌پوشانی توکنی معمولی، ۲) یکی مخفف
    (acronym) اون یکیه — مثل "MIT" در برابر "Massachusetts Institute of
    Technology" — که فقط با overlap توکنی هرگز match نمی‌شد."""
    if not u1 or not u2:
        return False
    t1, t2 = _university_tokens(u1), _university_tokens(u2)
    if t1 and t2:
        overlap = t1 & t2
        if len(overlap) / min(len(t1), len(t2)) >= 0.5:
            return True
    raw1 = re.findall(r"[A-Za-z]+", u1)
    raw2 = re.findall(r"[A-Za-z]+", u2)
    if len(raw1) == 1 and raw1[0].isupper() and 2 <= len(raw1[0]) <= 6:
        return raw1[0].lower() == _university_acronym(u2)
    if len(raw2) == 1 and raw2[0].isupper() and 2 <= len(raw2[0]) <= 6:
        return raw2[0].lower() == _university_acronym(u1)
    return False

_PROF_STRONG_ID_FIELDS = ("orcid_id", "openalex_id", "semantic_scholar_id", "dblp_pid")

def _professor_strong_ids(p: dict) -> list[str]:
    """شناسه‌های *قطعی* (نه استخراج‌شده از یک مقاله) — پیشوندهای DBLP_/CR_
    که خودِ همین فایل برای یک نتیجه‌ی استخراج‌شده از نویسنده‌ی مقاله می‌سازه
    عمداً رد می‌شن، چون این‌ها یک شناسه‌ی رسمی نیستن (دقیقاً همون مشکلی
    که این Identity Engine قراره حلش کنه)."""
    ids = []
    for field_name in _PROF_STRONG_ID_FIELDS:
        v = str(p.get(field_name, "") or "").strip()
        if v and not v.startswith(("DBLP_", "CR_")):
            ids.append(f"{field_name}:{v}")
    return ids

def _merge_professor_records(a: dict, b: dict) -> dict:
    """دو رکورد استاد که Identity Engine یک نفر تشخیص داده رو merge
    می‌کنه: فیلدهای غیرخالی حفظ می‌شن (اولویت با a، بعد از b پر می‌شه)،
    ولی papers (لیست عنوان مقاله از این منبع) union/dedup می‌شه — نه
    overwrite — تا اطلاعات هیچ منبعی گم نشه (دقیقاً هدف اصلی merge چندمنبعی)."""
    merged = dict(a)
    for key, val in b.items():
        if key == "papers":
            existing = merged.get("papers") or []
            existing_l = {str(t).lower() for t in existing}
            merged["papers"] = existing + [t for t in (val or []) if str(t).lower() not in existing_l]
            continue
        if key == "_merged_sources":
            continue
        if not merged.get(key) and val:
            merged[key] = val
    src_a = a.get("_merged_sources") or [a.get("_source", "")]
    src_b = b.get("_merged_sources") or [b.get("_source", "")]
    merged["_merged_sources"] = [s for s in dict.fromkeys(src_a + src_b) if s]
    merged["_identity_merged"] = True
    return merged

class ProfessorIdentityEngine:
    """رکوردهای استاد از منابع مختلف رو اضافه می‌کنه و خودکار موارد
    هم‌هویت رو merge می‌کنه (نگاه کن به کامنت بالای این بخش). به‌جای یک
    `uid` تکی شکننده، از سه لایه استفاده می‌کنه: شناسه‌ی قطعی → ایمیل
    دقیق → نام+دانشگاه fuzzy."""
    def __init__(self):
        self._records: list[dict] = []
        self._by_strong_id: dict[str, int] = {}
        self._by_email: dict[str, int] = {}

    def add(self, p: dict) -> dict:
        """رکورد جدید رو اضافه می‌کنه؛ اگه match پیدا بشه merge می‌کنه و
        رکورد merge‌شده رو برمی‌گردونه، وگرنه خود p رو (به‌عنوان رکورد
        جدید) برمی‌گردونه. caller باید همیشه از خروجی این متد استفاده کنه."""
        norm_name = _normalize_person_name(p.get("name", ""))
        if not norm_name:
            self._records.append(p)
            return p

        # ۱) شناسه‌ی قطعی (ORCID/OpenAlex/Semantic Scholar/DBLP pid) — بالاترین اطمینان
        for sid in _professor_strong_ids(p):
            idx = self._by_strong_id.get(sid)
            if idx is not None:
                return self._merge_at(idx, p)

        # ۲) ایمیل دقیق — عملاً قطعی (دو استاد یک ایمیل ندارن)
        email = (p.get("email", "") or "").strip().lower()
        if email:
            idx = self._by_email.get(email)
            if idx is not None:
                return self._merge_at(idx, p)

        # ۳) نام (دقیق یا initial-aware) + دانشگاه fuzzy — فقط وقتی هر دو
        # دانشگاه مشخص دارن و واقعاً هم‌پوشانی معنادار دارن.
        for idx, existing in enumerate(self._records):
            ex_norm = _normalize_person_name(existing.get("name", ""))
            if _name_identity_match(ex_norm, norm_name) and \
               _same_university(existing.get("university", ""), p.get("university", "")):
                return self._merge_at(idx, p)

        self._records.append(p)
        self._reindex(len(self._records) - 1)
        return p

    def _merge_at(self, idx: int, p: dict) -> dict:
        merged = _merge_professor_records(self._records[idx], p)
        self._records[idx] = merged
        self._reindex(idx)
        return merged

    def _reindex(self, idx: int):
        rec = self._records[idx]
        for sid in _professor_strong_ids(rec):
            self._by_strong_id[sid] = idx
        email = (rec.get("email", "") or "").strip().lower()
        if email:
            self._by_email[email] = idx

    def all(self) -> list[dict]:
        return self._records


def _search_professors_impl(field: str, country: str, count: int = 100,
                      client: dict | None = None, sources: list[str] | None = None,
                      pool_timeout: float = 40, skip_progressive_fallback: bool = False,
                      progress: dict | None = None) -> list[dict]:
    """Wrapper چند-کشوری روی _search_professors_impl_single — قبلاً وقتی
    کاربر چند کشور با هم می‌نوشت («Germany, Canada, Netherlands»)،
    _resolve_country_iso (پایین‌تر) فقط اولین کشور شناخته‌شده رو ISO
    می‌کرد و همون یکی به همه‌ی منابع ISO-محور (OpenAlex institution
    filter و...) پاس داده می‌شد — یعنی «Germany + Canada + Netherlands»
    در عمل فقط جستجوی «Germany» بود؛ بقیه‌ی کشورها بی‌سروصدا حذف می‌شدن.

    الان: اگه _resolve_countries_iso بیش از یک کشور تشخیص بده، پایپ‌لاین
    کامل (همون تابع سنگین قبلی، بدون هیچ تغییری — الان به اسم
    _search_professors_impl_single) رو یک‌بار جدا به‌ازای هر کشور (با
    سهم مساوی از count) اجرا می‌کنیم، هم‌زمان با ThreadPoolExecutor (نه
    سری — وگرنه با ۴ کشور، ۴ برابر کند می‌شد)، بعد نتایج رو merge/dedupe
    می‌کنیم. برای حالت تک‌کشوری یا «همه‌ی کشورها» (رشته‌ی خالی/«all»)،
    دقیقاً رفتار قبلی بدون هیچ overhead اضافه‌ای حفظ می‌شه."""
    countries = _resolve_countries_iso(country) if country and country.strip().lower() not in ("همه", "all", "*", "") else []
    if len(countries) <= 1:
        return _search_professors_impl_single(
            field, country, count, client, sources=sources, pool_timeout=pool_timeout,
            skip_progressive_fallback=skip_progressive_fallback, progress=progress)

    # سهم هر کشور از count کل — حداقل ۱۰ تا به هر کشور، حتی اگه count کلی
    # کم باشه (وگرنه با ۵ کشور و count=20، هر کدوم فقط ۴ تا می‌گرفت که
    # عملاً بی‌فایده‌ست).
    per_country = max(count // len(countries), 10)
    iso_to_name = {v: k for k, v in _COUNTRY_ISO.items()}
    merged: list[dict] = []
    seen_ids: set[str] = set()
    # ── فیکس: قبلاً `with ThreadPoolExecutor(...) as pool:` + as_completed
    # timeout بود — همون باگی که توی _run_wave (خط ۶۲۳۰) پیدا و فیکس شد:
    # خروج از `with` بعد از TimeoutError خودش `shutdown(wait=True)` صدا
    # می‌زد و منتظر *همه‌ی* thread های کند (کشورهایی که هنوز تموم نشده
    # بودن) می‌موند — یعنی pool_timeout+20 هیچ سقف واقعی‌ای تضمین
    # نمی‌کرد. الان pool دستی مدیریت می‌شه و با shutdown(wait=False) خارج
    # می‌شیم: کشورهای کند در پس‌زمینه به کارشون ادامه می‌دن (نتیجه‌شون دور
    # ریخته می‌شه) ولی تابع واقعاً بعد از pool_timeout+20 برمی‌گرده.
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(countries), 6))
    try:
        futs = {
            pool.submit(_search_professors_impl_single, field, iso_to_name.get(iso, iso),
                        per_country, client, sources=sources, pool_timeout=pool_timeout,
                        skip_progressive_fallback=skip_progressive_fallback, progress=None): iso
            for iso in countries
        }
        done, not_done = concurrent.futures.wait(futs, timeout=pool_timeout + 20)
        for fut in done:
            iso = futs[fut]
            try:
                sub_results = fut.result() or []
            except Exception as e:
                logger.warning(f"_search_professors_impl multi-country ({iso}): {e}")
                continue
            for p in sub_results:
                uid = p.get("email") or p.get("openalex_id") or p.get("name","")[:30]
                if uid and uid not in seen_ids:
                    seen_ids.add(uid)
                    merged.append(p)
        if not_done:
            logger.debug(f"{len(not_done)} country search(es) still running in background — نتیجه‌شون نادیده گرفته می‌شه")
    finally:
        pool.shutdown(wait=False)
    if progress is not None:
        progress["stage"] = "done"
        progress["done"] = progress.get("total", 1)
    return merged[:count]

def _search_professors_impl_single(field: str, country: str, count: int = 100,
                      client: dict | None = None, sources: list[str] | None = None,
                      pool_timeout: float = 40, skip_progressive_fallback: bool = False,
                      progress: dict | None = None) -> list[dict]:
    """
    جستجوی هوشمند استاد — طبق درخواست صریح، ترتیب اولویت عوض شد: قبلاً از
    دیتابیس‌های «Author» (OpenAlex/Semantic Scholar) شروع می‌شد که فقط
    محققانی رو پوشش می‌ده که مقاله‌ی indexed دارن — یعنی استادی که صفحه‌ی
    Faculty دانشکده‌ش هست ولی توی این دیتابیس‌ها Author نیست، اصلاً پیدا
    نمی‌شد. الان ترتیب برعکس شده و از صفحات واقعی دانشگاه شروع می‌کنه:

    ۱. University Faculty Page / Department Faculty / Lab Members  (وب،
       مستقیم از صفحات واقعی دانشگاه — نه یک دیتابیس Author)
    ۲. Google (به‌عنوان fallback همون مرحله‌ی ۱، اگه صفحه‌ی مستقیم پیدا نشد)
    ۳. OpenAlex topic → authors  (Railway-friendly، بدون key)
    ۴. Semantic Scholar author search  (Railway-friendly، بدون key)
    ۵. OpenAlex institution search  (اگه دانشگاه خاص بود)
    ۶. Crossref works → authors  (آخرین fallback)
    ۷. DBLP  (برای رشته‌های CS)

    همه‌ی این ۶ منبع (به‌جز progressive fallback در انتها) موازی صدا زده
    می‌شن — نه سری. هیچ‌وقت متوقف نمی‌شه — هر منبعی که fail کرد، بقیه
    مستقل ادامه می‌دن. اگه همه fail کردن، list خالی برمی‌گردونه (caller
    خودش پیام می‌ده).

    پارامترهای sources/pool_timeout/skip_progressive_fallback: برای «تلاش
    مجدد با یک دسته‌ی سورس متفاوت» (وقتی جستجوی اول با همه‌ی ۶ منبع
    نتیجه‌ای نداشت) — به‌جای اینکه هر تلاش مجدد دوباره کل pipeline
    سنگین (۶ منبع + progressive fallback + retry) رو از اول اجرا کنه
    (که هم کند بود هم اگه یه باگ سراسری وجود داشت، هر بار همون باگ رو
    می‌خوردیم)، هر تلاش مجدد فقط یک زیرمجموعه‌ی کوچیک و مجزا از منابع
    رو با سقف زمانی کوتاه‌تر امتحان می‌کنه — سریع‌تر، و اگه مشکل مال یه
    منبع خاص باشه، تلاش‌های بعدی که از منابع دیگه استفاده می‌کنن ازش
    عبور می‌کنن. `sources=None` یعنی رفتار پیش‌فرض قدیمی (همه‌ی ۶ منبع).
    """
    HEADERS = PROF_SEARCH_HEADERS
    country_q = "" if country.strip() in ("همه", "all", "*", "") else country
    # ── Professor Identity Engine (طبق درخواست صریح کاربر) ──────────────
    # قبلاً dedup فقط با یک uid تکی (email یا openalex_id یا name[:30])
    # انجام می‌شد که با کوچیک‌ترین اختلاف اسمی/نبود ایمیل کاملاً می‌شکست و
    # نمی‌تونست تشخیص بده "John Smith" از DBLP و "J. Smith" از OpenAlex
    # (یا برعکس، دو "John Smith" واقعاً متفاوت از دو دانشگاه) یک نفرن یا
    # نه. الان به‌جاش از _identity_engine.add() استفاده می‌شه که نام
    # (initial-aware) + دانشگاه (fuzzy) + شناسه‌ی قطعی (ORCID/OpenAlex/S2)
    # + ایمیل رو با هم می‌سنجه و رکوردهای واقعاً هم‌هویت رو merge می‌کنه
    # (نه فقط dedup می‌کنه) — یعنی اطلاعات چندمنبعیِ یک استاد واحد (مثلاً
    # papers از DBLP + email از Web Discovery) توی یک رکورد جمع می‌شه،
    # به‌جای این‌که دو رکورد نصفه‌واقعی جدا از هم بمونن.
    _identity_engine = ProfessorIdentityEngine()
    profs: list[dict] = []

    if client:
        smart_query, keywords = _build_smart_query(client)
    else:
        smart_query = f'"{field}"'
        keywords    = field.lower().split()

    logger.info(f"Professor search | query: {smart_query[:80]} | country: {country_q}")
    _search_t0 = time.time()

    def _add(p: dict):
        # می‌دیم به Identity Engine و لیست profs رو با نسخه‌ی canonical
        # (بعد از merge احتمالی) sync می‌کنیم — به‌جای append ساده، چون
        # یک merge ممکنه یک رکورد *موجود* رو به‌روزرسانی کنه، نه فقط یکی
        # جدید اضافه کنه (profs[:] = ... همون object لیست رو در جا آپدیت
        # می‌کنه، پس بقیه‌ی کد که به profs رفرنس داره درست کار می‌کنه).
        _identity_engine.add(p)
        profs[:] = _identity_engine.all()

    sub_fields = [p.strip() for p in re.split(r"[،,]", field) if p.strip()][:7] or [field]

    # ================================================================
    # اجرای موازی ۶ منبع با ThreadPoolExecutor — قبلاً یکی‌یکی و سری صدا
    # زده می‌شدن، یعنی اگه یه منبع کند بود/timeout می‌خورد، کل زنجیره کند
    # می‌شد. الان همه‌شون هم‌زمان صدا زده می‌شن (دقیقاً همون چیزی که
    # دیاگرام اصلی -"Search Orchestrator" با فلش‌های موازی- نشون می‌ده) و
    # فقط منتظر کندترین/timeoutخورده‌ترین‌شون می‌مونیم، نه مجموعشون.
    # ================================================================
    results_by_source: dict[str, list[dict]] = {}
    # نکته‌ی مهم: قبلاً از `with ThreadPoolExecutor(...) as pool:` استفاده
    # می‌شد — ولی `__exit__` اون خودش `shutdown(wait=True)` صدا می‌زنه،
    # یعنی حتی با گرفتنِ TimeoutError از as_completed، خروج از بلوک `with`
    # بازم منتظر تموم‌شدنِ *همه‌ی* thread ها (حتی اونایی که از ۴۰ ثانیه رد
    # شدن) می‌موند — یعنی سقف ۴۰ ثانیه فقط جلوی crash رو می‌گرفت، نه جلوی
    # تأخیر واقعی رو. الان pool رو دستی مدیریت می‌کنیم و با
    # `shutdown(wait=False)` خارج می‌شیم: منابع کند/گیرکرده در پس‌زمینه به
    # کارشون ادامه می‌دن (نتیجه‌شون دور ریخته می‌شه) ولی تابع اصلی معطلشون
    # نمی‌مونه — یعنی الان ۴۰ ثانیه واقعاً یه سقف زمانی سخت‌گیرانه‌ست.
    active_sources = sources if sources else PROF_SOURCES
    if progress is not None:
        progress["total"] = len(active_sources)
        progress["done"]  = 0
        progress["stage"] = "searching"
        progress["current"] = ""
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=len(active_sources))
    try:
        future_to_name = {
            pool.submit(run_prof_source, name, field=field, country_q=country_q,
                        keywords=keywords, sub_fields=sub_fields, count=count): name
            for name in active_sources
        }
        results_by_source = _run_futures_with_extension(
            future_to_name, pool_timeout, progress=progress, label="Professor search")
    finally:
        pool.shutdown(wait=False)


    # ── Merge: به همون ترتیب اولویت قبلی (Web Discovery اول، بعد
    # OpenAlex، ...) اضافه می‌کنیم تا رفتار de-dup و ترتیب نسبی طبیعی
    # بمونه؛ چون همه‌شون از قبل موازی fetch شدن، این حلقه دیگه هیچ
    # درخواست شبکه‌ای نداره — فقط merge سریع توی حافظه‌ست.
    for name in active_sources:
        before = len(profs)
        for p in results_by_source.get(name, []):
            _add(p)
            if len(profs) >= count:
                break
        logger.info(f"{name}: +{len(profs)-before} professors")
        if len(profs) >= count:
            break

    # ── Progressive fallback: اگه با کوئری کامل (شامل زیررشته‌های
    # پیشنهادی AI) هیچی پیدا نشد، یه بار دیگه فقط با خود رشته‌ی اصلی
    # (ساده‌شده، بدون پیشنهادها) و فیلتر بازتر امتحان می‌کنیم — به‌جای
    # اینکه کاربر رو با ۰ نتیجه رها کنیم.
    fallback_used = False
    if not profs and not skip_progressive_fallback:
        fallback_used = True
        primary_field = sub_fields[0] if sub_fields else field
        logger.warning(f"search_professors: 0 results for full query — retry با فقط '{primary_field}'")
        try:
            rc = _http_get_retry("https://api.openalex.org/topics",
                params={"search": primary_field, "per-page": 3, "select": "id,display_name"},
                headers=HEADERS, timeout=10, max_retries=1)
            topics = rc.json().get("results", [])
            topic_ids = "|".join(c["id"] for c in topics[:2]) if topics else ""
            filt = f"topics.id:{topic_ids}" if topic_ids else "works_count:>10"
            r2 = _http_get_retry("https://api.openalex.org/authors",
                params={"filter": filt, "sort": "cited_by_count:desc",
                        "per-page": min(count, 50),
                        "select": "id,display_name,last_known_institutions,works_count,cited_by_count"},
                headers=HEADERS, timeout=12, max_retries=1)
            for a in r2.json().get("results", []):
                insts = a.get("last_known_institutions") or []
                inst = insts[0] if insts else {}
                _add({"name": a.get("display_name",""), "email": "",
                      "university": inst.get("display_name",""),
                      "country": inst.get("country_code",""),
                      "openalex_id": a.get("id",""),
                      "url": inst.get("homepage_url",""),
                      "snippet": f"Works:{a.get('works_count',0)} Citations:{a.get('cited_by_count',0)}",
                      "papers": []})
            logger.info(f"Progressive fallback: +{len(profs)} professors with just '{primary_field}'")
        except Exception as e:
            logger.warning(f"Progressive fallback search: {e}")

    # ── مرتب‌سازی بر اساس keyword relevance ────────────────────
    if keywords and profs:
        # IDF واقعی روی متن مقالات همه‌ی کاندیدهای همین batch — به‌جای
        # فرکانس خام کلمه، شباهت معنایی حالا کلمات عمومی رشته (که توی
        # اکثر کاندیدها تکرار شدن) رو کم‌وزن، و کلمات واقعاً تمایزدهنده
        # رو پراهمیت می‌کنه. کاملاً deterministic و بدون هیچ درخواست
        # شبکه‌ی اضافه (فقط روی papersـی که همین الان جمع کردیم). اگه
        # batch خیلی کوچیک/بدون متن مقاله باشه (مثلاً همه از Web
        # Discovery‌ان که papers خالی دارن)، _build_idf دیکشنری خالی
        # برمی‌گردونه و compute_research_similarity خودکار به TF خام
        # fallback می‌کنه — یعنی رتبه‌بندی هیچ‌وقت crash یا NaN نمی‌ده.
        idf = _build_idf([" ".join(p.get("papers", []) or []) for p in profs])

        # ── لاگ تشخیصی «قبل/بعد» ─────────────────────────────────
        # چون امکان تست زنده با API واقعی OpenAlex/Semantic Scholar از
        # محیط توسعه در دسترس نیست، این‌جا رتبه‌بندی «نسخه‌ی قبلی» (TF
        # خام، بدون idf) رو هم فقط برای مقایسه محاسبه می‌کنیم — روی
        # همون داده‌ی واقعی که همین الان از منابع زنده جمع شده، بدون
        # هیچ درخواست شبکه‌ی اضافه (فقط یه محاسبه‌ی ریاضی سبک‌تر از خودِ
        # مرتب‌سازی اصلیه). فقط وقتی top-5 واقعاً فرق کرده لاگ می‌شه، تا
        # لاگ‌های عادی رو شلوغ نکنه. با یه جستجوی واقعی توی تلگرام، این
        # خط دقیقاً همون مقایسه‌ای رو که خواستید نشون می‌ده — روی داده‌ی
        # زنده‌ی خودتون، نه داده‌ی فرضی.
        try:
            old_top5 = sorted(
                profs, key=lambda p: _score_professor_relevance(p, keywords, client or {}),
                reverse=True)[:5]
            new_top5 = sorted(
                profs, key=lambda p: _score_professor_relevance(p, keywords, client or {}, idf=idf),
                reverse=True)[:5]
            old_names = [p.get("name") or p.get("url") or "?" for p in old_top5]
            new_names = [p.get("name") or p.get("url") or "?" for p in new_top5]
            if old_names != new_names:
                logger.info(
                    "🔬 TF-IDF رتبه‌ی top-5 رو نسبت به TF خام عوض کرد:\n"
                    f"  قبلی (TF خام) : {old_names}\n"
                    f"  جدید (TF-IDF): {new_names}")
        except Exception as e:
            # این فقط یه لاگ تشخیصیه — هیچ‌وقت نباید جستجوی واقعی رو
            # به‌خاطرش خراب کنه.
            logger.debug(f"TF-IDF diagnostic comparison skipped: {e}")

        # هر پروفسور رو با امتیاز واقعی (۰ تا ۱۰۰) تگ می‌کنیم — دقیقاً همون
        # چیزی که University Discovery Mode با _rank_score انجام می‌ده. این
        # تگ هم برای نمایش «🎯 تطابق: N%» به کاربر استفاده می‌شه، هم برای
        # این‌که classify_prof_search_result بتونه Exact را از Related
        # تشخیص بده (بدون این تگ، همه‌چیز محافظه‌کارانه Exact فرض می‌شه).
        scored = [(p, _score_professor_relevance(p, keywords, client or {}, idf=idf)) for p in profs]
        scored.sort(key=lambda t: t[1], reverse=True)
        profs = [p for p, _ in scored]
        for p, s in scored:
            p["_rank_score"] = round(s * 100)
        logger.info(f"Sorted {len(profs)} professors — top: {profs[0].get('name','?')} "
                    f"({profs[0].get('_rank_score')}%)")

    # ── Crossref enrichment فقط برای top-10 (نه همه — سرعت) ─────
    # توی حالت «تلاش مجدد سریع» (skip_progressive_fallback=True) این
    # مرحله کلاً رد می‌شه — چون یه حلقه‌ی سری از تا ۱۰ درخواست جداست
    # (هرکدوم تا ۱۰ ثانیه)، یعنی می‌تونه تا ۱۰۰ ثانیه به یه تلاشی که قرار
    # بود سریع باشه اضافه کنه؛ دقیقاً همون «هنگ کردن» که هدف این تغییرات
    # حذفشه.
    if not skip_progressive_fallback:
        for p in profs[:10]:
            if not p.get("papers") and p.get("name") and not p["openalex_id"].startswith("CR_"):
                try:
                    cr_papers = _fetch_crossref_papers(p["name"], limit=2)
                    if cr_papers:
                        p["papers"] = cr_papers
                except Exception:
                    pass  # enrichment اختیاریه — شکستش loop رو متوقف نمی‌کنه

    logger.info(f"search_professors done: {len(profs)} results for '{field}' in '{country}'")

    # ── ثبت metrics — کاملاً جانبی، هیچ‌وقت روی نتیجه یا سرعت جستجو اثر
    # نداره: فقط یک INSERT سریع SQLite (میکروثانیه) بعد از اینکه نتیجه
    # کامل آماده شده، و حتی اون هم try/except شده (defense in depth
    # روی خود db_log_search_metrics که خودش هم try/except داره). ──
    try:
        db_log_search_metrics(
            field=field, country=country,
            count_requested=count, count_returned=len(profs),
            elapsed_sec=time.time() - _search_t0,
            fallback_used=fallback_used,
            source_counts={name: len(results_by_source.get(name, [])) for name in active_sources},
        )
    except Exception as e:
        logger.debug(f"search metrics logging skipped: {e}")

    return profs[:count]

# ================================================================
# JOB SEARCH — real-time (فقط ۴۸ ساعت اخیر)
# ================================================================

def search_jobs_realtime(field: str, country: str, count: int = 50,
                          progress: dict | None = None) -> list[dict]:
    """Wrapper با Search Cache (TTL کوتاه‌تر از استاد، چون آگهی شغل زودتر
    منقضی/پر می‌شه): اگه همین (field, country) توی بازه‌ی
    JOB_SEARCH_CACHE_TTL_SEC (پیش‌فرض ۳۰ دقیقه) قبلاً جستجو شده، بدون
    صدا زدن دوباره‌ی همه‌ی منابع شغل، همون نتیجه‌ی کش‌شده برگردونده می‌شه.

    progress: دقیقاً مثل search_professors — دیکشنری مشترک اختیاری که
    caller می‌تونه با _live_search_progress_reporter بخونه و پیشرفت زنده
    نشون بده."""
    if progress is not None:
        progress["cache_hit"] = False
    cached = db_get_search_cache("job", field, country, ttl_sec=JOB_SEARCH_CACHE_TTL_SEC)
    if cached is not None:
        logger.info(f"search_jobs_realtime CACHE HIT | field={field!r} country={country!r} ({len(cached)} results)")
        if progress is not None:
            progress["cache_hit"] = True
            progress["stage"] = "done"
        return cached[:count]
    results = _search_jobs_realtime_impl(field, country, count, progress=progress)
    db_save_search_cache("job", field, country, results)
    return results

# ── تلاش‌های مجدد جستجوی شغل — دقیقاً همون فلسفه‌ی PROF_SOURCE_RETRY_GROUPS ──
# قبلاً دکمه‌ی «🔄 دوباره جستجو کن» عیناً همون تابع/همون آرگومان‌های تلاش
# اول رو دوباره صدا می‌زد — یعنی اگه بار اول با بازه‌ی ۴۸ ساعته و منابع
# استاندارد چیزی پیدا نشد، بار دوم هم دقیقاً همون نتیجه (خالی) رو می‌داد.
# الان هر تلاش مجدد آگاهانه یک استراتژی متفاوت داره: بازه‌ی زمانی گشادتر
# + فیلتر تطبیق شل‌تر (relaxed) — یعنی واقعاً منابع/فضای جستجوی بیشتری
# رو پوشش می‌ده، نه فقط تکرار کورکورانه‌ی همون کار.
JOB_RETRY_STRATEGIES: list[dict] = [
    {"max_age_hours": 24 * 4,  "relaxed": False},  # تلاش ۱: بازه رو به ۴ روز گشاد کن
    {"max_age_hours": 24 * 14, "relaxed": False},  # تلاش ۲: بازه رو به ۲ هفته گشاد کن
    {"max_age_hours": 24 * 30, "relaxed": True},   # تلاش ۳: یک ماه + فیلتر تطبیق رو شل کن
]

def search_jobs_retry_group(field: str, country: str, count: int, attempt_idx: int,
                             progress: dict | None = None) -> list[dict]:
    """صدا زده می‌شه وقتی کاربر دکمه‌ی «🔁 تلاش مجدد» رو می‌زنه (بعد از
    این‌که جستجوی کامل اول ۰ نتیجه داد). عمداً از کش رد می‌شه (چون نتیجه‌ی
    خالی اصلاً کش نمی‌شه) و مستقیم _search_jobs_realtime_impl رو با یک
    استراتژی گسترده‌تر (بازه‌ی زمانی بازتر و/یا فیلتر تطبیق شل‌تر) صدا
    می‌زنه — هر بار که کاربر دوباره «تلاش مجدد» بزنه، یک پله عمیق‌تر
    می‌ره، نه این‌که کورکورانه همون جستجوی قبلی رو تکرار کنه."""
    strat = JOB_RETRY_STRATEGIES[min(attempt_idx, len(JOB_RETRY_STRATEGIES) - 1)]
    logger.info(f"search_jobs_retry_group: attempt={attempt_idx} strategy={strat}")
    results = _search_jobs_realtime_impl(field, country, count, progress=progress,
                                          max_age_hours=strat["max_age_hours"],
                                          relaxed=strat["relaxed"])
    if results:
        db_save_search_cache("job", field, country, results)
    return results

# ================================================================
# COMPANY DISCOVERY MODE — تلاش ۴ برای کار، دقیقاً معادل
# University Discovery Mode برای اساتید (خط ~۴۳۱۶). قبلاً «تلاش مجدد»
# فقط همون ۱۰ منبع board/API رو با بازه‌ی زمانی بازتر دوباره صدا می‌زد؛
# اگه مشکل از خودِ آن منابع بود (رشته/کشور غیر-Remote که board‌های
# aggregator اصلاً پوششش نمی‌دن)، هیچ تلاش مجددی هیچ‌وقت چیزی پیدا
# نمی‌کرد. این مرحله کاملاً مستقل عمل می‌کنه: به‌جای board، مستقیم
# شرکت‌هایی که واقعاً این روزها توی این رشته/کشور استخدام می‌کنن رو
# کشف و صفحه‌ی careers خودشون رو می‌خونه — دقیقاً همون الگوی
# «دانشگاه → دانشکده → آزمایشگاه → استخراج استاد».
# ================================================================

def _company_discovery(field: str, country_q: str, limit: int = 8) -> list[dict]:
    """مرحله‌ی ۱ — به‌جای یک لیست ثابت از شرکت‌های «معروف»، شرکت‌هایی که
    طبق نتایج جستجوی وب واقعاً این روزها توی این رشته/کشور در حال
    استخدامن رو پیدا می‌کنه. خروجی: [{"name":..., "domain":...}]"""
    out: list[dict] = []
    if _ddg_breaker_is_open():
        return out
    country_l = (country_q or "").strip().lower()
    is_generic_country = country_l in ("", "remote", "all", "any", "همه", "*", "anywhere")
    country_part = f" {country_q}" if not is_generic_country else ""
    # ⚠️ فیکس درخواستی: قبلاً همیشه فقط ۲ کوئری با max_results=10 هرکدوم
    # زده می‌شد (سقف واقعی ~۲۰ کاندید خام)، یعنی حتی اگه limit بالاتر
    # می‌رفت (مثلاً ۲۵)، این تابع ساختاراً نمی‌تونست بیشتر از ۲۰ شرکت
    # پیدا کنه. الان تعداد کوئری و max_results هرکدوم متناسب با limit
    # بزرگ می‌شه.
    queries = [
        f'"{field}" companies hiring{country_part} careers',
        f'top companies "{field}"{country_part} careers page 2026',
        f'"{field}" jobs open positions{country_part} apply',
    ]
    per_query_results = max(10, (limit * 3) // len(queries) + 5)
    candidates: list[tuple[str, str, str]] = []  # (title, url, snippet)
    seen_urls: set[str] = set()
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=SEARCH_TIMEOUT_SEC) as ddgs:
            for q in queries:
                if len(candidates) >= limit * 3:
                    break
                try:
                    for r in ddgs.text(q, max_results=per_query_results):
                        url, title, body = r.get("href", ""), r.get("title", ""), r.get("body", "")
                        if url and title and url not in seen_urls:
                            seen_urls.add(url)
                            candidates.append((title, url, body))
                except Exception as e:
                    logger.debug(f"company discovery ddg '{q[:60]}': {e}")
        _ddg_breaker_record(success=True)
    except Exception as e:
        _ddg_breaker_record(success=False, hard_error=_is_hard_network_error(e))
        logger.debug(f"company discovery DDGS init: {e}")
        return out

    _blocked = ("wikipedia.org", "youtube.com", "reddit.com", "quora.com", "facebook.com",
                "instagram.com", "linkedin.com", "indeed.com", "glassdoor.com")
    candidates = [c for c in candidates if not any(b in c[1] for b in _blocked)]
    if not candidates:
        return out

    listing_block = "\n".join(
        f"[{i}] TITLE: {t}\nURL: {u}\nSNIPPET: {b[:200]}" for i, (t, u, b) in enumerate(candidates[:limit * 3])
    )
    try:
        prompt = f"""Below are web search results about companies that hire for the field "{field}". For
each numbered result that clearly names a REAL company (NOT a job board, listicle site, blog, or news
aggregator), extract the company name and, if visible in the URL or snippet, its careers-page domain
(just the bare domain, e.g. "acme.com"). Never invent a domain — leave it as an empty string if it is not
clearly present.
{listing_block}
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"companies": [{{"index": 0, "name": "Company Name", "domain": "domain.com or empty string"}}, ...]}}
Never include an index that is not listed above. Skip duplicates (same company mentioned twice)."""
        ai = run_ai_sync(call_ai(prompt, min_length=15))
        data = _extract_json_object(ai) or {}
        seen_names: set[str] = set()
        for item in (data.get("companies") or [])[:limit * 2]:
            idx = item.get("index")
            if not isinstance(idx, int) or not (0 <= idx < len(candidates)):
                continue
            name = str(item.get("name", "")).strip()
            if not name or name.lower() in seen_names:
                continue
            seen_names.add(name.lower())
            domain = str(item.get("domain", "")).strip().lower()
            if not domain:
                try:
                    from urllib.parse import urlparse
                    domain = urlparse(candidates[idx][1]).netloc.replace("www.", "")
                except Exception:
                    domain = ""
            out.append({"name": name, "domain": domain})
            if len(out) >= limit:
                break
        logger.info(f"Company Discovery: {len(out)} شرکت کاندید برای '{field}' در '{country_q}'")
    except Exception as e:
        logger.debug(f"company discovery AI extraction: {e}")
    return out

def _crawl_company_careers(company: dict, field: str, keywords: list[str]) -> str:
    """مرحله‌ی ۲ — صفحه‌ی careers/jobs همین شرکت رو با site: (وقتی دامنه
    پیدا شده) یا اسم دقیق شرکت پیدا و متنش رو برمی‌گردونه — دقیقاً همون
    مکانیزم _crawl_university_stage."""
    domain = company.get("domain", "")
    name = company.get("name", "")
    site_filter = f" site:{domain}" if domain else f' "{name}"'
    q_terms = " ".join(keywords[:3]) or field
    q = f'careers OR jobs OR "open positions" "{q_terms}"{site_filter}'
    if _ddg_breaker_is_open():
        return ""
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=SEARCH_TIMEOUT_SEC) as ddgs:
            results = list(ddgs.text(q.strip(), max_results=3))
        _ddg_breaker_record(success=True)
    except Exception as e:
        _ddg_breaker_record(success=False, hard_error=_is_hard_network_error(e))
        logger.debug(f"company careers crawl ddg '{q[:60]}': {e}")
        return ""
    texts: list[str] = []
    for r in results:
        url = r.get("href", "")
        if not url:
            continue
        try:
            resp = _safe_requests_get(url, headers=HEADERS, timeout=PAGE_FETCH_TIMEOUT_SEC)
            sig = _extract_page_signals(resp.text)
            if sig.get("page_text"):
                texts.append(sig["page_text"])
        except Exception:
            continue
    return "\n---\n".join(texts)[:6000]

def _job_extraction(page_text: str, field: str, company: dict, needed: int = 6) -> list[dict]:
    """مرحله‌ی ۳ — از متن خام صفحه‌ی careers همین شرکت، آگهی‌های شغلی
    واقعی رو با AI استخراج می‌کنه — دقیقاً همون منطق قابل‌اعتماد
    _professor_extraction: هیچ عنوان/ایمیلی که توی متن نباشه ساخته نمی‌شه."""
    out: list[dict] = []
    if not page_text:
        return out
    try:
        prompt = f"""Below is raw scraped text from {company.get('name') or 'a company'}'s careers/jobs
page. Extract real, currently open job postings that relate to this field: "{field}". Never invent
titles; only list ones that actually appear in the text below. If a direct application email is visible
as literal text near a listing, include it; otherwise leave it as an empty string. NEVER invent or guess
an email that doesn't literally appear in the text.
Raw text:
{page_text[:6000]}
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"jobs": [{{"title": "Job Title", "email": "email@company.com or empty string"}}, ...]}}
(max {needed} entries, only real-looking job titles — never navigation labels or generic section names)"""
        ai = run_ai_sync(call_ai(prompt, min_length=15))
        data = _extract_json_object(ai) or {}
        for j in (data.get("jobs") or [])[:needed]:
            title = str(j.get("title", "")).strip()
            email = str(j.get("email", "")).strip()
            if email and (not _EMAIL_FORMAT_RE.match(email) or email.lower() not in page_text.lower()):
                email = ""
            if title:
                out.append({"title": title, "email": email})
    except Exception as e:
        logger.debug(f"Job Extraction: {e}")
    return out

def _process_one_company_discovery(comp: dict, field: str, keywords: list[str],
                                    country_q: str) -> list[dict]:
    """پردازش کامل یک شرکت — جدا شد تا هر شرکت مستقل و موازی (توی
    ThreadPoolExecutor) پردازش بشه، دقیقاً معادل _process_one_university_discovery.

    مرحله‌ی ۱ (ترجیحی): fetch_jobs_via_known_ats — اگه این شرکت روی
    Greenhouse/Lever/Ashby/SmartRecruiters/Workday باشه، مستقیم از API
    واقعی همون ATS همه‌ی آگهی‌هاش رو می‌گیریم (نه یک صفحه‌ی HTML که ممکنه
    JS-heavy/Cloudflare/CAPTCHA/SPA باشه).
    مرحله‌ی ۲ (GenericCareerAdapter، فقط اگه مرحله‌ی ۱ چیزی نداد): همون
    کراول صفحه‌ی careers + AI extraction قدیمی — برای شرکت‌هایی که روی
    هیچ‌کدوم از ATSهای شناخته‌شده نیستن یا صفحه‌ی careers دستی‌شون دارن."""
    ats_jobs = fetch_jobs_via_known_ats(comp, field, keywords, needed=COMPANY_DISCOVERY_NEEDED)
    if ats_jobs:
        out = []
        for j in ats_jobs:
            j = dict(j)
            if not j.get("location") or j["location"] == "Unspecified":
                j["location"] = country_q or "Unspecified"
            out.append(j)
        return out

    # ── GenericCareerAdapter (fallback) ──────────────────────────
    page_text = _crawl_company_careers(comp, field, keywords)
    if not page_text:
        return []
    listings = _job_extraction(page_text, field, comp, needed=COMPANY_DISCOVERY_NEEDED)
    out = []
    for j in listings:
        out.append({
            "title": j["title"], "company": comp.get("name", ""),
            "location": country_q or "Unspecified",
            "url": f"https://{comp['domain']}" if comp.get("domain") else "",
            "created": "", "source": "CompanyDiscovery(Generic)",
            "email": j.get("email", ""),
            "description": "Found via Company Discovery Mode (careers page crawl, generic fallback)",
            "tags": keywords[:5],
        })
    return out

def company_discovery_mode_search(field: str, country: str, count: int,
                                   client: dict | None = None,
                                   progress: dict | None = None) -> list[dict]:
    """اجرای کامل Company Discovery Mode:
        Company Discovery → Careers Page Crawl → Job Extraction
    دقیقاً معادل university_discovery_mode_search برای اساتید — به‌جای
    وابستگی به بردهای Remote-محور، مستقیم شرکت‌هایی که این روزها توی این
    رشته/کشور استخدام می‌کنن رو پیدا و صفحه‌ی careers خودشون رو می‌خونه،
    یعنی برای رشته‌های غیرفنی/موقعیت‌های محلی هم واقعاً چیزی پیدا می‌کنه.

    ⚠️ فیکس درخواستی: قبلاً فقط ۸ شرکت (سری، یکی‌یکی) کراول می‌شد. الان
    COMPANY_DISCOVERY_LIMIT=۲۵ شرکت *موازی* (نه سری) کراول می‌شن، با
    همون _run_futures_with_extension (پنجره‌ی تمدید اگه اکثر شرکت‌ها
    هنوز genuinely در حال کراول‌شدن باشن) و progress زنده."""
    if client:
        _, keywords = _build_smart_query(client)
    else:
        keywords = field.lower().split()
    country_l = (country or "").strip().lower()
    country_q = "" if country_l in ("", "همه", "all", "*", "anywhere") else country

    companies = _company_discovery(field, country_q, limit=COMPANY_DISCOVERY_LIMIT)
    if not companies:
        if progress is not None:
            progress["stage"] = "done"
        return []

    if progress is not None:
        progress["stage"] = "searching"
        progress["total"] = len(companies)
        progress["done"] = 0
        progress["current"] = ""
        progress["source_labels"] = {}

    all_jobs: list[dict] = []
    seen_keys: set = set()
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(companies), DISCOVERY_CRAWL_WORKERS))
    try:
        futs = {
            pool.submit(_process_one_company_discovery, comp, field, keywords, country_q): comp.get("name", "?")
            for comp in companies
        }
        results_by_company = _run_futures_with_extension(
            futs, DISCOVERY_CRAWL_POOL_TIMEOUT, progress=progress,
            label="company_discovery_mode_search (crawl)")
    finally:
        pool.shutdown(wait=False)

    for comp_name, listings in results_by_company.items():
        for j in (listings or []):
            key = (j["title"].lower(), j.get("company", "").lower())
            if key in seen_keys:
                continue
            seen_keys.add(key)
            all_jobs.append(j)

    if progress is not None:
        progress["stage"] = "done"

    # سقف خروجی نهایی: هم DISCOVERY_TOP_N (که برای اساتید ۱۰۰ه) هم count
    # (که از EMAIL_QUOTA/JOB_QUOTA بالاتره) رعایت می‌شه — هرکدوم کوچیک‌تر بود.
    out_cap = min(count, DISCOVERY_TOP_N) if count else DISCOVERY_TOP_N
    all_jobs = all_jobs[:out_cap]

    logger.info(f"Company Discovery Mode done: {len(companies)} شرکت، "
                f"{len(all_jobs)} فرصت برای '{field}' در '{country}'")
    if all_jobs:
        db_save_search_cache("job", field, country, all_jobs)
    return all_jobs

# ================================================================
# BROAD SYNONYM SEARCH — تلاش ۵ برای کار، دقیقاً معادل
# orcid_and_broadweb_search برای اساتید. مشکل واقعی: فیلتر تطبیق فعلی
# (تابع _matches توی _search_jobs_realtime_impl) فقط کلمه‌به‌کلمه‌ی خودِ
# رشته‌ای که کاربر نوشته رو با عنوان آگهی مقایسه می‌کنه — یعنی کاربری که
# «Data Scientist» نوشته هیچ‌وقت آگهی‌های «ML Engineer»/«AI Engineer»/
# «Applied Scientist» رو نمی‌بینه، چون این کلمات توی رشته‌ی خودش نیستن.
# این مرحله از AI عنوان‌های شغلی مترادف/نزدیک رو می‌گیره و هر کدوم رو
# جدا جستجو می‌کنه — آخرین و گسترده‌ترین لایه.
# ================================================================

def _generate_job_title_synonyms(field: str) -> list[str]:
    """معادل _build_smart_query برای استاد: عنوان‌های شغلی مرتبط/مترادف
    رو از AI می‌گیره تا جستجوی مرحله‌ی بعد محدود به عین کلمه‌ی field نمونه."""
    try:
        prompt = f"""Field/job title: "{field}"
List up to 5 closely related or synonymous job titles a candidate in this field could also realistically
search and apply for (e.g. for "Data Scientist": "Machine Learning Engineer", "Data Analyst", "AI
Engineer", "Applied Scientist", "Research Scientist"). Only real, commonly-used job titles — never invent
niche/fake titles.
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"titles": ["Title 1", "Title 2", ...]}}"""
        ai = run_ai_sync(call_ai(prompt, min_length=10))
        data = _extract_json_object(ai) or {}
        titles = [str(t).strip() for t in (data.get("titles") or []) if str(t).strip()]
        return titles[:5]
    except Exception as e:
        logger.debug(f"job title synonym generation: {e}")
        return []

def job_src_broad_web(field: str, country_q: str, synonyms: list[str], count: int) -> list[dict]:
    """جستجوی وب گسترده روی خودِ field + هر عنوان مترادف جداگانه —
    برخلاف _discover_jobs_via_web_search (که فقط خودِ field رو سرچ
    می‌کنه)، این‌جا هر مترادف یک query جداست، پس آگهی‌هایی که با کلمه‌ی
    دقیق field تطبیق نداشتن ولی واقعاً همون شغل‌ان هم دیده می‌شن."""
    out: list[dict] = []
    if _ddg_breaker_is_open():
        return out
    country_l = (country_q or "").strip().lower()
    is_generic_country = country_l in ("", "remote", "all", "any", "همه", "*", "anywhere")
    country_part = f" {country_q}" if not is_generic_country else ""
    terms = ([field] + synonyms)[:4]
    queries = [f'"{t}" jobs{country_part} apply now' for t in terms]

    candidates: list[tuple[str, str, str]] = []
    seen_urls: set[str] = set()
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=SEARCH_TIMEOUT_SEC) as ddgs:
            for q in queries:
                if len(candidates) >= max(count * 2, 15):
                    break
                try:
                    for r in ddgs.text(q, max_results=8):
                        url, title, body = r.get("href", ""), r.get("title", ""), r.get("body", "")
                        if url and title and url not in seen_urls:
                            seen_urls.add(url)
                            candidates.append((title, url, body))
                except Exception as e:
                    logger.debug(f"broad synonym job search ddg '{q[:60]}': {e}")
        _ddg_breaker_record(success=True)
    except Exception as e:
        _ddg_breaker_record(success=False, hard_error=_is_hard_network_error(e))
        logger.debug(f"broad synonym job search DDGS init: {e}")
        return out

    _blocked = ("wikipedia.org", "youtube.com", "reddit.com", "quora.com", "facebook.com", "instagram.com")
    candidates = [c for c in candidates if not any(b in c[1] for b in _blocked)]
    if not candidates:
        return out

    listing_block = "\n".join(
        f"[{i}] TITLE: {t}\nSNIPPET: {b[:200]}" for i, (t, u, b) in enumerate(candidates[:20])
    )
    try:
        prompt = f"""Below are web search results for job postings related to "{field}" (including these
synonymous titles: {', '.join(terms)}). For each numbered result that genuinely looks like a real,
currently-open job posting (NOT a blog post, wiki page, forum thread, or general career-advice article),
extract a clean job title and the company name. Never invent a company name — if it is not clearly
stated, use an empty string. Never include an index that is not listed below.
{listing_block}
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"jobs": [{{"index": 0, "title": "Job Title", "company": "Company or empty string"}}, ...]}}"""
        ai = run_ai_sync(call_ai(prompt, min_length=15))
        data = _extract_json_object(ai) or {}
        for item in (data.get("jobs") or [])[:count]:
            idx = item.get("index")
            if not isinstance(idx, int) or not (0 <= idx < len(candidates)):
                continue
            title_orig, url, snippet = candidates[idx]
            out.append({
                "title":       str(item.get("title") or title_orig).strip(),
                "company":     str(item.get("company", "")).strip(),
                "location":    country_q if not is_generic_country else "Remote/Unspecified",
                "url":         url,
                "created":     "",
                "source":      "BroadSynonymSearch",
                "email":       "",
                "description": snippet[:400],
                "tags":        terms,
            })
    except Exception as e:
        logger.debug(f"broad synonym job search AI extraction: {e}")
    return out

def broad_synonym_job_search(field: str, country: str, count: int, client: dict | None = None) -> list[dict]:
    """تلاش ۵ — تولید عنوان‌های مترادف + جستجوی وب گسترده روی همه‌شون،
    دقیقاً معادل orcid_and_broadweb_search برای اساتید: آخرین و
    گسترده‌ترین لایه، بدون محدودیت به منابع/board‌های از‌قبل‌استفاده‌شده."""
    country_l = (country or "").strip().lower()
    country_q = "" if country_l in ("", "همه", "all", "*", "anywhere") else country
    synonyms = _generate_job_title_synonyms(field)
    jobs = job_src_broad_web(field, country_q, synonyms, count)

    seen_keys: set = set()
    deduped: list[dict] = []
    for j in jobs:
        key = j.get("url") or (j.get("title", ""), j.get("company", ""))
        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(j)

    logger.info(f"broad_synonym_job_search done: {len(deduped)} candidates for '{field}' "
                f"(synonyms: {synonyms}) in '{country}'")
    if deduped:
        db_save_search_cache("job", field, country, deduped)
    return deduped

TOTAL_JOB_RETRY_ATTEMPTS = 5  # همون معادل TOTAL_PROF_RETRY_ATTEMPTS برای کار

def run_job_retry_attempt(field: str, country: str, count: int, client: dict | None,
                           attempt_idx: int, progress: dict | None = None) -> dict:
    """لایه‌ی یکپارچه‌ی «تلاش مجدد» برای کار — دقیقاً معادل
    run_prof_retry_attempt (استاد):

        Retry 1 → Retry 2 → Retry 3 → Company Discovery Mode
        → عنوان‌های مترادف + جستجوی وب گسترده

    attempt_idx صفرشمار (اولین کلیک روی «🔁 تلاش مجدد» = ۰).
      • attempt_idx 0..2  → JOB_RETRY_STRATEGIES (سریع، همون ۱۰ منبع API/برد با
                             بازه‌ی زمانی بازتر/فیلتر شل‌تر — تلاش ۱،۲،۳)
      • attempt_idx 3     → Company Discovery Mode (تلاش ۴؛ کندتر، دقیق‌تر —
                             کراول مستقیم صفحه‌ی careers شرکت‌هایی که واقعاً
                             توی این رشته/کشور استخدام می‌کنن)
      • attempt_idx >= 4  → عنوان‌های شغلی مترادف + جستجوی وب گسترده (تلاش ۵؛
                             گسترده‌ترین و آخرین لایه — روی کلیک‌های بعدی هم
                             همینو دوباره تازه اجرا می‌کنه)

    خروجی: {"jobs": [...], "mode": "retry"|"company_discovery"|"broad_synonym",
             "label": "متن فارسی مرحله برای پیام به کاربر"}"""
    if attempt_idx < len(JOB_RETRY_STRATEGIES):
        jobs = search_jobs_retry_group(field, country, count, attempt_idx, progress)
        return {"jobs": jobs, "mode": "retry",
                "label": f"روش {attempt_idx + 1} از {TOTAL_JOB_RETRY_ATTEMPTS}"}
    if attempt_idx == len(JOB_RETRY_STRATEGIES):
        jobs = company_discovery_mode_search(field, country, count, client, progress=progress)
        return {"jobs": jobs, "mode": "company_discovery",
                "label": f"روش {attempt_idx + 1} از {TOTAL_JOB_RETRY_ATTEMPTS} — "
                         f"🏢 Company Discovery Mode (شرکت → صفحه‌ی careers → استخراج آگهی)"}
    jobs = broad_synonym_job_search(field, country, count, client)
    if progress is not None:
        progress["stage"] = "done"
    return {"jobs": jobs, "mode": "broad_synonym",
            "label": f"روش {min(attempt_idx + 1, TOTAL_JOB_RETRY_ATTEMPTS)} از {TOTAL_JOB_RETRY_ATTEMPTS} — "
                     f"🌐 عنوان‌های شغلی مترادف + جستجوی وب گسترده"}

def _discover_jobs_via_web_search(field: str, country: str, keywords: list[str], needed: int) -> list[dict]:
    """منبع اضافه‌ی جستجوی شغل، با همون فلسفه‌ی orchestrator که برای استاد
    پیاده شد: ۷ منبع بالا همه‌شون API-based و اساساً روی «مشاغل Remote/فنی»
    تمرکز دارن (Jobicy/Remotive/Himalayas/WeWorkRemotely/RemoteOK/Arbeitnow)
    — یعنی برای رشته‌های غیرفنی یا موقعیت‌های محلی/غیر-Remote عملاً چیزی
    گیر نمی‌آد، دقیقاً همون مشکلی که خودِ این فایل توی کامنت فیلتر کشور
    (پایین‌تر) به‌ش اشاره کرده. این تابع مستقل از اون ۷ API عمل می‌کنه:
    مستقیم روی نتایج جستجوی وب (DDG) کار می‌کنه.

    نکته‌ی امنیتی/دقتی مهم: برخلاف AI که فقط عنوان شغل/شرکت رو از
    عنوان+خلاصه‌ی نتیجه استخراج/تمیز می‌کنه، لینک (url) هرگز از AI نمیاد —
    همیشه دقیقاً همون چیزیه که DDG برگردونده. یعنی حتی اگه AI جای شرکت رو
    اشتباه بگیره، کاربر همیشه به یک لینک واقعی و موجود می‌رسه، نه یک URL
    جعلی. مثل بقیه‌ی pipeline: هر خطا (DDG قطع، breaker باز، AI جواب نده)
    فقط یعنی این منبع چیزی اضافه نمی‌کنه — هیچ‌وقت کل جستجو رو نمی‌شکنه."""
    out: list[dict] = []
    if needed <= 0 or _ddg_breaker_is_open():
        return out
    q_terms = " ".join(keywords[:3]) or field
    country_l = (country or "").strip().lower()
    is_generic_country = country_l in ("", "remote", "all", "any", "همه", "*", "anywhere")
    country_part = f" {country}" if not is_generic_country else ""
    queries = [
        f'"{q_terms}" jobs{country_part} apply now',
        f'"{q_terms}" careers{country_part} hiring',
        f'"{q_terms}" job opening{country_part}',
    ]

    candidates: list[tuple[str, str, str]] = []  # (title, url, snippet)
    seen_urls: set[str] = set()
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=SEARCH_TIMEOUT_SEC) as ddgs:
            for q in queries:
                if len(candidates) >= max(needed * 2, 10):
                    break
                try:
                    for r in ddgs.text(q, max_results=8):
                        url, title, body = r.get("href", ""), r.get("title", ""), r.get("body", "")
                        if url and title and url not in seen_urls:
                            seen_urls.add(url)
                            candidates.append((title, url, body))
                except Exception as e:
                    logger.debug(f"job web discovery ddg '{q[:60]}': {e}")
        _ddg_breaker_record(success=True)
    except Exception as e:
        _ddg_breaker_record(success=False, hard_error=_is_hard_network_error(e))
        logger.debug(f"job web discovery DDGS init: {e}")
        return out

    # حذف نتایجی که مشخصاً آگهی شغل نیستن (ویکی/فروم/شبکه‌ی اجتماعی و ...)
    _blocked = ("wikipedia.org", "youtube.com", "reddit.com", "quora.com", "facebook.com", "instagram.com")
    candidates = [c for c in candidates if not any(b in c[1] for b in _blocked)]
    if not candidates:
        return out

    listing_block = "\n".join(
        f"[{i}] TITLE: {t}\nSNIPPET: {b[:200]}" for i, (t, u, b) in enumerate(candidates[:15])
    )
    try:
        prompt = f"""Below are web search results for job postings in the field "{field}". For each numbered
result that genuinely looks like a real, currently-open job posting (NOT a blog post, wiki page, forum
thread, salary-comparison article, or general "how to become X" guide), extract a clean job title and the
company name. Never invent a company name — if it is not clearly stated in the title/snippet, use an empty
string. Never include an index that is not listed below.
{listing_block}
Respond with STRICT JSON only, nothing else, exactly this shape:
{{"jobs": [{{"index": 0, "title": "Job Title", "company": "Company or empty string"}}, ...]}}"""
        ai = run_ai_sync(call_ai(prompt, min_length=15))
        data = _extract_json_object(ai) or {}
        for item in (data.get("jobs") or [])[:needed]:
            idx = item.get("index")
            if not isinstance(idx, int) or not (0 <= idx < len(candidates)):
                continue
            title_orig, url, snippet = candidates[idx]
            out.append({
                "title":       str(item.get("title") or title_orig).strip(),
                "company":     str(item.get("company", "")).strip(),
                "location":    country if not is_generic_country else "Remote/Unspecified",
                "url":         url,  # همیشه مستقیم از DDG — هرگز از AI
                "created":     "",
                "source":      "WebSearch",
                "email":       "",
                "description": snippet[:400],
                "tags":        keywords[:5],
            })
    except Exception as e:
        logger.debug(f"job web discovery AI extraction: {e}")
    return out

# ================================================================
# ATS ADAPTERS — «شکنندگی» اصلی crawler عمومی این بود که به ساختار
# HTML/JS هر سایت وابسته است: اگر سایت JS-heavy/Cloudflare/CAPTCHA/
# robots-restricted/SPA/API-backed باشد، crawler ممکن است هیچ‌چیز
# نبیند و AI فقط با ۶۰۰۰ کاراکتر اول متن صفحه کار می‌کند.
#
# راه‌حل: به‌جای یک crawler عمومی، برای ATSهای پرکاربرد یک Adapter
# مجزا داریم که مستقیماً به JSON API عمومیِ خودِ همان ATS وصل می‌شود —
# همان API که خودِ شرکت‌ها برای امبد کردن آگهی‌ها روی careers page
# استفاده می‌کنند. این یعنی: بدون رندر JS، بدون Cloudflare/CAPTCHA
# (چون endpoint داده‌محور است نه صفحه‌ی مرورگر)، ساختار پاسخ همیشه
# ثابت و مستند است، و همه‌ی آگهی‌های شرکت یک‌جا برمی‌گردد (نه فقط یکی).
#
#   GreenhouseAdapter, LeverAdapter, AshbyAdapter, SmartRecruitersAdapter,
#   WorkdayAdapter  → هر کدام یک تابع _ats_fetch_* مستقل
#   GenericCareerAdapter → همان crawl+AI-extraction قدیمی (_crawl_company_careers
#     + _job_extraction)، فقط وقتی هیچ‌کدام از Adapterهای بالا جواب ندادند
# ================================================================

def _slugify_company(name: str) -> list[str]:
    """از اسم یا دامنه‌ی شرکت چند حدس محتمل برای 'token'ی که ATSها با آن
    شناخته می‌شوند می‌سازد. مثال: 'Acme Corp Inc' → ['acme-corp','acmecorp','acme']."""
    if not name:
        return []
    base = re.sub(r"\.(com|io|co|ai|org|net|dev)$", "", name.strip().lower())
    base = re.sub(r"[^a-z0-9\s-]", "", base)
    words = [w for w in re.split(r"[\s-]+", base) if w]
    if not words:
        return []
    generic = {"inc", "llc", "ltd", "corp", "corporation", "co", "group", "the", "gmbh"}
    words_clean = [w for w in words if w not in generic] or words
    candidates = ["-".join(words_clean), "".join(words_clean)]
    if len(words_clean) > 1:
        candidates.append(words_clean[0])
    seen: set = set()
    out = []
    for c in candidates:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


def _job_matches_field(title: str, description: str, keywords: list[str]) -> bool:
    """همون فیلتر ساده‌ی _matches بالاتر، ولی مستقل از یک closure خاص
    چون Adapterها ممکنه از توابع بیرون از _search_jobs_realtime_impl_single
    هم صدا زده بشن (مثلاً از Company Discovery Mode)."""
    if not keywords:
        return True
    hay = f"{title} {description}".lower()
    return any(k.lower() in hay for k in keywords if k and len(k) > 1)


def _ats_fetch_greenhouse(token: str, field: str, keywords: list[str], needed: int) -> list[dict]:
    """Greenhouse Job Board API — عمومی/بدون auth:
    https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"""
    out: list[dict] = []
    try:
        r = _safe_requests_get(
            f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
            params={"content": "true"}, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            return out
        data = r.json()
        for j in (data.get("jobs") or []):
            title = j.get("title", "")
            content = re.sub(r"<[^>]+>", " ", j.get("content", "") or "")
            if not title or not _job_matches_field(title, content, keywords):
                continue
            loc = (j.get("location") or {}).get("name", "")
            out.append({
                "title": title, "company": token, "location": loc or "Unspecified",
                "url": j.get("absolute_url", ""), "created": j.get("updated_at", ""),
                "source": "Greenhouse", "email": "",
                "description": re.sub(r"\s+", " ", content).strip()[:400],
                "tags": keywords[:5],
            })
            if len(out) >= needed:
                break
    except Exception as e:
        logger.debug(f"GreenhouseAdapter ({token}): {e}")
    return out


def _ats_fetch_lever(token: str, field: str, keywords: list[str], needed: int) -> list[dict]:
    """Lever Postings API — عمومی: https://api.lever.co/v0/postings/{token}?mode=json"""
    out: list[dict] = []
    try:
        r = _safe_requests_get(
            f"https://api.lever.co/v0/postings/{token}",
            params={"mode": "json"}, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            return out
        data = r.json()
        if not isinstance(data, list):
            return out
        for j in data:
            title = j.get("text", "")
            desc = re.sub(r"<[^>]+>", " ", (j.get("descriptionPlain") or j.get("description") or ""))
            if not title or not _job_matches_field(title, desc, keywords):
                continue
            categories = j.get("categories") or {}
            loc = categories.get("location", "")
            out.append({
                "title": title, "company": token, "location": loc or "Unspecified",
                "url": j.get("hostedUrl", ""), "created": "",
                "source": "Lever", "email": "",
                "description": re.sub(r"\s+", " ", desc).strip()[:400],
                "tags": keywords[:5],
            })
            if len(out) >= needed:
                break
    except Exception as e:
        logger.debug(f"LeverAdapter ({token}): {e}")
    return out


def _ats_fetch_ashby(token: str, field: str, keywords: list[str], needed: int) -> list[dict]:
    """Ashby Job Board API — عمومی: https://api.ashbyhq.com/posting-api/job-board/{token}"""
    out: list[dict] = []
    try:
        r = _safe_requests_get(
            f"https://api.ashbyhq.com/posting-api/job-board/{token}",
            params={"includeCompensation": "false"}, headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            return out
        data = r.json()
        for j in (data.get("jobs") or []):
            title = j.get("title", "")
            desc = j.get("descriptionPlain", "") or ""
            if not title or not _job_matches_field(title, desc, keywords):
                continue
            loc = j.get("location", "") or j.get("locationName", "")
            out.append({
                "title": title, "company": token, "location": loc or "Unspecified",
                "url": j.get("jobUrl") or j.get("applyUrl", ""), "created": j.get("publishedAt", ""),
                "source": "Ashby", "email": "",
                "description": re.sub(r"\s+", " ", desc).strip()[:400],
                "tags": keywords[:5],
            })
            if len(out) >= needed:
                break
    except Exception as e:
        logger.debug(f"AshbyAdapter ({token}): {e}")
    return out


def _ats_fetch_smartrecruiters(token: str, field: str, keywords: list[str], needed: int) -> list[dict]:
    """SmartRecruiters Postings API — عمومی:
    https://api.smartrecruiters.com/v1/companies/{token}/postings"""
    out: list[dict] = []
    try:
        r = _safe_requests_get(
            f"https://api.smartrecruiters.com/v1/companies/{token}/postings",
            headers=HEADERS, timeout=REQ_TIMEOUT)
        if r.status_code != 200:
            return out
        data = r.json()
        for j in (data.get("content") or []):
            title = j.get("name", "")
            if not title or not _job_matches_field(title, "", keywords):
                continue
            loc_obj = j.get("location") or {}
            loc = ", ".join(x for x in (loc_obj.get("city"), loc_obj.get("country")) if x)
            out.append({
                "title": title, "company": token, "location": loc or "Unspecified",
                "url": j.get("ref") or "", "created": j.get("releasedDate", ""),
                "source": "SmartRecruiters", "email": "", "description": "",
                "tags": keywords[:5],
            })
            if len(out) >= needed:
                break
    except Exception as e:
        logger.debug(f"SmartRecruitersAdapter ({token}): {e}")
    return out


_WORKDAY_HOSTS = ("wd1", "wd3", "wd5")
# این Adapter عمداً best-effort و کورکورانه‌ست (تا ۳ subdomain × ۲ site name
# = تا ۶ درخواست پشت‌سرهم فقط برای یک شرکت، اکثر وقت‌ها بدون هیچ hit). اگه
# با همون REQ_TIMEOUT=20s کلی که برای درخواست‌های مطمئن‌تر استفاده می‌شه
# صدا زده بشه، بدترین حالت (۶ درخواست × ۲۰ ثانیه) می‌تونه دو دقیقه فقط
# برای حدس‌زدن یک شرکت طول بکشه — دقیقاً همون کندی‌ای که Company Discovery
# Mode (تنها جایی که این Adapter صدا زده می‌شه) نباید داشته باشه. یک
# timeout کوتاه و مجزا (نه REQ_TIMEOUT) این ریسک رو محدود می‌کنه؛ چون
# best-effort است، از دست دادن یک نتیجه‌ی کند اهمیتی نداره — GenericCareerAdapter
# fallback هنوز سر جاشه.
_WORKDAY_ADAPTER_TIMEOUT_SEC = 5

def _ats_fetch_workday(tenant: str, field: str, keywords: list[str], needed: int) -> list[dict]:
    """Workday CxS API — تنانت‌محور؛ subdomain (wd1/wd3/wd5) و نام site
    (معمولاً External یا Careers) از پیش دقیقاً معلوم نیست، پس چند
    ترکیب رایج را امتحان می‌کنیم و اولین جواب موفق کافی است. برخلاف
    Greenhouse/Lever/Ashby که یک endpoint ثابت دارند، این یکی واقعاً
    best-effort است — اگر هیچ ترکیبی جواب نداد بی‌سروصدا لیست خالی
    برمی‌گرداند (هیچ‌وقت کل جستجو را کند/خراب نمی‌کند)."""
    out: list[dict] = []
    for host in _WORKDAY_HOSTS:
        for site in ("External", "Careers"):
            try:
                url = f"https://{tenant}.{host}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs"
                _assert_url_is_safe(url)  # همون چک SSRF که برای GETهای امن استفاده می‌شه
                r = requests.post(
                    url, json={"limit": 20, "offset": 0, "searchText": field},
                    headers={**HEADERS, "Content-Type": "application/json"},
                    timeout=_WORKDAY_ADAPTER_TIMEOUT_SEC)
                if r.status_code != 200:
                    continue
                data = r.json()
                postings = data.get("jobPostings") or []
                if not postings:
                    continue
                for j in postings:
                    title = j.get("title", "")
                    if not title or not _job_matches_field(title, "", keywords):
                        continue
                    out.append({
                        "title": title, "company": tenant,
                        "location": j.get("locationsText", "") or "Unspecified",
                        "url": f"https://{tenant}.{host}.myworkdayjobs.com{j.get('externalPath', '')}",
                        "created": j.get("postedOn", ""), "source": "Workday", "email": "",
                        "description": "", "tags": keywords[:5],
                    })
                    if len(out) >= needed:
                        return out
                if out:
                    return out
            except Exception as e:
                logger.debug(f"WorkdayAdapter ({tenant}/{host}/{site}): {e}")
    return out


ATS_ADAPTERS = {
    "greenhouse":      _ats_fetch_greenhouse,
    "lever":           _ats_fetch_lever,
    "ashby":           _ats_fetch_ashby,
    "smartrecruiters": _ats_fetch_smartrecruiters,
    "workday":         _ats_fetch_workday,
}


def fetch_jobs_via_known_ats(company: dict, field: str, keywords: list[str], needed: int = 8) -> list[dict]:
    """تلاش می‌کند آگهی‌های همین شرکت را مستقیماً از API واقعی یکی از
    ATSهای شناخته‌شده بگیرد — بدون هیچ وابستگی به ساختار HTML/JS سایت
    شرکت. اگر هیچ Adapter‌ای جواب نداد، لیست خالی برمی‌گرداند تا caller
    به GenericCareerAdapter (کراول + AI extraction) برگردد."""
    name = company.get("name", "")
    domain = company.get("domain", "")
    tokens: list[str] = []
    if domain:
        tokens.extend(_slugify_company(domain.split(".")[0]))
    tokens.extend(_slugify_company(name))
    seen: set = set()
    tokens = [t for t in tokens if t and not (t in seen or seen.add(t))]
    if not tokens:
        return []

    for token in tokens[:3]:  # حداکثر ۳ حدس توکن — جلوگیری از انفجار تعداد درخواست
        for adapter_name, adapter_fn in ATS_ADAPTERS.items():
            try:
                jobs = adapter_fn(token, field, keywords, needed)
            except Exception as e:
                logger.debug(f"ATS adapter {adapter_name} failed for token={token}: {e}")
                jobs = []
            if jobs:
                for j in jobs:
                    j["company"] = name or j.get("company", token)
                logger.info(f"ATS Adapter: '{name}' → {adapter_name} ({token}) → {len(jobs)} jobs")
                return jobs
    return []


def _extract_ats_token_from_url(url: str) -> tuple[str, str] | None:
    """از URL یک آگهی روی یکی از ATSهای شناخته‌شده، (نوع ATS، token شرکت)
    را استخراج می‌کند. اگر URL به هیچ‌کدام تعلق نداشت None برمی‌گرداند."""
    try:
        parts = urlsplit(url)
    except Exception:
        return None
    host = parts.netloc.lower()
    path_parts = [p for p in parts.path.split("/") if p]
    if not path_parts:
        return None
    if "boards.greenhouse.io" in host:
        return ("greenhouse", path_parts[0])
    if "jobs.lever.co" in host:
        return ("lever", path_parts[0])
    if "jobs.ashbyhq.com" in host:
        return ("ashby", path_parts[0])
    if "jobs.smartrecruiters.com" in host:
        return ("smartrecruiters", path_parts[0])
    return None


def _discover_jobs_via_ats_boards(field: str, country: str, keywords: list[str], needed: int) -> list[dict]:
    """قبلاً: از نتیجه‌ی DDG فقط لینک *یک* آگهی را می‌گرفت و عنوانش را
    از snippet با AI حدس می‌زد — یعنی برای هر شرکتی که DDG پیدا می‌کرد،
    فقط همان یک آگهی ایندکس‌شده دیده می‌شد، نه بقیه‌ی آگهی‌های واقعی آن
    شرکت. الان: از همان جستجوی DDG فقط برای پیدا کردن *توکن شرکت* روی
    هر ATS (Greenhouse/Lever/Ashby/SmartRecruiters) استفاده می‌کنیم، بعد
    مستقیم API واقعی همان ATS را صدا می‌زنیم — یعنی همه‌ی آگهی‌های مرتبط
    همان شرکت روی همان board برمی‌گردد، نه فقط یک حدس از روی متن نتیجه‌ی
    جستجو."""
    out: list[dict] = []
    if needed <= 0 or _ddg_breaker_is_open():
        return out
    q_terms = " ".join(keywords[:3]) or field
    country_l = (country or "").strip().lower()
    is_generic_country = country_l in ("", "remote", "all", "any", "همه", "*", "anywhere")
    country_part = f" {country}" if not is_generic_country else ""
    site_domains = ("boards.greenhouse.io", "jobs.lever.co", "jobs.ashbyhq.com", "jobs.smartrecruiters.com")
    queries = [f'site:{d} "{q_terms}"{country_part}' for d in site_domains]

    found_tokens: dict[str, str] = {}  # "{ats_type}:{token}" → ats_type
    try:
        from duckduckgo_search import DDGS
        with DDGS(timeout=SEARCH_TIMEOUT_SEC) as ddgs:
            for q in queries:
                if len(found_tokens) >= max(needed, 8):
                    break
                try:
                    for r in ddgs.text(q, max_results=8):
                        hit = _extract_ats_token_from_url(r.get("href", ""))
                        if not hit:
                            continue
                        ats_type, token = hit
                        found_tokens[f"{ats_type}:{token}"] = ats_type
                except Exception as e:
                    logger.debug(f"job ATS discovery ddg '{q[:60]}': {e}")
        _ddg_breaker_record(success=True)
    except Exception as e:
        _ddg_breaker_record(success=False, hard_error=_is_hard_network_error(e))
        logger.debug(f"job ATS discovery DDGS init: {e}")
        return out

    for key in list(found_tokens.keys())[:12]:  # سقف امن تعداد شرکت در هر فراخوانی
        ats_type, token = key.split(":", 1)
        adapter_fn = ATS_ADAPTERS.get(ats_type)
        if not adapter_fn:
            continue
        try:
            jobs = adapter_fn(token, field, keywords, needed=max(1, needed - len(out)))
            for j in jobs:
                if not is_generic_country and country and (j.get("location") in ("", "Unspecified")):
                    j["location"] = country
                out.append(j)
        except Exception as e:
            logger.debug(f"ATS board fetch ({ats_type}:{token}): {e}")
        if len(out) >= needed:
            break
    return out

# Adzuna فقط از این کشورها (با همین کدهای دوحرفی) پشتیبانی می‌کنه —
# https://developer.adzuna.com/overview — چون کاربرهای این بات معمولاً
# اسم کشور رو انگلیسی/آزاد تایپ می‌کنن نه کد ISO، این یه mapping ساده‌ی
# متن-به-کد است؛ اگه کشور توی این لیست نبود (یا Remote/همه بود)، Adzuna
# فقط silently رد می‌شه — نه crash، نه پیام خطا به کاربر.
_ADZUNA_COUNTRIES = {
    "austria": "at", "australia": "au", "brazil": "br", "canada": "ca",
    "germany": "de", "deutschland": "de", "france": "fr",
    "uk": "gb", "united kingdom": "gb", "britain": "gb", "england": "gb",
    "india": "in", "italy": "it", "mexico": "mx", "netherlands": "nl",
    "new zealand": "nz", "poland": "pl", "russia": "ru", "singapore": "sg",
    "usa": "us", "us": "us", "united states": "us", "america": "us",
    "south africa": "za",
}

def _adzuna_country_code(country: str) -> str | None:
    c = (country or "").strip().lower()
    if not c:
        return None
    if c in _ADZUNA_COUNTRIES:
        return _ADZUNA_COUNTRIES[c]
    # اگه کاربر چند کشور با کاما/فاصله نوشته («Germany Canada»)، اولین
    # کشوری که تشخیص داده می‌شه رو برمی‌گردونیم — بهتر از رد کردن کامل.
    for token in re.split(r"[,\s]+", c):
        if token in _ADZUNA_COUNTRIES:
            return _ADZUNA_COUNTRIES[token]
    return None

JOB_SEARCH_PHASES = ["quick_boards", "extra_boards", "ats_boards", "web_discovery"]
JOB_PHASE_LABELS_FA = {
    "quick_boards": "بردهای شغلی سریع (Jobicy، Remotive، Himalayas، WeWorkRemotely، RemoteOK، Arbeitnow)",
    "extra_boards": "منابع تکمیلی (Jobicy کلمات مرتبط، Adzuna)",
    "ats_boards":   "سیستم‌های ATS واقعی شرکت‌ها (Greenhouse، Lever)",
    "web_discovery": "جستجوی مستقیم وب (فراتر از بردهای استاندارد)",
}

def _search_jobs_realtime_impl(field: str, country: str, count: int = 50,
                                progress: dict | None = None,
                                max_age_hours: int | None = None,
                                relaxed: bool = False) -> list[dict]:
    """Wrapper چند-کشوری روی _search_jobs_realtime_impl_single — همون
    دلیل و همون الگوی _search_professors_impl (بالاتر): قبلاً
    _resolve_country_iso فقط اولین کشور شناخته‌شده‌ی رشته رو برمی‌گردوند،
    یعنی «Germany, Canada» عملاً می‌شد جستجوی «Germany» تنها. الان اگه
    _resolve_countries_iso بیش از یک کشور تشخیص بده، پایپ‌لاین کامل
    (که الان به اسم _search_jobs_realtime_impl_single هست، بدون هیچ
    تغییری در خودش) به‌ازای هر کشور جدا و هم‌زمان (ThreadPoolExecutor)
    اجرا می‌شه و نتایج merge/dedupe می‌شن. تک‌کشوری/«همه‌ی کشورها» دقیقاً
    رفتار قبلی رو حفظ می‌کنه، بدون overhead اضافه."""
    countries = _resolve_countries_iso(country) if country and country.strip().lower() not in ("همه", "all", "*", "") else []
    if len(countries) <= 1:
        return _search_jobs_realtime_impl_single(
            field, country, count, progress=progress,
            max_age_hours=max_age_hours, relaxed=relaxed)

    per_country = max(count // len(countries), 10)
    iso_to_name = {v: k for k, v in _COUNTRY_ISO.items()}
    merged: list[dict] = []
    seen_keys: set[str] = set()
    # ── فیکس: قبلاً `with ThreadPoolExecutor(...) as pool:` + as_completed
    # timeout=90 بود — همون باگی که توی _run_wave (خط ۶۲۳۰) پیدا و فیکس
    # شد: خروج از `with` بعد از TimeoutError خودش `shutdown(wait=True)`
    # صدا می‌زد و منتظر *همه‌ی* thread های کند (کشورهایی که هنوز تموم
    # نشده بودن، مثلاً وقتی DDG/AI provider کند بودن) می‌موند — یعنی
    # timeout=90 هیچ سقف واقعی‌ای تضمین نمی‌کرد و دقیقاً حس «هنگ وسط
    # سرچ» می‌داد. الان pool دستی مدیریت می‌شه و با shutdown(wait=False)
    # خارج می‌شیم: کشورهای کند در پس‌زمینه به کارشون ادامه می‌دن
    # (نتیجه‌شون دور ریخته می‌شه) ولی تابع واقعاً بعد از ۹۰ ثانیه برمی‌گرده.
    pool = concurrent.futures.ThreadPoolExecutor(max_workers=min(len(countries), 6))
    try:
        futs = {
            pool.submit(_search_jobs_realtime_impl_single, field, iso_to_name.get(iso, iso),
                        per_country, progress=None, max_age_hours=max_age_hours, relaxed=relaxed): iso
            for iso in countries
        }
        done, not_done = concurrent.futures.wait(futs, timeout=90)
        for fut in done:
            iso = futs[fut]
            try:
                sub_results = fut.result() or []
            except Exception as e:
                logger.warning(f"_search_jobs_realtime_impl multi-country ({iso}): {e}")
                continue
            for j in sub_results:
                key = j.get("url") or f"{j.get('title','')}|{j.get('company','')}"
                if key and key not in seen_keys:
                    seen_keys.add(key)
                    merged.append(j)
        if not_done:
            logger.debug(f"{len(not_done)} country search(es) still running in background — نتیجه‌شون نادیده گرفته می‌شه")
    finally:
        pool.shutdown(wait=False)
    if progress is not None:
        progress["stage"] = "done"
        progress["done"] = progress.get("total", 1)
    return merged[:count]

def _search_jobs_realtime_impl_single(field: str, country: str, count: int = 50,
                                progress: dict | None = None,
                                max_age_hours: int | None = None,
                                relaxed: bool = False) -> list[dict]:
    """جستجوی شغل از چند منبع رایگان (API + ATS واقعی + جستجوی وب).
    اگه یکی از دست رفت، بقیه جبران می‌کنن — هیچ‌وقت list خالی برنمی‌گرده.

    progress: دیکشنری مشترک اختیاری — دقیقاً مثل _search_professors_impl،
    برای نمایش زنده‌ی «فلان فاز بررسی شد» به کاربر بدون این‌که حس کنه بات
    هنگ کرده (پیاده‌سازی شده چون قبلاً این progress bar فقط برای جستجوی
    اساتید بود، نه برای جستجوی کار).

    max_age_hours: پیش‌فرض JOB_MAX_AGE_HOURS (۴۸ ساعت)؛ توی تلاش‌های مجدد
    (search_jobs_retry_group) این مقدار عمداً بازتر می‌شه (مثلاً ۹۶ یا ۳۳۶
    ساعت) چون اگه ۴۸ ساعت اخیر چیزی نداشت، منطقی‌ترین قدم بعدی گشاد کردن
    بازه‌ی زمانیه، نه امیدوار بودن به شانس.

    relaxed: اگه True باشه، فیلتر _matches (تطبیق دقیق کلمه‌ی رشته با
    عنوان/تگ) کنار گذاشته می‌شه — یعنی نتایج بیشتری از هر API نگه داشته
    می‌شن (دقت کمتر، ولی برای «حداقل یه چیزی پیدا شه» توی تلاش‌های مجدد
    مفیدتره)."""
    jobs = []
    _age_hours = max_age_hours if max_age_hours is not None else JOB_MAX_AGE_HOURS
    field_l = field.lower()
    field_words = field_l.split()
    HEADERS = {"User-Agent": "Mozilla/5.0 Chrome/124 Safari/537.36"}

    if progress is not None:
        progress["total"] = len(JOB_SEARCH_PHASES)
        progress["done"] = 0
        progress["stage"] = "searching"
        progress["current"] = ""
        progress["source_labels"] = JOB_PHASE_LABELS_FA

    def _mark_phase(name: str):
        if progress is not None:
            progress["done"] = progress.get("done", 0) + 1
            progress["current"] = name

    def _matches(title: str, tags: list) -> bool:
        if relaxed:
            return True
        t = title.lower()
        tags_l = [str(x).lower() for x in (tags or [])]
        return any(w in t or w in tags_l for w in field_words)

    def _append(j: dict):
        if len(jobs) < count:
            jobs.append(j)

    # ── ۱. Jobicy — Remote jobs JSON feed (رایگان، بدون key) ────
    if len(jobs) < count:
        try:
            r = requests.get("https://jobicy.com/api/v2/remote-jobs",
                params={"count": 50, "tag": field_words[0]},
                headers=HEADERS, timeout=REQ_TIMEOUT)
            for j in r.json().get("jobs", []):
                if not _matches(j.get("jobTitle",""), j.get("jobCategory",[])):
                    continue
                _append({
                    "title":    j.get("jobTitle", ""),
                    "company":  j.get("companyName", ""),
                    "location": "Remote",
                    "url":      j.get("url", ""),
                    "created":  j.get("pubDate", ""),
                    "source":   "Jobicy",
                    "email":    "",
                    "description": j.get("jobDescription", "")[:400],
                    "tags":     j.get("jobCategory", []),
                })
        except Exception as e:
            logger.warning(f"Jobicy: {e}")

    # ── ۲. Remotive — Remote tech jobs (رایگان، JSON) ──────────
    if len(jobs) < count:
        try:
            r = requests.get("https://remotive.com/api/remote-jobs",
                params={"search": field_words[0], "limit": 50},
                headers=HEADERS, timeout=REQ_TIMEOUT)
            for j in r.json().get("jobs", []):
                if not _matches(j.get("title",""), j.get("tags",[])):
                    continue
                _append({
                    "title":    j.get("title", ""),
                    "company":  j.get("company_name", ""),
                    "location": j.get("candidate_required_location", "Remote"),
                    "url":      j.get("url", ""),
                    "created":  j.get("publication_date", ""),
                    "source":   "Remotive",
                    "email":    "",
                    "description": j.get("description", "")[:400],
                    "tags":     j.get("tags", []),
                })
        except Exception as e:
            logger.warning(f"Remotive: {e}")

    # ── ۳. Himalayas — Tech jobs JSON (رایگان، بدون key) ────────
    if len(jobs) < count:
        try:
            r = requests.get("https://himalayas.app/jobs/api",
                params={"q": field, "limit": 30},
                headers=HEADERS, timeout=REQ_TIMEOUT)
            for j in r.json().get("jobs", []):
                if not _matches(j.get("title",""), j.get("skills",[])):
                    continue
                _append({
                    "title":    j.get("title", ""),
                    "company":  j.get("companyName", ""),
                    "location": j.get("locationRestrictions", ["Remote"])[0]
                                if j.get("locationRestrictions") else "Remote",
                    "url":      j.get("applicationLink") or j.get("url", ""),
                    "created":  j.get("createdAt", ""),
                    "source":   "Himalayas",
                    "email":    "",
                    "description": j.get("description", "")[:400],
                    "tags":     j.get("skills", []),
                })
        except Exception as e:
            logger.warning(f"Himalayas: {e}")

    # ── ۴. We Work Remotely — scrape RSS feed (بدون key) ────────
    if len(jobs) < count:
        try:
            r = requests.get(
                f"https://weworkremotely.com/remote-jobs.rss",
                headers=HEADERS, timeout=REQ_TIMEOUT)
            import xml.etree.ElementTree as ET
            root = ET.fromstring(r.content)
            for item in root.findall(".//item"):
                title_el = item.find("title")
                title = title_el.text if title_el is not None and title_el.text else ""
                if not _matches(title, []):
                    continue
                link_el = item.find("link")
                link = link_el.text if link_el is not None and link_el.text else ""
                pub_el = item.find("pubDate")
                pub = pub_el.text if pub_el is not None and pub_el.text else ""
                _append({
                    "title":    title,
                    "company":  "",
                    "location": "Remote",
                    "url":      link,
                    "created":  pub,
                    "source":   "WeWorkRemotely",
                    "email":    "",
                    "description": "",
                    "tags":     [],
                })
        except Exception as e:
            logger.warning(f"WeWorkRemotely: {e}")

    # ── ۵. RemoteOK — (رایگان، JSON) ────────────────────────────
    if len(jobs) < count:
        try:
            r = requests.get("https://remoteok.com/api",
                headers=HEADERS, timeout=REQ_TIMEOUT)
            for j in r.json()[1:]:
                if not isinstance(j, dict): continue
                if not _matches(j.get("position",""), j.get("tags",[])):
                    continue
                _append({
                    "title":    j.get("position", ""),
                    "company":  j.get("company", ""),
                    "location": "Remote",
                    "url":      j.get("url", ""),
                    "created":  datetime.fromtimestamp(j.get("epoch",0)).isoformat()
                                if j.get("epoch") else "",
                    "source":   "RemoteOK",
                    "email":    "",
                    "description": j.get("description", "")[:400],
                    "tags":     j.get("tags", []),
                })
                if len(jobs) >= count: break
        except Exception as e:
            logger.warning(f"RemoteOK: {e}")

    # ── ۶. Arbeitnow — اروپا و آلمان، رایگان JSON بدون key ──────
    if len(jobs) < count:
        try:
            r = requests.get("https://www.arbeitnow.com/api/job-board-api",
                params={"search": field_words[0]},
                headers=HEADERS, timeout=REQ_TIMEOUT)
            for j in r.json().get("data", []):
                if not _matches(j.get("title",""), j.get("tags",[])):
                    continue
                _append({
                    "title":    j.get("title", ""),
                    "company":  j.get("company_name", ""),
                    "location": j.get("location", "Germany"),
                    "url":      j.get("url", ""),
                    "created":  j.get("created_at", ""),
                    "source":   "Arbeitnow",
                    "email":    "",
                    "description": j.get("description", "")[:400],
                    "tags":     j.get("tags", []),
                })
        except Exception as e:
            logger.warning(f"Arbeitnow: {e}")

    _mark_phase("quick_boards")

    # ── ۷. Jobspy via RapidAPI-free tier ────────────────────────
    # اگه هنوز کم داریم، یه batch از Jobicy با tag متفاوت می‌گیریم
    if len(jobs) < count // 2:
        try:
            for tag in field_words[1:3]:  # کلمه‌های دیگه‌ی field رو هم امتحان کن
                r = requests.get("https://jobicy.com/api/v2/remote-jobs",
                    params={"count": 25, "tag": tag},
                    headers=HEADERS, timeout=REQ_TIMEOUT)
                for j in r.json().get("jobs", []):
                    if not _matches(j.get("jobTitle",""), j.get("jobCategory",[])):
                        continue
                    _append({
                        "title":    j.get("jobTitle", ""),
                        "company":  j.get("companyName", ""),
                        "location": "Remote",
                        "url":      j.get("url", ""),
                        "created":  j.get("pubDate", ""),
                        "source":   "Jobicy",
                        "email":    "",
                        "description": j.get("jobDescription", "")[:400],
                        "tags":     j.get("jobCategory", []),
                    })
                if len(jobs) >= count: break
        except Exception as e:
            logger.warning(f"Jobicy fallback: {e}")

    # ── ۸. Adzuna — تنها منبع با فیلتر *واقعی* کشوری (نه post-hoc متنی) ──
    # همه‌ی ۷ منبع بالا اساساً board‌های Remote/فنی هستن و هیچ‌کدوم واقعاً
    # به‌ازای کشور query نمی‌زنن (همون‌طور که کامنت فیلتر کشور پایین‌تر
    # توضیح می‌ده). Adzuna برعکسه: endpoint مجزا برای هر کشور داره
    # (adzuna.com/{country_code}/search) و برای کاربری که دنبال موقعیت
    # محلی/غیر-Remote توی یه کشور مشخصه، این تنها منبعیه که واقعاً همون
    # کشور رو جستجو می‌کنه، نه این‌که بعداً از یه لیست جهانی فیلترش کنیم.
    # اختیاریه — بدون ADZUNA_APP_ID/ADZUNA_APP_KEY فقط همین یک منبع رد
    # می‌شه، بقیه‌ی pipeline دست‌نخورده کار می‌کنه.
    if len(jobs) < count and ADZUNA_APP_ID and ADZUNA_APP_KEY:
        adzuna_country = _adzuna_country_code(country)
        if adzuna_country:
            try:
                r = requests.get(
                    f"https://api.adzuna.com/v1/api/jobs/{adzuna_country}/search/1",
                    params={
                        "app_id": ADZUNA_APP_ID,
                        "app_key": ADZUNA_APP_KEY,
                        "what": field,
                        "max_days_old": max(1, JOB_MAX_AGE_HOURS // 24),
                        "results_per_page": min(50, count),
                        "content-type": "application/json",
                    },
                    headers=HEADERS, timeout=REQ_TIMEOUT)
                for j in r.json().get("results", []):
                    _append({
                        "title":    j.get("title", ""),
                        "company":  (j.get("company") or {}).get("display_name", ""),
                        "location": (j.get("location") or {}).get("display_name", country),
                        "url":      j.get("redirect_url", ""),
                        "created":  j.get("created", ""),
                        "source":   "Adzuna",
                        "email":    "",
                        "description": (j.get("description") or "")[:400],
                        "tags":     [],
                    })
            except Exception as e:
                logger.warning(f"Adzuna: {e}")
        else:
            logger.debug(f"Adzuna: country code not recognized for {country!r}, source skipped")

    _mark_phase("extra_boards")

    # ── ۹. Greenhouse / Lever — لینک مستقیم به ATS واقعی شرکت‌ها ─────
    # قبل از Web Discovery عمومی صدا زده می‌شه چون site: محدود به دو
    # دامنه‌ی ATS واقعیه، پس نتایجش دقیق‌تر و مستقیم‌تر قابل اپلای‌ان.
    if len(jobs) < count:
        try:
            before = len(jobs)
            ats_discovered = _discover_jobs_via_ats_boards(field, country, field_words, needed=count - len(jobs))
            for d in ats_discovered:
                _append(d)
            logger.info(f"Greenhouse/Lever discovery (job search): +{len(jobs)-before} jobs")
        except Exception as e:
            logger.warning(f"Job ATS-board discovery: {e}")

    _mark_phase("ats_boards")

    # ── ۱۰. Web Discovery — مستقل از همه‌ی APIهای بالا (orchestrator) ────
    # همون فلسفه‌ی جستجوی استاد: به‌جای وابستگی صرف به چند API ثابت که
    # همه‌شون گرایش به «Remote/فنی» دارن، مستقیم از نتایج جستجوی وب هم
    # آگهی پیدا می‌کنیم — مخصوصاً وقتی کشور خاصی انتخاب شده (دقیقاً همون
    # نقطه‌ضعفی که کامنت فیلتر کشور پایین‌تر بهش اشاره می‌کنه).
    if len(jobs) < count:
        try:
            before = len(jobs)
            discovered = _discover_jobs_via_web_search(field, country, field_words, needed=count - len(jobs))
            for d in discovered:
                _append(d)
            logger.info(f"Web Discovery (job search): +{len(jobs)-before} jobs")
        except Exception as e:
            logger.warning(f"Job web discovery: {e}")

    _mark_phase("web_discovery")

    # ── فیلتر بر اساس کشور هدف ────────────────────────────────────
    # نکته‌ی مهم: پارامتر country قبلاً کاملاً بلااستفاده بود — هیچ‌کدوم
    # از این ۶ منبع query/filter واقعی بر اساس کشور ندارن (اکثرشون اصلاً
    # بردهای مشاغل Remote-محورن). یعنی کاربری که «Canada» یا «Japan»
    # می‌نوشت، دقیقاً همون نتایج «Germany» یا هر کشور دیگه‌ای رو می‌گرفت.
    # چون خود APIها این قابلیت رو ندارن، این‌جا با فیلتر متنی روی location
    # جبرانش می‌کنیم — آگهی‌های Remote/Worldwide (که برای هر کشوری قابل
    # اپلای‌ان) همیشه نگه داشته می‌شن.
    country_l = (country or "").strip().lower()
    is_generic_country = country_l in ("", "remote", "all", "any", "همه", "*", "anywhere")
    if not is_generic_country and jobs:
        def _location_matches(loc: str) -> bool:
            loc_l = (loc or "").lower()
            if not loc_l or any(k in loc_l for k in ("remote", "worldwide", "anywhere", "global")):
                return True
            return country_l in loc_l or any(w in loc_l for w in country_l.split() if len(w) > 2)
        filtered = [j for j in jobs if _location_matches(j.get("location", ""))]
        # اگه فیلتر همه چیز رو حذف کرد (خیلی سخت‌گیرانه بود)، به‌جای صفر
        # نتیجه با فهرست کامل‌تر ادامه می‌دیم — بهتر از هیچیه؛ کاربر
        # می‌تونه خودش موقع preview تصمیم بگیره کدوم رو می‌خواد.
        if filtered:
            jobs = filtered
        else:
            logger.info(f"Country filter '{country}' removed all {len(jobs)} jobs — falling back to unfiltered list")

    if progress is not None:
        progress["stage"] = "done"
    return jobs[:count]

# ================================================================
# JOB FILTER ENGINE — بعد از جستجو، قبل از اپلای
# ================================================================

JOB_MAX_AGE_DAYS = 30  # آگهی‌های قدیمی‌تر از این skip می‌شوند

def _parse_job_filters(raw: str) -> dict:
    """رشته‌ی آزاد فیلتر کاربر را به ساختار قابل استفاده تبدیل می‌کند.
    ساده و بدون regex سنگین — برای جلوگیری از هر نوع exception."""
    if not raw:
        return {}
    raw_l = raw.lower()
    result = {
        "remote_only":        "فقط remote" in raw_l or "remote only" in raw_l,
        "visa_only":          "visa sponsorship" in raw_l or "اسپانسر" in raw_l,
        "blacklist_companies": [],
        "whitelist_countries": [],
    }
    # شرکت‌های blacklist — بعد از کلمات «نه» یا «بدون» یا «no» می‌آیند
    for part in re.split(r"[،,\n]", raw):
        part = part.strip()
        bl_m = re.match(
            r"(?:نه|بدون|no|not|exclude|except)\s+(.+)",
            part, re.I)
        if bl_m:
            result["blacklist_companies"].append(bl_m.group(1).strip().lower())
    return result

def _is_stale_job(job: dict, max_days: int = JOB_MAX_AGE_DAYS) -> bool:
    """True اگر آگهی از max_days روز قبل قدیمی‌تر است."""
    created = job.get("created") or ""
    if not created:
        return False  # تاریخ نامشخص → رد نکن (محافظه‌کارانه)
    try:
        # فرمت‌های رایج: ISO 8601 یا epoch string
        if isinstance(created, (int, float)):
            dt = datetime.fromtimestamp(float(created))
        else:
            # حذف timezone suffix ساده
            created_clean = re.sub(r"[+-]\d{2}:\d{2}$|Z$", "", created.strip())
            dt = datetime.fromisoformat(created_clean)
        return (datetime.now() - dt).days > max_days
    except Exception:
        return False

def _job_freshness(job: dict) -> dict:
    """وضعیت تازگی آگهی + لحظه‌ای که خودمون همین الان چکش کردیم.
    قبلاً فقط ۳ حالت بود (تایید‌شده/نامشخص/قدیمی‌تر) و ادعای «۴۸ ساعت
    اخیر» برای منابعی که اصلاً تاریخ نمی‌دن هم یکسان به‌نظر می‌رسید. الان
    ۴ حالت صریح‌تر، دقیقاً منطبق با چیزی که واقعاً می‌دونیم:
      🟢 Posted Xh ago       → تاریخ داریم و واقعاً زیر ۲۴ ساعته
      🟡 Posted Xd ago       → تاریخ داریم، بین ۲۴ ساعت تا ۷ روزه
      🔴 Possibly stale      → تاریخ داریم ولی بین ۷ تا JOB_MAX_AGE_DAYS
                               روزه (هنوز حذف نشده چون زیر cutoff‌ه، ولی
                               دیگه واقعاً «تازه» نیست)
      ⚪ Date unknown        → منبع اصلاً تاریخ نمی‌ده، یا تاریخ داده‌شده
                               مشکوکه (مثلاً در آینده) — به داده اعتماد
                               نمی‌کنیم، نه اینکه ادعای غلط بدیم
    خروجی: {"badge": متن نمایشی, "checked_at": همین لحظه (زمانی که خودمون
    این آگهی رو دیدیم/چکش کردیم — نه زمان انتشار خودِ آگهی)، به‌صورت
    رشته‌ی قابل‌نمایش و هم به‌صورت datetime برای ذخیره در دیتابیس}."""
    now = datetime.now()
    checked_at_dt = now
    checked_at_str = now.strftime("%Y-%m-%d %H:%M")

    created = job.get("created") or ""
    if not created:
        return {"badge": "⚪ تاریخ انتشار نامشخص", "checked_at": checked_at_str,
                "checked_at_dt": checked_at_dt}
    try:
        if isinstance(created, (int, float)):
            dt = datetime.fromtimestamp(float(created))
        else:
            created_clean = re.sub(r"[+-]\d{2}:\d{2}$|Z$", "", created.strip())
            dt = datetime.fromisoformat(created_clean)
        age_hours = (now - dt).total_seconds() / 3600
        if age_hours < 0:
            # تاریخ در آینده = داده مشکوک، به آن اعتماد نمی‌کنیم
            return {"badge": "⚪ تاریخ انتشار نامشخص (داده مشکوک)", "checked_at": checked_at_str,
                    "checked_at_dt": checked_at_dt}
        if age_hours < 24:
            h = max(0, int(age_hours))
            return {"badge": f"🟢 {h} ساعت پیش منتشر شده" if h else "🟢 کمتر از ۱ ساعت پیش منتشر شده",
                    "checked_at": checked_at_str, "checked_at_dt": checked_at_dt}
        age_days = age_hours / 24
        if age_days <= 7:
            return {"badge": f"🟡 {int(age_days)} روز پیش منتشر شده",
                    "checked_at": checked_at_str, "checked_at_dt": checked_at_dt}
        # بین ۷ تا JOB_MAX_AGE_DAYS روز — هنوز به‌طور کامل فیلتر نشده
        # (نگاه کن به _is_stale_job) ولی صادقانه به کاربر می‌گیم دیگه
        # «تازه» نیست.
        return {"badge": f"🔴 حدود {int(age_days)} روز پیش — احتمالاً قدیمی شده",
                "checked_at": checked_at_str, "checked_at_dt": checked_at_dt}
    except Exception:
        return {"badge": "⚪ تاریخ انتشار نامشخص", "checked_at": checked_at_str,
                "checked_at_dt": checked_at_dt}

def _passes_job_filters(job: dict, filters: dict) -> tuple[bool, str]:
    """True اگر job از همه‌ی فیلترهای کاربر رد بشه.
    خروجی دوم: دلیل رد (برای لاگ)."""
    if not filters:
        return True, ""

    title_l   = (job.get("title","") or "").lower()
    company_l = (job.get("company","") or "").lower()
    loc_l     = (job.get("location","") or "").lower()
    desc_l    = (job.get("description","") or "").lower()

    # آگهی قدیمی
    if _is_stale_job(job):
        return False, f"آگهی قدیمی (تاریخ: {job.get('created','')})"

    # Blacklist شرکت
    for bl in filters.get("blacklist_companies", []):
        if bl and bl in company_l:
            return False, f"شرکت در Blacklist: {company_l}"

    # AI Memory: کلیدواژه/شرکت‌هایی که کاربر قبلاً (در همین یا سرویس قبلی)
    # گفته دوست ندارد — خودکار حذف می‌شود، دیگر نیازی به پرسیدن دوباره نیست.
    for dk in filters.get("disliked_keywords", []):
        if dk and (dk in title_l or dk in company_l or dk in desc_l):
            return False, f"مطابق موارد ناخواسته‌ی کاربر (AI Memory): {dk}"

    # فقط Remote
    if filters.get("remote_only"):
        is_remote = ("remote" in loc_l or "remote" in title_l or
                     "remote" in desc_l or job.get("source") == "RemoteOK")
        if not is_remote:
            return False, "فقط Remote درخواست شده ولی این آگهی Remote نیست"

    # فقط Visa Sponsorship
    if filters.get("visa_only"):
        has_visa = ("visa" in desc_l or "sponsor" in desc_l or
                    "relocation" in desc_l)
        if not has_visa:
            return False, "Visa Sponsorship در توضیح آگهی یافت نشد"

    return True, ""

# ================================================================
# JOB FINGERPRINT — همون شغل که روی چند منبع (LinkedIn، سایت شرکت،
# Indeed، برد دیگر) پیدا شده، به‌جای چند ردیف جدا، زیر یک شناسه‌ی
# یکتا (fingerprint = hash(company, title, location, normalized
# description)) ادغام می‌شه — و به کاربر «یافت‌شده در N منبع» نشون
# داده می‌شه.
# ================================================================
_FP_STOPWORDS = {"inc", "llc", "ltd", "corp", "corporation", "co", "the", "group", "gmbh", "srl", "sa", "plc"}

def _normalize_for_fingerprint(text: str) -> str:
    t = (text or "").lower().strip()
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    words = [w for w in t.split() if w and w not in _FP_STOPWORDS]
    return " ".join(words)

def job_fingerprint(job: dict) -> str:
    """شناسه‌ی یکتای یک «شغل واقعی»، مستقل از این‌که کدوم منبع پیداش
    کرده. description عمداً فقط با ۱۵ کلمه‌ی اول و وزن کم وارد می‌شه —
    عنوان+شرکت+لوکیشن سیگنال قابل‌اعتمادتری از متن کامل توضیح هستن
    (که بین منابع مختلف معمولاً کمی فرق داره)."""
    company  = _normalize_for_fingerprint(job.get("company", ""))
    title    = _normalize_for_fingerprint(job.get("title", ""))
    location = _normalize_for_fingerprint(job.get("location", ""))
    desc_words = _normalize_for_fingerprint(job.get("description", "")).split()[:15]
    raw = f"{company}|{title}|{location}|{' '.join(desc_words)}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

def _job_fingerprint_loose(job: dict) -> str:
    """امضای سبک‌تر فقط با company+title — برای ادغام مواردی که همون
    شغل با location کمی متفاوت (مثلاً 'Berlin' در یک منبع، 'Berlin,
    Germany' در منبع دیگر) ثبت شده."""
    company = _normalize_for_fingerprint(job.get("company", ""))
    title   = _normalize_for_fingerprint(job.get("title", ""))
    return hashlib.sha256(f"{company}|{title}".encode("utf-8")).hexdigest()[:16]

def merge_jobs_by_fingerprint(jobs: list[dict]) -> list[dict]:
    """آگهی‌هایی که company+title یکسان (case/فاصله/پسوند شرکتی بی‌اهمیت)
    دارن رو در یک آگهی ادغام می‌کنه و 'sources' (همه‌ی منابعی که این
    شغل روشون دیده شده) رو روی خروجی می‌ذاره — دقیقاً همون «Found on N
    sources» درخواستی. وقتی company یا title خالیه (منابعی مثل
    WeWorkRemotely که گاهی اسم شرکت نمی‌دن)، هر آگهی جدا نگه داشته
    می‌شه — هیچ‌وقت زیر یک سطل مشترک ادغام نمی‌شه."""
    def _score(j):
        s = 0
        if j.get("description") and len(j["description"]) > 100: s += 2
        if j.get("email"): s += 2
        if j.get("created"): s += 1
        return s

    groups: dict[str, list[dict]] = {}
    order: list[str] = []
    for j in jobs:
        company = (j.get("company") or "").strip()
        title = (j.get("title") or "").strip()
        key = _job_fingerprint_loose(j) if (company and title) else f"__unique_{id(j)}"
        if key not in groups:
            groups[key] = []
            order.append(key)
        groups[key].append(j)

    merged: list[dict] = []
    for key in order:
        group = groups[key]
        best = dict(max(group, key=_score))
        sources: list[str] = []
        seen_src: set = set()
        for j in group:
            src = j.get("source") or "Unknown"
            if src not in seen_src:
                seen_src.add(src)
                sources.append(src)
        best["sources"] = sources
        best["source_count"] = len(sources)
        best["fingerprint"] = job_fingerprint(best)
        merged.append(best)
    return merged

def format_job_sources_label(job: dict) -> str:
    """برچسب نمایشی «یافت‌شده در N منبع» — اگه فقط یک منبع باشه، فقط
    اسم همون یک منبع رو برمی‌گردونه (بدون ادعای اضافه)."""
    sources = job.get("sources") or ([job["source"]] if job.get("source") else [])
    if len(sources) <= 1:
        return sources[0] if sources else ""
    return f"یافت‌شده در {len(sources)} منبع: {'، '.join(sources)}"

def deduplicate_by_company(jobs: list[dict], max_per_company: int = 1) -> list[dict]:
    """اگر یک شرکت چند آگهی مشابه داشت، فقط بهترین آن (با بیشترین اطلاعات:
    توضیح کامل‌تر، آگهی جدیدتر) را نگه می‌دارد.
    'بهترین' = بیشترین امتیاز: وجود description (۲pt) + تاریخ جدید (۱pt)
    + وجود ایمیل HR (۲pt). در صورت تساوی، اولی نگه داشته می‌شود."""
    def _score(j):
        s = 0
        if j.get("description") and len(j["description"]) > 100: s += 2
        if j.get("email"): s += 2
        if j.get("created"): s += 1
        return s

    seen: dict[str, dict] = {}  # company_key → best_job
    for j in jobs:
        raw_company = (j.get("company") or "").strip().lower()
        # ⚠️ فیکس مهم: قبلاً وقتی company خالی بود (مثلاً منبع WeWorkRemotely
        # که اصلاً اسم شرکت رو توی RSS نمی‌ده — همیشه ""), همه‌ی این آگهی‌ها
        # زیر یک کلید مشترک "unknown" جمع می‌شدن و از ده‌ها آگهی واقعی و
        # کاملاً متفاوت فقط *یکی* زنده می‌موند — بقیه بی‌سروصدا دور ریخته
        # می‌شدن. این دقیقاً یکی از جاهایی بود که کاربر با «چندتا پیدا کرد»
        # مواجه می‌شد در حالی که منبع ده‌ها نتیجه‌ی واقعی برگردونده بود.
        # الان وقتی اسم شرکت نداریم، به‌جای یک سطل مشترک، از URL آگهی (که
        # برای هر آگهی یکتاست) به‌عنوان کلید استفاده می‌کنیم؛ فقط وقتی هر دو
        # آگهی واقعاً از یک شرکتِ *مشخص و شناخته‌شده* باشن با هم dedupe می‌شن.
        if raw_company:
            company_key = raw_company
        elif j.get("url"):
            company_key = j["url"].strip().lower()
        else:
            company_key = f"__no_company_no_url_{id(j)}"  # هیچ‌وقت با آگهی دیگه‌ای collide نمی‌کنه
        if company_key not in seen:
            seen[company_key] = j
        else:
            if _score(j) > _score(seen[company_key]):
                seen[company_key] = j

    # ترتیب اصلی حفظ می‌شود (مهم برای اینکه جدیدترین اول بیاد)
    best_set = set(id(v) for v in seen.values())
    return [j for j in jobs if id(j) in best_set]

# ================================================================
# EMAIL ENGINE — SMTP + AI
# ================================================================

def detect_smtp(email: str) -> tuple[str, int]:
    d = email.split("@")[-1].lower() if "@" in email else ""
    m = {"gmail.com":("smtp.gmail.com",587),
         "yahoo.com":("smtp.mail.yahoo.com",587),
         "outlook.com":("smtp-mail.outlook.com",587),
         "hotmail.com":("smtp-mail.outlook.com",587),
         "live.com":("smtp-mail.outlook.com",587),
         "protonmail.com":("smtp.protonmail.com",587),
         "zoho.com":("smtp.zoho.com",587)}
    return m.get(d, (f"smtp.{d}", 587))

# ── نشانه‌ی «حالت دستی» ─────────────────────────────────────────
# وقتی هیچ اتصال خودکاری (نه گوگل، نه SMTP) ممکن نشد، کاربر می‌تونه این
# حالت رو انتخاب کنه: بات دیگه سعی نمی‌کنه واقعاً ایمیل بفرسته، فقط متن
# آماده رو توی چت میده تا خود کاربر بفرسته. این مقدار در فیلد «پسورد»
# ذخیره می‌شود تا از همون مکانیزم snapshot موجود (که قبلاً برای
# App Password/گوگل ساخته شده) به‌صورت شفاف عبور کند.
MANUAL_MODE_SENTINEL = "MANUAL_MODE_NO_CREDENTIALS"

def _smtp_connect(host, port, timeout):
    """پورت 465 = SSL مستقیم (SMTP_SSL)، هر پورت دیگه (587/25/...) = STARTTLS.
    قبلاً بی‌قیدوشرط STARTTLS استفاده می‌شد که روی پورت 465 اصلاً کار
    نمی‌کند (و همیشه با خطای مبهم شکست می‌خورد) — همین یکی از دلایل
    رایج «پسورد درسته ولی وصل نمی‌شه» بود."""
    if port == 465:
        return smtplib.SMTP_SSL(host, port, timeout=timeout)
    s = smtplib.SMTP(host, port, timeout=timeout)
    s.ehlo(); s.starttls(); s.ehlo()
    return s

def _smtp_from_mx(domain: str) -> list[tuple[str, int]]:
    """
    از MX record دامنه، SMTP server واقعی را کشف می‌کند.
    این روش برای دانشگاه‌ها خیلی دقیق‌تر از حدس smtp.domain.com است.
    مثال: MX دانشگاه → aspmx.l.google.com → یعنی Google Workspace است.
    """
    candidates = []
    try:
        import dns.resolver
        answers = sorted(
            dns.resolver.resolve(domain, "MX", lifetime=6),
            key=lambda r: r.preference
        )
        for rdata in answers[:3]:
            mx_host = str(rdata.exchange).rstrip(".")
            mx_lower = mx_host.lower()

            # تشخیص provider از MX record
            if "google" in mx_lower or "googlemail" in mx_lower:
                # Google Workspace
                candidates += [("smtp.gmail.com", 587), ("smtp.gmail.com", 465)]
            elif "outlook" in mx_lower or "microsoft" in mx_lower or "protection.outlook" in mx_lower:
                # Microsoft 365 / Exchange Online
                candidates += [("smtp.office365.com", 587), ("smtp-mail.outlook.com", 587)]
            elif "yahoodns" in mx_lower:
                candidates += [("smtp.mail.yahoo.com", 587), ("smtp.mail.yahoo.com", 465)]
            elif "zoho" in mx_lower:
                candidates += [("smtp.zoho.com", 587), ("smtp.zoho.com", 465)]
            elif "protonmail" in mx_lower:
                candidates += [("smtp.protonmail.com", 587)]
            elif "amazonses" in mx_lower:
                candidates += [(f"email-smtp.{domain}", 587)]
            else:
                # سرور اختصاصی دانشگاه — MX host خودش احتمالاً SMTP هم هست
                candidates += [(mx_host, 587), (mx_host, 465)]
    except ImportError:
        pass  # dnspython نصب نیست — به fallback می‌رویم
    except Exception as e:
        logger.debug(f"MX lookup for {domain}: {e}")
    return candidates


def _smtp_from_autodiscover(domain: str) -> list[tuple[str, int]]:
    """
    استاندارد Microsoft Autodiscover — اکثر Exchange/Office365 و خیلی از
    دانشگاه‌ها (حتی non-Microsoft) این endpoint را پیاده‌سازی کرده‌اند.
    """
    candidates = []
    urls = [
        f"https://autodiscover.{domain}/autodiscover/autodiscover.xml",
        f"https://{domain}/autodiscover/autodiscover.xml",
    ]
    payload = f"""<?xml version="1.0" encoding="utf-8"?>
<Autodiscover xmlns="http://schemas.microsoft.com/exchange/autodiscover/outlook/requestschema/2006">
<Request><EMailAddress>user@{domain}</EMailAddress><AcceptableResponseSchema>
http://schemas.microsoft.com/exchange/autodiscover/outlook/responseschema/2006a
</AcceptableResponseSchema></Request></Autodiscover>"""
    headers = {"Content-Type": "text/xml", "User-Agent": "ManifestApplyBot/1.0"}
    for url in urls:
        try:
            r = requests.post(url, data=payload, headers=headers, timeout=6,
                              allow_redirects=True)
            if r.status_code == 200:
                host_m = re.search(r"<SmtpAddress>(.*?)</SmtpAddress>", r.text, re.I)
                port_m = re.search(r"<Port>(\d+)</Port>", r.text, re.I)
                if host_m:
                    h = host_m.group(1).strip()
                    p = int(port_m.group(1)) if port_m else 587
                    candidates.append((h, p))
                    logger.debug(f"Autodiscover {domain}: {h}:{p}")
                    return candidates
        except Exception:
            continue
    return candidates


def _smtp_from_autoconfig(domain: str) -> list[tuple[str, int]]:
    """
    استاندارد Mozilla/Thunderbird autoconfig — اکثر دانشگاه‌های اروپایی و
    خیلی از سرویس‌دهندگان غیر-Microsoft این را دارند.
    """
    candidates = []
    urls = [
        f"https://autoconfig.{domain}/mail/config-v1.1.xml",
        f"https://{domain}/.well-known/autoconfig/mail/config-v1.1.xml",
        f"https://autoconfig.thunderbird.net/v1.1/{domain}",
    ]
    for url in urls:
        try:
            r = _safe_requests_get(url, timeout=6, headers={"User-Agent": "ManifestApplyBot/1.0"})
            if r.status_code == 200 and "outgoingServer" in r.text:
                host_m = re.search(
                    r'<outgoingServer type="smtp">.*?<hostname>(.*?)</hostname>',
                    r.text, re.DOTALL | re.I)
                port_m = re.search(
                    r'<outgoingServer type="smtp">.*?<port>(\d+)</port>',
                    r.text, re.DOTALL | re.I)
                ssl_m  = re.search(
                    r'<outgoingServer type="smtp">.*?<socketType>(.*?)</socketType>',
                    r.text, re.DOTALL | re.I)
                if host_m:
                    h = host_m.group(1).strip()
                    p = int(port_m.group(1)) if port_m else 587
                    ssl_type = (ssl_m.group(1) or "").upper()
                    # اگه SSL مشخص شده و پورت نه، پورت رو تصحیح کن
                    if "SSL" in ssl_type and p not in (465, 587):
                        p = 465
                    candidates.append((h, p))
                    logger.debug(f"Autoconfig {domain}: {h}:{p}")
                    return candidates
        except Exception:
            continue
    return candidates


def _smtp_host_candidates(email: str, host_hint=None, port_hint=None):
    """
    کشف هوشمند SMTP server — به ترتیب اولویت:
    1. MX record (دقیق‌ترین — provider واقعی را شناسایی می‌کند)
    2. Microsoft Autodiscover (برای Exchange/Office365/دانشگاه‌های مایکروسافتی)
    3. Mozilla Autoconfig (برای دانشگاه‌های اروپایی و non-Microsoft)
    4. Known-provider map (Gmail/Yahoo/Outlook/...)
    5. الگوهای رایج (mail.domain, smtp.domain) — آخرین fallback

    برای دانشگاه‌ها این تفاوت را ایجاد می‌کند:
    - قبل: smtp.iau.ac.ir (حدس) → معمولاً اشتباه
    - الان: MX → mail.iau.ac.ir یا autodiscover → سرور واقعی
    """
    domain = email.split("@")[-1].lower() if "@" in email else ""
    seen, out = set(), []

    def _add(*pairs):
        for h, p in pairs:
            if h and (h, p) not in seen:
                seen.add((h, p)); out.append((h, p))

    # اگه کاربر دستی host داده، اول آن را امتحان کن
    if host_hint:
        _add((host_hint, port_hint or 587), (host_hint, 465 if port_hint != 465 else 587))

    # ۱. MX record
    _add(*_smtp_from_mx(domain))

    # ۲. Autodiscover (فقط اگه MX هنوز چیزی نداده یا دانشگاهی به نظر می‌رسد)
    if len(out) < 2 or any(d in domain for d in (".edu", ".ac.", ".uni-", ".edu.")):
        _add(*_smtp_from_autodiscover(domain))

    # ۳. Autoconfig
    if len(out) < 3:
        _add(*_smtp_from_autoconfig(domain))

    # ۴. Known providers (جدول ثابت — سریع، بدون network)
    _known = {
        "gmail.com":      [("smtp.gmail.com", 587), ("smtp.gmail.com", 465)],
        "yahoo.com":      [("smtp.mail.yahoo.com", 587), ("smtp.mail.yahoo.com", 465)],
        "outlook.com":    [("smtp-mail.outlook.com", 587)],
        "hotmail.com":    [("smtp-mail.outlook.com", 587)],
        "live.com":       [("smtp-mail.outlook.com", 587)],
        "protonmail.com": [("smtp.protonmail.com", 587)],
        "proton.me":      [("smtp.protonmail.com", 587)],
        "zoho.com":       [("smtp.zoho.com", 587), ("smtp.zoho.com", 465)],
        "icloud.com":     [("smtp.mail.me.com", 587)],
        "me.com":         [("smtp.mail.me.com", 587)],
        "aol.com":        [("smtp.aol.com", 587), ("smtp.aol.com", 465)],
        "gmx.com":        [("mail.gmx.com", 587), ("mail.gmx.com", 465)],
    }
    if domain in _known:
        _add(*_known[domain])

    # ۵. الگوهای رایج (fallback نهایی)
    _add(
        (f"mail.{domain}", 587),
        (f"mail.{domain}", 465),
        (f"smtp.{domain}", 587),
        (f"smtp.{domain}", 465),
        (f"mailhost.{domain}", 587),   # برخی دانشگاه‌ها
    )

    return out

def parse_custom_smtp(text: str) -> tuple[str, int] | None:
    """ورودی کاربر برای Server/Port را می‌گیرد؛ فرمت‌های رایج را قبول می‌کند:
    'mail.iau.ac.ir 587' یا دو خط جدا (سرور در خط اول، پورت در خط دوم) یا
    فقط سرور (که پورت پیش‌فرض 587 در نظر گرفته می‌شود). SSL/TLS خودش از
    روی پورت تشخیص داده می‌شود (465 = SSL، غیر آن = STARTTLS) — نیازی
    نیست کاربر این تفاوت فنی را بداند."""
    parts = text.replace(",", " ").split()
    if not parts:
        return None
    host = None
    port = 587
    for p in parts:
        pc = p.strip().strip(":")
        if pc.isdigit():
            port = int(pc)
        elif "." in pc:
            host = pc.lower()
    if not host:
        return None
    return host, port

def try_smtp_connect(email: str, pw: str, host_hint=None, port_hint=None) -> tuple[bool, str, str, int]:
    """چند روش را پشت‌سرهم امتحان می‌کند (Plan A سپس Plan B) و در اولین
    موفقیت برمی‌گردد. خروجی: (موفق؟, پیام‌خطای آخرین تلاش, host نهایی, port نهایی)."""
    if isinstance(pw, str) and pw.startswith(GOOGLE_AUTH_PREFIX):
        ok, err = test_smtp(email, pw, "", 0)
        return ok, err, "gmail-api", 0
    candidates = _smtp_host_candidates(email, host_hint, port_hint)
    last_err = "نامشخص"
    for h, p in candidates:
        ok, err = test_smtp(email, pw, h, p)
        if ok:
            return True, "", h, p
        last_err = err
    return False, last_err, candidates[0][0], candidates[0][1]

def test_smtp(email, pw, host, port) -> tuple[bool, str]:
    # اگر کاربر از «ورود با گوگل» استفاده کرده باشد، pw یک App Password
    # واقعی نیست بلکه بلوب OAuth است (پیشوند GOOGLE_AUTH_PREFIX) — همان
    # لحظه‌ی OAuth خودش تست اتصال بوده، اینجا فقط معتبر بودن creds را
    # چک می‌کنیم، بدون هیچ SMTP login ای.
    if isinstance(pw, str) and pw.startswith(GOOGLE_AUTH_PREFIX):
        try:
            _load_google_creds(pw[len(GOOGLE_AUTH_PREFIX):])
            return True, ""
        except Exception as e:
            return False, str(e)[:100]
    try:
        s = _smtp_connect(host, port, 15)
        s.login(email, pw); s.quit()
        return True, ""
    except smtplib.SMTPAuthenticationError:
        return False, "رمز اشتباه یا App Password فعال نیست"
    except Exception as e:
        return False, str(e)[:100]

async def _deliver_email(bot, chat_id, smtp_e, smtp_p, smtp_h, smtp_pt,
                          to_email, subject, body, sender_name="Manifest Apply") -> tuple[bool, str]:
    """این تابع دقیقاً همون چیزیه که کامنت داخل send_smtp بهش اشاره می‌کرد ولی
    قبلاً واقعاً نوشته نشده بود — یعنی «حالت دستی» تا الان همیشه هر ایمیل رو
    silently fail می‌کرد و کاربر هیچ‌وقت متن آماده رو نمی‌دید.
    اگه کاربر در «حالت دستی» باشه (چون هیچ اتصال خودکاری ممکن نشد)، به‌جای
    تلاش برای SMTP، بسته‌ی کامل و آماده (گیرنده + موضوع + متن + بهترین زمان
    ارسال) رو مستقیم توی همین چت می‌فرسته تا کاربر کپی/پیست و ارسال کنه.
    در غیر این صورت دقیقاً مثل قبل واقعاً از طریق SMTP/Gmail API ارسال می‌کنه."""
    if smtp_p == MANUAL_MODE_SENTINEL:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "📋 این ایمیل رو خودتون بفرستید (کپی کنید و توی ایمیل خودتون paste کنید):\n\n"
                    f"📮 گیرنده: {to_email}\n"
                    f"📌 موضوع: {subject}\n"
                    f"⏰ بهترین زمان ارسال: ساعات اداری (۹ صبح تا ۵ عصر به وقت محل دانشگاه/شرکت مقصد) "
                    "— نرخ پاسخ‌دهی بالاتره\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{body}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━"))
            return True, "manual"
        except Exception as e:
            logger.error(f"manual delivery message failed: {e}")
            return False, str(e)[:100]
    return await asyncio.to_thread(send_smtp, smtp_e, smtp_p, smtp_h, smtp_pt,
                                    to_email, subject, body, sender_name)

def send_smtp(smtp_email, smtp_pw, smtp_host, smtp_port,
              to_email, subject, body, sender_name="Manifest Apply") -> tuple[bool, str]:
    # همان نکته‌ی بالا: اگر ورود با گوگل بوده، از Gmail API استفاده کن
    # نه SMTP+پسورد. بقیه‌ی کد (همه‌ی جاهایی که send_smtp صدا زده می‌شود)
    # بدون هیچ تغییری کار می‌کند چون این تشخیص همین‌جا و فقط همین‌جا اتفاق می‌افتد.
    if isinstance(smtp_pw, str) and smtp_pw.startswith(GOOGLE_AUTH_PREFIX):
        return send_gmail_api(smtp_pw[len(GOOGLE_AUTH_PREFIX):], to_email, subject, body, sender_name)
    if smtp_pw == MANUAL_MODE_SENTINEL:
        # این حالت هیچ‌وقت نباید مستقیم به این تابع برسد — مسیر دستی باید
        # قبل از رسیدن به اینجا در _deliver_email گرفته شود. اگر به هر
        # دلیلی رسید (مثلاً یک مسیر فرعی که هنوز آپدیت نشده)، به‌جای یک
        # خطای گنگ SMTP، پیام روشن برمی‌گردانیم تا چیزی crash نکند.
        return False, "این ایمیل در حالت دستی است — ارسال خودکار غیرفعال است."

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{sender_name} <{smtp_email}>"
    msg["To"]      = to_email
    msg.attach(MIMEText(body, "plain", "utf-8"))
    html = f"<html><body style='font-family:Arial;font-size:14px'>{body.replace(chr(10),'<br>')}</body></html>"
    msg.attach(MIMEText(html, "html", "utf-8"))

    # Retry هوشمند: فقط خطاهای گذرا (تایم‌اوت/قطعی شبکه) دوباره امتحان
    # می‌شن — نه خطاهای دائمی (رمز غلط، صندوق پر، آدرس نامعتبر) که تکرارشون
    # فقط وقت تلف می‌کنه و نتیجه‌ش هیچ‌وقت عوض نمی‌شه.
    last_err = "خطای نامشخص"
    for attempt in range(2):
        try:
            srv = _smtp_connect(smtp_host, smtp_port, 20)
            srv.login(smtp_email, smtp_pw)
            srv.sendmail(smtp_email, to_email, msg.as_string()); srv.quit()
            return True, ""
        except smtplib.SMTPAuthenticationError as e:
            # رمز/App Password اشتباهه — دوباره امتحان کردن نتیجه رو عوض نمی‌کنه
            return False, f"رمز/App Password رد شد: {str(e)[:80]}"
        except smtplib.SMTPRecipientsRefused as e:
            err_l = str(e).lower()
            if any(k in err_l for k in ("mailbox full", "quota exceeded", "over quota",
                                          "user unknown", "no such user", "does not exist",
                                          "recipient rejected", "invalid recipient")):
                # صندوق پر یا آدرس نامعتبر — دائمیه، retry فایده نداره
                return False, f"گیرنده رد شد (صندوق پر یا آدرس نامعتبر): {str(e)[:80]}"
            last_err = f"گیرنده رد شد: {str(e)[:80]}"
        except smtplib.SMTPSenderRefused as e:
            # مشکل از فرستنده (مثلاً بلاک شدن حساب) — دائمی، retry فایده نداره
            return False, f"فرستنده رد شد: {str(e)[:80]}"
        except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError,
                TimeoutError, ConnectionResetError, OSError) as e:
            # خطاهای شبکه‌ای/اتصال — معمولاً گذرا هستن، ارزش یه retry رو دارن
            last_err = f"خطای اتصال: {str(e)[:80]}"
        except Exception as e:
            # هر خطای ناشناخته‌ی دیگه — محتاطانه یه بار retry می‌کنیم، بیشتر نه
            last_err = str(e)[:100]

        if attempt == 0:
            time.sleep(2)  # قبل از تلاش دوم، یه فاصله‌ی کوتاه (نه بی‌نهایت retry)

    return False, last_err

# ================================================================
# GOOGLE OAUTH — ورود با یک کلیک، بدون پسورد
# ================================================================
GOOGLE_AUTH_PREFIX = "GOOGLE_OAUTH:"
GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/userinfo.email",
    "openid",
]

# state (توکن یک‌بارمصرف در URL) → telegram_id. فقط در حافظه‌ست چون عمرش
# چند دقیقه‌ست (تا وقتی کاربر توی مرورگر تایید کنه)؛ اگر بات دقیقاً همون
# لحظه ری‌استارت بشه، کاربر فقط باید دوباره دکمه‌ی «ورود با گوگل» رو بزنه —
# بدون خطا یا داده‌ی گم‌شده.
_pending_google_states: dict[str, tuple[int, float]] = {}  # state -> (telegram_id, created_at)
# قبلاً enforcement انقضا فقط توسط _periodic_memory_cleanup (هر ۱ ساعت،
# state‌های قدیمی‌تر از ۳۰ دقیقه) انجام می‌شد — یعنی خودِ لحظه‌ی مصرف
# (do_GET) هیچ چک سنی نداشت و یک state تئوریاً تا نزدیک ۹۰ دقیقه (بدترین
# حالت بین دو اجرای cleanup) قابل استفاده می‌موند. الان do_GET هم مستقیم
# در لحظه‌ی مصرف چک می‌کنه.
GOOGLE_STATE_MAX_AGE_SEC = 600  # ۱۰ دقیقه — برای تایید معمول توی مرورگر کافیه

def _make_google_flow():
    return GoogleFlow.from_client_config(
        {"web": {
            "client_id": GOOGLE_CLIENT_ID,
            "client_secret": GOOGLE_CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [GOOGLE_REDIRECT_URI],
        }},
        scopes=GOOGLE_SCOPES,
        redirect_uri=GOOGLE_REDIRECT_URI,
    )

def build_google_auth_url(telegram_id: int) -> str:
    """لینک ورود گوگل مخصوص همین کاربر را می‌سازد و state رو برای تشخیص
    کاربر موقع برگشت از گوگل نگه می‌دارد."""
    state = secrets.token_urlsafe(24)
    _pending_google_states[state] = (telegram_id, time.time())
    flow = _make_google_flow()
    url, _ = flow.authorization_url(
        access_type="offline", include_granted_scopes="true",
        prompt="consent", state=state)
    return url

def exchange_google_code(code: str) -> tuple[bool, str, str]:
    """کد بازگشتی از گوگل را با توکن واقعی تعویض می‌کند.
    خروجی: (موفق؟, creds_json یا پیام خطا, ایمیل گوگل کاربر)"""
    try:
        flow = _make_google_flow()
        flow.fetch_token(code=code)
        creds = flow.credentials
        svc = google_build("oauth2", "v2", credentials=creds)
        info = svc.userinfo().get().execute()
        return True, creds.to_json(), info.get("email", "")
    except Exception as e:
        return False, str(e)[:200], ""

def _load_google_creds(blob: str):
    """blob = خروجی creds.to_json() (بدون پیشوند). اگر توکن منقضی شده
    باشد، خودکار refresh می‌کند."""
    creds = GoogleCredentials.from_authorized_user_info(json.loads(blob), GOOGLE_SCOPES)
    if creds.expired and creds.refresh_token:
        creds.refresh(GoogleAuthRequest())
    return creds

def send_gmail_api(creds_blob, to_email, subject, body, sender_name="Manifest Apply") -> tuple[bool, str]:
    try:
        creds = _load_google_creds(creds_blob)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["To"]      = to_email
        msg["From"]    = sender_name
        msg.attach(MIMEText(body, "plain", "utf-8"))
        html = f"<html><body style='font-family:Arial;font-size:14px'>{body.replace(chr(10),'<br>')}</body></html>"
        msg.attach(MIMEText(html, "html", "utf-8"))
        raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        svc = google_build("gmail", "v1", credentials=creds)
        svc.users().messages().send(userId="me", body={"raw": raw}).execute()
        return True, ""
    except Exception as e:
        return False, str(e)[:150]

def _clean_email_text(text: str) -> str:
    """ایمیل باید متن ساده و رسمی باشه، نه Markdown. این تابع ستاره‌های
    Bold/Italic، #هدینگ‌ها و بک‌تیک‌های احتمالی خروجی AI رو پاک می‌کنه تا
    ایمیل نهایی که میره دست استاد/HR تمیز و حرفه‌ای باشه."""
    if not text:
        return text
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)   # **bold**
    text = re.sub(r"(?<!\w)\*(.+?)\*(?!\w)", r"\1", text)  # *italic*
    text = re.sub(r"__(.+?)__", r"\1", text)        # __underline__
    text = re.sub(r"`{1,3}(.+?)`{1,3}", r"\1", text)  # `code`
    text = re.sub(r"^#{1,6}\s*", "", text, flags=re.MULTILINE)  # # headings
    return text.strip()

def _looks_non_english(text: str) -> bool:
    """چک کیفیت قبل از ارسال: اگه بخش قابل‌توجهی از متن حروف غیرلاتین
    (فارسی/عربی/...) باشه، یعنی مدل به‌جای انگلیسی خالص چیز دیگه‌ای نوشته
    (مثلاً یه جمله فارسی قاطی کرده). در این حالت باید fallback (که همیشه
    انگلیسی تمیزه) استفاده بشه، نه این خروجی."""
    if not text:
        return True
    non_latin = len(re.findall(r"[\u0600-\u06FF\u0590-\u05FF\u0400-\u04FF]", text))
    return non_latin > max(5, len(text) * 0.02)

# ── AI Quality Control ─────────────────────────────────────────────
# قبل از ارسال هر ایمیل: طول + Spam Score + سلامت پایه‌ای متن چک می‌شه.
# خیلی از ایمیل‌های AI بیش از حد طولانی درمیان (با اینکه توی prompt حد
# کلمه گفته شده)؛ این‌جا به‌جای اعتماد کورکورانه به دستور prompt،
# واقعاً بعد از تولید هم اندازه‌گیری و در صورت لزوم کوتاه می‌شه.
MAX_EMAIL_WORDS = 230  # کمی بیشتر از حد ۲۲۰ کلمه‌ی prompt، برای اغماض جزئی

SPAM_TRIGGER_PHRASES = [
    "click here", "buy now", "act now", "limited time", "100% free",
    "risk-free", "guarantee", "no cost", "order now", "subscribe now",
    "congratulations you", "cash bonus", "earn money", "work from home",
    "act immediately", "don't miss", "exclusive deal", "winner",
]

def _estimate_spam_score(subject: str, body: str) -> int:
    """امتیاز اسپم‌بودن ۰ تا ۱۰۰ — بر اساس عبارت‌های محرک اسپم، تعداد
    علامت تعجب، کلمات ALL-CAPS، و لینک‌های زیاد. یک ایمیل academic
    cold-outreach سالم باید نزدیک صفر باشه."""
    text   = f"{subject or ''} {body or ''}"
    text_l = text.lower()
    score = 0
    for phrase in SPAM_TRIGGER_PHRASES:
        if phrase in text_l:
            score += 15
    score += min(20, text.count("!") * 5)
    caps_words = re.findall(r"\b[A-Z]{4,}\b", text)
    score += min(20, len(caps_words) * 4)
    links = re.findall(r"https?://", text)
    score += min(20, max(0, len(links) - 1) * 10)
    if re.search(r"\$\d", text):
        score += 10
    return min(100, score)

def _truncate_to_word_limit(body: str, max_words: int = MAX_EMAIL_WORDS) -> str:
    """اگه AI حد کلمه رو رعایت نکرد، به‌جای دور انداختن کل ایمیل (که یعنی
    یه AI call دیگه، کندتر) سعی می‌کنیم سر یه جمله‌ی کامل قطعش کنیم —
    نتیجه هنوز طبیعی می‌خونه، نه نصفه‌ولا."""
    words = (body or "").split()
    if len(words) <= max_words:
        return body
    truncated = " ".join(words[:max_words])
    last_end = max(truncated.rfind("."), truncated.rfind("!"), truncated.rfind("?"))
    if last_end > len(truncated) * 0.5:
        truncated = truncated[:last_end + 1]
    return truncated.strip()

def assess_email_quality(subject: str, body: str) -> dict:
    """جمع‌بندی سه چک قبل از ارسال: طول، Spam Score، و سلامت پایه‌ای
    متن (تکرار کلمه، خالی نبودن). خروجی: {"word_count","spam_score",
    "issues": [...], "ok": bool}. "ok=False" یعنی باید fallback
    (تمپلیت ثابت و تمیز) استفاده بشه به‌جای این متن."""
    body = body or ""
    words = len(body.split())
    spam_score = _estimate_spam_score(subject or "", body)
    issues = []
    if words > MAX_EMAIL_WORDS:
        issues.append(f"too_long({words}w)")
    if spam_score >= 40:
        issues.append(f"high_spam_score({spam_score})")
    if re.search(r"\b(\w{3,})\b(\s+\1\b){1,}", body, re.I):
        issues.append("repeated_words")
    if not body or len(body) < 80:
        issues.append("too_short_or_empty")
    return {"word_count": words, "spam_score": spam_score, "issues": issues, "ok": not issues}

async def ai_review_email(client: dict, prof: dict, subject: str, body: str,
                           papers: list[dict] | None = None) -> dict:
    """🟠 AI Reviewer — مرحله‌ی جدا و مستقل از Quality Control قبلی
    (assess_email_quality که فقط طول/spam-score/تکرار کلمه رو چک می‌کنه).
    این‌جا محتوا در برابر داده‌های واقعاً استخراج‌شده راستی‌آزمایی می‌شه:
      ۱. اسم استاد درست است؟
      ۲. عنوان (Professor/Associate/Assistant Professor/Dr.) درست است؟
      ۳. نام دانشگاه درست است (یا حداقل با داده‌ی واقعی در تناقض نیست)؟
      ۴. متن بیش‌ازحد عمومی نیست (حداقل یک اشاره‌ی مشخص به همین استاد دارد)؟
      ۵. جزئیاتی که در متن آمده واقعاً از داده‌های استخراج‌شده (papers/prof)
         آمده‌اند یا AI جعل کرده؟

    Pipeline خواسته‌شده: Generate Email → AI Reviewer → Send.
    Fail-safe: هر خطایی (AI جواب نده، فرمت خراب، تایم‌اوت) → عبور
    (ok=True) برمی‌گردونه، نه رد — یک reviewer خراب نباید جلوی کل ارسال
    رو بگیره؛ _force_correct_salutation و assess_email_quality همچنان
    به‌عنوان خط دفاعی قطعی/بدون-AI سرجاشون هستن."""
    prof_name  = (prof.get("name") or "").strip()
    university = (prof.get("university") or "").strip()
    title      = (prof.get("title") or "").strip()
    if not subject or not body:
        return {"ok": False, "issues": ["empty"], "reason": "empty subject/body"}

    facts_lines = []
    for p in (papers or prof.get("papers") or [])[:3]:
        t = p.get("title", "") if isinstance(p, dict) else str(p)
        if t:
            facts_lines.append(f"- {t}")
    dept = prof.get("department", "")
    if dept:
        facts_lines.append(f"- Department: {dept}")
    interests = prof.get("research_interests") or []
    if interests:
        facts_lines.append(f"- Research interests: {', '.join(interests[:5])}")
    facts_block = "\n".join(facts_lines) if facts_lines else "(no extracted facts available)"

    prompt = f"""You are a strict pre-send reviewer for a cold email to a professor. Check the email below
against the VERIFIED DATA about the professor (not your general knowledge). Be conservative — if in doubt,
flag it.

Verified professor data:
- Full name: {prof_name or '(unknown)'}
- Academic title: {title or '(unknown)'}
- University: {university or '(unknown)'}
Verified facts the email is allowed to reference:
{facts_block}

Email subject: {subject}
Email body:
{body}

Respond with STRICT JSON only, nothing else, exactly this shape:
{{"name_correct": true/false,
  "title_correct": true/false,
  "university_correct": true/false,
  "too_generic": true/false,
  "has_unsupported_claim": true/false,
  "reason": "one short sentence, empty string if all checks pass"}}

Rules:
- name_correct: false only if the salutation uses the wrong name, a garbled name, or something clearly not
  a person's name (e.g. a country or generic word instead of the professor's name).
- title_correct: false only if a title is stated that contradicts the verified academic title above (no
  title mentioned at all is fine — do not flag that).
- university_correct: false only if a university name is stated that contradicts or misspells the verified
  university above (no mention at all is fine — do not flag that).
- too_generic: true if the body has NO specific reference at all to this professor's actual research,
  department, or lab (i.e. it could be sent unchanged to any random professor).
- has_unsupported_claim: true if the email references a specific paper title, finding, number, project
  name, or funding source that is NOT present in the verified facts above."""

    try:
        ai = await call_ai(prompt, min_length=15)
        data = _extract_json_object(ai)
        if not data:
            return {"ok": True, "issues": [], "reason": "reviewer parse failed — passed through"}
        issues = []
        if not data.get("name_correct", True):
            issues.append("wrong_name")
        if not data.get("title_correct", True):
            issues.append("wrong_title")
        if not data.get("university_correct", True):
            issues.append("wrong_university")
        if data.get("too_generic", False):
            issues.append("too_generic")
        if data.get("has_unsupported_claim", False):
            issues.append("unsupported_claim")
        return {"ok": not issues, "issues": issues, "reason": str(data.get("reason", ""))[:200]}
    except Exception as e:
        logger.warning(f"ai_review_email for {prof_name}: {e}")
        return {"ok": True, "issues": [], "reason": "reviewer error — passed through"}


async def generate_and_review_email(client: dict, prof: dict, abstract: str = "",
                                     adapted_resume: str = "",
                                     papers: list[dict] | None = None) -> tuple[str, str, dict]:
    """Generate Email → AI Reviewer یک‌جا: write_prof_email رو صدا می‌زنه، بعد
    ai_review_email روی خروجی اجرا می‌شه. اگه reviewer رد کرد (name/title/
    university اشتباه، بیش‌ازحد عمومی، یا ادعای بدون پشتوانه)، به‌جای همون
    متن از _fallback_email استفاده می‌شه — که مستقیماً و بدون AI از خودِ
    داده‌ی prof ساخته می‌شه، پس نام/عنوان/دانشگاهش تضمینی درسته."""
    subject, body = await write_prof_email(client, prof, abstract, adapted_resume, papers=papers)
    review = await ai_review_email(client, prof, subject, body, papers=papers)
    if not review["ok"]:
        logger.info(f"🔎 AI Reviewer rejected email for '{prof.get('name','?')}': "
                    f"{review['issues']} ({review['reason']}) — استفاده از fallback قطعی")
        subject = f"Prospective PhD Inquiry — {client.get('field','')}"
        body = _force_correct_salutation(_fallback_email(client, prof), prof)
    return subject, body, review


async def adapt_resume_for_target(client: dict, target: dict, target_type: str = "professor") -> str:
    """برای هر استاد یا آگهی کار، خلاصه‌ای از رزومه می‌سازیم که دقیقاً
    مرتبط‌ترین بخش‌های پیشینه‌ی کاربر رو برجسته کنه — نه یه رزومه‌ی ثابت
    برای همه. نتیجه‌ی این تابع مستقیم داخل prompt نوشتن ایمیل/کاور لتر
    استفاده می‌شه، نه جای رزومه‌ی خام اصلی.

    این تابع به هیچ‌وجه نمی‌تونه loop رو متوقف کنه:
    - هر خطایی رو می‌بلعه و رزومه‌ی خام اصلی رو برمی‌گردونه (fallback)
    - timeout داره (از call_ai ارث می‌بره) تا بی‌نهایت صبر نکنه
    - نتیجه‌ی خالی هم به‌جای خطا، fallback رو برمی‌گردونه"""
    raw_resume = (client.get("resume") or "").strip()
    if not raw_resume or len(raw_resume) < 50:
        return raw_resume  # رزومه‌ای نداریم، چیزی برای تطبیق نیست

    try:
        if target_type == "professor":
            papers_txt = "; ".join(
                p.get("title","") for p in (target.get("pub_list") or [])[:3]
            ) or target.get("papers_snippet", "") or ""
            target_context = (
                f"Professor: {target.get('name','')}\n"
                f"University: {target.get('university','')}\n"
                f"Research focus / recent papers: {papers_txt[:400]}\n"
                f"Research snippet: {target.get('snippet','')[:300]}"
            )
            instruction = (
                "Select and rewrite the 4-5 most relevant highlights from the applicant's resume "
                "that would resonate most with THIS professor's specific research. "
                "Emphasize skills, projects, or results that align with their research. "
                "Output ONLY the tailored highlight bullets, no intro, no headers, no extra text."
            )
        else:  # job
            target_context = (
                f"Job title: {target.get('title','')}\n"
                f"Company: {target.get('company','')}\n"
                f"Description: {target.get('description','')[:400]}\n"
                f"Required skills/tags: {', '.join(target.get('tags') or [])[:200]}"
            )
            instruction = (
                "Select and rewrite the 4-5 most relevant highlights from the applicant's resume "
                "that best match THIS specific job's requirements. "
                "Emphasize matching skills, measurable achievements, and relevant experience. "
                "Output ONLY the tailored highlight bullets, no intro, no headers, no extra text."
            )

        prompt = f"""You are a professional resume optimizer helping a job/PhD applicant.
Task: {instruction}
Target:
{target_context}
Applicant full resume:
{raw_resume[:2000]}
Applicant field: {client.get('field','')} | Education: {client.get('education','')}
Rules:
- English only. Plain text. No markdown bold/italic/headers.
- Maximum 5 bullet points, each starting with a dash (-).
- Each bullet: concrete, specific, achievement-focused. No generic filler phrases.
- If the resume has no clear overlap with the target, pick the closest skills and note transferability.
Output ONLY the 4-5 bullet points:"""

        result = await call_ai(prompt)
        if not result or len(result.strip()) < 30:
            return raw_resume  # AI جواب نداد یا کوتاه بود — fallback

        cleaned = _clean_email_text(result.strip())
        # یه چک ساده: باید حداقل یه dash/bullet داشته باشه
        if "-" not in cleaned and "•" not in cleaned:
            return raw_resume

        return cleaned

    except Exception as e:
        logger.warning(f"adapt_resume_for_target: {e} — falling back to raw resume")
        return raw_resume  # هیچ‌وقت خطا به loop بیرونی منتشر نمی‌شه

async def write_prof_email(client: dict, prof: dict, abstract: str = "",
                           adapted_resume: str = "", papers: list[dict] | None = None) -> tuple[str, str]:
    """(subject, body)
    adapted_resume: نسخه‌ی بهینه‌شده‌ی رزومه برای همین استاد خاص.
    اگه خالی باشه، از رزومه‌ی خام اصلی استفاده می‌شه.
    papers: لیست مقالات واقعی این استاد (حداکثر ۳ تای اول استفاده می‌شه) —
    این‌ها تنها «حقایقی»ان که به AI اجازه‌ی اشاره بهشون داده می‌شه، تا
    ادعای جعلی مثل «مقاله‌ی اخیرتون درباره‌ی X رو خوندم» وقتی واقعاً چنین
    مقاله‌ای بهش داده نشده، تولید نشه."""
    resume_to_use = (adapted_resume.strip() if adapted_resume and len(adapted_resume) > 30
                     else (client.get("resume", "") or "")[:1200])

    # ── حقایق مجاز — حداکثر ۳ مقاله‌ی واقعی با عنوان/سال/چکیده ──────
    # اگه papers کامل در دسترس نباشه (مثلاً caller قدیمی فقط abstract تک
    # پاراگرافی داده)، از همون best_paper/abstract قبلی fallback می‌کنیم
    # تا هیچ‌وقت خالی نمونه.
    if papers:
        facts_lines = []
        for i, p in enumerate(papers[:3], 1):
            title = (p.get("title") or "").strip()
            year  = p.get("year", "?")
            abst  = (p.get("abstract") or "")[:220].strip()
            if title:
                facts_lines.append(f'{i}. "{title}" ({year})' + (f": {abst}" if abst else ""))
        facts_block = "\n".join(facts_lines) if facts_lines else "(no specific paper details available)"
    else:
        papers_raw = prof.get("papers") or []
        best_paper_raw = papers_raw[0] if papers_raw else ""
        best_paper = best_paper_raw.get("title", "") if isinstance(best_paper_raw, dict) else str(best_paper_raw)
        facts_block = f'1. "{best_paper}": {abstract[:300]}' if best_paper else "(no specific paper details available)"

    # ── حقایق تکمیلی — عنوان/دپارتمان/علایق پژوهشی/پروژه‌های آزمایشگاه/
    # Funding فعال ────────────────────────────────────────────────────
    # این‌ها هم دقیقاً مثل papers، فقط از داده‌ی واقعاً استخراج‌شده (نه
    # حدس AI) میان — تا شخصی‌سازی محدود به «فقط یک مقاله» نمونه و ایمیل
    # واقعاً به دپارتمان/علایق/آزمایشگاه/بودجه‌ی همون استاد خاص اشاره کنه،
    # وقتی این دیتا موجود باشه.
    extra_facts_lines = []
    title_line = prof.get("title", "")
    dept = prof.get("department", "")
    if dept:
        extra_facts_lines.append(f"Department: {dept}")
    interests = prof.get("research_interests") or []
    if interests:
        extra_facts_lines.append(f"Stated research interests: {', '.join(interests[:5])}")
    lab = prof.get("lab_analysis") or {}
    lab_projects = lab.get("projects") or []
    if lab_projects:
        extra_facts_lines.append(f"Current lab projects: {', '.join(lab_projects[:4])}")
    if prof.get("has_funding") and prof.get("funding_evidence"):
        extra_facts_lines.append(f"Active funding signal: {prof['funding_evidence']}")
    if prof.get("accepting_students") and prof.get("accepting_evidence"):
        extra_facts_lines.append(f"Recruiting signal: {prof['accepting_evidence']}")
    extra_facts_block = "\n".join(extra_facts_lines) if extra_facts_lines else "(none available)"

    prompt = f"""Write a concise, personalized cold email (max 220 words) from a prospective PhD/research applicant.
Rules:
- The ENTIRE email (subject and body) must be written in formal, professional English — regardless of the language of the applicant data below.
- Plain text only. Do NOT use Markdown formatting: no **bold**, no *italics*, no # headings, no bullet symbols. Write it exactly as a real email a human would type.
- Do NOT write your own opening salutation line (no "Dear ..." / "Dear Sir/Madam" / anything). Start the BODY directly with the first sentence — the correct salutation (including the professor's exact title, e.g. "Dear Associate Professor Smith,") is added automatically afterward from verified data, so writing one yourself would be discarded and risks a wrong name or title.
- Open with something specific about the professor's work, and connect it to CONCRETE details from the applicant's background below (specific project, skill, or result) — not generic phrases. Clear, polite ask at the end.
- You may draw on the department, stated research interests, lab projects, and funding/recruiting signals below (if provided) to make the email feel genuinely researched — not just a single paper reference. Weave in at most one or two of these naturally; do not list them mechanically.
- CRITICAL ANTI-HALLUCINATION RULE: You may ONLY reference facts explicitly listed below (publications, department, research interests, lab projects, funding/recruiting signals). Do NOT claim to have "read", "followed", or be "familiar with" any paper, result, or detail that is not explicitly listed there. Do NOT invent specific numbers, methods, findings, project names, or funding sources. If the listed details are limited, keep the connection to the professor's general research area honest and general rather than fabricating specifics.
Professor: {prof.get('name','Professor')}{f' ({title_line})' if title_line else ''} | University: {prof.get('university','')}
Professor's actual recent publications (verified facts — max 3):
{facts_block}
Additional verified facts about this professor/lab (use sparingly, do not list mechanically):
{extra_facts_block}
Applicant: {client.get('first_name','Applicant')} | Field: {client.get('field','')} | Education: {client.get('education','')} | Publications: {client.get('publications','0')}
Applicant's most relevant highlights for this professor:
{resume_to_use if resume_to_use else "(no resume text provided)"}
Goal: {client.get('visa_type','PhD position')}
Respond EXACTLY: SUBJECT: [line]\nBODY:\n[body text only]"""

    ai = await call_ai(prompt)
    sub_m = re.search(r"SUBJECT:\s*(.+)", ai or "", re.I)
    bod_m = re.search(r"BODY:\s*(.+)", ai or "", re.I | re.DOTALL)
    subject = _clean_email_text(sub_m.group(1).strip()) if sub_m else f"Prospective PhD Inquiry — {client.get('field','')}"
    body    = _clean_email_text(bod_m.group(1).strip()) if bod_m else _fallback_email(client, prof)

    # ── AI Quality Control (قبل از ارسال) ────────────────────────────
    # ترتیب: زبان → طول (کوتاه‌سازی هوشمند، نه دور انداختن) → Spam Score/
    # سلامت متن. خیلی از ایمیل‌های AI با اینکه توی prompt حد ۲۲۰ کلمه
    # گفته شده، بازم طولانی درمیان — این‌جا واقعاً بعد از تولید اندازه‌گیری
    # می‌شه، نه فقط اعتماد به دستور prompt.
    if not body or _looks_non_english(body) or _looks_non_english(subject):
        subject = f"Prospective PhD Inquiry — {client.get('field','')}"
        body = _fallback_email(client, prof)
    else:
        body = _truncate_to_word_limit(body)
        quality = assess_email_quality(subject, body)
        if not quality["ok"]:
            logger.info(f"Email quality check failed for {prof.get('name','?')}: {quality['issues']} — using fallback template")
            subject = f"Prospective PhD Inquiry — {client.get('field','')}"
            body = _fallback_email(client, prof)
    # TASK 4/15: صرف‌نظر از این‌که کدوم مسیر بالا (AI، fallback زبان،
    # fallback کیفیت) اجرا شده، سلام‌نامه رو یک‌بار برای همیشه از روی
    # داده‌ی واقعی prof['name'] درست/جایگزین می‌کنیم.
    body = _force_correct_salutation(body, prof)
    return subject, body

def _build_salutation(prof: dict) -> str:
    """طبق TASK 4 و پیشنهاد تکمیلی «عدم اتکای کامل به AI»: سلام‌نامه‌ی
    ایمیل همیشه از روی prof['name'] واقعی (داده‌ی استخراج‌شده، نه چیزی که
    AI حدس زده) ساخته می‌شه — تا هیچ‌وقت چیزهایی مثل 'Dear Professor,'،
    'Dear Sir/Madam'، یا اسم اشتباه (مثلاً 'Dear Professor Germany' — که
    اسم کشور به‌جای اسم استاد نوشته شده) به ایمیل نهایی نرسه.

    Email Personalization: اگه عنوان دقیق (Associate/Assistant Professor،
    Dr. و ...) از صفحه‌ی واقعیِ استاد استخراج شده باشه (prof['title'])،
    به‌جای همیشه «Professor» عمومی، همون عنوان دقیق استفاده می‌شه — قبلاً
    حتی یک Associate/Assistant Professor هم همیشه «Dear Professor X»
    خطاب می‌شد که رسمی/دقیق نبود."""
    name = (prof.get("name") or "").strip()
    if not name:
        return "Dear Professor,"
    # اگه عنوان از قبل توی خود اسم استخراج‌شده هست، دوباره تکرارش نمی‌کنیم.
    stripped = re.sub(r"^(prof\.?|professor|associate professor|assistant professor|dr\.?|doctor)\s+",
                       "", name, flags=re.I).strip()
    if not stripped:
        return "Dear Professor,"
    extracted_title = (prof.get("title") or "").strip()
    # فقط عنوان‌های آکادمیک شناخته‌شده رو قبول می‌کنیم — اگه AI چیز عجیبی
    # مثل نام دپارتمان رو اشتباهی توی title گذاشته باشه، امن‌ترین حالت
    # (Professor) رو استفاده می‌کنیم، نه یه عنوان نامعتبر.
    title_l = extracted_title.lower()
    if re.fullmatch(r"(associate|assistant)\s+professor", title_l):
        salutation_title = extracted_title.title()
    elif "professor" in title_l:
        salutation_title = "Professor"
    elif title_l in ("dr", "dr.", "doctor"):
        salutation_title = "Dr."
    else:
        salutation_title = "Professor"
    return f"Dear {salutation_title} {stripped},"

def _force_correct_salutation(body: str, prof: dict) -> str:
    """صرف‌نظر از این‌که AI (یا نسخه‌ی fallback) چه خط سلامی نوشته، خط اول
    ایمیل رو با سلام درست جایگزین می‌کنیم — این‌طوری، حتی اگه AI اشتباه
    یا ناقص بنویسه، خروجی نهایی همیشه صحیحه (TASK 4 / TASK 15)."""
    if not body:
        return body
    correct = _build_salutation(prof)
    parts = body.split("\n", 1)
    if parts and re.match(r"^\s*dear\b", parts[0], re.I):
        rest = parts[1].lstrip("\n") if len(parts) > 1 else ""
    else:
        rest = body
    return f"{correct}\n\n{rest}" if rest else correct

def _fallback_email(client, prof) -> str:
    """Fallback قطعی (بدون AI) — همیشه کار می‌کنه، پس نباید هیچ‌وقت روی
    داده‌ای که ممکنه موجود نباشه crash کنه. Email Personalization: قبلاً
    این تابع همیشه دقیقاً یک الگوی ثابت بود («Your work on "{یک مقاله}"
    ...») صرف‌نظر از این‌که چه اطلاعات بیشتری (دپارتمان، علایق پژوهشی،
    پروژه‌های آزمایشگاه، Funding، ۲-۳ مقاله) در دسترس بود. الان — دقیقاً
    مثل نسخه‌ی AI — از هر کدوم از این‌ها که واقعاً استخراج شده باشن
    استفاده می‌کنه؛ چیزی که موجود نیست، به‌سادگی حذف می‌شه (نه حدس زده
    می‌شه)."""
    pn   = (prof.get("name") or "Professor").split()[-1]
    f    = client.get("field","your field")
    name = client.get("first_name","Applicant")
    edu  = client.get("education","graduate")
    goal = client.get("visa_type","PhD or research position")

    def _paper_title(p_raw):
        if isinstance(p_raw, dict):
            return (p_raw.get("title") or "").strip()
        return str(p_raw).strip() if p_raw else ""

    papers = prof.get("papers") or []
    titles = [t for t in (_paper_title(p) for p in papers[:2]) if t]

    if titles:
        if len(titles) == 1:
            work_line = f'Your work on "{titles[0][:70]}" directly relates to my research in {f}.'
        else:
            work_line = (f'Your work on "{titles[0][:70]}" and "{titles[1][:70]}" '
                         f'directly relates to my research in {f}.')
    else:
        work_line = f"Your research in {f} caught my attention."

    dept = (prof.get("department") or "").strip()
    dept_line = f" I am particularly drawn to the work being done in {dept}." if dept else ""

    interests = prof.get("research_interests") or []
    interest_line = ""
    if interests:
        interest_line = f" I was glad to see your stated focus on {', '.join(interests[:2])}, which aligns closely with my own interests."

    lab_projects = (prof.get("lab_analysis") or {}).get("projects") or []
    project_line = f" I would also be excited to contribute to your lab's work on {lab_projects[0]}." if lab_projects else ""

    funding_line = ""
    if prof.get("has_funding") and prof.get("funding_evidence"):
        funding_line = f" I understand your group may currently have funding available ({prof['funding_evidence']})."

    return f"""Dear Professor {pn},

{work_line}{dept_line}{interest_line}

I am {name}, a {edu} graduate specializing in {f}. I am actively seeking a {goal} opportunity and believe your research group would be an ideal environment.{project_line}{funding_line}

I would greatly appreciate a 15-minute conversation to discuss potential opportunities in your lab.

Best regards,
{name}"""

async def _revise_prof_email(client: dict, prof: dict, current_subject: str,
                              current_body: str, instruction: str) -> tuple[str, str]:
    """TASK 7 — ویرایش هوشمند ایمیل نمونه طبق دستور آزاد کاربر (مثلاً
    «رسمی‌تر شود»، «کوتاه‌تر شود»، «به مقاله استاد اشاره کن»، «درباره‌ی
    Funding بپرس» و ...) به‌جای نوشتن یک نسخه‌ی کاملاً تازه از صفر.
    برخلاف write_prof_email، اینجا هیچ فکت جدیدی (مقاله/دانشگاه/پروژه)
    اجازه‌ی وارد شدن نداره — فقط همون چیزی که از قبل توی متن هست بازنویسی
    می‌شه، تا خطر Hallucination جدید ایجاد نشه."""
    prompt = f"""Revise the following cold email to a professor according to the instruction below.
Rules:
- Keep it formal, professional English, plain text only (no Markdown: no **bold**, no # headings, no bullet symbols).
- Apply ONLY this instruction, nothing else: "{instruction}"
- Do NOT invent any new facts about the professor (papers, projects, results, funding) beyond what is already written in the current body below. If the instruction asks you to reference something not already present (e.g. a specific paper), keep the reference general/honest rather than fabricating specifics.
- Do NOT write your own opening salutation line (no "Dear ..."); start the BODY directly with the first sentence — the salutation is added automatically afterward.
- Keep it under 220 words.
Current subject: {current_subject}
Current body:
{current_body}
Respond EXACTLY: SUBJECT: [line]\nBODY:\n[body text only]"""
    ai = await call_ai(prompt)
    sub_m = re.search(r"SUBJECT:\s*(.+)", ai or "", re.I)
    bod_m = re.search(r"BODY:\s*(.+)", ai or "", re.I | re.DOTALL)
    subject = _clean_email_text(sub_m.group(1).strip()) if sub_m else current_subject
    body    = _clean_email_text(bod_m.group(1).strip()) if bod_m else current_body
    if not body or _looks_non_english(body) or _looks_non_english(subject):
        subject, body = current_subject, current_body
    else:
        body = _truncate_to_word_limit(body)
    body = _force_correct_salutation(body, prof)
    return subject, body

async def write_cover_letter(client: dict, job: dict, adapted_resume: str = "") -> tuple[str, str]:
    """Cover letter برای اپلای کار
    adapted_resume: نسخه‌ی بهینه‌شده‌ی رزومه برای همین شغل خاص."""
    resume_to_use = (adapted_resume.strip() if adapted_resume and len(adapted_resume) > 30
                     else (client.get("resume", "") or "")[:1200])
    fallback_body = (f"Dear Hiring Manager,\n\nI am applying for the {job.get('title','')} "
                      f"position at {job.get('company','your company')}.\n\nBest regards,\n"
                      f"{client.get('first_name','Applicant')}")
    prompt = f"""Write a professional cover letter (200 words max) for a job application.
Rules:
- The ENTIRE letter (subject and body) must be written in formal, professional English — regardless of the language of the applicant data below.
- Plain text only. Do NOT use Markdown formatting: no **bold**, no *italics*, no # headings, no bullet symbols. Write it exactly as a real email a human would type.
- Reference CONCRETE details from the applicant's background below (a specific project, skill, or achievement that fits this job) — not generic filler.
Job: {job.get('title','')} at {job.get('company','')} in {job.get('location','')}
Applicant: {client.get('first_name','Applicant')} | Field: {client.get('field','')} | Education: {client.get('education','')} | Experience: {client.get('work_exp','')}
Applicant's most relevant highlights for this job:
{resume_to_use if resume_to_use else "(no resume text provided)"}
Respond EXACTLY: SUBJECT: [email subject line]\nBODY:\n[cover letter text]"""

    ai = await call_ai(prompt)
    sub_m = re.search(r"SUBJECT:\s*(.+)", ai or "", re.I)
    bod_m = re.search(r"BODY:\s*(.+)", ai or "", re.I | re.DOTALL)
    subject = _clean_email_text(sub_m.group(1).strip()) if sub_m else f"Application for {job.get('title','')}"
    body    = _clean_email_text(bod_m.group(1).strip()) if bod_m else fallback_body
    if not body or len(body) < 60 or _looks_non_english(body) or _looks_non_english(subject):
        subject = f"Application for {job.get('title','')}"
        body = fallback_body
    else:
        body = _truncate_to_word_limit(body, max_words=210)  # کاور لتر کمی کوتاه‌تر از ایمیل استاده
        quality = assess_email_quality(subject, body)
        if not quality["ok"]:
            logger.info(f"Cover letter quality check failed for {job.get('title','?')}: {quality['issues']} — using fallback template")
            subject = f"Application for {job.get('title','')}"
            body = fallback_body
    return subject, body

# ================================================================
# AI SUPPORT — پشتیبانی در چت
# ================================================================

async def ai_support(question: str, context: dict) -> str:
    """پشتیبانی آنلاین با AI — در هر مرحله‌ای از فلو.
    memory_summary (اگر موجود باشه) خلاصه‌ی چیزهاییه که بات از قبل درباره‌ی
    همین کاربر یادشه (اساتید ایمیل‌زده‌شده و پاسخ‌ها، کشور مورد علاقه،
    رزومه، آخرین Applyها، مشاغل ناخواسته) — تا جواب‌ها واقعاً شخصی‌سازی‌شده
    باشن، نه جنریک."""
    memory_block = ""
    if context.get("memory_summary"):
        memory_block = f"\nWhat you already remember about this specific user:\n{context['memory_summary']}\n"
    prompt = f"""You are a helpful assistant for Manifest Apply immigration and job application service.
User context: service={context.get('service')}, step={context.get('step')}{memory_block}
User question: {question}
Answer briefly in Persian (Farsi). Max 150 words. Be helpful and specific. If the memory above is relevant to the question, use it naturally; don't mention that you were "given" this data.
If the memory above contains a "پرونده مهاجرتی" (Immigration Profile) block and the question is about a country/visa decision (e.g. "برای فلان کشور چطوره؟" / "شانسم چقدره؟"), answer as a short, concrete case analysis grounded ONLY in what's actually filled in that profile — a "با توجه به پرونده شما:" line followed by 2-4 short bullet points (e.g. strong points and the main weak point/gap). Never invent facts (age, scores, documents) that aren't in the profile; if a needed field is empty, say which field is missing instead of guessing."""
    answer = await call_ai(prompt)
    return answer or "متأسفانه در این لحظه پشتیبانی AI در دسترس نیست. با @manifestapply تماس بگیرید."

# ================================================================
# TELEGRAM SETUP
# ================================================================

from telegram import (Update, ReplyKeyboardMarkup, KeyboardButton,
                       InlineKeyboardMarkup, InlineKeyboardButton)
from telegram.ext import (Application, CommandHandler, MessageHandler,
                           CallbackQueryHandler, ContextTypes,
                           filters, BasePersistence, PersistenceInput)
from telegram.constants import ChatAction
from telegram.request import HTTPXRequest

# ── state keys ────────────────────────────────────────────────────
S = "svc_step"              # مرحله جاری (string)
S_SRV  = "svc_service"      # 'email' یا 'job'
S_TYPE = "svc_user_type"    # 'vip' یا 'normal'
S_SUB  = "svc_sub_id"       # subscription id
S_CLIENT = "svc_client"     # پروفایل کاربر
S_SMTP_E = "svc_smtp_email"
S_SMTP_P = "svc_smtp_pass"
S_SMTP_H = "svc_smtp_host"
S_SMTP_PORT = "svc_smtp_port"
S_AUTH_METHOD = "svc_auth_method"   # 'google' یا 'password'
S_PROFS  = "svc_profs"      # لیست اساتید
S_JOBS   = "svc_jobs"       # لیست شغل‌ها
S_PREVIEW_ID = "svc_preview_id"  # id ردیف preview_queue

# ── مقادیر step ───────────────────────────────────────────────────
ST_IDLE       = None
ST_MAIN_MENU  = "main_menu"
ST_SERVICE_DISCLAIMER = "service_disclaimer"  # هشدار اولیه قبل از شروع سرویس ایمیل به اساتید
ST_VIP_OR_NEW = "vip_or_new"
ST_VIP_CODE   = "vip_code"
ST_RECEIPT    = "receipt"
ST_FULLNAME   = "fullname"
ST_INFO_CONFIRM = "info_confirm"  # تایید نهایی اطلاعات قبل از رفتن سراغ پرداخت
ST_FULLNAME_CONFIRM = "fullname_confirm"
ST_PHONE      = "phone"
ST_COLLECT    = "collect"      # جمع‌آوری اطلاعات سرویس
ST_PROFILE_REUSE = "profile_reuse"  # تایید/رد استفاده از پروفایل ذخیره‌شده (AI Career Profile)
ST_AUTH_CHOICE = "auth_choice" # انتخاب: گوگل یا App Password
ST_WAIT_GOOGLE = "wait_google_auth"  # منتظر برگشت کاربر از مرورگر گوگل
ST_SMTP_EMAIL = "smtp_email"
ST_SMTP_PASS  = "smtp_pass"
ST_CUSTOM_SMTP = "custom_smtp"  # وقتی حدس‌های خودکار جواب نداد، کاربر خودش سرور/پورت را می‌دهد
ST_CONFIRM_PROF_SEARCH = "confirm_prof_search"  # تایید رشته/کشور قبل از جستجوی اساتید
ST_PROF_LIST  = "prof_list"
ST_JOB_FIELD  = "job_field"
ST_PREVIEW    = "preview"      # انتظار تایید ایمیل نمونه
ST_PREVIEW_SETTINGS = "preview_settings"  # تغییر رشته/کشور/رزومه از وسط پیش‌نمایش
ST_SENDING    = "sending"
ST_SUPPORT    = "support"      # پشتیبانی AI
ST_MEMORY_MENU    = "memory_menu"     # منوی AI Memory
ST_MEMORY_DISLIKE = "memory_dislike"  # منتظر تایپ شغل/شرکت ناخواسته
ST_IMM_MENU  = "immigration_profile_menu"  # منوی پرونده‌ی مهاجرتی (مشاهده/انتخاب فیلد)
ST_IMM_EDIT  = "immigration_profile_edit"  # منتظر مقدار جدید یک فیلد از پرونده‌ی مهاجرتی
# Personal Visa Agent — کاربر باید صریحاً ددلاین واقعی رو در دو قدم وارد
# کنه (اول تاریخ، بعد این‌که ددلاین برای چیه) — این تاریخ هیچ‌وقت توسط AI
# حدس زده نمی‌شه، فقط همینی که کاربر خودش تایپ می‌کنه.
ST_IMM_DEADLINE_DATE  = "immigration_deadline_date"
ST_IMM_DEADLINE_LABEL = "immigration_deadline_label"
ST_IMAP_OPTIN     = "imap_optin_menu" # روشن/خاموش کردن تشخیص خودکار bounce

# ── استعلام سفارت (فیچر جانبی، مستقل از سرویس ایمیل/شغل) ────────────
ST_EMBASSY_NATIONALITY = "embassy_nationality"
ST_EMBASSY_2ND_PASSPORT = "embassy_2nd_passport"
ST_EMBASSY_2ND_COUNTRY  = "embassy_2nd_passport_country"
ST_EMBASSY_COUNTRY = "embassy_country"
ST_EMBASSY_CATEGORY = "embassy_category"
ST_EMBASSY_RESULT = "embassy_result"

# ── Keyboards ─────────────────────────────────────────────────────
def mk(*rows): return ReplyKeyboardMarkup([[KeyboardButton(b) for b in r] for r in rows], resize_keyboard=True)

MAIN_KB = mk(
    ["📧 ارسال ایمیل به اساتید"],
    ["💼 اپلای کار حرفه‌ای"],
    ["🔎 اسکن ATS رزومه"],
    ["🔍 استعلام سفارت"],
    ["🧠 حافظه من (AI Memory)"],
    ["🆘 پشتیبانی"],
)
BACK_KB = mk(["🔙 برگشت"])

def embassy_nationality_kb():
    return mk(["🇮🇷 ایرانی"], ["ملیت دیگر (تایپ کنید)"], ["🔙 برگشت"])

def embassy_2nd_passport_kb():
    return mk(["✅ دارم"], ["❌ ندارم"], ["🔙 برگشت"])

def embassy_country_kb():
    rows = [[site["label_fa"]] for site in EMBASSY_OFFICIAL_SITES.values()]
    rows.append(["🔙 برگشت"])
    return mk(*rows)

def embassy_category_kb():
    rows = [[label] for label in EMBASSY_INQUIRY_KEYS.values()]
    rows.append(["🔙 برگشت"])
    return mk(*rows)

_EMBASSY_LABEL_TO_KEY = {v["label_fa"]: k for k, v in EMBASSY_OFFICIAL_SITES.items()}
_EMBASSY_CATEGORY_LABEL_TO_KEY = {v: k for k, v in EMBASSY_INQUIRY_KEYS.items()}

async def _start_embassy_inquiry(update, context):
    """نقطه‌ی ورود مشترک — از منوی اصلی یا از منوی بعد از پرداخت صدا زده
    می‌شه. prev_{S} ذخیره می‌شه تا دکمه‌ی برگشت دقیقاً به همون‌جایی که
    کاربر ازش وارد شده برگرده، نه همیشه منوی اصلی."""
    ud = context.user_data
    ud[f"prev_{S}"] = ud.get(S)
    ud[S] = ST_EMBASSY_NATIONALITY
    await safe(update.message,
        "🔍 استعلام سفارت\n\n"
        "این بخش مستقیماً از سایت رسمی سفارت/مرجع مهاجرتی کشور مقصد شما "
        "اطلاعات به‌روز می‌گیره تا توی روند اپلای کمکتون کنه.\n\n"
        "۱️⃣ ملیت شما چیه؟",
        reply_markup=embassy_nationality_kb())
    return 0
VIP_KB  = mk(["⭐ مشتری VIP هستم"], ["🆕 تازه‌وارد هستم"], ["🔙 برگشت"])

def memory_menu_kb():
    return mk(
        ["📋 نمایش خلاصه حافظه"],
        ["🗂 پرونده مهاجرتی من"],
        ["📬 ثبت پاسخ اساتید"],
        ["📭 ردیابی خودکار ایمیل‌های برگشتی"],
        ["🚫 افزودن شغل/شرکت ناخواسته"],
        ["🔙 برگشت"],
    )

def immigration_profile_kb(visa_agent_on: bool = False):
    rows = [[label] for _, label in IMMIGRATION_PROFILE_FIELDS]
    rows.append(["⏰ تنظیم ددلاین واقعی"])
    rows.append(["🔴 خاموش کردن دستیار هفتگی"] if visa_agent_on
                else ["🧭 روشن کردن دستیار هفتگی مهاجرت"])
    rows.append(["🔙 برگشت"])
    return mk(*rows)

def imap_optin_kb(enabled: bool):
    return mk(
        ["🔴 خاموش کن"] if enabled else ["✅ روشن کن (با رضایت من)"],
        ["🔙 برگشت"],
    )

def auth_choice_kb():
    rows = []
    if GOOGLE_LOGIN_ENABLED:
        rows.append(["🔵 ورود سریع با گوگل (پیشنهادی)"])
    rows.append(["🔑 با Gmail App Password"])
    rows.append(["🔙 برگشت"])
    return mk(*rows)

# قبل از این‌که بات واقعاً جستجو (اساتید یا فرصت شغلی) رو شروع کنه، این
# صفحه رشته/کشور رو دقیقاً همون چیزی که قراره باهاش جستجو کنه نشون میده
# و منتظر تایید صریح کاربر می‌مونه — یعنی هیچ‌وقت با یه مقدار پیش‌فرض یا
# قدیمی (که کاربر ندیده) جستجو یا اپلای شروع نمی‌شه.
SEARCH_CONFIRM_KB = mk(["✅ تایید و شروع جستجو"], ["✏️ ویرایش رشته/کشور"], ["🔙 برگشت"])

async def _show_manual_offer(update, context, reason_text: str):
    """صفحه‌ی «هر دو تلاش شکست خورد» رو نشون می‌ده. برخلاف smtp_fail_count
    (که هر بار کاربر یه ایمیل/پسورد تازه امتحان می‌کنه صفر می‌شه)، شمارنده‌ی
    این تابع هیچ‌وقت ریست نمی‌شه — یعنی بعد از ۳ دور کامل شکست (نه ۳ تلاش
    خام، بلکه ۳ باری که کاربر به همین صفحه رسیده)، گزینه‌ی retry رو کلاً
    حذف می‌کنیم تا کاربر توی یه حلقه‌ی بی‌پایان گیر نکنه."""
    ud = context.user_data
    ud["_manual_offer_rounds"] = ud.get("_manual_offer_rounds", 0) + 1
    rounds = ud["_manual_offer_rounds"]

    if rounds >= 3:
        await safe(update.message,
            f"{reason_text}\n\n"
            "چند بار امتحان کردیم و نشد — برای اینکه وقتتون تلف نشه، بریم سراغ حالت آماده:\n\n"
            "📋 بات همین الان موضوع، متن ایمیل، اطلاعات استاد و بهترین زمان ارسال رو "
            "براتون آماده می‌کنه و شما فقط کپی/پیست و ارسال می‌کنید.",
            reply_markup=mk(["📋 بات آماده کنه، خودم بفرستم"], ["🔙 برگشت"]))
        return

    await safe(update.message,
        f"{reason_text}\n\n"
        "می‌توانید یکی از گزینه‌های زیر را انتخاب کنید:\n\n"
        "① امتحان با سرور SMTP دیگر\n(اگر تنظیمات SMTP دیگری دارید)\n\n"
        "② امتحان با ایمیل یا App Password دیگر\n(مثلاً Gmail یا Outlook دیگر)\n\n"
        "③ دریافت متن آماده ایمیل\n(موضوع، متن، اطلاعات استاد و زمان پیشنهادی ارسال)",
        reply_markup=mk(
            ["🛠 خودم آدرس SMTP رو دارم"],
            ["🔁 دوباره با ایمیل/پسورد دیگه"],
            ["📋 بات آماده کنه، خودم بفرستم"],
            ["🔙 برگشت"]))

PREVIEW_KB = mk(
    ["✅ تایید و ارسال (بقیه خودکار ادامه پیدا می‌کنه)"],
    ["⏭ بررسی استاد بعدی"],
    ["✏️ ویرایش موضوع"],
    ["✏️ ویرایش متن ایمیل"],
    ["🔄 نسخه جدید بنویسید"],
    ["⚙️ تغییر تنظیمات (رشته/کشور/رزومه)"],
    ["🔙 برگشت"],
)

# وقتی از وسط پیش‌نمایش، کاربر می‌خواد رشته/کشور/رزومه رو عوض کنه — بدون
# اینکه لیست اساتید/پیش‌نمایش فعلی‌اش کامل از دست بره یا به منوی اصلی
# پرتاب بشه. Parent این State دقیقاً ST_PREVIEW است (نه Main Menu).
PREVIEW_SETTINGS_KB = mk(
    ["🌍 تغییر کشور / دانشگاه هدف"],
    ["🔬 تغییر رشته / تخصص"],
    ["📝 تغییر رزومه"],
    ["🔙 برگشت"],
)

# TASK 7 — گزینه‌های آماده‌ی ویرایش متن (کاربر می‌تونه یکی از این‌ها رو
# بزنه یا خودش آزادانه دستور دیگه‌ای تایپ کنه).
EDIT_BODY_SUGGEST_KB = mk(
    ["رسمی‌تر شود"],
    ["کوتاه‌تر شود"],
    ["پروژه‌ها بیشتر معرفی شود"],
    ["رزومه بیشتر معرفی شود"],
    ["مقاله استاد اشاره شود"],
    ["Funding پرسیده شود"],
    ["🔙 برگشت"],
)

# ── ارسال ایمن ────────────────────────────────────────────────────
import random
import httpx
from telegram.error import RetryAfter, TimedOut, NetworkError

# روی هاستینگ‌هایی که ترافیک خروجی از یک پراکسی داخلی رد می‌شه (مثل
# PythonAnywhere)، بعضی وقتا خود همون پراکسی چند ده ثانیه (یا حتی چند
# دقیقه) زیر بار قفل می‌کنه و 503 برمی‌گردونه — یه خطای واقعی توی بات یا
# قطعی تلگرام نیست، فقط یه لایه‌ی بین راهی موقتاً جواب نمی‌ده. قبلاً فقط
# ۳ تلاش با فاصله‌ی کوتاه (مجموعاً ~۱۲ ثانیه) امتحان می‌شد که برای این
# نوع outageهای طولانی‌تر کافی نبود و پیام گم می‌شد. الان با تعداد تلاش
# بیشتر و backoff نمایی سقف‌دار (+jitter تا چند تلاش هم‌زمان قفل یه لحظه
# رو نشکنن) مجموع پنجره‌ی retry به چند دقیقه می‌رسه — بدون این‌که هیچ‌وقت
# بی‌نهایت منتظر بمونیم یا رشته‌ی اصلی بات رو قفل کنیم.
NETWORK_RETRY_ATTEMPTS   = 8
NETWORK_RETRY_BASE_SEC   = 2
NETWORK_RETRY_CAP_SEC    = 30
# httpx گاهی خطاهای خودش (ProxyError, ConnectError, ReadError, ...) رو
# مستقیم بالا می‌ده، بدون این‌که python-telegram-bot همیشه بتونه بپیچدش
# توی NetworkError خودش — پس صریحاً هم اینا رو retry می‌کنیم.
_NETWORK_EXC = (TimedOut, NetworkError, httpx.HTTPError, httpx.TransportError, ConnectionError, OSError)

def _network_backoff(attempt: int) -> float:
    base = min(NETWORK_RETRY_BASE_SEC * (2 ** attempt), NETWORK_RETRY_CAP_SEC)
    return base + random.uniform(0, base * 0.3)

async def safe(msg, text: str, **kw):
    """ارسال پیام که در برابر Flood Control تلگرام (RetryAfter) و خطاهای
    موقت شبکه/پراکسی (که ممکنه چند ده ثانیه طول بکشن) مقاوم است، به‌جای
    اینکه بلافاصله شکست بخورد."""
    text = str(text)
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        sent = False
        for attempt in range(NETWORK_RETRY_ATTEMPTS):
            try:
                await msg.reply_text(chunk, **kw)
                sent = True
                break
            except RetryAfter as e:
                # تلگرام صریحاً گفته چقدر صبر کنیم — Flood Control است، خطای واقعی نیست
                wait = min(e.retry_after + 1, 60)
                logger.warning(f"Flood control: sleeping {wait}s before retry")
                await asyncio.sleep(wait)
            except _NETWORK_EXC as e:
                wait = _network_backoff(attempt)
                logger.warning(
                    f"safe send network hiccup (attempt {attempt+1}/{NETWORK_RETRY_ATTEMPTS}, "
                    f"retrying in {wait:.1f}s): {type(e).__name__}: {e}")
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error(f"safe send error: {e}")
                try:
                    await msg.reply_text("⚠️ خطا در ارسال پیام. دوباره /start بزنید.")
                except Exception:
                    pass
                return
        if not sent:
            # همه‌ی تلاش‌ها با خطای شبکه/پراکسی شکست خوردن — دیگه سعی نمی‌کنیم
            # با همون کانال شکسته یه پیام خطا هم بفرستیم (فقط دوباره fail می‌شه)؛
            # فقط لاگ می‌کنیم تا خود پردازش پیام کاربر (state، دیتابیس و...) گم نشه.
            logger.error(f"safe send: giving up after {NETWORK_RETRY_ATTEMPTS} attempts (persistent network/proxy issue)")

# ── اطلاع به ادمین ────────────────────────────────────────────────
async def safe_send(bot, chat_id, text: str, **kw):
    """مثل safe() ولی با chat_id کار می‌کند، نه با یک Message موجود —
    لازم چون گاهی باید برای یک کاربر از context یک کاربر دیگر (مثلاً ادمین)
    پیام بفرستیم، جایی که msg.reply_text در دسترس نیست."""
    text = str(text)
    for chunk in [text[i:i+4000] for i in range(0, len(text), 4000)]:
        sent = False
        for attempt in range(NETWORK_RETRY_ATTEMPTS):
            try:
                await bot.send_message(chat_id=chat_id, text=chunk, **kw)
                sent = True
                break
            except RetryAfter as e:
                wait = min(e.retry_after + 1, 60)
                logger.warning(f"Flood control: sleeping {wait}s before retry")
                await asyncio.sleep(wait)
            except _NETWORK_EXC as e:
                wait = _network_backoff(attempt)
                logger.warning(
                    f"safe_send network hiccup (attempt {attempt+1}/{NETWORK_RETRY_ATTEMPTS}, "
                    f"retrying in {wait:.1f}s): {type(e).__name__}: {e}")
                await asyncio.sleep(wait)
            except Exception as e:
                logger.error(f"safe_send error: {e}")
                return
        if not sent:
            logger.error(f"safe_send: giving up after {NETWORK_RETRY_ATTEMPTS} attempts (persistent network/proxy issue), chat_id={chat_id}")



async def notify_admin(bot, text: str, photo_id: str = None):
    if not ADMIN_CHAT_ID: return
    try:
        if photo_id:
            await bot.send_photo(chat_id=ADMIN_CHAT_ID, photo=photo_id, caption=text)
        else:
            await bot.send_message(chat_id=ADMIN_CHAT_ID, text=text)
    except Exception as e:
        logger.error(f"notify_admin error: {e}")

# ── اجرای امن تسک‌های پس‌زمینه ─────────────────────────────────────
# asyncio.create_task اگر کوروتین داخلش Exception بیندازد، آن را جایی
# گزارش نمی‌دهد مگر اینکه صریحاً نتیجه‌ی Task را بخوانیم — یعنی خطا کاملاً
# بی‌صدا گم می‌شود و کاربر برای همیشه منتظر پیام "ارسال کامل شد" می‌ماند.
# این wrapper هر خطایی را لاگ می‌کند، به ادمین اطلاع می‌دهد و برای کاربر
# هم پیام واضح می‌فرستد تا دستی /start بزند، به‌جای معلق ماندن بی‌دلیل.
async def _run_bg_task(coro, context, chat_id, label: str):
    try:
        await coro
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.critical(f"Background task '{label}' crashed for chat {chat_id}: {e}")
        traceback.print_exc()
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"❌ در حین «{label}» خطای غیرمنتظره‌ای رخ داد و فرآیند متوقف شد.\n"
                     f"تیم پشتیبانی مطلع شد. لطفاً /start بزنید یا با @manifestapply تماس بگیرید.")
        except Exception:
            pass
        await notify_admin(context.bot,
            f"🔥 Background task crash\nlabel={label}\nchat_id={chat_id}\nerror={e}")

# ================================================================
# HANDLERS
# ================================================================

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """کاربر می‌تواند هر زمان /cancel بزند تا هم یک ارسال در حال اجرا واقعاً
    متوقف شود (نه اینکه در پس‌زمینه مخفیانه ادامه پیدا کند)، هم مکالمه به
    منوی اصلی برگردد."""
    uid = update.effective_user.id
    task = _active_send_tasks.get(uid)
    stopped = False
    if task and not task.done():
        task.cancel()
        stopped = True
    context.user_data.clear()
    context.user_data[S] = ST_MAIN_MENU
    if stopped:
        await update.message.reply_text(
            "🛑 عملیات ارسال متوقف شد. هر ایمیل/اپلایی که تا همین لحظه واقعاً "
            "رفته بود، در گزارش‌های قبلی ثبت شده و دوباره ارسال نمی‌شود.\n\n"
            "به منوی اصلی برگشتید. /start را بزنید.")
    else:
        await update.message.reply_text(
            "چیزی برای لغو کردن در حال اجرا نبود. به منوی اصلی برگشتید. /start را بزنید.")

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ud  = context.user_data
    uid = update.effective_user.id
    await asyncio.to_thread(db_upsert_user, uid,
                             update.effective_user.username,
                             update.effective_user.first_name)

    # ── بررسی: آیا snapshot تایید‌شده‌ی معلقی داریم؟ (یعنی بین جمع‌آوری
    # اطلاعات و تایید پرداخت، بات ری‌استارت شده بود) اگه بله، به‌جای نشون
    # دادن منوی اصلی، مستقیم ادامه‌ی روند رو شروع می‌کنیم — چیزی از کاربر
    # دوباره نمی‌پرسیم.
    # نکته‌ی مهم بعد از اضافه‌شدن Task 2 (SQLiteUserDataPersistence): الان
    # ud با ری‌استارت خودکار از دیتابیس برمی‌گرده و ممکنه کاربر خیلی جلوتر
    # از "wait_approval" رفته باشه (مثلاً وسط SMTP یا لیست اساتید). این
    # snapshot محدود (فقط S_CLIENT/S_SRV/S_TYPE) نباید اون پیشرفت رو خراب
    # کنه؛ فقط وقتی واقعاً هنوز توی "wait_approval" گیر کردیم (یا ud خالیه
    # چون این اولین بارمونه) اجرا می‌شه.
    u = await asyncio.to_thread(db_get_user, uid)
    already_advanced = ud.get(S) not in (None, ST_MAIN_MENU, "wait_approval")
    if u and u.get("sub_id") and not already_advanced and await asyncio.to_thread(db_is_approved, u["sub_id"]):
        snap = await asyncio.to_thread(db_get_pending_snapshot, u["sub_id"])
        if snap:
            ud.clear()
            ud.update(snap)
            ud[S_SUB] = u["sub_id"]
            await update.message.reply_text(
                "🎉 پرداخت شما قبلاً تایید شده — ادامه‌ی روند از همان‌جایی که "
                "مانده بودید شروع می‌شود، چیزی از دست نرفته:")
            await _advance_after_approval(context.bot, update.effective_chat.id, ud, snap.get(S_SRV))
            await asyncio.to_thread(db_delete_pending_snapshot, u["sub_id"])
            return 0

    # نکته: /start همچنان مثل قبل یک ریست کامل و آگاهانه‌ست (دقیقاً مثل
    # /cancel) — اگه کاربر وسط یک مرحله باشه و به‌جای /start فقط پیام
    # بعدیش رو بفرسته (رفتار عادی)، dispatch() با ud بازیابی‌شده از
    # SQLiteUserDataPersistence دقیقاً از همون قدم ادامه می‌ده؛ اینجا فقط
    # مسیر خاصِ «تایید پرداخت معلق» بود که باید محافظت می‌شد.
    #
    # ── حالت جدید: کاربر approved و از قبل جلوتر از wait_approval رفته
    # بود (already_advanced=True) — یعنی وسط SMTP/جستجو/پیش‌نمایش کار
    # بوده و از بات بیرون افتاده یا هر دلیلی /start زده. این حالت باید از
    # ریست کامل معاف بشه: subscription و پرداختش دست‌نخورده‌ست، فقط باید
    # همون قدمی که توش بود رو دوباره نشونش بدیم — نه از اول ثبت‌نام/پرداخت.
    # مهم‌ترین چک همینه: خودِ subscription که ud[S_SUB] بهش اشاره می‌کنه
    # (نه لزوماً آخرین sub کلی کاربر) approved/completed باشه، و state
    # فعلی‌اش یکی از WORK_PHASE_STATES شناخته‌شده باشه — برای هر چیز
    # دیگه (مثلاً وسط ST_COLLECT بودن) عمداً به رفتار امنِ زیر (ریست کامل)
    # برمی‌گردیم، چون بازسازی دقیق اون State‌ها پیچیده‌تره و پرداخت دوباره
    # هم توشون در خطر نیست (COLLECT همیشه قبل از تایید/پرداخته).
    if already_advanced and ud.get(S) == ST_SENDING:
        # ارسال پس‌زمینه با ud قطع نمی‌شه (Task مستقل خودش رو داره)، ولی
        # اگه /start بزنه باید همون یادآوری همیشگی رو ببینه، نه این‌که
        # بی‌سروصدا به منوی اصلی پرت بشه و فکر کنه ارسال گم شده.
        if _has_active_send(uid):
            await update.message.reply_text(
                "⏳ ارسال قبلی هنوز در حال انجامه — هر ایمیل/اپلای که بره، همینجا "
                "بهتون اطلاع می‌دیم.\nبرای متوقف کردنش: /cancel",
                reply_markup=BACK_KB)
        else:
            ud[S] = ST_MAIN_MENU
            await update.message.reply_text(
                "به نظر می‌رسه ارسال قبلی تموم شده. به منوی اصلی برگشتید:",
                reply_markup=MAIN_KB)
        return 0

    if already_advanced and ud.get(S) in WORK_PHASE_STATES:
        sid_in_session = ud.get(S_SUB)
        session_sub = await asyncio.to_thread(db_get_sub, sid_in_session) if sid_in_session else None
        # ⚠️ فیکس باگ: قبلاً "completed" (یعنی سهمیه‌ی این subscription
        # تموم شده) هم مثل "approved" کاربر رو مستقیم به همون قدمی که
        # توش بود برمی‌گردوند — یعنی کسی که ۱۵۰ ایمیل/اپلایش تموم شده
        # بود، با /start دوباره وسط همون کار قدیمی (که دیگه سهمیه نداره)
        # گیر می‌کرد و اصلاً راهی برای ثبت‌نام/خرید مجدد نداشت. الان فقط
        # "approved" این مسیر رو می‌گیره؛ "completed" از این if رد می‌شه و
        # به ریست کامل زیر می‌رسه که کاربر رو آزاد می‌ذاره دوباره از منوی
        # اصلی سرویس رو انتخاب و ثبت‌نام کنه.
        if session_sub and session_sub["status"] == "approved":
            step_now = ud.get(S)
            await update.message.reply_text(
                "🎉 اشتراک شما فعاله — نیازی به ثبت‌نام یا پرداخت دوباره نیست. "
                "از همون‌جایی که بودید ادامه می‌دید:")
            msg_text, kb = _reentry_payload(step_now, ud)
            await safe(update.message, msg_text, reply_markup=kb)
            return 0
        # subscription این کاربر دیگه approved/completed نیست (نمونه‌ی نادر:
        # ادمین دستی status رو عوض کرده) — عمداً به ریست کامل زیر می‌ریم،
        # چون ادامه دادن یک کار روی subscription نامعتبر خطرناک‌تره.

    ud.clear()
    ud[S] = ST_MAIN_MENU

    # نرخ دلار برای نمایش در منوی اصلی
    rate, src = await asyncio.to_thread(fetch_usd_rate)
    if rate:
        price_t  = rate * PRICE_USD
        price_og = rate * PRICE_ORIGINAL_USD
        price_line = (
            f"   ~~${PRICE_ORIGINAL_USD} دلار ({fmt_toman(price_og)})~~ → *${PRICE_USD} دلار*\n"
            f"   معادل امروز: *{fmt_toman(price_t)}*\n"
            f"   (نرخ: {rate:,} تومان | {src})\n\n"
        )
    else:
        price_line = (
            f"   ~~${PRICE_ORIGINAL_USD} دلار~~ → *${PRICE_USD} دلار* (با نرخ دلار روز)\n\n"
        )

    await update.message.reply_text(
        f"👋 سلام {update.effective_user.first_name or 'عزیز'}!\n\n"
        "🚀 به Manifest Apply Services Bot خوش آمدید.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "📧 ارسال ایمیل به اساتید\n"
        "   ۵۰۰ ایمیل شخصی‌سازی‌شده با هوش مصنوعی\n\n"
        "💼 اپلای کار حرفه‌ای\n"
        "   با دقیق‌ترین تکنیک‌های اپلای، روزانه به\n"
        "   بهترین فرصت‌های شغلی real-time اپلای کنید.\n\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 قیمت هر دو سرویس (یکسان):\n"
        f"{price_line}"
        "━━━━━━━━━━━━━━━━━━━━━━\n"
        "سرویس موردنظر را انتخاب کنید:",
        reply_markup=MAIN_KB
    )
    return 0

# ── Rate limiting ساده (per-user) ────────────────────────────────
# قبلاً هیچ محدودیتی روی تعداد پیام یک کاربر در بازه‌ی زمانی نبود، یعنی
# یک کاربر (مثلاً به اشتباه یا عمداً) می‌تونست با اسپم پیام، صف AI/SMTP
# رو غرق کنه. این یک محدودیت سبک و درون‌حافظه‌ای (کافی برای یک بات با
# ترافیک متوسط) اضافه می‌کنه — نیازی به Redis/DB جداگانه نداره.
RATE_LIMIT_MAX_MSGS = 20     # حداکثر پیام
RATE_LIMIT_WINDOW_S = 60     # در این بازه (ثانیه)
_rate_limit_hits: dict[int, list] = {}

# ── تشخیص رفتار مشکوک + Temp Block ──────────────────────────────────
# فرق این با rate-limit بالا: rate-limit فقط همون لحظه رو کند می‌کنه
# («صبر کن، دوباره امتحان کن») — یعنی یه کاربر عادی که عجله داره هم
# بهش می‌خوره و بعد از چندتا ثانیه صبر، دوباره عادی کار می‌کنه. اینجا
# برعکس: اگه یه uid *مکرراً* (نه یه‌بار تصادفی) به سقف rate-limit
# می‌خوره — که یعنی رفتارش شبیه یه اسکریپت خودکار/حمله‌ست نه یه انسان
# عجول — برای یه بازه‌ی مشخص کاملاً مسدود می‌شه و به ادمین خبر داده
# می‌شه. عمداً درون‌حافظه‌ای (نه دیتابیس)، چون این یه محافظت لحظه‌ای/
# موقته: اگه بات ری‌استارت بشه، مسدودیت پاک می‌شه و کاربر از نو شروع
# می‌کنه — قابل قبوله، چون این فقط جلوی flood لحظه‌ای رو می‌گیره، مثل
# خودِ rate-limit بالا که همیشه هم درون‌حافظه‌ای بوده.
SUSPICIOUS_TRIP_THRESHOLD = 3     # چندبار به سقف rate-limit بخوره...
SUSPICIOUS_TRIP_WINDOW_S  = 600   # ...توی این بازه (۱۰ دقیقه)...
SUSPICIOUS_BLOCK_MINUTES  = 30    # ...که به این مدت مسدود بشه
_suspicious_trips: dict[int, list] = {}
_blocked_until: dict[int, float] = {}

def _is_temp_blocked(uid: int) -> bool:
    until = _blocked_until.get(uid)
    if not until:
        return False
    if time.time() >= until:
        _blocked_until.pop(uid, None)
        return False
    return True

def _register_rate_limit_trip(uid: int) -> bool:
    """هر بار که _rate_limited(uid) واقعاً True برگردونه صدا زده می‌شه.
    اگه uid توی SUSPICIOUS_TRIP_WINDOW_S ثانیه، SUSPICIOUS_TRIP_THRESHOLD
    بار یا بیشتر به سقف خورده باشه، تازه اونجا مسدود می‌شه — یعنی یه
    فلاد کوتاه یک‌باره کافی نیست، باید *الگوی تکرارشونده* باشه. خروجی:
    True یعنی همین الان تازه مسدود شد (برای لاگ/اطلاع‌رسانی)."""
    now = time.time()
    trips = _suspicious_trips.setdefault(uid, [])
    while trips and now - trips[0] > SUSPICIOUS_TRIP_WINDOW_S:
        trips.pop(0)
    trips.append(now)
    if len(trips) >= SUSPICIOUS_TRIP_THRESHOLD and not _is_temp_blocked(uid):
        _blocked_until[uid] = now + SUSPICIOUS_BLOCK_MINUTES * 60
        trips.clear()
        return True
    return False

# ── رجیستری Task های ارسال در حال اجرا (به‌ازای هر کاربر) ───────────
# چرا لازمه: اگه یه کاربر دوبار پشت‌سرهم روی «تایید» بزنه (مثلاً دوتاپ)،
# یا هر مسیر دیگه‌ای باعث بشه دوتا _send_emails_bg/_send_jobs_bg همزمان
# برای همون کاربر اجرا بشه، بدون این گارد ممکنه یک استاد دوبار ایمیل
# بگیره. همچنین همین رجیستری پایه‌ی دستور /cancel هست.
_active_send_tasks: dict[int, asyncio.Task] = {}

def _register_send_task(uid: int, task: asyncio.Task):
    _active_send_tasks[uid] = task
    def _cleanup(_):
        if _active_send_tasks.get(uid) is task:
            _active_send_tasks.pop(uid, None)
    task.add_done_callback(_cleanup)

def _has_active_send(uid: int) -> bool:
    t = _active_send_tasks.get(uid)
    return bool(t and not t.done())

def _rate_limited(uid: int) -> bool:
    now = time.time()
    hits = _rate_limit_hits.setdefault(uid, [])
    # پاک کردن ضربه‌های قدیمی‌تر از پنجره‌ی زمانی
    while hits and now - hits[0] > RATE_LIMIT_WINDOW_S:
        hits.pop(0)
    hits.append(now)
    return len(hits) > RATE_LIMIT_MAX_MSGS

# ── قفل per-user برای سریالایز کردن پردازش پیام/کال‌بک یک کاربر ────
# جلوگیری از اجرای هم‌زمان چند Action برای یک کاربر (کلیک سریع و
# چندباره روی دکمه‌ها، یا پیام‌های پشت‌سرهم قبل از این‌که پاسخ قبلی
# برسه). هم dispatch() (دکمه‌های Reply Keyboard/متن اصلی) و هم
# inline_callback (دکمه‌های Inline) از همین قفل مشترک استفاده می‌کنن —
# چون هر دو در نهایت روی همون ud/context.user_data یک کاربر کار می‌کنن.
_user_action_locks: dict[int, asyncio.Lock] = {}

def _get_user_action_lock(uid: int) -> asyncio.Lock:
    lock = _user_action_locks.get(uid)
    if lock is None:
        lock = asyncio.Lock()
        _user_action_locks[uid] = lock
    return lock

# ================================================================
# TASK 1 — Parent State Map (دکمه‌ی برگشت هرگز نباید به Main Menu بپره
# مگر پرنت واقعی‌اش خودِ Main Menu باشه)
# ================================================================
# قبلاً "🔙 برگشت" در تقریباً همه‌ی State ها (SMTP، جستجوی اساتید،
# پیش‌نمایش و ...) به یک Handler عمومی می‌خورد که همیشه ud.clear() می‌کرد
# و به Main Menu می‌رفت — یعنی هر پیشرفتی (نام، رزومه، SMTP، لیست
# اساتید) با یک "برگشت" ساده کامل از دست می‌رفت. این نگاشت، Parent
# واقعی هر State رو مشخص می‌کنه؛ داده‌ها (ud) دیگه هیچ‌وقت پاک نمی‌شن.
STATE_PARENT = {
    ST_SERVICE_DISCLAIMER:  ST_MAIN_MENU,
    ST_VIP_OR_NEW:           ST_SERVICE_DISCLAIMER,   # برای سرویس email — برای job در دیسپچ override می‌شه
    ST_VIP_CODE:             ST_VIP_OR_NEW,
    ST_FULLNAME:             ST_VIP_OR_NEW,
    ST_FULLNAME_CONFIRM:     ST_FULLNAME,
    ST_PHONE:                ST_FULLNAME_CONFIRM,
    ST_PROFILE_REUSE:        ST_MAIN_MENU,
    ST_INFO_CONFIRM:         ST_PHONE,
    ST_RECEIPT:              ST_INFO_CONFIRM,
    "choose_apply_method":   ST_MAIN_MENU,
    "choose_job_method":     ST_MAIN_MENU,
    ST_AUTH_CHOICE:          "choose_apply_method",
    ST_WAIT_GOOGLE:          ST_AUTH_CHOICE,
    ST_SMTP_EMAIL:           ST_AUTH_CHOICE,
    ST_SMTP_PASS:            ST_SMTP_EMAIL,
    "manual_offer":          ST_SMTP_PASS,
    ST_CUSTOM_SMTP:          "manual_offer",
    ST_CONFIRM_PROF_SEARCH:  ST_PROF_LIST,
    ST_PROF_LIST:            ST_AUTH_CHOICE,
    "prof_targeting":        ST_PROF_LIST,
    ST_JOB_FIELD:            "choose_job_method",
    ST_PREVIEW:              ST_PROF_LIST,
    ST_PREVIEW_SETTINGS:     ST_PREVIEW,   # تغییر رشته/کشور/رزومه از وسط پیش‌نمایش -> برگشت باید به همون پیش‌نمایش برگرده، نه منوی اصلی
    "wait_approval":         ST_MAIN_MENU,
    # --- موارد قبلاً جا افتاده در STATE_PARENT (باگ اصلی این Task) ---
    # ST_COLLECT جداگانه و با منطق «یک سوال قبل‌تر» در خودِ Handler برگشت
    # مدیریت می‌شه (نه از طریق این نگاشت)، پس عمداً این‌جا نیومده — پایین‌تر
    # هم یک محافظ اضافه شده تا اگه یه‌وقت این ورودی حذف شد باز خراب نشه.
    "job_search_empty":      ST_JOB_FIELD,       # نتیجه‌ی خالی جستجوی شغل -> برگشت به تایید رشته/کشور کار
    "closed_feedback":       ST_MAIN_MENU,       # بعد از اتمام کار؛ خودش دکمه‌ی برگشت نشون نمی‌ده ولی fallback امن لازمه
    "correction_confirm":    "closed_feedback",  # تایید ارسال ایمیل اصلاحی -> برگشت به فیدبک/اصلاح
    ST_SUPPORT:              ST_MAIN_MENU,       # عادتاً با prev_{S} مدیریت می‌شه؛ این فقط یک fallback ایمنه
    ST_MEMORY_MENU:          ST_MAIN_MENU,       # عادتاً با prev_{S} مدیریت می‌شه؛ این فقط یک fallback ایمنه
    ST_MEMORY_DISLIKE:       ST_MEMORY_MENU,
    ST_IMAP_OPTIN:           ST_MEMORY_MENU,
    ST_IMM_MENU:             ST_MEMORY_MENU,
    ST_IMM_EDIT:             ST_IMM_MENU,
    ST_IMM_DEADLINE_DATE:    ST_IMM_MENU,
    ST_IMM_DEADLINE_LABEL:   ST_IMM_DEADLINE_DATE,
    ST_SENDING:              ST_MAIN_MENU,       # فقط fallback؛ رفتار واقعی زیر با چک _has_active_send هندل می‌شه
    # --- استعلام سفارت (ST_EMBASSY_NATIONALITY عمداً این‌جا نیست — پرنتش
    # پویاست، از prev_{S} خونده می‌شه، چون ورودش هم از منوی اصلی هم از
    # وسط روند پرداخت ممکنه؛ پایین‌تر در دیسپچ جداگانه هندل می‌شه) ---
    ST_EMBASSY_2ND_PASSPORT: ST_EMBASSY_NATIONALITY,
    ST_EMBASSY_2ND_COUNTRY:  ST_EMBASSY_2ND_PASSPORT,
    ST_EMBASSY_COUNTRY:      ST_EMBASSY_2ND_PASSPORT,
    ST_EMBASSY_CATEGORY:     ST_EMBASSY_COUNTRY,
    ST_EMBASSY_RESULT:       ST_EMBASSY_CATEGORY,
}

# ── مرحله‌های «کاری» بعد از تایید/پرداخت ─────────────────────────────
# این‌ها همه‌ی State هایی هستن که فقط *بعد* از عبور از ثبت‌نام/پرداخت (یا
# تایید VIP) بهشون می‌رسیم — یعنی کاربر قطعاً یک subscription approved
# داره. عمداً ST_COLLECT و ST_SENDING این‌جا نیستن چون هرکدوم منطق
# برگشت/resume مخصوص به خودشون رو دارن (بالاتر/پایین‌تر جدا هندل می‌شن).
# دو جا استفاده می‌شه:
#  ۱. دکمه‌ی «🔙 برگشت»: به‌جای رفتن مرحله‌به‌مرحله (که کاربر مجبور بود
#     چند بار پشت‌سرهم بزنه تا به منوی اصلی برسه)، از هر کدوم از این
#     State ها مستقیم به منوی اصلی می‌ریم — تا اگه کاربر می‌خواد هم‌زمان
#     از سرویس دیگه هم استفاده کنه، سریع بتونه سوییچ کنه.
#  ۲. /start: اگه کاربر approved بوده و وسط یکی از این مرحله‌ها بود
#     (مثلاً از بات بیرون افتاد یا بات ری‌استارت شد)، دیگه نباید کل روند
#     (چه برسه به پرداخت) از اول شروع بشه — همون مرحله دوباره نشونش
#     می‌دیم؛ subscription و approval دست‌نخورده می‌مونن.
WORK_PHASE_STATES = {
    "choose_apply_method", "choose_job_method",
    ST_AUTH_CHOICE, ST_WAIT_GOOGLE, ST_SMTP_EMAIL, ST_SMTP_PASS,
    "manual_offer", ST_CUSTOM_SMTP,
    ST_CONFIRM_PROF_SEARCH, ST_PROF_LIST, "prof_targeting",
    ST_JOB_FIELD, ST_PREVIEW, ST_PREVIEW_SETTINGS,
    "job_search_empty", "correction_confirm",
}

def _reentry_payload(state: str, ud: dict):
    """پیام + کیبورد مناسب برای وقتی که با دکمه‌ی برگشت به این State
    می‌رسیم. برای مواردی که پیام ورودی اصلی‌شون به داده‌ی پویا (لیست
    اساتید تازه‌جستجو‌شده و ...) وابسته بود، یک پیام امن و عمومی نشون
    داده می‌شه؛ نکته‌ی مهم اینه که خودِ State و داده‌ها دست‌نخورده می‌مونن."""
    client = ud.get(S_CLIENT, {}) or {}
    if state == ST_SERVICE_DISCLAIMER:
        return ("بازگشتید. برای ادامه «✅ متوجه شدم، ادامه بده» را بزنید:",
                mk(["✅ متوجه شدم، ادامه بده"], ["🔙 برگشت"]))
    if state == ST_VIP_OR_NEW:
        return ("آیا مشتری VIP هستید یا برای اولین بار از این سرویس استفاده می‌کنید؟", VIP_KB)
    if state == ST_VIP_CODE:
        return ("⭐ کد VIP خود را وارد کنید:", BACK_KB)
    if state == ST_FULLNAME:
        return ("نام کامل خود را (به انگلیسی) وارد کنید:", BACK_KB)
    if state == ST_FULLNAME_CONFIRM:
        name = ud.get("pending_fullname", "")
        return (f"نام کامل شما: *{name}*\n\nهمین درست است؟", mk(["✅ درست است"], ["✏️ ویرایش کنم"]))
    if state == ST_PHONE:
        return ("📱 شماره تماس خود را وارد کنید:", BACK_KB)
    if state == ST_PROFILE_REUSE:
        return ("یکی از گزینه‌های زیر را انتخاب کنید:",
                mk(["✅ همینو استفاده کن"], ["✏️ به‌روزرسانی کنم"], ["🔙 برگشت"]))
    if state == ST_INFO_CONFIRM:
        return ("بازگشتید. یکی از گزینه‌های زیر را انتخاب کنید:",
                mk(["✅ بله، درسته"], ["✏️ می‌خوام ویرایش کنم"], ["🔙 برگشت"]))
    if state == ST_RECEIPT:
        return ("بازگشتید. رسید پرداخت را (عکس یا متن) بفرستید:", BACK_KB)
    if state == "choose_apply_method":
        return ("📧 آدرس ایمیل فرستنده را از کدام روش می‌خواهید ارسال شود؟",
                mk(["📨 از ایمیل شخصی خودم"], ["🔙 برگشت"]))
    if state == "choose_job_method":
        return ("💼 روش اپلای کار را انتخاب کنید:",
                mk(["📨 ایمیل مستقیم به HR"], ["🔗 لیست فرصت‌ها و لینک‌ها"], ["🔙 برگشت"]))
    if state == ST_AUTH_CHOICE:
        return ("یکی از گزینه‌های زیر را انتخاب کنید:", auth_choice_kb())
    if state == ST_WAIT_GOOGLE:
        return ("⏳ منتظر تکمیل ورود با گوگل هستیم، یا:",
                mk(["🔵 تلاش دوباره با گوگل"], ["🔑 با Gmail App Password"], ["🔙 برگشت"]))
    if state == ST_SMTP_EMAIL:
        return ("📧 آدرس ایمیل فرستنده را وارد کنید:", BACK_KB)
    if state == ST_SMTP_PASS:
        return ("🔒 رمز ایمیل را دوباره وارد کنید:", BACK_KB)
    if state == "manual_offer":
        return ("یکی از گزینه‌های زیر را انتخاب کنید:",
                mk(["🛠 خودم آدرس SMTP رو دارم"], ["🔁 دوباره با ایمیل/پسورد دیگه"],
                   ["📋 بات آماده کنه، خودم بفرستم"], ["🔙 برگشت"]))
    if state == ST_CUSTOM_SMTP:
        return ("🖥 آدرس سرور SMTP و پورت رو دوباره بفرستید:", BACK_KB)
    if state == ST_CONFIRM_PROF_SEARCH:
        field, country = client.get("field", "—"), client.get("countries", "—")
        return (f"📚 رشته: {field}\n🌍 کشور(ها): {country}\n\nتایید یا ویرایش کنید:", SEARCH_CONFIRM_KB)
    if state in (ST_PROF_LIST, "prof_targeting"):
        return ("حالا لیست اساتید:\n\n1️⃣ «🤖 بات پیدا کند» — بات خودش جستجو می‌کند\n"
                 "2️⃣ لینک/اسم استاد — یا چند خط با ایمیل اساتید\n\nانتخاب کنید:",
                mk(["🤖 بات خودش پیدا کند"], ["📋 لیست خودم دارم"], ["🔙 برگشت"]))
    if state == ST_JOB_FIELD:
        return (f"رشته جستجو: {client.get('field','')}\nکشور: {client.get('countries','')}\n\n"
                 "تایید یا ویرایش کنید:", SEARCH_CONFIRM_KB)
    if state == ST_PREVIEW:
        return ("بازگشتید به پیش‌نمایش ایمیل. یکی از گزینه‌های زیر را انتخاب کنید:", PREVIEW_KB)
    if state == ST_PREVIEW_SETTINGS:
        field, country = client.get("field", "—"), client.get("countries", "—")
        return (f"📚 رشته فعلی: {field}\n🌍 کشور فعلی: {country}\n\nچه چیزی را می‌خواهید تغییر دهید؟",
                PREVIEW_SETTINGS_KB)
    if state == "wait_approval":
        return ("⏳ منتظر تایید تیم Manifest Apply هستیم — به محض تایید خودکار ادامه می‌دهیم.", BACK_KB)
    if state == "job_search_empty":
        return (f"📚 رشته: {client.get('field','—')}\n🌍 کشور(ها): {client.get('countries','—')}\n\n"
                "تایید یا ویرایش کنید:", SEARCH_CONFIRM_KB)
    if state == "closed_feedback":
        return ("بازگشتید. اگر موضوع یا اعتراضی دارید همینجا بنویسید، مستقیم برای تیم ارسال می‌شود:",
                MAIN_KB)
    if state == ST_EMBASSY_2ND_PASSPORT:
        return ("پاسپورت دوم دارید؟", embassy_2nd_passport_kb())
    if state == ST_EMBASSY_2ND_COUNTRY:
        return ("پاسپورت دوم مربوط به چه کشوریه؟ (تایپ کنید)", BACK_KB)
    if state == ST_EMBASSY_COUNTRY:
        return ("برای کدوم سفارت/کشور می‌خواهید استعلام بگیرید؟", embassy_country_kb())
    if state == ST_EMBASSY_CATEGORY:
        return ("کدوم دسته اطلاعات رو می‌خواهید؟", embassy_category_kb())
    if state == ST_EMBASSY_RESULT:
        return ("یکی از گزینه‌های زیر را انتخاب کنید:",
                mk(["🔁 دسته‌ی دیگر"], ["🔙 برگشت"]))
    return ("به منوی اصلی برگشتید:", MAIN_KB)

async def dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wrapper نازک روی روتر اصلی: پردازش پیام‌های یک کاربر رو پشت قفل
    per-user سریالایز می‌کنه (همون قفلی که inline_callback هم استفاده
    می‌کنه) — تا کلیک/پیام سریع و پشت‌سرهم باعث اجرای هم‌زمان دو Action
    روی همون ud نشه (مثلاً یک استاد دوبار ایمیل نگیره، یا وسط ثبت یک
    مرحله، مرحله‌ی بعدی هم‌زمان پردازش نشه)."""
    uid = update.effective_user.id if update.effective_user else None
    if uid is None:
        return await _dispatch_impl(update, context)
    async with _get_user_action_lock(uid):
        return await _dispatch_impl(update, context)

async def _dispatch_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """روتر اصلی — همه پیام‌های متنی از اینجا رد می‌شوند"""
    ud   = context.user_data
    text = (update.message.text or "").strip() if update.message else ""
    uid  = update.effective_user.id
    step = ud.get(S)

    logger.debug(f"[{uid}] step={step} text={text[:30]}")

    if _is_temp_blocked(uid):
        # کاربر مسدود موقته — حتی لاگ warning هم نمی‌زنیم (قبلاً موقع
        # مسدودشدن یک‌بار لاگ/اطلاع‌رسانی شده)، فقط سبک رد می‌شیم تا خودِ
        # این چک هزینه‌ای روی بار پردازش نذاره.
        return 0

    if _rate_limited(uid):
        logger.warning(f"[{uid}] rate limited")
        try:
            await asyncio.to_thread(db_log_security_event, "blocked_request", uid, "rate limit hit")
        except Exception:
            pass
        if _register_rate_limit_trip(uid):
            logger.warning(f"[{uid}] 🚨 suspicious activity → temp-blocked for {SUSPICIOUS_BLOCK_MINUTES}m")
            try:
                await asyncio.to_thread(db_log_security_event, "suspicious_activity", uid,
                    f"repeated rate-limit trips → blocked {SUSPICIOUS_BLOCK_MINUTES}m")
            except Exception:
                pass
            try:
                await notify_admin(context.bot,
                    f"🚨 رفتار مشکوک\nکاربر: {uid}\n"
                    f"چندین‌بار پشت‌سرهم به سقف نرخ پیام خورده — {SUSPICIOUS_BLOCK_MINUTES} دقیقه مسدود شد.")
            except Exception:
                pass
            await safe(update.message,
                f"⛔️ به‌خاطر تعداد زیاد درخواست، دسترسیتون برای {SUSPICIOUS_BLOCK_MINUTES} دقیقه موقتاً محدود شد.")
            return 0
        await safe(update.message,
            "⏳ کمی سریع پیام می‌فرستید — لطفاً چند لحظه صبر کنید و دوباره امتحان کنید.")
        return 0

    # ── دستور /start در هر جایی ──────────────────────────────────
    if text == "/start":
        return await cmd_start(update, context)

    # ── اسکن ATS رزومه — دستور مستقل، از هر مرحله‌ای قابل‌اجراست ──
    # اگه رزومه از آخرین اسکن عوض نشده باشه، run_ats_scan_flow خودش بدون
    # AI call اضافه از cache جواب می‌ده — پس زدن مکرر این دکمه هزینه‌ای نداره.
    if text == "🔎 اسکن ATS رزومه":
        client = ud.get(S_CLIENT) or {}
        if not (client.get("resume") or "").strip():
            # هنوز وسط جمع‌آوری پروفایل نیست — از دیتابیس (اگه قبلاً یک‌بار
            # پروفایل ثبت کرده) می‌خونیم تا لازم نباشه از اول همه‌چیز بپرسیم.
            try:
                profile = await asyncio.to_thread(db_get_career_profile, uid)
            except Exception as e:
                logger.error(f"[uid={uid}] db_get_career_profile in ATS scan: {e}")
                profile = None
            client = profile or {}
        if not (client.get("resume") or "").strip():
            await safe(update.message,
                "📄 هنوز رزومه‌ای برای شما ثبت نشده.\n\n"
                "اول یک‌بار از «💼 اپلای کار حرفه‌ای» پروفایل و رزومه‌تون رو ثبت کنید، "
                "بعد می‌تونید همیشه از همین دکمه دوباره اسکن ATS بگیرید.",
                reply_markup=MAIN_KB)
            return 0
        await run_ats_scan_flow(update, context, uid, client, mandatory=False)
        return 0

    # ── پشتیبانی AI — در هر مرحله‌ای ────────────────────────────
    if text == "🆘 پشتیبانی":
        ud[f"prev_{S}"] = step
        ud[S] = ST_SUPPORT
        await safe(update.message,
            "🆘 پشتیبانی آنلاین\n\nسوال خود را بنویسید.\n"
            "برای بازگشت: 🔙 برگشت",
            reply_markup=BACK_KB)
        return 0

    if step == ST_SUPPORT:
        if text == "🔙 برگشت":
            ud[S] = ud.pop(f"prev_{S}", ST_MAIN_MENU)
            await safe(update.message, "بازگشتید.", reply_markup=MAIN_KB)
            return 0
        try:
            memory_summary = await asyncio.to_thread(db_get_memory_summary, uid)
        except Exception as e:
            logger.error(f"[uid={uid}] db_get_memory_summary in support: {e}")
            memory_summary = ""
        ctx_data = {"service": ud.get(S_SRV), "step": ud.get(S), "memory_summary": memory_summary}
        answer = await ai_support(text, ctx_data)
        await safe(update.message, f"🤖 {answer}\n\n"
            "سوال دیگری دارید؟ بنویسید. برای برگشت: 🔙",
            reply_markup=BACK_KB)
        return 0

    # ── AI Memory — در هر مرحله‌ای ──────────────────────────────────
    # بات یادش می‌مونه: کدوم استاد ایمیل زده/چه جوابی گرفته، کدوم کشور
    # مورد علاقه‌ست، چه رزومه‌ای ثبت شده، آخرین Applyها، و کدوم
    # شغل/شرکت‌ها ناخواسته‌ان — همه‌شون خودکار توی جستجوهای بعدی اعمال
    # می‌شن، این منو فقط برای مرور/ویرایش دستی‌شونه.
    if text == "🧠 حافظه من (AI Memory)":
        ud[f"prev_{S}"] = step
        ud[S] = ST_MEMORY_MENU
        await safe(update.message,
            "🧠 حافظه AI\n\n"
            "بات این موارد رو درباره‌ی شما یادش می‌مونه و خودکار توی سرویس‌های "
            "بعدی استفاده می‌کنه: کدام استاد ایمیل زده و چه جوابی گرفته، کشور "
            "مورد علاقه، رزومه‌ی ثبت‌شده، آخرین Applyها، مشاغل/شرکت‌های "
            "ناخواسته، و پرونده‌ی کامل مهاجرتی شما (تحصیلات، هدف ویزا، ریجکت "
            "قبلی، نقاط ضعف، استراتژی).\n\nیکی از گزینه‌های زیر را انتخاب کنید:",
            reply_markup=memory_menu_kb())
        return 0

    if step == ST_MEMORY_MENU:
        if text == "🔙 برگشت":
            ud[S] = ud.pop(f"prev_{S}", ST_MAIN_MENU)
            await safe(update.message, "بازگشتید.", reply_markup=MAIN_KB)
            return 0

        if text == "📋 نمایش خلاصه حافظه":
            try:
                summary = await asyncio.to_thread(db_get_memory_summary, uid)
            except Exception as e:
                logger.error(f"[uid={uid}] db_get_memory_summary: {e}")
                summary = ""
            await safe(update.message,
                ("📋 خلاصه‌ی حافظه‌ی شما:\n\n" + summary) if summary else
                "📭 هنوز چیزی درباره‌ی شما ذخیره نشده — بعد از اولین سرویس این‌جا پر می‌شود.",
                reply_markup=memory_menu_kb())
            return 0

        if text == "📬 ثبت پاسخ اساتید":
            try:
                pending = await asyncio.to_thread(db_get_emails_pending_reply_check, uid, 6)
            except Exception as e:
                logger.error(f"[uid={uid}] db_get_emails_pending_reply_check: {e}")
                pending = []
            if not pending:
                await safe(update.message,
                    "📭 ایمیل ثبت‌نشده‌ای برای بررسی پاسخ وجود ندارد.",
                    reply_markup=memory_menu_kb())
                return 0
            await safe(update.message,
                f"📬 وضعیت پاسخ {len(pending)} ایمیل اخیر را مشخص کنید:",
                reply_markup=memory_menu_kb())
            for e in pending:
                kb = InlineKeyboardMarkup([[
                    InlineKeyboardButton("✅ مثبت",   callback_data=f"replyset_positive_{e['id']}_{uid}"),
                    InlineKeyboardButton("❌ منفی",   callback_data=f"replyset_negative_{e['id']}_{uid}"),
                    InlineKeyboardButton("🚫 بی‌پاسخ", callback_data=f"replyset_none_{e['id']}_{uid}"),
                ]])
                try:
                    await context.bot.send_message(
                        chat_id=update.effective_chat.id,
                        text=f"👤 {e['name'] or e['email']}\n📧 {e['email']}\n🗓 {e['sent_at']}",
                        reply_markup=kb)
                except Exception as ex:
                    logger.error(f"[uid={uid}] sending reply-check message failed: {ex}")
            return 0

        if text == "📭 ردیابی خودکار ایمیل‌های برگشتی":
            try:
                enabled = await asyncio.to_thread(db_get_imap_optin, uid)
            except Exception as e:
                logger.error(f"[uid={uid}] db_get_imap_optin: {e}")
                enabled = False
            ud[S] = ST_IMAP_OPTIN
            status_line = "🟢 روشن" if enabled else "🔴 خاموش"
            await safe(update.message,
                f"📭 ردیابی خودکار ایمیل‌های برگشتی — وضعیت فعلی: {status_line}\n\n"
                "این قابلیت چیکار می‌کنه:\n"
                "• فقط دنبال پیام‌های *برگشتی خودکار* (bounce/Mailer Daemon) توی صندوق شما می‌گرده — "
                "نه جواب واقعی استادها یا هر ایمیل شخصی دیگه‌ای\n"
                "• صندوق فقط به‌صورت read-only باز می‌شه؛ هیچ ایمیلی حذف/تغییر/علامت‌گذاری نمی‌شه\n"
                "• اگه آدرس ایمیل یه استاد قبلاً بهش فرستادیم توی متن یه پیام bounce دیده بشه، "
                "دیگه دوباره باهاش تماس نمی‌گیریم (چون یعنی اون آدرس فعال نیست)\n"
                "• هر وقت بخواید می‌تونید خاموشش کنید\n\n"
                "برای فعال شدنش، SMTP شما باید از قبل توی سرویس ایمیل/شغل تنظیم شده باشه.",
                reply_markup=imap_optin_kb(enabled))
            return 0

        if text == "🚫 افزودن شغل/شرکت ناخواسته":
            ud[S] = ST_MEMORY_DISLIKE
            await safe(update.message,
                "🚫 اسم شغل(ها) یا شرکت(هایی) که دیگه نمی‌خواید بات نشونتون بده رو "
                "بنویسید (با کاما یا خط جدید جدا کنید).\n"
                "مثال: Sales, Marketing, ShadyCompany Inc",
                reply_markup=BACK_KB)
            return 0

        if text == "🗂 پرونده مهاجرتی من":
            try:
                imm = await asyncio.to_thread(db_get_immigration_profile, uid)
            except Exception as e:
                logger.error(f"[uid={uid}] db_get_immigration_profile: {e}")
                imm = {}
            try:
                visa_agent_on = await asyncio.to_thread(db_get_visa_agent_optin, uid)
            except Exception as e:
                logger.error(f"[uid={uid}] db_get_visa_agent_optin: {e}")
                visa_agent_on = False
            ud[S] = ST_IMM_MENU
            await safe(update.message,
                _format_immigration_profile_block(imm) + "\n\n"
                "این پرونده خودکار توی پشتیبانی AI و استعلام سفارت استفاده می‌شه — "
                "هرچی کامل‌تر باشه، تحلیل دقیق‌تری می‌گیرید (مثل «مشکل اصلی مدرک زبانه»).\n\n"
                "برای ویرایش، یکی از فیلدها رو انتخاب کنید:",
                reply_markup=immigration_profile_kb(visa_agent_on))
            return 0

        await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=memory_menu_kb())
        return 0

    if step == ST_IMM_MENU:
        if text == "🔙 برگشت":
            ud[S] = ST_MEMORY_MENU
            await safe(update.message, "بازگشتید.", reply_markup=memory_menu_kb())
            return 0

        if text == "⏰ تنظیم ددلاین واقعی":
            ud[S] = ST_IMM_DEADLINE_DATE
            await safe(update.message,
                "📅 تاریخ واقعی ددلاین رو به شکل سال-ماه-روز بنویسید (مثلاً 2026-09-10).\n"
                "این تاریخ رو *فقط شما* وارد می‌کنید — بات هیچ‌وقت خودش ددلاین حدس نمی‌زنه، "
                "چون یه تاریخ اشتباه توی یه پرونده‌ی واقعی مهاجرتی می‌تونه واقعاً ضرر بزنه.",
                reply_markup=BACK_KB)
            return 0

        if text in ("🧭 روشن کردن دستیار هفتگی مهاجرت", "🔴 خاموش کردن دستیار هفتگی"):
            turning_on = text.startswith("🧭")
            try:
                await asyncio.to_thread(db_set_visa_agent_optin, uid, turning_on)
            except Exception as e:
                logger.error(f"[uid={uid}] db_set_visa_agent_optin: {e}")
                await safe(update.message, "⚠️ تغییر ذخیره نشد، دوباره امتحان کنید.",
                    reply_markup=immigration_profile_kb(not turning_on))
                return 0
            if turning_on:
                await safe(update.message,
                    "✅ دستیار هفتگی روشن شد.\n"
                    "هر هفته، فقط بر اساس همین پرونده (نه چیز دیگه‌ای)، ۳ کار ضروری اون هفته "
                    "براتون می‌فرستم. اگه ددلاین واقعی هم ثبت کرده باشید، شمار روزهای مونده رو هم می‌گم.\n"
                    "هر وقت خواستید از همین منو خاموشش کنید.",
                    reply_markup=immigration_profile_kb(True))
            else:
                await safe(update.message, "🔴 دستیار هفتگی خاموش شد.",
                    reply_markup=immigration_profile_kb(False))
            return 0

        field_key = _IMMIGRATION_LABEL_TO_KEY.get(text)
        if not field_key:
            await safe(update.message, "یکی از فیلدهای زیر را انتخاب کنید:", reply_markup=immigration_profile_kb())
            return 0
        ud["_imm_edit_field"] = field_key
        ud[S] = ST_IMM_EDIT
        await safe(update.message,
            f"مقدار جدید برای «{text}» را بنویسید:\n(برای پاک‌کردن، یک فاصله بفرستید)",
            reply_markup=BACK_KB)
        return 0

    if step == ST_IMM_DEADLINE_DATE:
        if text == "🔙 برگشت":
            ud[S] = ST_IMM_MENU
            await safe(update.message, "لغو شد.", reply_markup=immigration_profile_kb())
            return 0
        raw = text.strip()
        try:
            parsed = datetime.strptime(raw, "%Y-%m-%d")
        except ValueError:
            await safe(update.message,
                "⚠️ فرمت درست نبود. لطفاً دقیقاً به شکل سال-ماه-روز بنویسید، مثلاً 2026-09-10.",
                reply_markup=BACK_KB)
            return 0
        if parsed.date() < datetime.now().date():
            await safe(update.message,
                "⚠️ این تاریخ گذشته. یک تاریخ آینده بنویسید (سال-ماه-روز).",
                reply_markup=BACK_KB)
            return 0
        ud["_visa_deadline_date"] = parsed.strftime("%Y-%m-%d")
        ud[S] = ST_IMM_DEADLINE_LABEL
        await safe(update.message,
            "این ددلاین برای چیه؟ (مثلاً: ارسال درخواست دانشگاه X، تمدید ویزا، آپلود مدارک)",
            reply_markup=BACK_KB)
        return 0

    if step == ST_IMM_DEADLINE_LABEL:
        if text == "🔙 برگشت":
            ud.pop("_visa_deadline_date", None)
            ud[S] = ST_IMM_DEADLINE_DATE
            await safe(update.message, "دوباره تاریخ رو بنویسید (سال-ماه-روز):", reply_markup=BACK_KB)
            return 0
        date_str = ud.pop("_visa_deadline_date", None)
        if not date_str:
            ud[S] = ST_IMM_MENU
            await safe(update.message, "یک مشکل موقت پیش اومد — دوباره از منو انتخاب کنید:",
                reply_markup=immigration_profile_kb())
            return 0
        try:
            await asyncio.to_thread(db_set_visa_agent_deadline, uid, date_str, text.strip())
        except Exception as e:
            logger.error(f"[uid={uid}] db_set_visa_agent_deadline: {e}")
            await safe(update.message, "⚠️ ذخیره نشد، دوباره امتحان کنید.", reply_markup=immigration_profile_kb())
            ud[S] = ST_IMM_MENU
            return 0
        try:
            visa_agent_on = await asyncio.to_thread(db_get_visa_agent_optin, uid)
        except Exception:
            visa_agent_on = False
        ud[S] = ST_IMM_MENU
        await safe(update.message,
            f"✅ ذخیره شد: {date_str} — {text.strip()}\n"
            + ("دستیار هفتگی روشنه، پس این ددلاین توی پیام‌های هفتگی هم دیده می‌شه."
               if visa_agent_on else
               "برای این‌که این ددلاین توی پیام هفتگی هم بیاد، دستیار هفتگی رو روشن کنید."),
            reply_markup=immigration_profile_kb(visa_agent_on))
        return 0

    if step == ST_IMM_EDIT:
        if text == "🔙 برگشت":
            ud.pop("_imm_edit_field", None)
            ud[S] = ST_IMM_MENU
            await safe(update.message, "لغو شد.", reply_markup=immigration_profile_kb())
            return 0
        field_key = ud.pop("_imm_edit_field", None)
        if not field_key:
            ud[S] = ST_IMM_MENU
            await safe(update.message, "یک مشکل موقت پیش اومد — دوباره از منو انتخاب کنید:",
                reply_markup=immigration_profile_kb())
            return 0
        try:
            await asyncio.to_thread(db_set_career_profile_field, uid, field_key, text.strip())
        except Exception as e:
            logger.error(f"[uid={uid}] db_set_career_profile_field({field_key}): {e}")
            await safe(update.message, "⚠️ ذخیره نشد، دوباره امتحان کنید.", reply_markup=immigration_profile_kb())
            ud[S] = ST_IMM_MENU
            return 0
        try:
            imm = await asyncio.to_thread(db_get_immigration_profile, uid)
        except Exception:
            imm = {}
        ud[S] = ST_IMM_MENU
        await safe(update.message,
            "✅ ذخیره شد.\n\n" + _format_immigration_profile_block(imm),
            reply_markup=immigration_profile_kb())
        return 0

    if step == ST_MEMORY_DISLIKE:
        if text == "🔙 برگشت":
            ud[S] = ST_MEMORY_MENU
            await safe(update.message, "باشه، لغو شد.", reply_markup=memory_menu_kb())
            return 0
        parts = [p.strip() for p in re.split(r"[،,\n]", text) if p.strip()]
        if not parts:
            await safe(update.message, "متنی دریافت نشد — دوباره بنویسید:", reply_markup=BACK_KB)
            return 0
        try:
            merged = await asyncio.to_thread(db_add_disliked_jobs, uid, parts)
        except Exception as e:
            logger.error(f"[uid={uid}] db_add_disliked_jobs: {e}")
            merged = ""
        # Application Brain: از همین متن آزاد، سیگنال‌های ساختاریافته
        # (work_mode/seniority/company_stage/sponsorship) هم استخراج و
        # شمارش می‌شه — وقتی یک الگو به‌اندازه‌ی کافی تکرار بشه، جستجوی
        # بعدی خودکار طبقش تغییر می‌کنه (نگاه کن به _apply_learned_filters).
        try:
            await asyncio.to_thread(db_record_learned_signals, uid, parts)
        except Exception as e:
            logger.error(f"[uid={uid}] db_record_learned_signals: {e}")
        ud[S] = ST_MEMORY_MENU
        shown = ", ".join(merged.split("|")) if merged else ", ".join(parts)
        await safe(update.message,
            f"✅ ثبت شد. از این به بعد این موارد خودکار در جستجوی شغل حذف می‌شوند:\n{shown}",
            reply_markup=memory_menu_kb())
        return 0

    if step == ST_IMAP_OPTIN:
        if text == "🔙 برگشت":
            ud[S] = ST_MEMORY_MENU
            await safe(update.message, "بازگشتید.", reply_markup=memory_menu_kb())
            return 0

        if text == "✅ روشن کن (با رضایت من)":
            smtp_e, smtp_p = ud.get(S_SMTP_E), ud.get(S_SMTP_P)
            if not (smtp_e and smtp_p):
                await safe(update.message,
                    "⚠️ هنوز SMTP شما تنظیم نشده — اول باید یک‌بار از سرویس «📧 ارسال ایمیل به اساتید» "
                    "یا «💼 اپلای کار حرفه‌ای» (مسیر ایمیل مستقیم) وارد بشید و SMTP رو تست کنید، "
                    "بعد برگردید اینجا روشنش کنید.",
                    reply_markup=imap_optin_kb(False))
                return 0
            try:
                await asyncio.to_thread(db_set_imap_optin, uid, True)
            except Exception as e:
                logger.error(f"[uid={uid}] db_set_imap_optin(True): {e}")
                await safe(update.message, "⚠️ مشکلی پیش اومد، دوباره امتحان کنید.", reply_markup=imap_optin_kb(False))
                return 0
            await safe(update.message,
                "✅ روشن شد. از این به بعد هر روز صندوق شما فقط برای پیام‌های برگشتی خودکار چک می‌شه.",
                reply_markup=memory_menu_kb())
            ud[S] = ST_MEMORY_MENU
            return 0

        if text == "🔴 خاموش کن":
            try:
                await asyncio.to_thread(db_set_imap_optin, uid, False)
            except Exception as e:
                logger.error(f"[uid={uid}] db_set_imap_optin(False): {e}")
            await safe(update.message, "✅ خاموش شد.", reply_markup=memory_menu_kb())
            ud[S] = ST_MEMORY_MENU
            return 0

        await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:",
                    reply_markup=imap_optin_kb(await asyncio.to_thread(db_get_imap_optin, uid)))
        return 0

    # ── برگشت عمومی — Task 1: هر State باید Parent خودش را داشته باشد ──
    # قبلاً این‌جا همیشه ud.clear() می‌کرد و به Main Menu می‌رفت — یعنی
    # داخل SMTP/جستجوی اساتید/پیش‌نمایش ایمیل و... زدن برگشت کل پیشرفت
    # کاربر (نام، رزومه، تنظیمات SMTP، لیست اساتید) رو پاک می‌کرد. الان
    # طبق STATE_PARENT فقط یک قدم به عقب می‌ریم و هیچ داده‌ای پاک نمی‌شه؛
    # Main Menu فقط وقتی نشون داده می‌شه که یا Parent واقعی خودش Main
    # Menu باشه، یا کاربر از همون Main Menu دوباره «برگشت» بزنه.
    if text == "🔙 برگشت" and step not in (ST_MAIN_MENU, ST_IDLE):
        # مورد خاص COLLECT: Parent هر سوال، سوال قبلی خودشه، نه یک State
        # کاملاً متفاوت — این‌جا با کم‌کردن شماره‌ی سوال مدیریت می‌شه.
        if step == ST_COLLECT:
            idx = ud.get("collect_idx", 0)
            if ud.pop("awaiting_resume_text", False) or ud.pop("awaiting_recommenders", False):
                # داخل یک زیر-سوال بودیم (مثلاً «متن رزومه رو تایپ کن») —
                # فقط همون زیر-سوال لغو می‌شه و به سوال اصلی همین idx برمی‌گردیم.
                qs = _collect_questions_for(ud.get(S_SRV))
                await safe(update.message, "باشه، لغو شد.")
                await _collect_next_or_finish(update, context, qs, idx)
                return 0
            if idx <= 0:
                # اولین سوال COLLECT بود — یک قدم قبل‌تر (شماره تماس) برمی‌گردیم.
                ud[S] = ST_PHONE
                await safe(update.message, "📱 شماره تماس خود را وارد کنید:", reply_markup=BACK_KB)
                return 0
            idx -= 1
            ud["collect_idx"] = idx
            qs = _collect_questions_for(ud.get(S_SRV))
            await _collect_next_or_finish(update, context, qs, idx)
            return 0

        # مورد خاص SENDING: تا وقتی Task ارسال پس‌زمینه هنوز فعاله، «برگشت»
        # نباید بی‌سروصدا کاربر رو به Main Menu ببره (این دقیقاً همون باگ
        # قدیمی بود) — باید صریح بگیم ارسال در جریانه و /cancel رو پیشنهاد بدیم.
        if step == ST_SENDING:
            if _has_active_send(uid):
                await safe(update.message,
                    "⏳ ارسال هنوز در حال انجامه — هر ایمیل که بره، همینجا بهتون اطلاع می‌دیم.\n"
                    "برای متوقف کردنش می‌تونید /cancel رو بزنید.", reply_markup=BACK_KB)
                return 0
            ud[S] = ST_MAIN_MENU
            await safe(update.message,
                "به نظر می‌رسه ارسال قبلی تموم شده. به منوی اصلی برگشتید.",
                reply_markup=MAIN_KB)
            return 0

        # مورد خاص prof_targeting: اگه وسط تایپ کشور/رشته‌ی جدید بودیم
        # («🌍 تغییر کشور...» زده بود ولی هنوز متنش رو نفرستاده)، برگشت
        # باید فقط همون زیر-سوال رو لغو کنه و به همون منوی prof_targeting
        # برگرده — نه اینکه flag روشن بمونه و دفعه‌ی بعد که کاربر از این
        # State دوباره رد بشه، اولین پیامش اشتباهی به‌عنوان جواب کشور/رشته
        # خونده بشه.
        if step == "prof_targeting" and (ud.pop("_awaiting_prof_country", False)
                                          or ud.pop("_awaiting_prof_field", False)):
            await safe(update.message, "باشه، لغو شد.")
            msg_text, kb = _reentry_payload("prof_targeting", ud)
            await safe(update.message, msg_text, reply_markup=kb)
            return 0

        # مورد خاص ST_PREVIEW_SETTINGS: دقیقاً همون منطق بالا — اگه وسط
        # تایپ کشور/رشته/رزومه‌ی جدید بودیم، برگشت فقط همون زیر-سوال رو
        # لغو می‌کنه و به منوی «تغییر تنظیمات» برمی‌گردیم؛ پیش‌نمایش و
        # لیست اساتید فعلی هیچ‌وقت پاک نمی‌شن.
        if step == ST_PREVIEW_SETTINGS and (ud.pop("_awaiting_preview_country", False)
                                             or ud.pop("_awaiting_preview_field", False)
                                             or ud.pop("_awaiting_preview_resume", False)):
            await safe(update.message, "باشه، لغو شد.")
            msg_text, kb = _reentry_payload(ST_PREVIEW_SETTINGS, ud)
            await safe(update.message, msg_text, reply_markup=kb)
            return 0

        # مورد خاص ST_EMBASSY_NATIONALITY: اولین قدم فیچر استعلام سفارت —
        # پرنتش ثابت نیست (ممکنه از منوی اصلی وارد شده باشیم یا از وسط
        # روند پرداخت/choose_apply_method)، پس از prev_{S} که در
        # _start_embassy_inquiry ذخیره شده می‌خونیم، نه STATE_PARENT.
        if step == ST_EMBASSY_NATIONALITY:
            parent = ud.pop(f"prev_{S}", ST_MAIN_MENU) or ST_MAIN_MENU
            ud[S] = parent
            msg_text, kb = _reentry_payload(parent, ud)
            await safe(update.message, msg_text, reply_markup=kb)
            return 0

        parent = STATE_PARENT.get(step, ST_MAIN_MENU)
        # ST_VIP_OR_NEW برای سرویس «اپلای کار» پرنتش مستقیم Main Menu است
        # (این سرویس صفحه‌ی Disclaimer جداگانه‌ای مثل سرویس ایمیل نداره).
        if step == ST_VIP_OR_NEW and ud.get(S_SRV) == "job":
            parent = ST_MAIN_MENU
        # ── فیکس: قبلاً هر State داخل WORK_PHASE_STATES (جستجوی اساتید،
        # SMTP، پیش‌نمایش، جستجوی کار و...) با یک «برگشت» مستقیم می‌پرید
        # به منوی اصلی، به‌جای یک قدم به عقب (مثلاً از لیست اساتید باید
        # می‌رفت به انتخاب روش ورود ایمیل، ولی می‌رفت منوی اصلی). این
        # دقیقاً همون چیزی بود که کاربر گزارش داد: «وسط کار برمی‌گرده
        # منوی اصلی». الان این override حذف شده — STATE_PARENT بالا
        # همیشه پرنت *واقعی* هر State رو مشخص می‌کنه و برگشت دقیقاً یک
        # قدم به عقب می‌ره، نه پرش به منوی اصلی. داده‌ها (پروفایل،
        # پیش‌نمایش، لیست اساتید) هیچ‌وقت پاک نمی‌شن؛ فقط ud[S] عوض می‌شه.
        ud[S] = parent
        msg_text, kb = _reentry_payload(parent, ud)
        await safe(update.message, msg_text, reply_markup=kb)
        return 0

    # ── برگشت از خودِ Main Menu (یا وقتی State نامشخص/جدیده) ─────────
    if text == "🔙 برگشت":
        ud.clear()
        ud[S] = ST_MAIN_MENU
        await update.message.reply_text("به منوی اصلی برگشتید:", reply_markup=MAIN_KB)
        return 0

    # ── منوی اصلی ────────────────────────────────────────────────
    if step == ST_MAIN_MENU or step is None:
        if text == "📧 ارسال ایمیل به اساتید":
            existing = await asyncio.to_thread(db_get_latest_sub_for_service, uid, "email")
            # ⚠️ فیکس باگ: "completed" یعنی این subscription خاص تموم شده
            # (سهمیه مصرف شده) — نباید مانع ثبت‌نام/خرید جدید بشه. قبلاً
            # اینجا "completed" هم مثل "approved" رفتار می‌کرد و کاربر رو
            # قفل می‌کرد؛ فقط "approved" واقعاً باید resume بشه.
            if existing and existing["status"] == "approved":
                await _resume_existing_service(update, context, existing, "email")
                return 0
            ud[S_SRV] = "email"
            ud[S] = ST_SERVICE_DISCLAIMER
            await safe(update.message,
                "📧 سرویس ارسال ایمیل به اساتید\n\n"
                "قبل از شروع، دو نکته‌ی مهم:\n\n"
                "1️⃣ بات تلاش می‌کنه مستقیم از طریق ایمیل خودتون (Gmail، ایمیل دانشگاهی، یا هر ایمیل "
                "دیگه) به اساتید ایمیل بزنه. اگر به هر دلیلی این اتصال ممکن نشد (خیلی از ایمیل‌های "
                "دانشگاهی اتصال بیرونی رو باز نمی‌ذارن)، نگران نباشید — بات به‌جای گیر کردن، ایمیل و "
                "تمام اطلاعات لازم هر استاد رو همینجا توی همین چت براتون آماده می‌کنه، و شما فقط "
                "باید خودتون از طریق ایمیل خودتون ارسالش کنید.\n\n"
                "2️⃣ برای بهترین نتیجه لازم دارید:\n"
                "• یک رزومه (یا حداقل خلاصه‌ای از تحصیلات و سوابق‌تون) **به زبان انگلیسی** آماده داشته باشید\n"
                "• ترجیحاً دو نامه‌ی توصیه (Recommendation Letter) هم آماده باشه — نداشتنش مشکلی نیست، "
                "ولی روی نتیجه‌ی کار تاثیر مثبت داره\n\n"
                "آماده‌اید ادامه بدیم؟",
                reply_markup=mk(["✅ متوجه شدم، ادامه بده"], ["🔙 برگشت"]))
            return 0
        if text == "💼 اپلای کار حرفه‌ای":
            existing = await asyncio.to_thread(db_get_latest_sub_for_service, uid, "job")
            # ⚠️ همون فیکس بالا برای سرویس اپلای کار.
            if existing and existing["status"] == "approved":
                await _resume_existing_service(update, context, existing, "job")
                return 0
            ud[S_SRV] = "job"
            ud[S] = ST_VIP_OR_NEW
            await safe(update.message,
                "💼 سرویس اپلای کار حرفه‌ای\n\n"
                "آیا مشتری VIP هستید یا برای اولین بار از این سرویس استفاده می‌کنید؟",
                reply_markup=VIP_KB)
            return 0
        if text == "🔍 استعلام سفارت":
            return await _start_embassy_inquiry(update, context)
        await safe(update.message, "از منوی پایین انتخاب کنید:", reply_markup=MAIN_KB)
        return 0

    # ── هشدار اولیه‌ی سرویس ایمیل به اساتید ──────────────────────
    if step == ST_SERVICE_DISCLAIMER:
        if text == "✅ متوجه شدم، ادامه بده":
            ud[S] = ST_VIP_OR_NEW
            await safe(update.message,
                "آیا مشتری VIP هستید یا برای اولین بار از این سرویس استفاده می‌کنید؟",
                reply_markup=VIP_KB)
            return 0
        await safe(update.message,
            "برای ادامه، لطفاً روی «✅ متوجه شدم، ادامه بده» بزنید:",
            reply_markup=mk(["✅ متوجه شدم، ادامه بده"], ["🔙 برگشت"]))
        return 0

    # ── VIP یا تازه‌وارد ──────────────────────────────────────────
    if step == ST_VIP_OR_NEW:
        if text == "⭐ مشتری VIP هستم":
            ud[S_TYPE] = "vip"
            ud[S] = ST_VIP_CODE
            await safe(update.message,
                "⭐ کد VIP خود را وارد کنید:",
                reply_markup=BACK_KB)
            return 0
        if text == "🆕 تازه‌وارد هستم":
            ud[S_TYPE] = "normal"
            ud[S] = ST_FULLNAME
            await safe(update.message,
                "بسیار خب! قبل از پرداخت، اول اطلاعاتتون رو می‌گیریم تا سرویس دقیقاً "
                "مطابق نیازتون شخصی‌سازی بشه — پرداخت آخرین قدمه.\n\n"
                "لطفاً نام کامل خود را (به انگلیسی) وارد کنید:",
                reply_markup=BACK_KB)
            return 0
        await safe(update.message, "یکی از گزینه‌های پایین را انتخاب کنید:", reply_markup=VIP_KB)
        return 0

    # ── کد VIP ───────────────────────────────────────────────────
    if step == ST_VIP_CODE:
        # کیبورد فارسی خیلی وقت‌ها اعداد رو خودکار به ارقام فارسی/عربی
        # تبدیل می‌کنه (مثلاً «۲۰۲۷» به‌جای «2027») — بدون این نرمال‌سازی،
        # حتی کد درست هم match نمی‌شد.
        _fa_digits = "۰۱۲۳۴۵۶۷۸۹"
        _ar_digits = "٠١٢٣٤٥٦٧٨٩"
        normalized_code = text.strip()
        for i, d in enumerate(_fa_digits):
            normalized_code = normalized_code.replace(d, str(i))
        for i, d in enumerate(_ar_digits):
            normalized_code = normalized_code.replace(d, str(i))
        if normalized_code == VIP_CODE:
            sid = await asyncio.to_thread(db_create_sub, uid,
                                           ud.get(S_SRV), "vip")
            ud[S_SUB] = sid
            ud[S_TYPE] = "vip"
            ud[S] = ST_FULLNAME
            svc_name = "ارسال ایمیل به اساتید" if ud.get(S_SRV)=="email" else "اپلای کار"

            # snapshot اولیه — بات ممکنه قبل از تموم شدن collect ری‌استارت کنه.
            # بعداً در _collect_next_or_finish آپدیت می‌شه.
            await asyncio.to_thread(db_save_pending_snapshot, sid, json.dumps({
                S_SRV: ud.get(S_SRV), S_TYPE: "vip", S_CLIENT: {},
            }, ensure_ascii=False))

            await notify_admin(context.bot,
                f"🌟 درخواست دسترسی VIP\n"
                f"👤 {update.effective_user.first_name} (@{update.effective_user.username})\n"
                f"🆔 {uid}\n"
                f"🛠 سرویس: {svc_name}\n\n"
                f"برای تایید:\n/vip_approve_{sid}_{uid}\n"
                f"برای رد:\n/vip_reject_{sid}_{uid}")
            await safe(update.message,
                "✅ کد VIP پذیرفته شد!\n\n"
                "ریکوئست شما برای تیم Manifest Apply ارسال شد.\n"
                "پس از تایید، دسترسی فعال خواهد شد.\n\n"
                "لطفاً نام کامل خود را وارد کنید:",
                reply_markup=BACK_KB)
            return 0
        else:
            # کد اشتباه — گزینه برگشت
            # نکته: قبلاً این دکمه متن "🔙 برگشت و استفاده از سرویس عادی" داشت
            # که با هیچ Handler ای مطابقت نداشت (فقط "🔙 برگشت" چک می‌شد)،
            # پس کاربر برای همیشه همینجا گیر می‌کرد و به منوی اصلی برنمی‌گشت.
            # با استفاده از BACK_KB واقعی، همون دکمه استاندارد "🔙 برگشت" رو
            # نشون می‌دیم که already در بالای dispatch مدیریت می‌شه.
            await safe(update.message,
                "❌ کد وارد‌شده معتبر نیست.\n\n"
                "اگر کد VIP ندارید، با دکمه‌ی زیر به منوی اصلی برگردید و "
                "از سرویس عادی استفاده کنید:",
                reply_markup=BACK_KB)
            return 0

    # ── رسید پرداخت عادی ──────────────────────────────────────────
    if step == ST_RECEIPT:
        if update.message and update.message.photo:
            photo_id = update.message.photo[-1].file_id
            # همون قیمتی که توی _show_payment محاسبه و به کاربر نشون داده
            # شده رو دوباره استفاده می‌کنیم — نه یه fetch جدید (که هم یه
            # درخواست شبکه‌ی اضافه‌ست هم ممکنه با نرخ نشون‌داده‌شده فرق
            # کنه). اگه اون موقع نرخ در دسترس نبود، اینجا هم می‌دونیم که
            # کاربر خودش با نرخ روز حساب کرده — به‌جای عدد اشتباه یا کرش،
            # صادقانه "نامشخص" نشون می‌دیم.
            price_t = ud.get("pay_price_toman")
            sid = await asyncio.to_thread(db_create_sub, uid, ud.get(S_SRV), "normal", price_t or 0)
            ud[S_SUB] = sid
            svc_name = "📧 ارسال ایمیل به اساتید" if ud.get(S_SRV)=="email" else "💼 اپلای کار"
            uname = f"@{update.effective_user.username}" if update.effective_user.username else "—"
            price_line = f"{fmt_toman(price_t)} (${PRICE_USD})" if price_t else \
                         f"نامشخص — کاربر با نرخ روز خودش حساب کرده (${PRICE_USD})"

            # ── ارسال تضمینی اسکرین‌شات با دکمه‌های تایید/رد ─────────
            admin_caption = (
                f"💳 رسید پرداخت جدید\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"👤 {update.effective_user.first_name} ({uname})\n"
                f"🆔 Telegram ID: {uid}\n"
                f"🛠 سرویس: {svc_name}\n"
                f"💰 مبلغ: {price_line}\n"
                f"📋 Sub ID: {sid}\n"
                f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M')}\n"
                f"━━━━━━━━━━━━━━━━\n"
                f"برای تایید: /approve_{sid}_{uid}\n"
                f"برای رد: /reject_{sid}_{uid}"
            )
            inline_kb = InlineKeyboardMarkup([
                [
                    InlineKeyboardButton("✅ تایید پرداخت", callback_data=f"pay_ok_{sid}_{uid}"),
                    InlineKeyboardButton("❌ رد",           callback_data=f"pay_no_{sid}_{uid}"),
                ]
            ])

            if ADMIN_CHAT_ID:
                try:
                    await context.bot.send_photo(
                        chat_id=ADMIN_CHAT_ID,
                        photo=photo_id,
                        caption=admin_caption,
                        reply_markup=inline_kb
                    )
                except Exception as e:
                    logger.error(f"admin photo send error: {e}")
                    # fallback: ارسال متن بدون عکس
                    try:
                        await context.bot.send_message(
                            chat_id=ADMIN_CHAT_ID,
                            text=f"⚠️ خطا در ارسال عکس\n{admin_caption}")
                    except Exception as e2:
                        logger.error(f"admin text fallback error: {e2}")
            else:
                logger.warning(f"ADMIN_CHAT_ID not set — sub {sid} created but admin not notified")

            await safe(update.message,
                "✅ رسید پرداخت دریافت شد!\n\n"
                "📨 اسکرین‌شات برای تیم Manifest Apply ارسال شد.\n"
                "⏳ معمولاً ظرف ۱-۲ ساعت بررسی می‌شود.",
                reply_markup=BACK_KB)
            # اطلاعات کاربر (نام، شماره، رشته، رزومه و...) از قبل کامل
            # گرفته شده — همین الان فقط باید منتظر تاییدیه بمونیم یا (اگه
            # به هر دلیلی از قبل approved بوده) بی‌درنگ ادامه بدیم.
            await _check_approval_and_proceed(update, context, ud.get(S_SRV))
            return 0
        else:
            await safe(update.message,
                "📸 لطفاً تصویر (اسکرین‌شات) رسید پرداخت را ارسال کنید.\n"
                "متن قبول نیست — باید تصویر باشد.\n\n"
                "اگه توی این مرحله مشکلی داشتید یا سوالی بود، به پشتیبانی پیام بدید: @manifestapply")
            return 0

    # ── نام کامل ─────────────────────────────────────────────────
    if step == ST_FULLNAME:
        if len(text) < 3:
            await safe(update.message, "نام کامل خود را وارد کنید (حداقل ۳ حرف):")
            return 0
        # باید انگلیسی باشه چون همین اسم مستقیم توی ایمیل‌های رسمی به اساتید/HR
        # استفاده می‌شه — اسم فارسی توی یک ایمیل انگلیسی رسمی بد به‌نظر می‌رسه.
        if not re.match(r"^[A-Za-z][A-Za-z\s\.\-']{2,59}$", text):
            await safe(update.message,
                "❌ لطفاً نام و نام‌خانوادگی را **به انگلیسی** و با حروف لاتین وارد کنید "
                "(مثال: Ali Rezaei):")
            return 0
        ud["pending_fullname"] = text
        ud[S] = ST_FULLNAME_CONFIRM
        await safe(update.message,
            f"نام کامل شما: *{text}*\n\n"
            "همین درست است؟ (این اسم دقیقاً همینطور در ایمیل‌های رسمی استفاده می‌شود)",
            reply_markup=mk(["✅ درست است"], ["✏️ ویرایش کنم"]))
        return 0

    if step == ST_FULLNAME_CONFIRM:
        if text == "✏️ ویرایش کنم":
            ud[S] = ST_FULLNAME
            await safe(update.message, "نام کامل خود را دوباره (به انگلیسی) وارد کنید:")
            return 0
        full_name = ud.pop("pending_fullname", text)
        await asyncio.to_thread(db_set_user, uid, full_name=full_name)
        ud[S] = ST_PHONE
        await safe(update.message, "📱 شماره تماس خود را وارد کنید:")
        return 0

    # ── شماره تماس ───────────────────────────────────────────────
    if step == ST_PHONE:
        # اعتبارسنجی قدیمی فقط طول رشته (بعد از حذف هر چیزی جز رقم/+) رو
        # چک می‌کرد — یعنی ورودی‌هایی مثل "++++++++" یا رشته‌ای با «+»
        # وسط شماره (مثل "12+34+56+78") هم رد می‌شدن، و هیچ سقفی هم روی
        # طول نبود (یه پیام خیلی طولانی می‌تونست بی‌دلیل قبول بشه). اینجا:
        #  ۱. اول یک نرمال‌سازی سبک: ارقام فارسی/عربی → لاتین (دقیقاً مثل
        #     کد VIP بالاتر)، چون کیبورد فارسی خیلی وقت‌ها شماره رو با
        #     ارقام فارسی می‌فرسته و اون‌وقت حتی شماره‌ی درست هم رد می‌شد.
        #  ۲. الگوی واقعی شماره تلفن: فقط یک «+» اختیاری در ابتدا، بعدش
        #     فقط رقم، بین ۸ تا ۱۵ رقم (استاندارد E.164) — نه رقم شمارش
        #     «+»های پراکنده به‌عنوان کاراکتر معتبر.
        _fa_digits = "۰۱۲۳۴۵۶۷۸۹"
        _ar_digits = "٠١٢٣٤٥٦٧٨٩"
        normalized = text.strip()[:40]  # سقف طول قبل از هر پردازشی — جلوگیری از ورودی غیرمنطقی طولانی
        for i, d in enumerate(_fa_digits):
            normalized = normalized.replace(d, str(i))
        for i, d in enumerate(_ar_digits):
            normalized = normalized.replace(d, str(i))
        # فاصله/خط‌تیره/پرانتز بین ارقام (فرمت‌های رایج مثل "0912 345 6789"
        # یا "(0912) 345-6789") نادیده گرفته می‌شن، ولی خودِ رقم‌ها حفظ می‌شن.
        cleaned = re.sub(r"[\s\-\(\)]", "", normalized)
        m = re.fullmatch(r"\+?\d{8,15}", cleaned)
        if not m:
            await safe(update.message,
                "❌ شماره تماس معتبر نیست.\n"
                "لطفاً فقط رقم (و در صورت نیاز «+» کد کشور در ابتدا) وارد کنید، "
                "بین ۸ تا ۱۵ رقم — مثال: 09123456789 یا +989123456789:")
            return 0
        phone = cleaned
        await asyncio.to_thread(db_set_user, uid, phone=phone)
        u = await asyncio.to_thread(db_get_user, uid)
        if not u:
            # حالت نادر ولی واقعی: ردیف کاربر توی جدول users به هر دلیلی
            # (مثلاً دیتابیس بین ری‌استارت‌ها عوض/ریست شده ولی session
            # قبلی از SQLiteUserDataPersistence برگشته و کاربر مستقیم از
            # همین قدم (ST_PHONE) ادامه پیدا کرده، بدون این‌که cmd_start
            # دوباره db_upsert_user رو صدا بزنه) خالیه. قبلاً اینجا
            # `u.get(...)` مستقیم روی None صدا زده می‌شد و کل پردازش پیام
            # (و در نتیجه، از دید کاربر، خودِ بات) با یک exception خفه‌شده
            # متوقف می‌موند — دقیقاً همون «قفل‌شدن وسط گرفتن شماره تماس».
            # این‌جا به‌جای کرش، ردیف کاربر رو دوباره می‌سازیم و ادامه می‌دیم.
            await asyncio.to_thread(
                db_upsert_user, uid, update.effective_user.username, update.effective_user.first_name)
            await asyncio.to_thread(db_set_user, uid, phone=phone)
            u = await asyncio.to_thread(db_get_user, uid) or {}
            logger.warning(f"[uid={uid}] db_get_user غیرمنتظره None بود در قدم ST_PHONE — کاربر بازسازی شد")
        await notify_admin(context.bot,
            f"📋 اطلاعات کاربر جدید\n"
            f"👤 {u.get('full_name','')} | 📱 {phone}\n"
            f"🆔 {uid} | 🛠 {ud.get(S_SRV,'')}")
        ud[S] = ST_COLLECT
        await _start_collect(update, context)
        return 0

    # ── ارسال در حال انجام (پس‌زمینه) ──────────────────────────────
    # این حالت هم قبلاً ست می‌شد ولی handler ای براش نبود — اگه کاربر وسط
    # ارسال پس‌زمینه هر پیامی می‌فرستاد (کنجکاوی، پرسیدن وضعیت)، به
    # fallback عمومی «متوجه نشدم» می‌خورد.
    if step == ST_SENDING:
        if _has_active_send(uid):
            await safe(update.message,
                "⏳ ارسال هنوز در حال انجامه — هر ایمیل که بره، همینجا بهتون اطلاع می‌دیم.\n"
                "برای متوقف کردنش می‌تونید /cancel رو بزنید.")
        else:
            # ارسال قبلاً تموم شده (یا دیگه هیچ Task فعالی نیست) ولی state
            # هنوز ST_SENDING مونده — کاربر رو گیر نمی‌ندازیم.
            ud[S] = ST_MAIN_MENU
            await safe(update.message,
                "به نظر می‌رسه ارسال قبلی تموم شده. به منوی اصلی برگشتید.",
                reply_markup=MAIN_KB)
        return 0

    # ── جمع‌آوری اطلاعات سرویس ───────────────────────────────────
    if step == ST_COLLECT:
        return await _handle_collect(update, context)

    # ── تایید نهایی اطلاعات قبل از پرداخت ─────────────────────────
    if step == ST_INFO_CONFIRM:
        if text == "✅ بله، درسته":
            await _show_payment(update, context)
            return 0
        if text == "✏️ می‌خوام ویرایش کنم":
            await _begin_fresh_collect(update, context)
            return 0
        await safe(update.message, "یکی از گزینه‌های زیر رو انتخاب کنید:",
            reply_markup=mk(["✅ بله، درسته"], ["✏️ می‌خوام ویرایش کنم"], ["🔙 برگشت"]))
        return 0

    # ── تایید/به‌روزرسانی پروفایل ذخیره‌شده (AI Career Profile) ──────
    if step == ST_PROFILE_REUSE:
        saved = ud.get("_saved_profile") or {}
        if text == "✅ همینو استفاده کن":
            ud[S_CLIENT] = dict(saved)
            ud.pop("_saved_profile", None)
            ud[S] = ST_COLLECT
            return await _finish_collect(update, context)
        if text == "✏️ به‌روزرسانی کنم":
            await _begin_fresh_collect(update, context)
            return 0
        await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:",
            reply_markup=mk(["✅ همینو استفاده کن"], ["✏️ به‌روزرسانی کنم"], ["🔙 برگشت"]))
        return 0

    # ── انتخاب روش ورود: گوگل یا App Password ────────────────────
    if step == ST_AUTH_CHOICE:
        if text == "🔵 ورود سریع با گوگل (پیشنهادی)" and GOOGLE_LOGIN_ENABLED:
            url = build_google_auth_url(uid)
            ud[S] = ST_WAIT_GOOGLE
            ud[S_AUTH_METHOD] = "google"
            await update.message.reply_text(
                "🔵 روی دکمه‌ی زیر بزنید، Gmail خودتان را انتخاب و اجازه‌ی «ارسال ایمیل» را "
                "بدهید. بعد از تایید، بدون هیچ کار دیگه‌ای خودکار همینجا ادامه می‌دیم — "
                "نیازی به وارد کردن پسورد نیست.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔵 ورود با گوگل", url=url)]]))
            return 0
        if text == "🔑 با Gmail App Password":
            ud[S] = ST_SMTP_EMAIL
            ud[S_AUTH_METHOD] = "password"
            await safe(update.message,
                "📧 آدرس ایمیل فرستنده را وارد کنید:\n"
                "(مثال: yourname@gmail.com)",
                reply_markup=BACK_KB)
            return 0
        await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=auth_choice_kb())
        return 0

    # ── در انتظار برگشت کاربر از مرورگر گوگل ─────────────────────
    if step == ST_WAIT_GOOGLE:
        if text == "🔵 تلاش دوباره با گوگل":
            ud[S] = ST_AUTH_CHOICE
            await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=auth_choice_kb())
            return 0
        if text == "🔑 با Gmail App Password":
            ud[S] = ST_SMTP_EMAIL
            ud[S_AUTH_METHOD] = "password"
            await safe(update.message,
                "📧 آدرس ایمیل فرستنده را وارد کنید:", reply_markup=BACK_KB)
            return 0
        await safe(update.message,
            "⏳ هنوز منتظر تکمیل ورود با گوگل در مرورگر هستیم.\n"
            "اگر دکمه را زده و اجازه دادید ولی چیزی این‌جا نیامد، چند لحظه صبر کنید، یا:",
            reply_markup=mk(["🔵 تلاش دوباره با گوگل"], ["🔑 با Gmail App Password"], ["🔙 برگشت"]))
        return 0

    # ── ایمیل SMTP ───────────────────────────────────────────────
    if step == ST_SMTP_EMAIL:
        if not re.match(r"[^@]+@[^@]+\.[^@]+", text):
            await safe(update.message, "❌ آدرس ایمیل معتبر نیست. دوباره وارد کنید:")
            return 0
        host, port = detect_smtp(text)
        ud[S_SMTP_E] = text; ud[S_SMTP_H] = host; ud[S_SMTP_PORT] = port
        ud["smtp_fail_count"] = 0
        ud[S] = ST_SMTP_PASS
        await safe(update.message,
            f"✅ ایمیل: {text}\n🔒 رمز را وارد کنید:\n\n"
            "• برای Gmail/Yahoo/Outlook: باید «App Password» بسازید (رمز اصلی ایمیل قبول نمی‌شود).\n"
            "  💡 Gmail: myaccount.google.com/apppasswords\n"
            "• برای ایمیل دانشگاهی/سازمانی: همان رمز عادی ورود به همان ایمیل را وارد کنید.",
            reply_markup=BACK_KB)
        return 0

    if step == ST_SMTP_PASS:
        if len(text) < 6:
            await safe(update.message, "❌ رمز خیلی کوتاه است.")
            return 0
        ud["_pending_pw"] = text
        await safe(update.message, "🔄 در حال تلاش برای اتصال (چند روش را امتحان می‌کنیم)...")
        ok, err, used_host, used_port = await asyncio.to_thread(
            try_smtp_connect, ud[S_SMTP_E], text, ud.get(S_SMTP_H), ud.get(S_SMTP_PORT))

        if not ok:
            ud["smtp_fail_count"] = ud.get("smtp_fail_count", 0) + 1
            if ud["smtp_fail_count"] >= 2:
                # هم روی هاست/پورت اول و هم بعد از یک بار تلاش دوباره‌ی
                # کاربر (که خودش چند حالت دیگر را هم پشت‌صحنه امتحان کرد)
                # ناموفق بودیم — احتمال زیاد این ایمیل اصلاً اتصال بیرونی
                # را باز نگذاشته (خیلی از ایمیل‌های دانشگاهی همین‌طورند).
                # به‌جای گیر افتادن کاربر در یک حلقه‌ی بی‌پایان، مسیر
                # جایگزین (ارسال دستی) را پیشنهاد می‌دهیم.
                ud[S] = "manual_offer"
                await _show_manual_offer(update, context,
                    f"❌ با این اطلاعات نتونستیم به‌صورت خودکار به این ایمیل وصل بشیم.\n"
                    f"(دلیل فنی: {err})\n\n"
                    "این معمولاً یعنی این ایمیل اتصال از بیرون (SMTP) رو باز نگذاشته — خیلی از "
                    "ایمیل‌های دانشگاهی همین‌طورن.")
                return 0
            await safe(update.message,
                f"❌ اتصال ناموفق: {err}\n\nرمز را دوباره وارد کنید، یا اگر مطمئن نیستید درست است، "
                "دوباره از همون ایمیل امتحان کنید (چند روش دیگه رو خودکار امتحان می‌کنیم):")
            return 0

        ud[S_SMTP_P] = text
        ud[S_SMTP_H] = used_host; ud[S_SMTP_PORT] = used_port
        ud[S_AUTH_METHOD] = "password"
        ud.pop("_pending_pw", None)

        # نکته‌ی مهم: قبلاً این‌جا بدون توجه به نوع سرویس (email/job) همیشه
        # به ST_PROF_LIST می‌رفت — یعنی کاربری که برای «اپلای کار» ایمیلش
        # را وارد کرده بود، ناگهان پیام «حالا لیست اساتید» می‌دید که برای
        # اون سرویس بی‌معنی بود. الان بر اساس S_SRV شاخه می‌کنیم.
        if ud.get(S_SRV) == "job":
            client = ud.get(S_CLIENT, {})
            ud[S] = ST_JOB_FIELD
            await safe(update.message,
                "✅ اتصال ایمیل موفق!\n\n"
                f"رشته جستجو: {client.get('field','')}\n"
                f"کشور: {client.get('countries','')}\n\n"
                "رشته دقیق برای جستجو را تایید یا ویرایش کنید:",
                reply_markup=SEARCH_CONFIRM_KB)
            return 0

        ud[S] = ST_PROF_LIST
        await safe(update.message,
            "✅ اتصال ایمیل موفق!\n\n"
            "حالا لیست اساتید:\n\n"
            "1️⃣ «🤖 بات پیدا کند» — بات خودش جستجو می‌کند\n"
            "2️⃣ لینک/اسم استاد — یا چند خط با ایمیل اساتید\n\n"
            "انتخاب کنید:",
            reply_markup=mk(["🤖 بات خودش پیدا کند"],["📋 لیست خودم دارم"],["🔙 برگشت"]))
        return 0

    # ── هر دو روش اتصال شکست خورد → پیشنهاد حالت دستی ────────────
    if step == "manual_offer":
        capped = ud.get("_manual_offer_rounds", 0) >= 3
        if text == "🛠 خودم آدرس SMTP رو دارم" and not capped:
            ud[S] = ST_CUSTOM_SMTP
            await safe(update.message,
                "🖥 آدرس سرور SMTP و پورت رو بفرستید (از تنظیمات وبمیل دانشگاه‌تون یا پشتیبانی IT بگیرید).\n\n"
                "می‌تونید توی یه پیام هر دو رو بفرستید، مثلاً:\n"
                "`mail.iau.ac.ir 587`\n\n"
                "نیازی نیست SSL یا TLS رو خودتون مشخص کنید — بات از روی پورت خودش تشخیص می‌ده.",
                reply_markup=BACK_KB)
            return 0
        if text == "📋 بات آماده کنه، خودم بفرستم":
            ud[S_SMTP_P]      = MANUAL_MODE_SENTINEL
            ud[S_SMTP_H]      = "manual"
            ud[S_SMTP_PORT]   = 0
            ud[S_AUTH_METHOD] = "manual"
            note = ("✅ باشه! از این به بعد بات همه‌چیز (ایمیل گیرنده + موضوع + متن آماده) رو "
                    "توی همین چت براتون میفرسته و شما فقط باید کپی و ارسال کنید.\n\n")
            if ud.get(S_SRV) == "job":
                client = ud.get(S_CLIENT, {})
                ud[S] = ST_JOB_FIELD
                await safe(update.message,
                    note +
                    f"رشته جستجو: {client.get('field','')}\n"
                    f"کشور: {client.get('countries','')}\n\n"
                    "رشته دقیق برای جستجو را تایید یا ویرایش کنید:",
                    reply_markup=SEARCH_CONFIRM_KB)
                return 0
            ud[S] = ST_PROF_LIST
            await safe(update.message,
                note +
                "حالا لیست اساتید:\n\n"
                "1️⃣ «🤖 بات پیدا کند» — بات خودش جستجو می‌کند\n"
                "2️⃣ لینک/اسم استاد — یا چند خط با ایمیل اساتید\n\n"
                "انتخاب کنید:",
                reply_markup=mk(["🤖 بات خودش پیدا کند"],["📋 لیست خودم دارم"],["🔙 برگشت"]))
            return 0
        if text == "🔁 دوباره با ایمیل/پسورد دیگه" and not capped:
            ud["smtp_fail_count"] = 0
            ud[S] = ST_SMTP_EMAIL
            await safe(update.message, "📧 آدرس ایمیل فرستنده را وارد کنید:", reply_markup=BACK_KB)
            return 0
        if capped:
            await safe(update.message,
                "برای اینکه وقتتون تلف نشه، فقط گزینه‌ی «متن آماده» در دسترسه:",
                reply_markup=mk(["📋 بات آماده کنه، خودم بفرستم"], ["🔙 برگشت"]))
            return 0
        await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=mk(
            ["🛠 خودم آدرس SMTP رو دارم"],
            ["📋 بات آماده کنه، خودم بفرستم"], ["🔁 دوباره با ایمیل/پسورد دیگه"], ["🔙 برگشت"]))
        return 0

    # ── کاربر خودش سرور/پورت SMTP را می‌دهد (آخرین تلاش قبل از حالت دستی) ──
    if step == ST_CUSTOM_SMTP:
        parsed = parse_custom_smtp(text)
        if not parsed:
            await safe(update.message,
                "❌ فرمت رو متوجه نشدم. آدرس سرور و پورت رو بفرستید، مثلاً:\n`mail.iau.ac.ir 587`",
                reply_markup=BACK_KB)
            return 0
        host, port = parsed
        pw = ud.get("_pending_pw", "")
        if not pw:
            # اگه پسوردی که قبلاً امتحان کرده بود رو نداریم (مثلاً بات ری‌استارت
            # شده)، از کاربر دوباره می‌خوایم — بدون این‌که کرش کنیم یا گیر بیفته.
            ud[S] = ST_SMTP_PASS
            ud[S_SMTP_H] = host; ud[S_SMTP_PORT] = port
            await safe(update.message,
                f"✅ سرور ثبت شد: {host}:{port}\n🔒 حالا رمز ایمیل را دوباره وارد کنید:",
                reply_markup=BACK_KB)
            return 0
        await safe(update.message, "🔄 در حال تست اتصال با این تنظیمات...")
        ok, err = await asyncio.to_thread(test_smtp, ud[S_SMTP_E], pw, host, port)
        if not ok:
            ud[S] = "manual_offer"
            await _show_manual_offer(update, context, f"❌ با {host}:{port} هم وصل نشد.\n(دلیل فنی: {err})")
            return 0

        ud[S_SMTP_P] = pw
        ud[S_SMTP_H] = host; ud[S_SMTP_PORT] = port
        ud[S_AUTH_METHOD] = "password"
        ud.pop("_pending_pw", None)
        if ud.get(S_SRV) == "job":
            client = ud.get(S_CLIENT, {})
            ud[S] = ST_JOB_FIELD
            await safe(update.message,
                "✅ اتصال ایمیل موفق!\n\n"
                f"رشته جستجو: {client.get('field','')}\n"
                f"کشور: {client.get('countries','')}\n\n"
                "رشته دقیق برای جستجو را تایید یا ویرایش کنید:",
                reply_markup=SEARCH_CONFIRM_KB)
            return 0
        ud[S] = ST_PROF_LIST
        await safe(update.message,
            "✅ اتصال ایمیل موفق!\n\n"
            "حالا لیست اساتید:\n\n"
            "1️⃣ «🤖 بات پیدا کند» — بات خودش جستجو می‌کند\n"
            "2️⃣ لینک/اسم استاد — یا چند خط با ایمیل اساتید\n\n"
            "انتخاب کنید:",
            reply_markup=mk(["🤖 بات خودش پیدا کند"],["📋 لیست خودم دارم"],["🔙 برگشت"]))
        return 0

    # ── لیست اساتید ─────────────────────────────────────────────
    if step == ST_PROF_LIST:
        client = ud.get(S_CLIENT, {})
        field  = client.get("field", "")
        country= client.get("countries", "Germany")

        if text == "🤖 بات خودش پیدا کند":
            if not field:
                await safe(update.message, "رشته تحصیلی خود را بنویسید:")
                ud["waiting_field"] = True
                return 0
            ud[S] = ST_CONFIRM_PROF_SEARCH
            await safe(update.message,
                f"📚 رشته: {field or '—'}\n"
                f"🌍 کشور(ها): {country or '—'}\n\n"
                "اگه درسته «✅ تایید و شروع جستجو» رو بزنید، یا برای تغییر «✏️ ویرایش» رو بزنید:",
                reply_markup=SEARCH_CONFIRM_KB)
            return 0

        if ud.get("waiting_field"):
            ud.pop("waiting_field")
            client["field"] = text; ud[S_CLIENT] = client
            ud[S] = ST_CONFIRM_PROF_SEARCH
            await safe(update.message,
                f"📚 رشته: {text or '—'}\n"
                f"🌍 کشور(ها): {country or '—'}\n\n"
                "اگه درسته «✅ تایید و شروع جستجو» رو بزنید، یا برای تغییر «✏️ ویرایش» رو بزنید:",
                reply_markup=SEARCH_CONFIRM_KB)
            return 0

        if text == "📋 لیست خودم دارم":
            await safe(update.message,
                "لیست را وارد کنید (هر خط یک استاد):\n"
                "فرمت: نام | ایمیل  یا  فقط ایمیل  یا  لینک",
                reply_markup=BACK_KB)
            ud["waiting_list"] = True
            return 0

        if ud.get("waiting_list"):
            ud.pop("waiting_list")
            profs = _parse_prof_list(text)
            if not profs:
                await safe(update.message, "❌ فرمت نادرست. دوباره وارد کنید.")
                ud["waiting_list"] = True
                return 0
            ud[S_PROFS] = profs
            await _start_email_preview(update, context)
            return 0

        await safe(update.message, "یکی از گزینه‌ها را انتخاب کنید.",
            reply_markup=mk(["🤖 بات خودش پیدا کند"],["📋 لیست خودم دارم"],["🔙 برگشت"]))
        return 0

    # ── تایید/ویرایش رشته و کشور قبل از جستجوی اساتید ────────────
    if step == ST_CONFIRM_PROF_SEARCH:
        client = ud.get(S_CLIENT, {})

        if text == "✅ تایید و شروع جستجو":
            field   = client.get("field", "")
            country = client.get("countries") or ""
            deep_all_countries = bool(client.get("_deep_search_all_countries")) or not country
            country_display = "🌍 همه‌ی کشورها (جستجوی عمیق و کامل)" if deep_all_countries else country

            # ── AI Recommendation + AI CV Analyzer ─────────────────
            # هر دو هم‌زمان (asyncio.gather) اجرا می‌شن، نه پشت‌سرهم —
            # یعنی این دو تحلیل جدید هیچ زمان اضافه‌ای به کاربر تحمیل
            # نمی‌کنن (به‌جای دو انتظار جدا، یکی مثل قبل طول می‌کشه).
            # هر دو کاملاً fail-safe: خطای هرکدوم فقط باعث می‌شه همون
            # بخش نمایش داده نشه، نه توقف یا کرش جستجو.
            related, cv_report = await asyncio.gather(
                suggest_related_fields(field, client.get("resume", "")),
                analyze_cv_quality(client),
                return_exceptions=True)
            if isinstance(related, Exception):
                related = []
            if isinstance(cv_report, Exception):
                cv_report = None

            search_field = field
            msg_parts = []
            if cv_report and cv_report.get("strength") in ("weak", "moderate"):
                tip_lines = []
                if cv_report.get("missing_publications"):
                    tip_lines.append("✔ مقالات/انتشارات کمه یا مشخص نیست")
                if cv_report.get("missing_skills"):
                    tip_lines.append("✔ مهارت‌های کمتر ذکرشده: " + ", ".join(cv_report["missing_skills"]))
                if cv_report.get("improvement_tip"):
                    tip_lines.append(f"💡 {cv_report['improvement_tip']}")
                if tip_lines:
                    strength_label = "ضعیف" if cv_report["strength"] == "weak" else "متوسط"
                    msg_parts.append(
                        f"📄 تحلیل سریع رزومه (وضعیت: {strength_label}):\n" + "\n".join(tip_lines) +
                        "\n(این فقط یه پیشنهاده — همین الان هم ادامه می‌دیم، نیازی به توقف نیست)")

            if related:
                search_field = field + ", " + ", ".join(related)
                rec_lines = "\n".join(f"✔ {r}" for r in related)
                msg_parts.append(
                    f"🔍 جستجو کردید: {field}\n\n"
                    f"💡 پیشنهاد هوش مصنوعی (برای پیدا کردن اساتید بیشتر، این حوزه‌های "
                    f"مرتبط هم جستجو می‌شن):\n{rec_lines}")

            init_text = ("\n\n".join(msg_parts) + f"\n\n🔍 در حال جستجوی دقیق اساتید {field} در {country_display}...\n⏳ لطفاً صبر کنید."
                         if msg_parts else f"🔍 در حال جستجوی دقیق اساتید {field} در {country_display}...\n⏳ لطفاً صبر کنید.")
            progress_msg_id = None
            try:
                sent_msg = await update.message.reply_text(init_text)
                progress_msg_id = sent_msg.message_id
            except Exception as e:
                logger.warning(f"progress message send failed: {e}")

            # ── پیام پیشرفت زنده: هر ۴ ثانیه edit می‌شه (نه پیام جدید) تا
            # کاربر ببینه چند درصد از منابع بررسی شده — به‌جای این‌که فکر
            # کنه بات هنگ کرده یا سطحی/رهگذری داره می‌گرده. ──────────────
            search_progress: dict = {}
            reporter_task = None
            if progress_msg_id:
                reporter_task = asyncio.create_task(_live_search_progress_reporter(
                    context.bot, update.effective_chat.id, progress_msg_id, search_progress))

            logger.info(f"[uid={uid}] شروع جستجوی اساتید | field={search_field!r} country={country!r} "
                        f"deep_all_countries={deep_all_countries}")
            try:
                profs = await asyncio.to_thread(
                    search_professors, search_field, country, EMAIL_QUOTA, client, search_progress)
            except Exception as e:
                # این‌جا عمداً کل مکالمه رو با خطای عمومی و «دوباره /start
                # بزن» خراب نمی‌کنیم — چون کاربر تا همینجا (اطلاعات،
                # پرداخت و...) رو طی کرده و از دست دادنش خیلی بده. به‌جاش
                # همینجا با یه دکمه‌ی «تلاش دوباره» نگهش می‌داریم.
                logger.error(f"search_professors crashed: {e}")
                traceback.print_exc()
                if reporter_task:
                    reporter_task.cancel()
                await safe(update.message,
                    "⚠️ توی جستجوی اساتید یه خطای غیرمنتظره پیش اومد.\n"
                    "اطلاعات و پیشرفتتون گم نشده — می‌تونید همین الان دوباره امتحان کنید:",
                    reply_markup=SEARCH_CONFIRM_KB)
                return 0
            finally:
                if reporter_task and not reporter_task.done():
                    reporter_task.cancel()
            if progress_msg_id:
                try:
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id, message_id=progress_msg_id,
                        text=f"✅ جستجو تمام شد — {len(profs)} استاد پیدا شد. در حال آماده‌سازی...")
                except Exception:
                    pass
            ud[S_PROFS] = profs
            await _start_email_preview(update, context)
            return 0

        if text == "✏️ ویرایش رشته/کشور":
            ud["_editing_prof_search"] = True
            await safe(update.message,
                "رشته و کشور رو با کاما جدا و در یک پیام بفرستید، مثلاً:\n"
                "Computer Science, Germany\n\n"
                "یا فقط یکی رو بفرستید (مثلاً فقط اسم کشور) تا فقط همون عوض بشه.\n\n"
                f"یا برای جستجوی عمیق در همه‌ی کشورها، دکمه‌ی «{ALL_COUNTRIES_LABEL}» رو بزنید:",
                reply_markup=mk([ALL_COUNTRIES_LABEL], ["🔙 برگشت"]))
            return 0

        if ud.pop("_editing_prof_search", False):
            if text == ALL_COUNTRIES_LABEL:
                client["countries"] = ""
                client["_deep_search_all_countries"] = True
            else:
                parts = [p.strip() for p in text.split(",") if p.strip()]
                if len(parts) >= 2:
                    client["field"] = parts[0]; client["countries"] = parts[1]
                elif len(parts) == 1:
                    client["countries"] = parts[0]
                client["_deep_search_all_countries"] = False
            ud[S_CLIENT] = client

        await safe(update.message,
            f"📚 رشته: {client.get('field','') or '—'}\n"
            f"🌍 کشور(ها): {client.get('countries','') or '—'}\n\n"
            "اگه درسته «✅ تایید و شروع جستجو» رو بزنید، یا برای تغییر «✏️ ویرایش» رو بزنید:",
            reply_markup=SEARCH_CONFIRM_KB)
        return 0

    # ── پیش‌نمایش ایمیل ─────────────────────────────────────────
    if step == ST_PREVIEW:
        return await _handle_preview(update, context)

    # ── تغییر رشته/کشور/رزومه از وسط پیش‌نمایش ─────────────────────
    # Parent این State دقیقاً ST_PREVIEW است (STATE_PARENT بالا) — یعنی
    # دکمه‌ی برگشت از این‌جا، و از هر زیر-سوالش، همیشه به همون پیش‌نمایشی
    # که کاربر قبلاً داشت برمی‌گرده، نه به منوی اصلی.
    if step == ST_PREVIEW_SETTINGS:
        client = ud.get(S_CLIENT, {})

        if ud.pop("_awaiting_preview_country", False):
            new_country = text.strip()
            if not new_country:
                ud["_awaiting_preview_country"] = True
                await safe(update.message, "متنی دریافت نشد — کشور هدف جدید را بنویسید:", reply_markup=BACK_KB)
                return 0
            client["countries"] = new_country
            ud[S_CLIENT] = client
            await safe(update.message, f"🔍 جستجوی مجدد در «{new_country}»...")
            try:
                profs = await asyncio.to_thread(
                    search_professors, client.get("field",""), new_country, EMAIL_QUOTA, client)
            except Exception as e:
                logger.error(f"search_professors (preview-settings country retry) crashed: {e}")
                traceback.print_exc()
                await safe(update.message,
                    "⚠️ خطای غیرمنتظره پیش اومد. یه کشور دیگه امتحان کنید یا برگردید:",
                    reply_markup=PREVIEW_SETTINGS_KB)
                return 0
            ud[S_PROFS] = profs
            if not profs:
                # قبلاً اینجا فقط PREVIEW_SETTINGS_KB (تغییر کشور/رشته/رزومه) نشون
                # داده می‌شد — یعنی «تلاش مجدد»، «هر ۲۴ ساعت» و «لیست خودم دارم»
                # که توی مسیر اصلی (بعد از اولین جستجو) هست، اینجا نبود. الان
                # دقیقاً همون منو رو نشون می‌دیم؛ state همچنان ST_PREVIEW_SETTINGS
                # می‌مونه، پس دکمه‌ی برگشت طبق STATE_PARENT درست به ST_PREVIEW
                # برمی‌گرده (نه منوی اصلی) — رفتار قبلی این بخش دست‌نخورده می‌مونه.
                ud["_prof_retry_attempt"] = 0
                cls_header = classify_prof_search_result([]).get("header", "")
                await safe(update.message,
                    f"{cls_header}\n\n"
                    f"با «{new_country}» استادی پیدا نشد.\n\n"
                    f"می‌خواهید چه کاری انجام دهید؟",
                    reply_markup=mk(
                        ["🔁 تلاش مجدد"],
                        ["🌍 تغییر کشور / دانشگاه هدف"],
                        ["🔬 تغییر رشته / تخصص"],
                        ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                        ["📋 لیست خودم دارم"],
                        ["🔙 برگشت"]
                    ))
                return 0
            cls_header = classify_prof_search_result(profs).get("header", "")
            await safe(update.message, cls_header)
            await _render_prof_preview(update, context, 0)
            return 0

        if text == "🌍 تغییر کشور / دانشگاه هدف":
            ud["_awaiting_preview_country"] = True
            await safe(update.message,
                "کشور هدف جدید را بنویسید:\n(مثال: Germany یا Canada Netherlands)",
                reply_markup=BACK_KB)
            return 0

        if ud.pop("_awaiting_preview_field", False):
            new_field = text.strip()
            if not new_field:
                ud["_awaiting_preview_field"] = True
                await safe(update.message, "متنی دریافت نشد — رشته/تخصص جدید را بنویسید:", reply_markup=BACK_KB)
                return 0
            client["field"] = new_field
            ud[S_CLIENT] = client
            await safe(update.message, f"🔍 جستجوی مجدد برای «{new_field}»...")
            try:
                profs = await asyncio.to_thread(
                    search_professors, new_field, client.get("countries",""), EMAIL_QUOTA, client)
            except Exception as e:
                logger.error(f"search_professors (preview-settings field retry) crashed: {e}")
                traceback.print_exc()
                await safe(update.message,
                    "⚠️ خطای غیرمنتظره پیش اومد. دوباره امتحان کنید یا برگردید:",
                    reply_markup=PREVIEW_SETTINGS_KB)
                return 0
            ud[S_PROFS] = profs
            if not profs:
                # همون توضیح بالا (تغییر کشور) اینجا هم صدق می‌کنه — منوی کامل
                # به‌جای PREVIEW_SETTINGS_KB محدود، ولی state دست‌نخورده.
                ud["_prof_retry_attempt"] = 0
                cls_header = classify_prof_search_result([]).get("header", "")
                await safe(update.message,
                    f"{cls_header}\n\n"
                    f"با «{new_field}» استادی پیدا نشد.\n\n"
                    f"می‌خواهید چه کاری انجام دهید؟",
                    reply_markup=mk(
                        ["🔁 تلاش مجدد"],
                        ["🌍 تغییر کشور / دانشگاه هدف"],
                        ["🔬 تغییر رشته / تخصص"],
                        ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                        ["📋 لیست خودم دارم"],
                        ["🔙 برگشت"]
                    ))
                return 0
            cls_header = classify_prof_search_result(profs).get("header", "")
            await safe(update.message, cls_header)
            await _render_prof_preview(update, context, 0)
            return 0

        if text == "🔬 تغییر رشته / تخصص":
            ud["_awaiting_preview_field"] = True
            await safe(update.message, "رشته/تخصص جدید را بنویسید:", reply_markup=BACK_KB)
            return 0

        if ud.pop("_awaiting_preview_resume", False):
            new_resume = (update.message.text or "").strip() if update.message else ""
            if not new_resume or len(new_resume) < 30:
                ud["_awaiting_preview_resume"] = True
                await safe(update.message,
                    "متن خیلی کوتاه بود — لطفاً حداقل چند جمله درباره‌ی تحصیلات/سابقه/دستاوردهاتون "
                    "به انگلیسی بنویسید:", reply_markup=BACK_KB)
                return 0
            client["resume"] = new_resume
            client["resume_is_file"] = False
            client.pop("resume_file_id", None)
            ud[S_CLIENT] = client
            await asyncio.to_thread(db_save_career_profile, update.effective_user.id, client)
            await safe(update.message, "✅ رزومه به‌روزرسانی شد. در حال بازنویسی ایمیل نمونه با رزومه‌ی جدید...")
            idx = ud.get("preview_idx", 0)
            await _render_prof_preview(update, context, idx)
            return 0

        if text == "📝 تغییر رزومه":
            ud["_awaiting_preview_resume"] = True
            await safe(update.message,
                "📝 متن جدید رزومه/خلاصه‌ی سوابق خود را **به زبان انگلیسی** ارسال کنید "
                "(فقط متن — برای آپلود فایل PDF/Word باید از منوی اصلی، ویرایش کامل پروفایل را انجام دهید):",
                reply_markup=BACK_KB)
            return 0

        # ── دکمه‌های منوی «چیزی پیدا نشد» — همون رفتار prof_targeting، فقط
        # اینجا state تغییر نمی‌کنه (به‌جز مسیر «لیست خودم دارم») تا دکمه‌ی
        # برگشت طبق STATE_PARENT همچنان به همون پیش‌نمایش قبلی برگرده.
        if text == "🔁 تلاش مجدد":
            field   = client.get("field", "")
            country = client.get("countries", "")
            attempt = ud.get("_prof_retry_attempt", 0)
            ud["_prof_retry_attempt"] = attempt + 1
            # Retry 1 → Retry 2 → Retry 3 → University Discovery Mode →
            # ORCID کلیدواژه‌ای + جستجوی وب گسترده — لیبل نمایشی رو مستقیم
            # از خروجی run_prof_retry_attempt می‌گیریم (خودش تصمیم می‌گیره
            # کدوم مرحله اجرا بشه)، پس فقط یک لیبل موقت ساده تا قبل از
            # رسیدن نتیجه نشون می‌دیم.
            stage_label = (f"روش {attempt+1} از {TOTAL_PROF_RETRY_ATTEMPTS}"
                           if attempt < TOTAL_PROF_RETRY_ATTEMPTS
                           else f"روش {TOTAL_PROF_RETRY_ATTEMPTS} از {TOTAL_PROF_RETRY_ATTEMPTS}")
            # ⚠️ فیکس درخواستی: قبلاً این هندلر progress dict نمی‌ساخت و
            # زنده‌گزارش (_live_search_progress_reporter) صدا زده نمی‌شد —
            # یعنی کاربر فقط همین یک پیام ثابت رو می‌دید و تا اتمام کامل
            # (که با University/Company Discovery Mode می‌تونه چند ده
            # ثانیه طول بکشه) هیچ آپدیتی نداشت. الان دقیقاً مثل جستجوی
            # اول، یک پیام قابل‌ویرایش + progress dict زنده می‌سازیم.
            prof_progress: dict = {}
            reporter_task = None
            try:
                sent_msg = await update.message.reply_text(f"🔁 در حال تلاش مجدد ({stage_label})...\n⏳ لطفاً صبر کنید.")
                reporter_task = asyncio.create_task(_live_search_progress_reporter(
                    context.bot, update.effective_chat.id, sent_msg.message_id, prof_progress))
            except Exception as e:
                logger.warning(f"prof retry progress message send failed: {e}")
            try:
                result = await asyncio.to_thread(
                    run_prof_retry_attempt, field, country, EMAIL_QUOTA, client, attempt, prof_progress)
            except Exception as e:
                logger.error(f"run_prof_retry_attempt (preview-settings) crashed: {e}")
                traceback.print_exc()
                if reporter_task:
                    reporter_task.cancel()
                await safe(update.message,
                    "⚠️ این تلاش هم با خطا مواجه شد. می‌تونید دوباره «🔁 تلاش مجدد» بزنید "
                    "(روش بعدی امتحان می‌شه) یا کشور/رشته رو عوض کنید:",
                    reply_markup=mk(
                        ["🔁 تلاش مجدد"],
                        ["🌍 تغییر کشور / دانشگاه هدف"],
                        ["🔬 تغییر رشته / تخصص"],
                        ["🔙 برگشت"]
                    ))
                return 0
            finally:
                if reporter_task and not reporter_task.done():
                    reporter_task.cancel()
            profs = result["profs"]
            ud[S_PROFS] = profs
            if not profs:
                # هیچ Match قوی‌ای پیدا نشد — ولی اگه University Discovery
                # Mode کاندیدهای «نزدیک» (زیر آستانه‌ی match ولی نه بی‌ربط)
                # پیدا کرده باشه، به‌جای پیام خشک صفر نتیجه، شفاف پیشنهادشون
                # می‌کنیم؛ تصمیم نهایی با خود کاربره.
                close = result.get("close", [])
                if close:
                    ud["_prof_close_candidates"] = close
                    cls_header = classify_prof_search_result([], close).get("header", "")
                    await safe(update.message,
                        f"{cls_header}\n\n"
                        f"هیچ استادی که دقیقاً با رزومه‌ی شما Match باشد پیدا نشد، "
                        f"اما {len(close)} استاد نزدیک پیدا شد.\n\n"
                        f"می‌خواهید آن‌ها را ببینید؟",
                        reply_markup=mk(
                            ["👀 نمایش استادهای نزدیک"],
                            ["🔁 تلاش مجدد"],
                            ["🌍 تغییر کشور / دانشگاه هدف"],
                            ["🔬 تغییر رشته / تخصص"],
                            ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                            ["📋 لیست خودم دارم"],
                            ["🔙 برگشت"]
                        ))
                    return 0
                tried_all_five = ud["_prof_retry_attempt"] >= TOTAL_PROF_RETRY_ATTEMPTS
                extra = ("\n\nهر ۵ روش (منابع API سریع، University Discovery Mode روی دانشگاه‌های "
                         "واقعی این رشته/کشور، و ORCID + جستجوی وب گسترده) رو امتحان کردیم و هنوز "
                         "چیزی پیدا نشد — احتمالاً این ترکیب رشته/کشور واقعاً نتیجه‌ی کمی داره. "
                         "بهتره کشور رو گسترش بدید یا رشته رو کلی‌تر کنید.") if tried_all_five else ""
                cls_header = classify_prof_search_result([], []).get("header", "")
                await safe(update.message,
                    f"{cls_header}{extra}",
                    reply_markup=mk(
                        ["🔁 تلاش مجدد"],
                        ["🌍 تغییر کشور / دانشگاه هدف"],
                        ["🔬 تغییر رشته / تخصص"],
                        ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                        ["📋 لیست خودم دارم"],
                        ["🔙 برگشت"]
                    ))
                return 0
            cls_header = classify_prof_search_result(profs).get("header", "")
            await safe(update.message, cls_header)
            await _render_prof_preview(update, context, 0)
            return 0

        if text == "👀 نمایش استادهای نزدیک":
            close = ud.pop("_prof_close_candidates", [])
            if not close:
                await safe(update.message,
                    "⚠️ لیست استادهای نزدیک دیگه در دسترس نیست — لطفاً دوباره «🔁 تلاش مجدد» بزنید.",
                    reply_markup=mk(["🔁 تلاش مجدد"], ["🔙 برگشت"]))
                return 0
            ud[S_PROFS] = close
            await safe(update.message,
                f"👀 در حال آماده‌سازی پیش‌نمایش برای {len(close)} استاد نزدیک "
                f"(این‌ها دقیقاً Match قوی نبودن، ولی به رزومه‌تون نزدیک‌ان)...")
            await _render_prof_preview(update, context, 0)
            return 0

        if text == "🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده":
            if context.application.job_queue is None:
                await safe(update.message,
                    "⚠️ متاسفانه این قابلیت روی این نسخه از بات فعال نیست.\n"
                    "می‌تونید دستی کشور/رشته رو عوض کنید یا بعداً دوباره امتحان کنید:",
                    reply_markup=mk(["🌍 تغییر کشور / دانشگاه هدف"], ["🔬 تغییر رشته / تخصص"], ["🔙 برگشت"]))
                return 0
            await asyncio.to_thread(db_add_pending_search, uid, "professor",
                client.get("field", ""), client.get("countries", ""))
            ud[S] = ST_MAIN_MENU
            await safe(update.message,
                "✅ باشه! هر ۲۴ ساعت خودمون دوباره می‌گردیم و به محض پیدا شدن استاد "
                "مرتبط، همینجا بهتون خبر می‌دیم — کاری لازم نیست انجام بدید.\n\n"
                "فعلاً می‌تونید از منوی اصلی به کارای دیگه برسید.",
                reply_markup=MAIN_KB)
            return 0

        if text == "📋 لیست خودم دارم":
            # از این نقطه به بعد دیگه یه لیست کاملاً جدید شروع می‌شه (نه ادامه‌ی
            # همون پیش‌نمایش قبلی) — پس دقیقاً مثل مسیر اصلی، state به
            # ST_PROF_LIST می‌ره؛ همون رفتاری که در prof_targeting هم هست.
            ud[S] = ST_PROF_LIST
            ud["waiting_list"] = True
            await safe(update.message,
                "لیست را وارد کنید (هر خط یک استاد):\n"
                "فرمت: نام | ایمیل  یا  فقط ایمیل  یا  لینک",
                reply_markup=BACK_KB)
            return 0

        await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=PREVIEW_SETTINGS_KB)
        return 0

    # ── جستجوی شغل ───────────────────────────────────────────────
    if step == ST_JOB_FIELD:
        client = ud.get(S_CLIENT, {})

        if text == "✅ تایید و شروع جستجو":
            field   = client.get("field", "")
            country = client.get("countries") or ""
            deep_all_countries = bool(client.get("_deep_search_all_countries")) or not country
            country_display = "🌍 همه‌ی کشورها (جستجوی عمیق و کامل)" if deep_all_countries else country
            ud["_job_retry_attempt"] = 0  # شمارنده‌ی تلاش مجدد رو برای این جستجوی تازه صفر می‌کنیم

            # ── اسکن ATS اجباری — فقط بار اول یا وقتی رزومه از آخرین اسکن
            # عوض شده باشه (تشخیص با هش متن رزومه)؛ اگه همون رزومه‌ی قبلی رو
            # قبلاً اسکن کرده باشیم، run_ats_scan_flow خودش بی‌صدا رد می‌شه
            # (بدون AI call اضافه). اگه اسکن هم شکست بخوره (بعد از retry
            # داخلیش)، جستجوی کار همچنان ادامه پیدا می‌کنه — اسکن ATS کمکیه،
            # نباید کل قابلیت اپلای رو گروگان بگیره.
            try:
                await run_ats_scan_flow(update, context, uid, client, mandatory=True)
            except Exception as e:
                logger.error(f"run_ats_scan_flow (mandatory gate) crashed: {e}")

            progress_msg_id = None
            try:
                sent_msg = await update.message.reply_text(
                    f"🔍 در حال جستجوی دقیق فرصت‌های شغلی {field} در {country_display}...\n⏳ لطفاً صبر کنید.")
                progress_msg_id = sent_msg.message_id
            except Exception as e:
                logger.warning(f"job progress message send failed: {e}")

            job_progress: dict = {}
            reporter_task = None
            if progress_msg_id:
                reporter_task = asyncio.create_task(_live_search_progress_reporter(
                    context.bot, update.effective_chat.id, progress_msg_id, job_progress,
                    kind="فرصت‌های شغلی"))

            try:
                jobs = await asyncio.to_thread(search_jobs_realtime, field, country, JOB_QUOTA, job_progress)
            except Exception as e:
                logger.error(f"search_jobs_realtime crashed: {e}")
                traceback.print_exc()
                if reporter_task:
                    reporter_task.cancel()
                await safe(update.message,
                    "⚠️ توی جستجوی فرصت‌های شغلی یه خطای غیرمنتظره پیش اومد.\n"
                    "اطلاعات و پیشرفتتون گم نشده — می‌تونید همین الان دوباره امتحان کنید:",
                    reply_markup=SEARCH_CONFIRM_KB)
                return 0
            finally:
                if reporter_task and not reporter_task.done():
                    reporter_task.cancel()
            if progress_msg_id:
                try:
                    await context.bot.edit_message_text(
                        chat_id=update.effective_chat.id, message_id=progress_msg_id,
                        text=f"✅ جستجو تمام شد — {len(jobs)} فرصت شغلی پیدا شد.")
                except Exception:
                    pass
            ud[S_JOBS] = jobs

            if not jobs:
                await safe(update.message,
                    f"⚠️ فرصت شغلی برای «{field}» در «{country_display}» پیدا نشد.\n\n"
                    f"پیشنهاد:\n"
                    f"• «🔄 دوباره جستجو کن» رو بزنید — هر بار با بازه‌ی زمانی گسترده‌تر و منابع "
                    f"بیشتری دوباره می‌گردیم، نه دقیقاً همون جستجوی قبلی\n"
                    f"• کشور را گسترش دهید (مثلاً Remote یا همه‌ی کشورها)\n"
                    f"• رشته را کلی‌تر کنید (مثلاً «Software Engineer» به‌جای «Rust Backend Senior»)\n"
                    f"• یا اگه می‌خواید، خودمون هر ۲۴ ساعت یک‌بار دوباره براتون می‌گردیم و به محض "
                    f"پیدا شدن فرصت مرتبط بهتون خبر می‌دیم\n\n"
                    f"می‌خواهید چه کاری انجام دهید؟",
                    reply_markup=mk(
                        ["🔄 دوباره با همین تنظیمات جستجو کن"],
                        ["🌍 تغییر کشور هدف کار"],
                        ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                        ["🔙 برگشت"]
                    ))
                ud["_job_retry_field"] = field
                ud["_job_retry_country"] = country
                ud[S] = "job_search_empty"
                return 0

            if _has_active_send(uid):
                await safe(update.message,
                    "⏳ یک عملیات ارسال دیگر همین الان برای شما در حال اجراست — "
                    "صبر کنید تمام شود، یا با /cancel لغوش کنید.", reply_markup=MAIN_KB)
                return 0

            await safe(update.message,
                f"✅ {len(jobs)} فرصت شغلی یافت شد!\n\n"
                "🚀 در حال شروع اپلای...\n"
                "بات خودش به‌ترتیب می‌ره سراغ تک‌تک فرصت‌های باقی‌مونده — لازم نیست چیزی بزنید. "
                "نتیجه‌ی هر فرصت همین چت اطلاع داده می‌شود تا کل لیست (یا سقف "
                f"{JOB_QUOTA} فرصت) تموم بشه.")
            # نکته: قبلاً SMTP اصلاً به _send_jobs_bg پاس داده نمی‌شد، برای همین
            # حتی وقتی کاربر مسیر «ایمیل مستقیم به HR» را انتخاب و SMTP‌اش را
            # تست‌شده وارد کرده بود، اپلای ایمیلی هیچ‌وقت واقعاً اجرا نمی‌شد و
            # همه‌چیز به «فقط لینک پورتال» سقوط می‌کرد. الان مقادیر SMTP همراه
            # snapshot کاربر پاس داده می‌شوند (اگر مسیر email انتخاب نشده باشد،
            # این مقادیر خالی می‌مانند و رفتار قبلی/لینک پورتال دست‌نخورده می‌ماند).
            _t = asyncio.create_task(
                _run_bg_task(_send_jobs_bg(context, update.effective_chat.id, uid, jobs, client, ud.get(S_SUB),
                                smtp_e=ud.get(S_SMTP_E), smtp_p=ud.get(S_SMTP_P),
                                smtp_h=ud.get(S_SMTP_H, "smtp.gmail.com"), smtp_pt=ud.get(S_SMTP_PORT, 587)),
                             context, update.effective_chat.id, "اپلای کار"))
            _register_send_task(uid, _t)
            return 0

        if text == "✏️ ویرایش رشته/کشور":
            ud["_editing_job_search"] = True
            await safe(update.message,
                "رشته و کشور رو با کاما جدا و در یک پیام بفرستید، مثلاً:\n"
                "Computer Science, Germany\n\n"
                "یا فقط یکی رو بفرستید (مثلاً فقط اسم کشور) تا فقط همون عوض بشه.\n\n"
                f"یا برای جستجوی عمیق در همه‌ی کشورها، دکمه‌ی «{ALL_COUNTRIES_LABEL}» رو بزنید:",
                reply_markup=mk([ALL_COUNTRIES_LABEL], ["🔙 برگشت"]))
            return 0

        if ud.pop("_editing_job_search", False):
            if text == ALL_COUNTRIES_LABEL:
                client["countries"] = ""
                client["_deep_search_all_countries"] = True
            else:
                parts = [p.strip() for p in text.split(",") if p.strip()]
                if len(parts) >= 2:
                    client["field"] = parts[0]; client["countries"] = parts[1]
                elif len(parts) == 1:
                    client["countries"] = parts[0]
                client["_deep_search_all_countries"] = False
            ud[S_CLIENT] = client
        elif not client.get("field") and text.strip():
            # اولین بار که رشته اصلاً ثبت نشده — رو به‌عنوان رشته ثبت کن
            client["field"] = text
            ud[S_CLIENT] = client

        # همیشه قبل از هر جستجویی، دقیقاً همین رشته/کشور رو نشون بده و
        # منتظر تایید صریح بمون — تا هیچ‌وقت جستجو با مقداری که کاربر
        # ندیده/تایید نکرده شروع نشه.
        await safe(update.message,
            f"📚 رشته: {client.get('field','') or '—'}\n"
            f"🌍 کشور(ها): {client.get('countries','') or '—'}\n\n"
            "اگه درسته «✅ تایید و شروع جستجو» رو بزنید، یا برای تغییر «✏️ ویرایش» رو بزنید:",
            reply_markup=SEARCH_CONFIRM_KB)
        return 0

    # ── انتخاب روش ایمیل ─────────────────────────────────────────
    if step == "choose_apply_method":
        if text == "📨 از ایمیل شخصی خودم":
            ud[S] = ST_AUTH_CHOICE
            await safe(update.message,
                "چطور می‌خواهید وارد ایمیل خودتان شوید؟\n\n"
                "🔵 با گوگل: یک کلیک، بدون پسورد\n"
                "🔑 با App Password: روش قدیمی، نیاز به ساخت رمز مخصوص در تنظیمات گوگل",
                reply_markup=auth_choice_kb())
            return 0
        if text == "🔍 استعلام سفارت":
            return await _start_embassy_inquiry(update, context)
        await safe(update.message, "گزینه‌ای را انتخاب کنید.",
            reply_markup=mk(["📨 از ایمیل شخصی خودم"],["🔍 استعلام سفارت"],["🔙 برگشت"]))
        return 0

    # ── انتخاب روش اپلای کار ──────────────────────────────────────
    if step == "choose_job_method":
        client = ud.get(S_CLIENT, {})
        if text == "🔍 استعلام سفارت":
            return await _start_embassy_inquiry(update, context)
        if text == "📨 ایمیل مستقیم به HR":
            ud["job_method"] = "email"
            ud[S] = ST_AUTH_CHOICE
            await safe(update.message,
                "چطور می‌خواهید وارد ایمیل خودتان شوید؟\n\n"
                "🔵 با گوگل: یک کلیک، بدون پسورد\n"
                "🔑 با App Password: روش قدیمی، نیاز به ساخت رمز مخصوص در تنظیمات گوگل",
                reply_markup=auth_choice_kb())
            return 0
        if text == "🔗 لیست فرصت‌ها و لینک‌ها":
            ud["job_method"] = "portal"
            ud[S] = ST_JOB_FIELD
            await safe(update.message,
                f"رشته جستجو: {client.get('field','')}\n"
                f"کشور: {client.get('countries','')}\n\n"
                "رشته دقیق برای جستجو را تایید یا ویرایش کنید:",
                reply_markup=SEARCH_CONFIRM_KB)
            return 0
        await safe(update.message, "گزینه‌ای را انتخاب کنید.",
            reply_markup=mk(["📨 ایمیل مستقیم به HR"],["🔗 لیست فرصت‌ها و لینک‌ها"],
                             ["🔍 استعلام سفارت"],["🔙 برگشت"]))
        return 0

    # ── استعلام سفارت — کاملاً جانبی، هیچ‌جا سرویس ایمیل/شغل رو صدا
    # نمی‌زنه. هر شاخه‌ی این بلوک هم try/except خودش رو داره تا یک خطای
    # غیرمنتظره اینجا هرگز کل dispatch رو کرش نکنه.
    if step == ST_EMBASSY_NATIONALITY:
        if text == "ملیت دیگر (تایپ کنید)":
            await safe(update.message, "ملیت خود را تایپ کنید:", reply_markup=BACK_KB)
            return 0
        nat = "ایرانی" if text == "🇮🇷 ایرانی" else text.strip()[:60]
        if not nat:
            await safe(update.message, "یکی از گزینه‌ها را انتخاب یا ملیت را تایپ کنید:",
                reply_markup=embassy_nationality_kb())
            return 0
        ud["_embassy_nat"] = nat
        ud[S] = ST_EMBASSY_2ND_PASSPORT
        await safe(update.message, "۲️⃣ پاسپورت دوم دارید؟", reply_markup=embassy_2nd_passport_kb())
        return 0

    if step == ST_EMBASSY_2ND_PASSPORT:
        if text == "✅ دارم":
            ud[S] = ST_EMBASSY_2ND_COUNTRY
            await safe(update.message, "پاسپورت دوم مربوط به چه کشوریه؟ (تایپ کنید)", reply_markup=BACK_KB)
            return 0
        if text == "❌ ندارم":
            ud["_embassy_2nd_passport"] = ""
            ud[S] = ST_EMBASSY_COUNTRY
            await safe(update.message,
                "برای کدوم سفارت/کشور می‌خواهید استعلام بگیرید؟", reply_markup=embassy_country_kb())
            return 0
        await safe(update.message, "یکی از گزینه‌ها را انتخاب کنید:", reply_markup=embassy_2nd_passport_kb())
        return 0

    if step == ST_EMBASSY_2ND_COUNTRY:
        ud["_embassy_2nd_passport"] = text.strip()[:60]
        ud[S] = ST_EMBASSY_COUNTRY
        await safe(update.message,
            "برای کدوم سفارت/کشور می‌خواهید استعلام بگیرید؟", reply_markup=embassy_country_kb())
        return 0

    if step == ST_EMBASSY_COUNTRY:
        key = _EMBASSY_LABEL_TO_KEY.get(text)
        if not key:
            await safe(update.message,
                "فعلاً فقط برای سفارت‌های زیر استعلام داریم — یکی را انتخاب کنید:",
                reply_markup=embassy_country_kb())
            return 0
        ud["_embassy_country"] = key
        ud[S] = ST_EMBASSY_CATEGORY
        await safe(update.message, "کدوم دسته اطلاعات رو می‌خواهید؟", reply_markup=embassy_category_kb())
        return 0

    if step == ST_EMBASSY_CATEGORY:
        cat_key = _EMBASSY_CATEGORY_LABEL_TO_KEY.get(text)
        if not cat_key:
            await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:",
                reply_markup=embassy_category_kb())
            return 0
        embassy_country = ud.get("_embassy_country", "")
        await safe(update.message,
            "🔄 در حال بررسی سایت رسمی، چند لحظه صبر کنید...\n"
            "(اگه سایت رسمی جواب نده، خودکار سراغ منابع دیگه هم می‌ریم)")
        result = None
        try:
            result = await run_embassy_inquiry(
                embassy_country, cat_key, ud.get("_embassy_nat", ""), ud.get("_embassy_2nd_passport", ""))
        except Exception as e:
            # لایه‌ی دفاعی دوم — حتی اگه یک باگ پیش‌بینی‌نشده داخل
            # run_embassy_inquiry رخ بده، dispatch کرش نمی‌کنه.
            logger.error(f"[uid={uid}] embassy inquiry crashed: {e}")
            result = None
        if not result:
            await safe(update.message,
                "⚠️ فعلاً نتونستیم اطلاعات این دسته رو نه از سایت رسمی و نه از منابع دیگه بگیریم "
                "(یا هنوز این کشور/دسته رو نداریم). می‌تونید دوباره امتحان کنید "
                "یا دسته/کشور دیگری را انتخاب کنید:",
                reply_markup=embassy_category_kb())
            return 0
        ud[S] = ST_EMBASSY_RESULT
        points = "\n".join(f"• {p}" for p in result.get("key_points", []))
        site_label = EMBASSY_OFFICIAL_SITES.get(embassy_country, {}).get("label_fa", embassy_country)
        source_type = result.get("source_type", "official")
        if source_type == "official":
            conf = SOURCE_CONFIDENCE["official"]
            tag_line = f"✅ نوع منبع: رسمی (مستقیم از سایت سفارت/مرجع مهاجرتی) — اطمینان {conf}٪"
            footer = ("ℹ️ این خلاصه از صفحه‌ی رسمی سفارت/مرجع استخراج شده؛ برای اطمینان کامل حتماً "
                      "خودتون هم سایت رسمی رو چک کنید.")
            source_line = f"🔗 منبع رسمی: {result.get('source_url','')}"
        else:
            conf = SOURCE_CONFIDENCE["unofficial"]
            tag_line = f"⚠️ نوع منبع: غیررسمی (سایت رسمی در دسترس نبود — از جستجوی عمومی وب جمع شده) — اطمینان {conf}٪"
            footer = ("ℹ️ این اطلاعات از سایت رسمی نیست و ممکنه کامل/دقیق نباشه — حتماً قبل از هر "
                      "تصمیمی خودتون از سایت رسمی سفارت/مرجع مربوطه هم استعلام بگیرید.")
            source_line = f"🔗 منابع (غیررسمی): {result.get('source_url','')}"
        # وضعیت استعلام باید شفاف باشه: اگه نتیجه از کش (حداکثر ۴۸ ساعت
        # قبل) اومده، صادقانه می‌گیم — نه اینکه انگار همین الان دوباره از
        # منبع خونده شده؛ اگه بررسی تازه بوده هم صریح اعلام می‌کنیم.
        if result.get("from_cache"):
            status_line = f"🗄 وضعیت: بررسی انجام شد (از کش — آخرین بررسی: {result.get('cached_at','نامشخص')})"
        else:
            status_line = "🆕 وضعیت: بررسی انجام شد (همین الان تازه از منبع خونده شد)"
        await safe(update.message,
            f"{site_label} — {EMBASSY_INQUIRY_KEYS.get(cat_key, cat_key)}\n"
            f"{status_line}\n"
            f"{tag_line}\n\n"
            f"{result.get('summary','')}\n\n"
            f"{points}\n\n"
            f"{source_line}\n\n"
            f"{footer}",
            reply_markup=mk(["🔁 دسته‌ی دیگر"], ["🔙 برگشت"]))
        return 0

    if step == ST_EMBASSY_RESULT:
        if text == "🔁 دسته‌ی دیگر":
            ud[S] = ST_EMBASSY_CATEGORY
            await safe(update.message, "کدوم دسته اطلاعات رو می‌خواهید؟", reply_markup=embassy_category_kb())
            return 0
        await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:",
            reply_markup=mk(["🔁 دسته‌ی دیگر"], ["🔙 برگشت"]))
        return 0

    # ── wait_approval ─────────────────────────────────────────────
    if step == "wait_approval":
        await safe(update.message,
            "⏳ هنوز در انتظار تایید تیم هستیم.\n"
            "به محض تایید، پیام دریافت می‌کنید.")
        return 0

    # ── بعد از اتمام کار (۵۰۰/۵۰۰): فیدبک یا درخواست اصلاح ──────────
    if step == "closed_feedback":
        # تشخیص ساده: آیا پیام کاربر به یکی از اساتید/شرکت‌های اخیر اشاره داره؟
        recent = await asyncio.to_thread(db_get_recent_emails, uid, 30)
        match = None
        lowered = text.lower()
        for r in recent:
            last_name = (r["name"].split()[-1] if r.get("name") else "").lower()
            if last_name and len(last_name) > 2 and last_name in lowered:
                match = r
                break
        if match:
            ud["correction_target"] = match
            ud[S] = "correction_confirm"
            await safe(update.message,
                f"متوجه شدم احتمالاً منظورتون ایمیلی‌ست که برای «{match['name']}» "
                f"({match['email']}) فرستادیم.\n\n"
                f"می‌خواید یک ایمیل پیگیری/عذرخواهی براش بفرستیم؟",
                reply_markup=mk(["✅ بله، بفرست"], ["🔙 برگشت"]))
            return 0
        # فیدبک عمومی — مستقیم برای ادمین + راه پاسخ‌دهی
        await notify_admin(context.bot,
            f"💬 فیدبک/اعتراض از کاربر {uid} ({update.effective_user.first_name}):\n\n"
            f"{text}\n\nبرای پاسخ: /reply_{uid} <متن پاسخ>")
        await safe(update.message,
            "✅ پیام شما دریافت شد و مستقیم برای تیم ارسال شد.\n"
            "به‌محض پاسخ ادمین، همینجا بهتون اطلاع می‌دیم.",
            reply_markup=MAIN_KB)
        return 0

    if step == "correction_confirm":
        target = ud.get("correction_target")
        if text == "✅ بله، بفرست" and target:
            smtp_e = ud.get(S_SMTP_E); smtp_p = ud.get(S_SMTP_P)
            smtp_h = ud.get(S_SMTP_H, "smtp.gmail.com"); smtp_pt = ud.get(S_SMTP_PORT, 587)
            if not smtp_e or not smtp_p:
                await safe(update.message,
                    "⚠️ اطلاعات ایمیل شما دیگر در دسترس نیست (مثلاً بات ری‌استارت شده). "
                    "لطفاً موضوع را مستقیم به @manifestapply بگید تا پیگیری کنیم.",
                    reply_markup=MAIN_KB)
                ud[S] = "closed_feedback"
                return 0
            subj = f"Follow-up regarding my previous email — {target['subject'][:50]}"
            body = (f"Dear {target['name']},\n\n"
                    f"I would like to kindly follow up on my previous email regarding "
                    f"\"{target['subject']}\". I apologize if there was any mistake in that message, "
                    f"and I appreciate your time and consideration.\n\nBest regards")
            ok, err = await _deliver_email(context.bot, update.effective_chat.id,
                smtp_e, smtp_p, smtp_h, smtp_pt, target["email"], subj, body)
            if ok:
                await asyncio.to_thread(db_log_email, uid, target["name"], target["email"], subj, "sent")
                await safe(update.message, f"✅ ایمیل پیگیری/عذرخواهی برای {target['name']} فرستاده شد.",
                    reply_markup=MAIN_KB)
                await notify_admin(context.bot,
                    f"✏️ ایمیل اصلاحی برای {target['name']} ({target['email']}) "
                    f"توسط کاربر {uid} فرستاده شد.")
            else:
                await safe(update.message, "❌ ارسال ناموفق بود.\nبه @manifestapply اطلاع دادیم.",
                    reply_markup=MAIN_KB)
                await notify_admin(context.bot,
                    f"⚠️ ارسال ایمیل اصلاحی برای کاربر {uid} ناموفق: {err}")
            ud[S] = "closed_feedback"
            return 0
        ud[S] = "closed_feedback"
        await safe(update.message, "باشه، لغو شد. اگر موضوع دیگه‌ای هست بگید.", reply_markup=MAIN_KB)
        return 0

    # ── prof_targeting: بعد از نتیجه‌ی خالی جستجوی استاد ───────────
    # این حالت قبلاً ست می‌شد ولی هیچ handler ای براش نبود — یعنی کاربری
    # که ۰ استاد پیدا می‌کرد، هر پیامی می‌فرستاد به fallback عمومی «متوجه
    # نشدم» می‌خورد و عملاً گیر می‌کرد. الگوش دقیقاً مثل job_search_empty.
    if step == "prof_targeting":
        client = ud.get(S_CLIENT, {})

        if text == "🔁 تلاش مجدد":
            attempt = ud.get("_prof_retry_attempt", 0)
            ud["_prof_retry_attempt"] = attempt + 1
            # Retry 1 → Retry 2 → Retry 3 → University Discovery Mode →
            # ORCID کلیدواژه‌ای + جستجوی وب گسترده — لیبل نمایشی رو مستقیم
            # از خروجی run_prof_retry_attempt می‌گیریم (خودش تصمیم می‌گیره
            # کدوم مرحله اجرا بشه)، پس فقط یک لیبل موقت ساده تا قبل از
            # رسیدن نتیجه نشون می‌دیم.
            stage_label = (f"روش {attempt+1} از {TOTAL_PROF_RETRY_ATTEMPTS}"
                           if attempt < TOTAL_PROF_RETRY_ATTEMPTS
                           else f"روش {TOTAL_PROF_RETRY_ATTEMPTS} از {TOTAL_PROF_RETRY_ATTEMPTS}")
            # ⚠️ همون فیکس بالا: progress dict + گزارش زنده به‌جای یک
            # پیام ثابت که کاربر رو تا اتمام کامل بدون هیچ آپدیتی می‌ذاشت.
            prof_progress: dict = {}
            reporter_task = None
            try:
                sent_msg = await update.message.reply_text(f"🔁 در حال تلاش مجدد ({stage_label})...\n⏳ لطفاً صبر کنید.")
                reporter_task = asyncio.create_task(_live_search_progress_reporter(
                    context.bot, update.effective_chat.id, sent_msg.message_id, prof_progress))
            except Exception as e:
                logger.warning(f"prof retry (prof_targeting) progress message send failed: {e}")
            try:
                result = await asyncio.to_thread(
                    run_prof_retry_attempt,
                    client.get("field",""), client.get("countries",""), EMAIL_QUOTA, client, attempt, prof_progress)
            except Exception as e:
                logger.error(f"run_prof_retry_attempt crashed: {e}")
                traceback.print_exc()
                if reporter_task:
                    reporter_task.cancel()
                await safe(update.message,
                    "⚠️ این تلاش هم با خطا مواجه شد. می‌تونید دوباره «🔁 تلاش مجدد» بزنید "
                    "(روش بعدی امتحان می‌شه) یا کشور/رشته رو عوض کنید:",
                    reply_markup=mk(
                        ["🔁 تلاش مجدد"],
                        ["🌍 تغییر کشور / دانشگاه هدف"],
                        ["🔬 تغییر رشته / تخصص"],
                        ["🔙 برگشت"]
                    ))
                return 0
            finally:
                if reporter_task and not reporter_task.done():
                    reporter_task.cancel()
            profs = result["profs"]
            ud[S_PROFS] = profs
            if not profs:
                # هیچ Match قوی‌ای پیدا نشد — اگه University Discovery Mode
                # کاندیدهای «نزدیک» (زیر آستانه‌ی match ولی نه بی‌ربط) پیدا
                # کرده باشه، شفاف پیشنهادشون می‌کنیم؛ تصمیم دست کاربره.
                close = result.get("close", [])
                if close:
                    ud["_prof_close_candidates"] = close
                    cls_header = classify_prof_search_result([], close).get("header", "")
                    await safe(update.message,
                        f"{cls_header}\n\n"
                        f"هیچ استادی که دقیقاً با رزومه‌ی شما Match باشد پیدا نشد، "
                        f"اما {len(close)} استاد نزدیک پیدا شد.\n\n"
                        f"می‌خواهید آن‌ها را ببینید؟",
                        reply_markup=mk(
                            ["👀 نمایش استادهای نزدیک"],
                            ["🔁 تلاش مجدد"],
                            ["🌍 تغییر کشور / دانشگاه هدف"],
                            ["🔬 تغییر رشته / تخصص"],
                            ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                            ["🔙 برگشت"]
                        ))
                    return 0
                tried_all_five = ud["_prof_retry_attempt"] >= TOTAL_PROF_RETRY_ATTEMPTS
                extra = ("\n\nهر ۵ روش (منابع API سریع، University Discovery Mode روی دانشگاه‌های "
                         "واقعی این رشته/کشور، و ORCID + جستجوی وب گسترده) رو امتحان کردیم و هنوز "
                         "چیزی پیدا نشد — احتمالاً این ترکیب رشته/کشور واقعاً نتیجه‌ی کمی داره. "
                         "بهتره کشور رو گسترش بدید یا رشته رو کلی‌تر کنید." ) if tried_all_five else ""
                cls_header = classify_prof_search_result([], []).get("header", "")
                await safe(update.message,
                    f"{cls_header}{extra}",
                    reply_markup=mk(
                        ["🔁 تلاش مجدد"],
                        ["🌍 تغییر کشور / دانشگاه هدف"],
                        ["🔬 تغییر رشته / تخصص"],
                        ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                        ["🔙 برگشت"]
                    ))
                return 0
            await _start_email_preview(update, context)
            return 0

        if text == "👀 نمایش استادهای نزدیک":
            close = ud.pop("_prof_close_candidates", [])
            if not close:
                await safe(update.message,
                    "⚠️ لیست استادهای نزدیک دیگه در دسترس نیست — لطفاً دوباره «🔁 تلاش مجدد» بزنید.",
                    reply_markup=mk(["🔁 تلاش مجدد"], ["🔙 برگشت"]))
                return 0
            ud[S_PROFS] = close
            await safe(update.message,
                f"👀 در حال آماده‌سازی پیش‌نمایش برای {len(close)} استاد نزدیک "
                f"(این‌ها دقیقاً Match قوی نبودن، ولی به رزومه‌تون نزدیک‌ان)...")
            await _start_email_preview(update, context)
            return 0

        if text == "🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده":
            if context.application.job_queue is None:
                # اگه job-queue نصب نباشه (پکیج اختیاری python-telegram-bot[job-queue])،
                # این قابلیت نمی‌تونه فعال بشه — به‌جای قول دروغ، صادقانه می‌گیم.
                await safe(update.message,
                    "⚠️ متاسفانه این قابلیت روی این نسخه از بات فعال نیست.\n"
                    "می‌تونید دستی کشور/رشته رو عوض کنید یا بعداً دوباره امتحان کنید:",
                    reply_markup=mk(["🌍 تغییر کشور / دانشگاه هدف"], ["🔬 تغییر رشته / تخصص"], ["🔙 برگشت"]))
                return 0
            await asyncio.to_thread(db_add_pending_search, uid, "professor",
                client.get("field",""), client.get("countries",""))
            ud[S] = ST_MAIN_MENU
            await safe(update.message,
                "✅ باشه! هر ۲۴ ساعت خودمون دوباره می‌گردیم و به محض پیدا شدن استاد "
                "مرتبط، همینجا بهتون خبر می‌دیم — کاری لازم نیست انجام بدید.\n\n"
                "فعلاً می‌تونید از منوی اصلی به کارای دیگه برسید.",
                reply_markup=MAIN_KB)
            return 0

        if text == "🌍 تغییر کشور / دانشگاه هدف":
            ud["_awaiting_prof_country"] = True
            await safe(update.message,
                "کشور هدف جدید را بنویسید:\n(مثال: Germany یا Canada Netherlands)",
                reply_markup=BACK_KB)
            return 0

        if ud.pop("_awaiting_prof_country", False):
            new_country = text.strip()
            client["countries"] = new_country
            ud[S_CLIENT] = client
            await safe(update.message, f"🔍 جستجوی مجدد در «{new_country}»...")
            try:
                profs = await asyncio.to_thread(
                    search_professors, client.get("field",""), new_country, EMAIL_QUOTA, client)
            except Exception as e:
                logger.error(f"search_professors (country retry) crashed: {e}")
                traceback.print_exc()
                await safe(update.message,
                    "⚠️ خطای غیرمنتظره پیش اومد. یه کشور دیگه امتحان کنید یا به منو برگردید:",
                    reply_markup=BACK_KB)
                return 0
            ud[S_PROFS] = profs
            await _start_email_preview(update, context)
            return 0

        if text == "🔬 تغییر رشته / تخصص":
            ud["_awaiting_prof_field"] = True
            await safe(update.message, "رشته/تخصص جدید را بنویسید:", reply_markup=BACK_KB)
            return 0

        if ud.pop("_awaiting_prof_field", False):
            new_field = text.strip()
            client["field"] = new_field
            ud[S_CLIENT] = client
            await safe(update.message, f"🔍 جستجوی مجدد برای «{new_field}»...")
            try:
                profs = await asyncio.to_thread(
                    search_professors, new_field, client.get("countries",""), EMAIL_QUOTA, client)
            except Exception as e:
                logger.error(f"search_professors (field retry) crashed: {e}")
                traceback.print_exc()
                await safe(update.message,
                    "⚠️ خطای غیرمنتظره پیش اومد. دوباره امتحان کنید یا به منو برگردید:",
                    reply_markup=BACK_KB)
                return 0
            ud[S_PROFS] = profs
            await _start_email_preview(update, context)
            return 0

        if text == "📋 لیست خودم دارم":
            ud[S] = ST_PROF_LIST
            ud["waiting_list"] = True
            await safe(update.message,
                "لیست را وارد کنید (هر خط یک استاد):\n"
                "فرمت: نام | ایمیل  یا  فقط ایمیل  یا  لینک",
                reply_markup=BACK_KB)
            return 0

        ud[S] = ST_MAIN_MENU
        await safe(update.message, "به منوی اصلی برگشتید.", reply_markup=MAIN_KB)
        return 0

    # ── job_search_empty: بعد از نتیجه‌ی خالی جستجوی کار ───────
    if step == "job_search_empty":
        client = ud.get(S_CLIENT, {})
        retry_field   = ud.pop("_job_retry_field", client.get("field",""))
        retry_country = ud.pop("_job_retry_country", client.get("countries",""))
        chat_id = update.effective_chat.id

        if text == "🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده":
            if context.application.job_queue is None:
                await safe(update.message,
                    "⚠️ متاسفانه این قابلیت روی این نسخه از بات فعال نیست.\n"
                    "می‌تونید دستی دوباره امتحان کنید یا کشور رو عوض کنید:",
                    reply_markup=mk(["🔄 دوباره با همین تنظیمات جستجو کن"], ["🌍 تغییر کشور هدف کار"], ["🔙 برگشت"]))
                return 0
            await asyncio.to_thread(db_add_pending_search, uid, "job", retry_field, retry_country)
            ud[S] = ST_MAIN_MENU
            await safe(update.message,
                "✅ باشه! هر ۲۴ ساعت خودمون دوباره می‌گردیم و به محض پیدا شدن فرصت "
                "مرتبط، همینجا بهتون خبر می‌دیم — کاری لازم نیست انجام بدید.\n\n"
                "فعلاً می‌تونید از منوی اصلی به کارای دیگه برسید.",
                reply_markup=MAIN_KB)
            return 0

        # ── فیکس: قبلاً این دکمه دقیقاً همون جستجوی اول رو (همون بازه‌ی
        # ۴۸ ساعته، همون منابع) دوباره صدا می‌زد — یعنی اگه بار اول چیزی
        # پیدا نشد، تلاش مجدد هم همیشه همون نتیجه‌ی خالی رو می‌داد. الان
        # از search_jobs_retry_group استفاده می‌کنیم که هر بار یک استراتژی
        # واقعاً متفاوت (بازه‌ی زمانی بازتر، فیلتر شل‌تر) امتحان می‌کنه —
        # و شمارنده‌ی تلاش (_job_retry_attempt) با هر کلیک یک پله جلو می‌ره.
        if text == "🔄 دوباره با همین تنظیمات جستجو کن":
            if _has_active_send(uid):
                await safe(update.message,
                    "⏳ یک عملیات ارسال دیگر همین الان برای شما در حال اجراست — "
                    "صبر کنید تمام شود، یا با /cancel لغوش کنید.", reply_markup=MAIN_KB)
                return 0

            attempt_idx = ud.get("_job_retry_attempt", 0)
            ud["_job_retry_attempt"] = attempt_idx + 1

            progress_msg_id = None
            try:
                sent_msg = await update.message.reply_text(
                    f"🔍 تلاش مجدد {attempt_idx+1} — در حال جستجوی عمیق‌تر «{retry_field}» در «{retry_country or 'همه‌ی کشورها'}»...\n⏳ لطفاً صبر کنید.")
                progress_msg_id = sent_msg.message_id
            except Exception as e:
                logger.warning(f"job retry progress message send failed: {e}")
            job_progress: dict = {}
            reporter_task = None
            if progress_msg_id:
                reporter_task = asyncio.create_task(_live_search_progress_reporter(
                    context.bot, chat_id, progress_msg_id, job_progress, kind="فرصت‌های شغلی"))

            try:
                # ⚠️ فیکس: قبلاً همیشه فقط search_jobs_retry_group صدا زده
                # می‌شد که فقط بازه‌ی زمانی/فیلتر رو روی همون منابع aggregator
                # عوض می‌کرد. الان از run_job_retry_attempt استفاده می‌کنیم که
                # از تلاش ۴ به بعد واقعاً منابع/استراتژی متفاوت (Company
                # Discovery Mode، عنوان‌های مترادف) رو امتحان می‌کنه — دقیقاً
                # معادل pipeline پنج‌مرحله‌ای اساتید.
                retry_result = await asyncio.to_thread(
                    run_job_retry_attempt, retry_field, retry_country, JOB_QUOTA, client, attempt_idx, job_progress)
                jobs = retry_result["jobs"]
            except Exception as e:
                logger.error(f"run_job_retry_attempt crashed: {e}")
                traceback.print_exc()
                if reporter_task:
                    reporter_task.cancel()
                await safe(update.message,
                    "⚠️ باز هم خطای غیرمنتظره پیش اومد. اطلاعاتتون گم نشده — می‌تونید دوباره امتحان کنید یا به منو برگردید:",
                    reply_markup=mk(["🔄 دوباره با همین تنظیمات جستجو کن"], ["🌍 تغییر کشور هدف کار"], ["🔙 برگشت"]))
                ud["_job_retry_field"] = retry_field
                ud["_job_retry_country"] = retry_country
                return 0
            finally:
                if reporter_task and not reporter_task.done():
                    reporter_task.cancel()
            if progress_msg_id:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id, message_id=progress_msg_id,
                        text=f"✅ {retry_result['label']} — {len(jobs)} فرصت شغلی پیدا شد.")
                except Exception:
                    pass
            ud[S_JOBS] = jobs
            if not jobs:
                more_left = attempt_idx + 1 < TOTAL_JOB_RETRY_ATTEMPTS
                await safe(update.message,
                    ("⚠️ بازهم نتیجه‌ای نبود. می‌تونید یک بار دیگه تلاش مجدد بزنید (روش بعدی رو امتحان "
                     "می‌کنیم)، کشور/رشته را تغییر دهید، یا برگردید:" if more_left else
                     "⚠️ بازهم نتیجه‌ای نبود — همه‌ی سطح‌های تلاش مجدد رو امتحان کردیم. بهترین کار الان "
                     "تغییر کشور (یا انتخاب «همه‌ی کشورها») یا کلی‌تر کردن رشته‌ست:"),
                    reply_markup=mk(["🔄 دوباره با همین تنظیمات جستجو کن"], ["🌍 تغییر کشور هدف کار"], ["🔙 برگشت"]))
                ud["_job_retry_field"] = retry_field
                ud["_job_retry_country"] = retry_country
                return 0

            await safe(update.message, f"✅ {len(jobs)} فرصت یافت شد! در حال شروع اپلای...")
            ud[S] = ST_JOB_FIELD
            _t = asyncio.create_task(_run_bg_task(
                _send_jobs_bg(context, chat_id, uid, jobs, client, ud.get(S_SUB),
                              smtp_e=ud.get(S_SMTP_E), smtp_p=ud.get(S_SMTP_P),
                              smtp_h=ud.get(S_SMTP_H, "smtp.gmail.com"), smtp_pt=ud.get(S_SMTP_PORT, 587)),
                context, chat_id, "اپلای کار"))
            _register_send_task(uid, _t)
            return 0

        if text == "🌍 تغییر کشور هدف کار":
            ud["_awaiting_job_country"] = True
            await safe(update.message,
                f"کشور هدف جدید را بنویسید:\n(مثال: Remote یا Germany Canada Australia)\n\n"
                f"یا برای جستجوی عمیق در همه‌ی کشورها، دکمه‌ی «{ALL_COUNTRIES_LABEL}» رو بزنید:",
                reply_markup=mk([ALL_COUNTRIES_LABEL], ["🔙 برگشت"]))
            return 0

        if ud.pop("_awaiting_job_country", False):
            if _has_active_send(uid):
                await safe(update.message,
                    "⏳ یک عملیات ارسال دیگر همین الان برای شما در حال اجراست — "
                    "صبر کنید تمام شود، یا با /cancel لغوش کنید.", reply_markup=MAIN_KB)
                return 0

            if text == ALL_COUNTRIES_LABEL:
                new_country = ""
                client["_deep_search_all_countries"] = True
            else:
                new_country = text.strip()
                client["_deep_search_all_countries"] = False
            client["countries"] = new_country
            ud[S_CLIENT] = client
            ud["_job_retry_attempt"] = 0  # کشور عوض شده — دوباره از تلاش صفر شروع می‌کنیم
            new_country_display = new_country or "🌍 همه‌ی کشورها (جستجوی عمیق و کامل)"

            progress_msg_id = None
            try:
                sent_msg = await update.message.reply_text(f"🔍 در حال جستجوی دقیق در «{new_country_display}»...\n⏳ لطفاً صبر کنید.")
                progress_msg_id = sent_msg.message_id
            except Exception as e:
                logger.warning(f"job country-change progress message send failed: {e}")
            job_progress: dict = {}
            reporter_task = None
            if progress_msg_id:
                reporter_task = asyncio.create_task(_live_search_progress_reporter(
                    context.bot, chat_id, progress_msg_id, job_progress, kind="فرصت‌های شغلی"))

            try:
                jobs = await asyncio.to_thread(search_jobs_realtime, retry_field, new_country, JOB_QUOTA, job_progress)
            except Exception as e:
                logger.error(f"search_jobs_realtime (country change) crashed: {e}")
                traceback.print_exc()
                if reporter_task:
                    reporter_task.cancel()
                await safe(update.message,
                    "⚠️ خطای غیرمنتظره پیش اومد. یه کشور دیگه امتحان کنید یا به منو برگردید:",
                    reply_markup=mk([ALL_COUNTRIES_LABEL], ["🔙 برگشت"]))
                return 0
            finally:
                if reporter_task and not reporter_task.done():
                    reporter_task.cancel()
            if progress_msg_id:
                try:
                    await context.bot.edit_message_text(
                        chat_id=chat_id, message_id=progress_msg_id,
                        text=f"✅ جستجو تمام شد — {len(jobs)} فرصت شغلی پیدا شد.")
                except Exception:
                    pass
            ud[S_JOBS] = jobs
            if not jobs:
                await safe(update.message,
                    f"⚠️ بازهم نتیجه‌ای در «{new_country_display}» نبود.",
                    reply_markup=mk(["🔄 دوباره با همین تنظیمات جستجو کن"], ["🌍 تغییر کشور هدف کار"], ["🔙 برگشت"]))
                ud["_job_retry_field"] = retry_field
                ud["_job_retry_country"] = new_country
                ud[S] = "job_search_empty"
                return 0

            await safe(update.message, f"✅ {len(jobs)} فرصت یافت شد! در حال شروع اپلای...")
            ud[S] = ST_JOB_FIELD
            _t = asyncio.create_task(_run_bg_task(
                _send_jobs_bg(context, chat_id, uid, jobs, client, ud.get(S_SUB),
                              smtp_e=ud.get(S_SMTP_E), smtp_p=ud.get(S_SMTP_P),
                              smtp_h=ud.get(S_SMTP_H, "smtp.gmail.com"), smtp_pt=ud.get(S_SMTP_PORT, 587)),
                context, chat_id, "اپلای کار"))
            _register_send_task(uid, _t)
            return 0

        ud[S] = ST_MAIN_MENU
        await safe(update.message, "به منوی اصلی برگشتید.", reply_markup=MAIN_KB)
        return 0

    # ── fallback: State نامعتبر/ناشناخته ────────────────────────────
    # به این‌جا فقط وقتی می‌رسیم که step فعلی (ud[S]) با هیچ‌کدوم از
    # if step == ... های بالا مچ نشده باشه — یعنی یک مقدار خراب/ناشناخته
    # (مثلاً باگ، دستکاری دستی دیتابیس، یا نسخه‌ی قدیمی‌تر بات که یک
    # State دیگه تولید کرده بود). قبلاً این‌جا فقط پیام «متوجه نشدم» با
    # کیبورد منوی اصلی نشون داده می‌شد ولی ud[S] دست‌نخورده (همون مقدار
    # نامعتبر) می‌موند — یعنی کاربر با کیبورد منوی اصلی مواجه می‌شد ولی
    # واقعاً هنوز توی همون State خراب گیر بود؛ دفعه‌ی بعد که از منوی
    # اصلی چیزی می‌زد، دوباره به همین fallback می‌خورد (حلقه‌ی بی‌پایان).
    # فیکس: State رو واقعاً *بازیابی* می‌کنیم (نه فقط ظاهرش رو عوض
    # کنیم) — به‌جای پرت کردن مستقیم و بی‌سروصدا، صریحاً State رو به
    # یک State شناخته‌شده و امن (Main Menu) برمی‌گردونیم تا از این به بعد
    # State واقعی و کیبورد نمایش‌داده‌شده هم‌خوان باشن؛ هیچ داده‌ای
    # (پروفایل، sub id، لیست اساتید/شغل‌ها و...) پاک نمی‌شه، فقط ud[S].
    logger.warning(f"[uid={uid}] State نامعتبر/ناشناخته بازیابی شد: step={step!r}")
    ud[S] = ST_MAIN_MENU
    await safe(update.message,
        "⚠️ یه مشکل موقت توی مرحله‌ی قبلی پیش اومد — نگران نباشید، اطلاعاتتون گم نشده.\n"
        "به منوی اصلی برگردوندمتون:",
        reply_markup=MAIN_KB)
    return 0

# ── عکس (رسید) ────────────────────────────────────────────────────
async def photo_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get(S) == ST_RECEIPT:
        return await dispatch(update, context)

# ── فایل (رزومه و غیره) ──────────────────────────────────────────
async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # اجازه می‌دیم در هر مرحله‌ای که منتظر رزومه هستیم (یا هر مرحله‌ی
    # دیگه‌ای که در آینده فایل بخواد)، فایل مستقیم بره داخل dispatch
    if context.user_data.get(S) in (ST_COLLECT,):
        return await dispatch(update, context)

# ================================================================
# PAYMENT FLOW
# ================================================================

async def _show_payment(update, context):
    ud  = context.user_data
    await update.message.reply_text("⏳ در حال محاسبه‌ی نرخ لحظه‌ای دلار...")
    rate, src = await asyncio.to_thread(fetch_usd_rate)

    svc = "ارسال ایمیل به اساتید" if ud.get(S_SRV)=="email" else "اپلای کار حرفه‌ای"

    # جزئیات سرویس انتخاب‌شده
    if ud.get(S_SRV) == "email":
        svc_detail = (
            "📧 سرویس ارسال ایمیل به اساتید:\n"
            "✅ تحلیل پروفایل تحقیقاتی شما\n"
            "✅ پیدا کردن اساتید مرتبط با رشته شما\n"
            "✅ خواندن مقالات و abstract هر استاد\n"
            "✅ نوشتن ایمیل شخصی‌سازی‌شده برای هر استاد\n"
            "✅ ارسال تضمینی ۵۰۰ ایمیل\n"
            "✅ گزارش هر ایمیل در این چت\n"
            "✅ تکمیل در ۲ تا ۳ روز کاری"
        )
    else:
        svc_detail = (
            "💼 سرویس اپلای کار حرفه‌ای:\n"
            "✅ جستجوی فرصت‌های شغلی تازه (در اولویت، ۴۸ ساعت اخیر)\n"
            "✅ فیلتر بر اساس رشته و کشور هدف شما\n"
            "✅ نوشتن Cover Letter شخصی برای هر آگهی\n"
            "✅ اپلای مستقیم از طریق ایمیل یا پورتال\n"
            "✅ گزارش هر اپلای در این چت\n"
            "✅ فقط فرصت‌های واقعی (بدون آگهی‌های بیش از ۳۰ روز)"
        )

    if not rate:
        # نتونستیم نرخ لحظه‌ای رو از هیچ منبعی بگیریم — به‌جای نشون دادن یه
        # قیمت تقریبی/قدیمی که می‌تونه اشتباه باشه، صادقانه از کاربر
        # می‌خوایم خودش با نرخ آزاد امروز حساب کنه.
        ud["pay_price_toman"] = None
        await safe(update.message,
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💳 سرویس انتخابی: {svc}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{svc_detail}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⚠️ در حال حاضر نتونستیم نرخ لحظه‌ای دلار رو دریافت کنیم.\n\n"
            f"لطفاً مبلغ رو خودتون بر اساس **نرخ دلار آزاد امروز** حساب کنید:\n"
            f"💵 مبلغ: *${PRICE_USD} دلار* (با تخفیف؛ قیمت اصلی ${PRICE_ORIGINAL_USD} دلار)\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"💳 شماره کارت:\n"
            f"`{CARD_NUMBER}`\n"
            f"👤 به نام: {CARD_OWNER}\n\n"
            f"📌 مراحل پرداخت:\n"
            f"1️⃣ معادل ریالی ${PRICE_USD} دلار (با نرخ آزاد امروز) رو به کارت بالا واریز کنید\n"
            f"2️⃣ از رسید پرداخت اسکرین‌شات بگیرید\n"
            f"3️⃣ اسکرین‌شات را همین‌جا بفرستید\n"
            f"4️⃣ پس از تایید تیم، دسترسی فوری فعال می‌شود\n\n"
            f"❓ اگه توی محاسبه یا واریز مشکلی داشتید یا هر سوالی بود، به پشتیبانی پیام بدید: @manifestapply",
            reply_markup=BACK_KB)
        ud[S] = ST_RECEIPT
        return

    price_t   = rate * PRICE_USD
    price_og  = rate * PRICE_ORIGINAL_USD
    ud["pay_price_toman"] = price_t

    await safe(update.message,
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💳 سرویس انتخابی: {svc}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"{svc_detail}\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"~~قیمت اصلی: ${PRICE_ORIGINAL_USD} دلار ({fmt_toman(price_og)})~~\n"
        f"✅ قیمت با تخفیف: *${PRICE_USD} دلار*\n"
        f"💰 معادل امروز: *{fmt_toman(price_t)}*\n"
        f"   (نرخ دلار: {rate:,} تومان | منبع: {src})\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💳 شماره کارت:\n"
        f"`{CARD_NUMBER}`\n"
        f"👤 به نام: {CARD_OWNER}\n\n"
        f"📌 مراحل پرداخت:\n"
        f"1️⃣ مبلغ *{fmt_toman(price_t)}* را به کارت بالا واریز کنید\n"
        f"2️⃣ از رسید پرداخت اسکرین‌شات بگیرید\n"
        f"3️⃣ اسکرین‌شات را همین‌جا بفرستید\n"
        f"4️⃣ پس از تایید تیم، دسترسی فوری فعال می‌شود\n\n"
        f"❓ اگه مشکلی در روند پرداخت داشتید یا سوالی بود، به پشتیبانی پیام بدید: @manifestapply",
        reply_markup=BACK_KB)
    ud[S] = ST_RECEIPT

# ================================================================
# COLLECT — جمع‌آوری اطلاعات سرویس
# ================================================================

OPTIONAL_SKIP_LABEL = "⏭ ندارم / رد کن"
OPTIONAL_SKIP_KB = mk([OPTIONAL_SKIP_LABEL], ["🔙 برگشت"])

# ── انتخاب صریح «یک/چند کشور خاص» یا «جستجوی عمیق همه‌ی کشورها» ────────
# قبلاً فقط با یک متن راهنما («یا بنویسید همه») به کاربر گفته می‌شد که
# می‌تونه بنویسه «همه» — یعنی یک انتخاب واقعی و دکمه‌دار نبود و خیلی از
# کاربرها اصلاً متوجه این گزینه نمی‌شدن. الان یک دکمه‌ی صریح داریم که با
# زدنش، کشور روی رشته‌ی خالی ست می‌شه (یعنی پایین‌تر توی pipeline جستجو،
# هیچ فیلتر کشوری اعمال نمی‌شه و همه‌ی منابع/کشورها بدون محدودیت جستجو
# می‌شن) و یک flag (_deep_search_all_countries) هم ست می‌شه که به کاربر و
# به لاگ‌ها صریح نشون بده این یک جستجوی عمیق/سراسریه، نه یک جستجوی سطحی.
ALL_COUNTRIES_LABEL = "🌍 همه کشورها (جستجوی عمیق و کامل)"
COUNTRIES_Q_KB = mk([ALL_COUNTRIES_LABEL], ["🔙 برگشت"])
# سوالاتی که جواب‌شون اختیاریه — اگه کاربر رد کنه، فقط رشته‌ی خالی ذخیره
# می‌شه، نه متن دکمه‌ی «رد کن».
OPTIONAL_COLLECT_KEYS = {"gpa", "linkedin", "github", "scholar", "skills"}

COLLECT_QUESTIONS = [
    ("field",      "🔬 رشته تحصیلی / تخصص شما چیست؟\n(مثال: Machine Learning, Biomedical Engineering)"),
    ("education",  "📚 آخرین مقطع تحصیلی؟\n(لیسانس / ارشد / دکتری)"),
    ("gpa",        "🎯 معدل؟ (اختیاری — اگر نمی‌خواید بگید، رد کنید)"),
    ("countries",
     "🌍 کشور هدف مهاجرت / اپلای؟\n"
     "می‌توانید چند کشور بنویسید (با ویرگول جدا کنید):\n"
     "مثال: آلمان، کانادا، هلند, استرالیا\n\n"
     "یا بنویسید «همه» تا همه‌ی کشورها جستجو شود."),
    ("visa_type",  "🎓 هدف؟\n(PhD position / Research collaboration / Full-time job)"),
    ("work_exp",   "💼 سابقه کاری مرتبط؟\n(مثال: ۳ سال توسعه‌دهنده Python)"),
    ("publications","📄 تعداد مقالات؟ (اگر ندارید: 0)"),
    ("skills",     "🛠 مهارت‌های کلیدی؟ (با ویرگول جدا کنید — اختیاری)\nمثال: Python, PyTorch, SQL, AWS"),
    ("linkedin",   "🔗 لینک LinkedIn؟ (اختیاری — اگر ندارید رد کنید)"),
    ("github",     "💻 لینک GitHub؟ (اختیاری — اگر ندارید رد کنید)"),
    ("scholar",    "🎓 لینک Google Scholar؟ (اختیاری — اگر ندارید رد کنید)"),
    ("job_filters",
     "🎛 فیلترهای جستجو (اختیاری — اگر ندارید «ندارم» بنویسید):\n\n"
     "می‌توانید بنویسید:\n"
     "• شرکت‌هایی که نمی‌خواهید: مثلاً «نه Amazon, نه Uber»\n"
     "• فقط Remote: بنویسید «فقط Remote»\n"
     "• فقط Visa Sponsorship: بنویسید «فقط Visa Sponsorship»\n"
     "• ترکیب: مثلاً «فقط Remote — نه شرکت‌های زیر ۵۰ نفر»\n\n"
     "این فیلترها هم در جستجوی کار، هم در ارسال ایمیل به اساتید اعمال می‌شوند."),
]

# ── سوال رزومه: باید انگلیسی باشه، ولی اگه نداره گزینه‌ی جایگزین داریم ──
RESUME_Q = (
    "resume",
    "📝 رزومه‌ی خود را **به زبان انگلیسی** ارسال کنید "
    "(می‌توانید فایل PDF/Word را همینجا بفرستید، یا متنش را کپی/پیست کنید).\n\n"
    "⚠️ چون این رزومه مستقیماً برای شخصی‌سازی ایمیل‌ها/کاور لتر استفاده می‌شود، "
    "هرچه دقیق‌تر و انگلیسی‌تر باشد، نتیجه بهتر است."
)
RESUME_KB = mk(["📄 رزومه ندارم — متن انگلیسی می‌نویسم"], ["🔙 برگشت"])

# ── فقط برای سرویس ایمیل به اساتید: دو توصیه‌نامه ─────────────────────
RECOMMEND_Q = (
    "recommendations",
    "📩 آیا **دو نامه‌ی توصیه (Recommendation Letter)** از اساتید قبلی خود دارید؟\n\n"
    "⚠️ نکته‌ی مهم: در روند اپلای به اساتید، وجود دو توصیه‌نامه (یا حداقل ذکر "
    "نام دو استادی که حاضرند توصیه‌تان کنند، در رزومه/CV) تاثیر زیادی روی "
    "پذیرفته‌شدن ایمیل داره. اگر ندارید مشکلی نیست، فقط این نکته را بدانید."
)
RECOMMEND_KB = mk(["✅ دارم"], ["❌ ندارم"], ["🔙 برگشت"])

RECOMMEND_NAME_Q = (
    "recommenders_info",
    "👨‍🏫 نام و ایمیل این دو استاد را بنویسید (هر کدام یک خط)، و اینکه "
    "دقیقاً چه همکاری‌ای با هرکدام داشتید (مثلاً استاد راهنما، پروژه مشترک و...):\n\n"
    "اگر مطمئن نیستید یا فعلاً در دسترس‌تان نیست، می‌توانید رد کنید."
)
RECOMMEND_NAME_KB = mk(["⏭ فعلاً ندارم / رد کن"], ["🔙 برگشت"])

def _collect_questions_for(svc):
    """سوالات جمع‌آوری بسته به سرویس فرق می‌کنه — توصیه‌نامه فقط برای
    ایمیل به اساتید معنی داره، نه اپلای کار."""
    qs = list(COLLECT_QUESTIONS) + [RESUME_Q]
    if svc == "email":
        qs += [RECOMMEND_Q]
    return qs

async def _start_collect(update, context):
    ud = context.user_data
    saved = await asyncio.to_thread(db_get_career_profile, update.effective_user.id)
    # فقط وقتی پروفایل رو پیشنهاد بده که واقعاً چیز قابل‌استفاده‌ای توش
    # باشه (رشته یا رزومه) — پروفایل خالی/ناقص که فایده‌ای نداره پیشنهاد نشه.
    if saved and (saved.get("field") or saved.get("resume")):
        ud["_saved_profile"] = saved
        ud[S] = ST_PROFILE_REUSE
        await safe(update.message,
            "📇 یک پروفایل ذخیره‌شده از قبل پیدا کردیم:\n\n"
            f"🔬 رشته: {saved.get('field') or '—'}\n"
            f"📚 مقطع: {saved.get('education') or '—'}\n"
            f"🌍 کشور هدف: {saved.get('countries') or '—'}\n"
            f"💼 سابقه کار: {saved.get('work_exp') or '—'}\n"
            f"🛠 مهارت‌ها: {saved.get('skills') or '—'}\n"
            f"🔗 LinkedIn: {saved.get('linkedin') or '—'}\n"
            f"💻 GitHub: {saved.get('github') or '—'}\n"
            f"📄 رزومه: {'✅ موجود' if saved.get('resume') else '—'}\n\n"
            "می‌خواید همین اطلاعات رو استفاده کنیم، یا به‌روزرسانی‌شون کنیم؟",
            reply_markup=mk(["✅ همینو استفاده کن"], ["✏️ به‌روزرسانی کنم"], ["🔙 برگشت"]))
        return
    await _begin_fresh_collect(update, context)

async def _begin_fresh_collect(update, context):
    ud = context.user_data
    ud.pop("_saved_profile", None)
    ud["collect_idx"] = 0
    client = {}
    ud[S_CLIENT] = client
    qs = _collect_questions_for(ud.get(S_SRV))
    await safe(update.message,
        "📋 چند سوال کوتاه برای شخصی‌سازی سرویس (فقط یک بار می‌پرسیم — "
        "دفعات بعد از همین پروفایل استفاده می‌شود).\n\n"
        "⚠️ لطفاً اطلاعات را **دقیق و صحیح** وارد کنید — مسئولیت درستی اطلاعاتی "
        "که می‌فرستید بر عهده‌ی خودتان است و روی کیفیت نتیجه‌ی نهایی تاثیر مستقیم دارد.\n\n"
        f"سوال ۱ از {len(qs)}:\n\n{qs[0][1]}",
        reply_markup=BACK_KB)

async def _handle_collect(update, context):
    ud  = context.user_data
    idx = ud.get("collect_idx", 0)
    client = ud.get(S_CLIENT, {})
    qs = _collect_questions_for(ud.get(S_SRV))
    key, _ = qs[idx]

    # ── سوال رزومه: هم فایل قبول می‌کنیم هم متن، هم دکمه‌ی "ندارم" ──
    if key == "resume":
        if update.message.text == "📄 رزومه ندارم — متن انگلیسی می‌نویسم":
            ud["awaiting_resume_text"] = True
            await safe(update.message,
                "✍️ باشه، لطفاً یک خلاصه از تحصیلات، سابقه‌کاری و دستاوردهای خود را "
                "**به زبان انگلیسی** در همین پیام بعدی بنویسید:",
                reply_markup=BACK_KB)
            return 0
        if ud.pop("awaiting_resume_text", False):
            client[key] = update.message.text or ""
            client["resume_is_file"] = False
        elif update.message.document:
            doc = update.message.document
            # ── اعتبارسنجی سریع قبل از دانلود ────────────────────────
            # فایل خیلی بزرگ یا فرمت پشتیبانی‌نشده رو همین‌جا رد می‌کنیم؛
            # وگرنه ممکنه دانلود/استخراج طول بکشه یا شکست بخوره و کاربر
            # فکر کنه بات گیر کرده یا کرش کرده.
            MAX_RESUME_MB = 10
            allowed_ext = (".pdf", ".doc", ".docx", ".txt", ".rtf")
            fname = (doc.file_name or "").lower()
            if doc.file_size and doc.file_size > MAX_RESUME_MB * 1024 * 1024:
                await safe(update.message,
                    f"❌ حجم فایل بیشتر از {MAX_RESUME_MB} مگابایته. یه نسخه‌ی سبک‌تر (PDF/Word) بفرستید، "
                    "یا روی «📄 رزومه ندارم — متن انگلیسی می‌نویسم» بزنید.")
                return 0
            if fname and not fname.endswith(allowed_ext):
                await safe(update.message,
                    "❌ این فرمت پشتیبانی نمی‌شه. لطفاً PDF یا Word (doc/docx) بفرستید، "
                    "یا روی «📄 رزومه ندارم — متن انگلیسی می‌نویسم» بزنید.")
                return 0

            client["resume_is_file"] = True
            client["resume_file_id"] = doc.file_id
            # فایل رزومه رو مستقیم برای ادمین هم بفرست تا رکورد داشته باشیم
            if ADMIN_CHAT_ID:
                try:
                    await context.bot.send_document(chat_id=ADMIN_CHAT_ID,
                        document=doc.file_id,
                        caption=f"📄 رزومه کاربر {update.effective_user.id}")
                    await asyncio.to_thread(
                        db_log_audit, ADMIN_CHAT_ID, "System", "Forwarded user document to admin",
                        update.effective_user.id, "Success")
                except Exception as e:
                    logger.error(f"forward resume to admin: {e}")
                    await asyncio.to_thread(
                        db_log_audit, ADMIN_CHAT_ID, "System", "Forwarded user document to admin",
                        update.effective_user.id, "Failure", detail=str(e)[:200])

            # ── استخراج واقعی متن رزومه ────────────────────────────
            # قبلاً این‌جا فقط یک placeholder ذخیره می‌شد و AI هیچ‌وقت متن
            # واقعی رزومه رو نمی‌دید. الان فایل دانلود و متنش استخراج می‌شه.
            await safe(update.message, "📄 در حال خواندن رزومه...")
            try:
                tg_file = await context.bot.get_file(doc.file_id)
                file_bytes = bytes(await tg_file.download_as_bytearray())
            except Exception as e:
                logger.error(f"resume download failed: {e}")
                file_bytes = b""
            resume_text = await asyncio.to_thread(
                extract_resume_text, file_bytes, doc.file_name or "") if file_bytes else ""

            if not resume_text or len(resume_text) < 40:
                # استخراج ناموفق (فرمت پشتیبانی‌نشده، اسکن تصویری، خطای شبکه و...) —
                # به‌جای ادامه‌ی خاموش با یک رزومه‌ی خالی/بی‌معنی، صریح از کاربر
                # می‌خواهیم متن رو خودش پیست کنه تا کیفیت مچینگ/ایمیل پایین نیاد.
                ud["awaiting_resume_text"] = True
                await safe(update.message,
                    "⚠️ نتوانستیم متن این فایل را به‌طور کامل بخوانیم "
                    "(فرمت پشتیبانی‌نشده یا اسکن تصویری).\n\n"
                    "لطفاً خلاصه‌ی رزومه‌تان را **به‌صورت متن و به انگلیسی** همین‌جا بفرستید "
                    "تا کیفیت تحلیل و ایمیل‌ها پایین نیاید:",
                    reply_markup=BACK_KB)
                return 0

            client[key] = resume_text
        elif update.message.text:
            client[key] = update.message.text
            client["resume_is_file"] = False
        else:
            await safe(update.message, "لطفاً رزومه را به‌صورت فایل یا متن بفرستید:", reply_markup=RESUME_KB)
            return 0
        ud[S_CLIENT] = client
        idx += 1
        ud["collect_idx"] = idx
        return await _collect_next_or_finish(update, context, qs, idx)

    # ── جواب به سوال "اسم و ایمیل اساتید توصیه‌کننده" (باید قبل از چک
    # key=="recommendations" باشه، چون idx بین این دو پیام عوض نمی‌شه) ──
    if ud.pop("awaiting_recommenders", False):
        text = (update.message.text or "").strip()
        client["recommenders_info"] = "" if text == "⏭ فعلاً ندارم / رد کن" else text
        ud[S_CLIENT] = client
        idx += 1
        ud["collect_idx"] = idx
        return await _collect_next_or_finish(update, context, qs, idx)

    # ── سوال توصیه‌نامه (فقط سرویس ایمیل) ────────────────────────────
    if key == "recommendations":
        text = (update.message.text or "").strip()
        client[key] = text
        ud[S_CLIENT] = client
        if text == "✅ دارم":
            ud["collect_idx"] = idx  # می‌مونیم همینجا و سوال بعدی (اسم اساتید) رو جدا می‌پرسیم
            ud["awaiting_recommenders"] = True
            await safe(update.message, RECOMMEND_NAME_Q[1], reply_markup=RECOMMEND_NAME_KB)
            return 0
        idx += 1
        ud["collect_idx"] = idx
        return await _collect_next_or_finish(update, context, qs, idx)

    # ── بقیه‌ی سوالات معمولی متنی ────────────────────────────────────
    text = (update.message.text or "").strip()
    if key in OPTIONAL_COLLECT_KEYS and text == OPTIONAL_SKIP_LABEL:
        text = ""
    if key == "countries" and text == ALL_COUNTRIES_LABEL:
        # کشور رو خالی می‌ذاریم (یعنی هیچ فیلتر کشوری اعمال نمی‌شه) و
        # صریحاً flag جستجوی عمیق سراسری رو ست می‌کنیم — این flag بعداً
        # هم توی پیام «در حال جستجو» به کاربر نشون داده می‌شه و هم باعث
        # می‌شه merge/retry pipeline عمیق‌تر (شمار بیشتر منابع/تلاش) اجرا بشه.
        text = ""
        client["_deep_search_all_countries"] = True
    elif key == "countries":
        client["_deep_search_all_countries"] = False
    client[key] = text
    ud[S_CLIENT] = client
    idx += 1
    ud["collect_idx"] = idx
    return await _collect_next_or_finish(update, context, qs, idx)

async def _collect_next_or_finish(update, context, qs, idx):
    ud = context.user_data
    client = ud.get(S_CLIENT, {})
    if idx < len(qs):
        key = qs[idx][0]
        kb = (RESUME_KB if key == "resume" else
              RECOMMEND_KB if key == "recommendations" else
              COUNTRIES_Q_KB if key == "countries" else
              OPTIONAL_SKIP_KB if key in OPTIONAL_COLLECT_KEYS else BACK_KB)
        await safe(update.message,
            f"سوال {idx+1} از {len(qs)}:\n\n{qs[idx][1]}",
            reply_markup=kb)
        return 0

    # همه سوالات تمام شد
    return await _finish_collect(update, context)

async def _finish_collect(update, context):
    """جمع‌بندی نهایی پروفایل — چه از انتهای پرسیدن سوالات صدا زده بشه، چه
    وقتی کاربر یه پروفایل ذخیره‌شده‌ی قبلی رو مستقیم تایید می‌کنه (بدون
    پرسیدن دوباره‌ی هیچ سوالی)."""
    ud = context.user_data
    client = ud.get(S_CLIENT, {})
    svc = ud.get(S_SRV)
    client["first_name"] = update.effective_user.first_name or "Applicant"
    ud[S_CLIENT] = client

    # ── ذخیره‌ی پروفایل برای دفعات بعد (AI Career Profile) ──────────
    # دفعه‌ی بعد که همین کاربر — چه همین سرویس چه سرویس دیگه — دوباره
    # بیاد، این سوالات دیگه پرسیده نمی‌شن.
    try:
        await asyncio.to_thread(db_save_career_profile, update.effective_user.id, client)
    except Exception as e:
        logger.error(f"db_save_career_profile: {e}")

    # ── فوروارد کامل پروفایل جمع‌آوری‌شده برای ادمین (رکورد/پیگیری) ──
    # نکته: resume می‌تونه الان تا ۶۰۰۰ کاراکتر باشه (متن استخراج‌شده از فایل)
    # و notify_admin برخلاف safe_send هیچ chunking‌ای نداره — پیام تلگرام هم
    # سقف ۴۰۹۶ کاراکتری داره. برای همین این‌جا فقط یک preview کوتاه از رزومه
    # می‌فرستیم؛ متن/فایل کامل رزومه از قبل جدا برای ادمین فوروارد شده.
    def _fmt_field(k, v):
        if k == "resume" and isinstance(v, str) and len(v) > 300:
            return f"• {k}: {v[:300]}... (متن کامل جداگانه بالا فوروارد شد)"
        return f"• {k}: {v}"
    profile_txt = "\n".join(_fmt_field(k, v) for k, v in client.items()
                             if k not in ("resume_file_id",) and v)
    await notify_admin(context.bot,
        f"📋 پروفایل کامل کاربر {update.effective_user.id} "
        f"({'ایمیل اساتید' if svc=='email' else 'اپلای کار'}):\n\n{profile_txt}")

    if ud.get(S_TYPE) == "vip":
        # VIP از قبل (موقع وارد کردن کد) درخواست تاییدش برای ادمین رفته و
        # هیچ پرداختی نداره — همین‌جا یا منتظر تاییدیه می‌مونه یا (اگه از
        # قبل approved شده) بی‌درنگ ادامه پیدا می‌کنه.
        await _check_approval_and_proceed(update, context, svc)
        return 0

    # ── کاربر عادی: هنوز پرداختی انجام نشده ──────────────────────
    # قبل از رفتن سراغ پرداخت، خلاصه‌ی اطلاعاتی که جمع کردیم رو نشون
    # می‌دیم و تاییدش رو می‌گیریم — تا هم خطای تایپی/اطلاعات ناقص همین‌جا
    # اصلاح بشه، هم کاربر دقیقاً بدونه داره برای چی پول می‌ده.
    ud[S] = ST_INFO_CONFIRM
    summary = await _build_info_summary(update, client)
    await safe(update.message,
        "📋 این اطلاعاتیه که ثبت کردیم — لطفاً مرور کنید:\n\n" + summary +
        "\n\n✅ اگه همه‌چی درست و کامله، تایید کنید تا مرحله‌ی پرداخت رو نشونتون بدم.",
        reply_markup=mk(["✅ بله، درسته"], ["✏️ می‌خوام ویرایش کنم"], ["🔙 برگشت"]))
    return 0

async def _build_info_summary(update, client: dict) -> str:
    u = await asyncio.to_thread(db_get_user, update.effective_user.id)
    lines = []
    if u and u.get("full_name"):
        lines.append(f"👤 نام: {u['full_name']}")
    if u and u.get("phone"):
        lines.append(f"📱 شماره: {u['phone']}")
    for k, label in [
        ("field", "🔬 رشته"), ("education", "📚 مقطع"), ("countries", "🌍 کشور هدف"),
        ("gpa", "🎯 معدل"), ("work_exp", "💼 سابقه کار"), ("skills", "🛠 مهارت‌ها"),
        ("linkedin", "🔗 LinkedIn"), ("github", "💻 GitHub"), ("scholar", "🎓 Scholar"),
    ]:
        v = client.get(k)
        if v:
            lines.append(f"{label}: {v}")
    lines.append(f"📄 رزومه: {'✅ موجود' if client.get('resume') else '— (وارد نشده)'}")
    return "\n".join(lines)

async def _check_approval_and_proceed(update, context, svc):
    """آخرین قدم مشترک، چه برای VIP (بعد از collect) چه برای کاربر عادی
    (بعد از ارسال رسید پرداخت): اگه ادمین از قبل تایید کرده، بی‌درنگ و
    بدون هیچ مکثی ادامه می‌ده؛ وگرنه صبر می‌کنه و به محض تایید (توسط
    _resume_user_after_approval) خودکار ادامه پیدا می‌کنه — بدون این‌که
    کاربر مجبور باشه کاری بکنه یا /start بزنه."""
    ud = context.user_data
    sid = ud.get(S_SUB)
    if not await asyncio.to_thread(db_is_approved, sid):
        ud[S] = "wait_approval"
        # snapshot رو هم توی دیتابیس ذخیره می‌کنیم — نه فقط به این خاطر که اگر
        # بات ری‌استارت شد چیزی گم نشه، بلکه چون اصل داده همین الان هم توی
        # user_data معتبره؛ این فقط یک لایه‌ی اطمینان اضافه‌ست.
        if sid:
            await asyncio.to_thread(db_save_pending_snapshot, sid, json.dumps({
                S_CLIENT: ud.get(S_CLIENT, {}), S_SRV: svc, S_TYPE: ud.get(S_TYPE),
            }, ensure_ascii=False))
        await safe(update.message,
            "⏳ منتظر تایید تیم Manifest Apply هستیم.\n"
            "به محض تایید، ادامه‌ی روند **خودکار** برای شما شروع می‌شود "
            "و لازم نیست کاری انجام بدید یا /start بزنید — همینجا منتظر بمانید.",
            reply_markup=BACK_KB)
        return

    await _advance_after_approval(context.bot, update.effective_chat.id, ud, svc)

async def _resume_existing_service(update, context, sub: dict, svc: str):
    """کاربری که از قبل برای همین سرویس (email یا job) approved بوده —
    نباید با کلیک دوباره روی همون دکمه‌ی منوی اصلی از نو وارد ثبت‌نام/
    پرداخت بشه؛ مستقیم با subscription موجود ادامه می‌دیم (نه یکی جدید).

    ⚠️ فیکس باگ: قبلاً وقتی status == "completed" بود (یعنی سهمیه‌ی این
    subscription قبلاً تموم شده)، این تابع کاربر رو با یک پیام «با
    پشتیبانی تماس بگیرید» به بن‌بست می‌رسوند و هیچ راهی برای ثبت‌نام/خرید
    مجدد نمی‌ذاشت — درحالی‌که کاربر باید بتونه دوباره از صفر (سرویس جدید،
    پرداخت جدید) ثبت‌نام کنه. الان چون فراخوان‌کننده‌ها (منوی اصلی و
    cmd_start) از قبل فقط status == "approved" رو به این تابع می‌فرستن،
    این حالت عملاً نباید پیش بیاد؛ ولی به‌عنوان محافظ در برابر race
    condition (بین چک status و رسیدن به اینجا، status ممکنه توسط یک
    عملیات هم‌زمان به completed تغییر کرده باشه)، به‌جای بن‌بست، همون
    مسیر ثبت‌نام تازه (دقیقاً مثل نبود subscription قبلی) رو اجرا
    می‌کنیم — نه پیام بی‌فایده‌ی «با پشتیبانی تماس بگیرید»."""
    ud  = context.user_data
    uid = update.effective_user.id
    if sub["status"] != "approved":
        logger.warning(f"[uid={uid}] _resume_existing_service با status={sub['status']!r} "
                        f"صدا زده شد (نه approved) — به ثبت‌نام تازه هدایت می‌شه")
        ud[S_SRV] = svc
        ud[S] = ST_MAIN_MENU
        await safe(update.message,
            "به‌نظر می‌رسه اشتراک قبلی‌تون برای این سرویس دیگه فعال نیست (سهمیه تموم شده). "
            "می‌تونید از منوی اصلی دوباره همین سرویس رو انتخاب و از نو (با پرداخت جدید) "
            "ثبت‌نام کنید:",
            reply_markup=MAIN_KB)
        return
    ud[S_SRV]  = svc
    ud[S_SUB]  = sub["id"]
    ud[S_TYPE] = sub.get("user_type") or "normal"
    try:
        profile = await asyncio.to_thread(db_get_career_profile, uid)
    except Exception as e:
        logger.error(f"[uid={uid}] db_get_career_profile in _resume_existing_service: {e}")
        profile = None
    if profile:
        ud[S_CLIENT] = dict(profile)
    await safe(update.message,
        "🎉 شما قبلاً برای این سرویس تایید شدید — نیازی به پرداخت یا ثبت‌نام دوباره نیست، "
        "مستقیم ادامه می‌دیم:")
    await _advance_after_approval(context.bot, update.effective_chat.id, ud, svc)

async def _advance_after_approval(bot, chat_id, ud, svc=None):
    """قدم بعدی بعد از تایید پرداخت — چه همون لحظه تایید شده باشه (کاربر از
    قبل approved بوده) چه چند دقیقه بعد توسط ادمین. طراحی شده که هم از
    _handle_collect صدا زده بشه (با update.message در دسترس) هم از
    Handlerهای ادمین (که فقط بات و chat_id کاربر رو دارن، نه update خودش)."""
    svc = svc or ud.get(S_SRV)
    if svc == "email":
        # پرسش روش اپلای
        ud[S] = "choose_apply_method"
        await safe_send(bot, chat_id,
            "📧 آدرس ایمیل فرستنده را از کدام روش می‌خواهید ارسال شود?\n\n"
            "1️⃣ از ایمیل شخصی خودتان (Gmail/Outlook)\n"
            "   → نیاز به App Password دارد\n\n"
            "2️⃣ سرویس ارسال Manifest Apply\n"
            "   → بدون نیاز به تنظیمات — تیم ما ارسال می‌کند\n"
            "   (در حال توسعه — به زودی)",
            reply_markup=mk(
                ["📨 از ایمیل شخصی خودم"],
                ["🔍 استعلام سفارت"],
                ["🔙 برگشت"]
            ))
    else:  # job
        # پرسش روش اپلای کار
        ud[S] = "choose_job_method"
        await safe_send(bot, chat_id,
            "💼 روش اپلای کار را انتخاب کنید:\n\n"
            "1️⃣ ایمیل مستقیم به HR\n"
            "   → نیاز به ایمیل SMTP شما دارد\n"
            "   → Cover Letter شخصی نوشته می‌شود\n\n"
            "2️⃣ لیست فرصت‌ها + لینک پورتال\n"
            "   → بات فرصت‌ها را پیدا می‌کند\n"
            "   → شما در پورتال اپلای می‌کنید\n"
            "   (سریع‌تر، بدون نیاز به ایمیل SMTP)",
            reply_markup=mk(
                ["📨 ایمیل مستقیم به HR"],
                ["🔗 لیست فرصت‌ها و لینک‌ها"],
                ["🔍 استعلام سفارت"],
                ["🔙 برگشت"]
            ))
    return 0

# ================================================================
# EMAIL PREVIEW — پیش‌نمایش + تایید + timeout
# ================================================================

async def _start_email_preview(update, context):
    ud     = context.user_data
    profs  = ud.get(S_PROFS, [])
    client = ud.get(S_CLIENT, {})

    if not profs:
        field   = client.get("field","")
        country = client.get("countries","")
        # قبلاً این‌جا مستقیم به منوی اصلی می‌رفتیم و کل State ساخته‌شده‌ی
        # prof_targeting (تغییر کشور/رشته، جستجوی ۲۴ساعته، لیست خودم دارم)
        # دور زده می‌شد — دقیقاً همون باگی که کاربر گزارش داد. الان درست
        # مثل job_search_empty، به prof_targeting می‌ریم تا هم گزینه‌های
        # واقعی داشته باشه، هم دکمه‌ی برگشتش طبق STATE_PARENT به
        # ST_PROF_LIST برگرده، نه به منوی اصلی.
        ud["_prof_retry_attempt"] = 0  # برای چرخش دسته‌ی منابع در دکمه‌ی «تلاش مجدد»
        cls_header = classify_prof_search_result([]).get("header", "")
        await safe(update.message,
            f"{cls_header}\n\n"
            f"⚠️ استادی برای «{field}» در «{country}» با جستجوی اولیه پیدا نشد — ایمیلی ارسال نشد.\n\n"
            f"پیشنهاد:\n"
            f"• همین الان با روش دیگه‌ای دوباره بگردیم (بدون نیاز به تایپ چیزی)\n"
            f"• کشور را گسترش دهید (مثلاً Remote یا Germany Canada)\n"
            f"• رشته را کلی‌تر کنید\n"
            f"• یا اگه می‌خواید، خودمون هر ۲۴ ساعت یک‌بار دوباره براتون می‌گردیم و به محض "
            f"پیدا شدن استاد مرتبط بهتون خبر می‌دیم\n\n"
            f"می‌خواهید چه کاری انجام دهید؟",
            reply_markup=mk(
                ["🔁 تلاش مجدد"],
                ["🌍 تغییر کشور / دانشگاه هدف"],
                ["🔬 تغییر رشته / تخصص"],
                ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                ["📋 لیست خودم دارم"],
                ["🔙 برگشت"]
            ))
        ud[S] = "prof_targeting"
        return

    cls_header = classify_prof_search_result(profs).get("header", f"✅ {len(profs)} استاد یافت شد!")
    await safe(update.message, f"{cls_header}\n✍️ در حال استخراج اطلاعات و نوشتن ایمیل نمونه...")

    # ── مانیتورینگ مداوم ─────────────────────────────────────────
    # اینجا تنها نقطه‌ی مشترکیه که همه‌ی مسیرهای جستجوی استاد (جستجوی
    # اولیه، تلاش مجدد، ویرایش رشته/کشور، University Discovery) بعد از
    # پیدا کردن حداقل یک نتیجه بهش می‌رسن — پس به‌جای اضافه کردن این
    # فراخوانی به تک‌تک اون شاخه‌ها (که فراموش کردن یکیشون آسونه)، فقط
    # همینجا یک‌بار مانیتور رو می‌سازیم/تازه می‌کنیم. تا وقتی این
    # subscription فعاله، از این به بعد هر ۲۴ ساعت خودکار دوباره جستجو
    # می‌شه و فقط استادهای *جدید* (که قبلاً به همین کاربر ایمیل نرفته)
    # گزارش می‌شن.
    try:
        await asyncio.to_thread(
            db_create_or_refresh_monitor, update.effective_user.id, ud.get(S_SUB), "professor",
            client.get("field", ""), client.get("countries", ""))
    except Exception as e:
        logger.error(f"db_create_or_refresh_monitor (professor): {e}")

    await _render_prof_preview(update, context, 0)


async def _render_prof_preview(update, context, idx: int):
    """تولید و نمایش پیش‌نمایش ایمیل برای profs[idx] — نوشتن ایمیل نمونه +
    AI Reviewer + نمایش ساختاریافته. هم برای اولین پیش‌نمایش (از
    _start_email_preview) و هم برای «⏭ بررسی استاد بعدی» و برگشت از
    «⚙️ تغییر تنظیمات» استفاده می‌شه — تا رفتار همیشه یکسان باشه.

    اگه یک preview قبلی (pid دیگه، مثلاً استاد قبلی که داشتیم بررسی
    می‌کردیم) هنوز pending بود، همین‌جا status اش «superseded» می‌شه —
    که یعنی تسک ۱۵-دقیقه‌ای auto-send اون pid دیگه چیزی رو خودکار
    ارسال نمی‌کنه (فقط آخرین preview که کاربر واقعاً می‌بینه معتبره)."""
    ud     = context.user_data
    profs  = ud.get(S_PROFS, [])
    client = ud.get(S_CLIENT, {})
    if not profs:
        # این حالت عملاً نباید پیش بیاد (caller همیشه بعد از چک profs
        # صدا می‌زنه)، ولی محض احتیاط یه fallback امن داریم — نه crash.
        await safe(update.message, "⚠️ لیست اساتید در دسترس نیست. به منوی جستجو برگردید.", reply_markup=BACK_KB)
        ud[S] = "prof_targeting"
        return
    n = len(profs)
    # ── جستجوی «استاد بعدی» فقط رو به جلو، بدون دور زدن (wraparound) ──
    # قبلاً idx = idx % n و candidate_idx = (idx + tried) % n بود — یعنی
    # وقتی «⏭ بررسی استاد بعدی» به انتهای لیست می‌رسید، جستجو از سر لیست
    # (idx=0) دوباره شروع می‌شد و یک استادِ از قبل نمایش‌داده‌شده دوباره
    # به‌عنوان «استاد بعدی/جدید» نشون داده می‌شد — دقیقاً همون باگی که
    # گزارش شده بود (تکرار/Loop). الان: idx==0 یعنی جستجوی تازه (لیست تازه
    # نتیجه گرفته شده) پس شمارنده‌ی «قبلاً نمایش داده‌شده» ریست می‌شه؛ برای
    # idx>0 (یعنی از «استاد بعدی» صدا زده شدیم) فقط بازه‌ی idx..n-1 گشته
    # می‌شه — هرگز به عقب/به استاد قبلی برنمی‌گرده.
    is_fresh_search = (idx == 0)
    if is_fresh_search:
        ud["_prof_shown_idx"] = []
    idx = max(0, min(idx, n - 1))

    await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)

    # ── نام و ایمیل معتبر — پیش‌شرط اجباری برای نمایش/ارسال ─────────
    # قبلاً اگه استادی نام یا ایمیل مشخصی نداشت (مثلاً فقط یه لینک ازش
    # پیدا شده بود)، همینطوری با «—» به‌جای نام/ایمیل به کاربر نشون داده
    # می‌شد. الان: برای هر کاندید اول pipeline کامل (که ایمیل رو هم پیدا
    # می‌کنه) اجرا می‌شه؛ اگه بعدش هم نام یا ایمیل معتبر نداشت، این استاد
    # کاملاً رد می‌شه و بدون هیچ نمایشی می‌ریم سراغ استاد بعدی توی لیست —
    # تا وقتی یکی با نام+ایمیل واقعی پیدا بشه یا لیست (فقط رو به جلو) تموم بشه.
    prof = None
    papers: list = []
    candidate_idx = idx
    while candidate_idx < n:
        candidate = profs[candidate_idx]
        try:
            _preview_verdict, cand_papers = await get_professor_relevance(client, candidate)
        except Exception as e:
            logger.warning(f"get_professor_relevance for preview ({candidate.get('name','?')}): {e}")
            cand_papers = candidate.get("papers") or []

        name_ok  = bool((candidate.get("name") or "").strip())
        email_ok = bool((candidate.get("email") or "").strip())
        if not (name_ok and email_ok):
            logger.info(f"Skipping professor without valid name/email (name_ok={name_ok}, email_ok={email_ok}): {candidate.get('name') or candidate.get('url') or '?'}")
            candidate_idx += 1
            continue

        idx  = candidate_idx
        prof = candidate
        papers = cand_papers
        break

    if prof is None:
        if not is_fresh_search:
            # «استاد بعدی» تا انتهای لیست گشته و دیگه کاندید معتبری نمونده.
            # پیش‌نمایش/State فعلی دست‌نخورده می‌مونه (هنوز superseded نشده،
            # چون اون کار پایین‌تر و فقط بعد از پیدا شدن قطعی جایگزین انجام
            # می‌شه) — فقط پیام روشن می‌دیم؛ نه Loop به استادهای قبلی، نه
            # پرش به منوی اصلی. کاربر می‌تونه همین پیش‌نمایش فعلی رو تایید/
            # ارسال کنه یا از منو گزینه‌ی دیگه‌ای بزنه.
            await safe(update.message,
                "✅ به آخر لیست اساتید یافت‌شده رسیدید — استاد دیگری (با نام و ایمیل معتبر) "
                "برای نمایش وجود ندارد.\n\nمی‌توانید همین پیش‌نمایش فعلی را تایید و ارسال کنید، "
                "یا از منوی زیر گزینه‌ی دیگری انتخاب کنید:",
                reply_markup=PREVIEW_KB)
            return
        # هیچ استادی با نام و ایمیل معتبر توی این لیست پیدا نشد — به‌جای
        # نمایش یه پیش‌نمایش ناقص، صادقانه به کاربر می‌گیم. قبلاً این‌جا
        # مستقیم می‌رفتیم منوی اصلی؛ الان مثل حالت «صفر نتیجه» به
        # prof_targeting می‌ریم تا کاربر بتونه کشور/رشته رو عوض کنه یا
        # لیست خودش رو بده — و دکمه‌ی برگشتش هم طبق STATE_PARENT درست به
        # ST_PROF_LIST بره، نه منوی اصلی.
        field   = client.get("field","")
        country = client.get("countries","")
        await safe(update.message,
            f"⚠️ در بین اساتید یافت‌شده برای «{field}» در «{country}»، هیچ‌کدام نام و ایمیل "
            f"معتبری نداشتند — ایمیلی ارسال نشد.\n\n"
            f"می‌خواهید چه کاری انجام دهید؟",
            reply_markup=mk(
                ["🌍 تغییر کشور / دانشگاه هدف"],
                ["🔬 تغییر رشته / تخصص"],
                ["🔁 هر ۲۴ ساعت خودت دوباره بگرد و خبر بده"],
                ["📋 لیست خودم دارم"],
                ["🔙 برگشت"]
            ))
        ud[S] = "prof_targeting"
        return

    # ── فقط الان (بعد از پیدا شدن قطعی یک استاد معتبر جدید) پیش‌نمایش
    # قبلی رو superseded می‌کنیم — نه زودتر. قبلاً این کار قبل از شروع
    # جستجوی کاندید بعدی انجام می‌شد؛ یعنی اگه کاندید معتبر جدیدی پیدا
    # نمی‌شد (بالا، حالت anchor «به آخر لیست رسیدید»)، پیش‌نمایش خوبِ فعلی
    # قبلاً بی‌دلیل superseded شده بود. الان تا وقتی جایگزین واقعی پیدا
    # نشه، دست‌نخورده می‌مونه.
    old_pid = ud.get(S_PREVIEW_ID)
    if old_pid:
        try:
            await asyncio.to_thread(db_decide_preview, old_pid, "superseded")
        except Exception as e:
            logger.debug(f"marking old preview {old_pid} superseded: {e}")
        context.application.bot_data.pop(f"preview_snapshot_{old_pid}", None)
        await asyncio.to_thread(db_delete_preview_snapshot, old_pid)

    # این استاد به‌عنوان «بررسی‌شده» ثبت می‌شه — سیستم می‌دونه آخرین
    # استاد بررسی‌شده کدومه (preview_idx پایین‌تر) و دیگه دوباره به‌عنوان
    # نتیجه‌ی «جدید» به این استاد برنمی‌گرده.
    shown = ud.get("_prof_shown_idx", [])
    if idx not in shown:
        shown.append(idx)
    ud["_prof_shown_idx"] = shown

    if papers:
        prof["papers"] = papers  # تا "🔄 نسخه جدید بنویسید" هم بعداً بهش دسترسی داشته باشه

    prof_with_pub = dict(prof)
    prof_with_pub["pub_list"] = papers

    try:
        adapted_resume = await adapt_resume_for_target(client, prof_with_pub, "professor")
    except Exception as e:
        logger.warning(f"adapt_resume_for_target for preview: {e}")
        adapted_resume = ""

    # ── تولید ایمیل نمونه با Retry — هیچ‌وقت نباید کل مکالمه رو crash کنه ──
    subj = body = ""
    for attempt in range(2):
        try:
            subj, body, _review = await generate_and_review_email(
                client, prof, "", adapted_resume or "", papers=papers)
            if subj and body:
                break
        except Exception as e:
            logger.warning(f"write_prof_email attempt {attempt+1} failed: {e}")
        if attempt == 0:
            await asyncio.sleep(1.5)
    if not subj or not body:
        try:
            body = _force_correct_salutation(_fallback_email(client, prof), prof)
            subj = f"Prospective {client.get('visa_type','Research')} Applicant — {client.get('field','')}"
        except Exception as e:
            logger.error(f"even _fallback_email failed: {e}")
            subj = "Prospective Applicant"
            body = f"Dear Professor,\n\nI am {client.get('first_name','a')} student interested in your research. I would welcome the opportunity to discuss potential collaboration.\n\nBest regards,\n{client.get('first_name','Applicant')}"

    # ذخیره در preview_queue — یک ردیف جدید (نه آپدیت قبلی) تا هر استاد
    # pid مستقل خودش رو داشته باشه و «تایید و ارسال» همیشه دقیقاً به
    # همون استادی که الان روی صفحه است اشاره کنه.
    pid = await asyncio.to_thread(
        db_add_preview, update.effective_user.id,
        prof.get("name",""), prof.get("email",""), subj, body)
    ud[S_PREVIEW_ID] = pid
    ud["preview_idx"] = idx
    ud[S] = ST_PREVIEW

    # TASK 3 — پیش‌نمایش ساختاریافته: استاد / دانشگاه / دپارتمان / پژوهش
    # مرتبط / ایمیل / موضوع / متن — به‌جای یک خط فشرده‌ی نام+ایمیل.
    papers_titles = [p.get("title","") if isinstance(p, dict) else str(p)
                      for p in (papers or [])][:1]
    research_line = papers_titles[0] if papers_titles else (client.get("field") or "—")
    title_line = prof.get("title","")
    dept_line = prof.get("department","")
    extra_meta = ""
    if title_line:
        extra_meta += f"🎓 عنوان: {title_line}\n"
    if dept_line:
        extra_meta += f"🏛 دپارتمان: {dept_line}\n"
    # درصد تطابق (_rank_score) — فقط وقتی pipeline واقعاً محاسبه‌اش کرده
    # (لیست دستی خود کاربر این تگ رو نداره، پس اونجا نشون داده نمی‌شه).
    if prof.get("_rank_score") is not None:
        extra_meta += f"🎯 تطابق: {prof.get('_rank_score')}%\n"
    preview_text = (
        f"📧 پیش‌نمایش ایمیل ({idx+1} از {len(profs)}):\n\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"👨‍🏫 استاد: {prof.get('name','') or '—'}\n"
        f"🏫 دانشگاه: {prof.get('university','') or '—'}\n"
        f"{extra_meta}"
        f"🔬 پژوهش مرتبط: {research_line}\n"
        f"📮 ایمیل: {prof.get('email','') or '—'}\n"
        f"📌 موضوع: {subj}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"{body[:1200]}{'...' if len(body)>1200 else ''}\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"⏰ *{PREVIEW_TIMEOUT//60} دقیقه* فرصت دارید بررسی و ویرایش کنید.\n"
        f"اگر ظرف این مدت پاسخی ندید، همین ایمیل تایید‌شده در نظر گرفته می‌شود و ارسال ایمیل‌ها "
        f"**خودکار و بدون تاخیر** شروع می‌شود — پس اگر می‌خواهید چیزی تغییر کند، همین الان بگویید.\n\n"
        f"ℹ️ این فقط پیش‌نمایش *یک نمونه* از {len(profs)} استاده. با زدن «✅ تایید کنید و ارسال کنید»، "
        f"بات خودش به‌ترتیب سراغ تک‌تک بقیه‌ی استادها هم می‌ره و بدون این‌که لازم باشه دوباره کاری "
        f"کنید، تا {len(profs)}اُمی (یا تا سقف {EMAIL_QUOTA} ایمیل) ادامه می‌ده — نتیجه‌ی هر کدوم رو "
        f"همینجا بهتون خبر می‌ده."
    )
    await safe(update.message, preview_text, reply_markup=PREVIEW_KB)

    # ── snapshot برای auto-send ──────────────────────────────────
    # context.user_data فقط داخل همین Update در دسترس است و به تسک
    # timeout جداگانه پاس داده نمی‌شود؛ قبلاً همین باعث می‌شد که بعد
    # از ۳۰ دقیقه فقط پیام "ارسال شروع شد" فرستاده شود ولی هیچ ایمیلی
    # واقعاً ارسال نشود. این‌جا یک snapshot از داده‌های لازم (لیست
    # اساتید + مشخصات SMTP + پروفایل کاربر) را هم در bot_data (مسیر
    # سریع حالت عادی) و هم در دیتابیس (preview_snapshots — تا با
    # ری‌استارت وسط مهلت گم نشه، نگاه کن به _on_bot_startup) ذخیره
    # می‌کنیم تا تسک timeout بتواند واقعاً ارسال را انجام دهد.
    snap = {
        "profs": profs, "client": client,
        S_SMTP_E: ud.get(S_SMTP_E, ""), S_SMTP_P: ud.get(S_SMTP_P, ""),
        S_SMTP_H: ud.get(S_SMTP_H, "smtp.gmail.com"),
        S_SMTP_PORT: ud.get(S_SMTP_PORT, 587),
        S_SUB: ud.get(S_SUB),
    }
    context.application.bot_data[f"preview_snapshot_{pid}"] = snap
    await asyncio.to_thread(
        db_save_preview_snapshot, pid, update.effective_user.id, update.effective_chat.id, snap)

    # timeout task — بعد از مهلت تعیین‌شده خودکار ارسال می‌شود (فقط اگه
    # این pid هنوز pending باشه — یعنی کاربر بین این مدت به preview
    # دیگه‌ای سوییچ نکرده باشه)
    asyncio.create_task(_preview_timeout(
        context, update.effective_chat.id, update.effective_user.id, pid))


class _StartupCtx:
    """یک context ساختگی حداقلی، فقط برای صدا زدن _preview_timeout از
    _on_bot_startup — که برخلاف هندلرهای عادی (که context واقعی PTB
    باهاشون میاد)، فقط به application دسترسی داره. _preview_timeout فقط
    به context.bot و context.application.bot_data نیاز داره؛ همین دو
    تا کافیه، نیازی به mock کردن کل ContextTypes نیست."""
    __slots__ = ("application", "bot")
    def __init__(self, application):
        self.application = application
        self.bot = application.bot

async def _preview_timeout(context, chat_id, uid, pid, delay: float | None = None):
    """مهلت صبر — اگر کاربر تایید نکرد، خودکار ارسال.
    delay=None یعنی مهلت کامل PREVIEW_TIMEOUT (حالت عادی — preview
    تازه ساخته شده). یک عدد یعنی «چقدر از مهلت باقی مونده» — برای
    reconciliation بعد از ری‌استارت وسط مهلت (نگاه کن به _on_bot_startup)،
    که در اون حالت باید فقط باقی‌مونده‌ی زمان صبر کنیم، نه از اول ۱۵ دقیقه."""
    try:
        await asyncio.sleep(PREVIEW_TIMEOUT if delay is None else max(delay, 0))
        p = await asyncio.to_thread(db_get_preview, pid)
        snap = context.application.bot_data.pop(f"preview_snapshot_{pid}", None)
        if not snap:
            # bot_data (persistent نیست) یا از دست رفته یا اصلاً روی این
            # process هیچ‌وقت ست نشده بود — قبل از تسلیم شدن، از دیتابیس
            # (preview_snapshots) هم امتحان می‌کنیم. حالت عادی (بدون
            # ری‌استارت وسط کار) هیچ‌وقت به این fallback نیاز نداره چون
            # bot_data بالا موفق می‌شه.
            snap = await asyncio.to_thread(db_get_preview_snapshot, pid)
        if p and p["status"] == "pending":
            await asyncio.to_thread(db_decide_preview, pid, "auto_approved")
            try:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"⏰ {PREVIEW_TIMEOUT//60} دقیقه گذشت — ارسال ایمیل‌ها به صورت خودکار شروع شد...")
            except Exception as e:
                logger.error(f"preview timeout notify: {e}")

            if not snap or not snap.get("profs"):
                # snapshot نه در bot_data نه در دیتابیس پیدا نشد — این
                # فقط برای previewهایی ممکنه پیش بیاد که قبل از این فیکس
                # ساخته شدن (رکورد قدیمی‌تر که هیچ‌وقت در preview_snapshots
                # ذخیره نشده). واقعاً چیزی برای بازیابی نیست.
                logger.warning(f"preview {pid}: snapshot missing (even in DB), cannot auto-send")
                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text="⚠️ اطلاعات ارسال یافت نشد (احتمالاً بات ری‌استارت شده). "
                             "لطفاً دوباره /start بزنید و ادامه دهید.")
                except Exception:
                    pass
                await asyncio.to_thread(db_delete_preview_snapshot, pid)
                return
            # همان مسیر ارسال دستی را با داده‌های ذخیره‌شده صدا می‌زنیم — و
            # همون متن نهایی preview (شامل ویرایش‌های احتمالی کاربر قبل از
            # اتمام مهلت) رو هم برای استاد اول پاس می‌دیم.
            approved_first = {"professor_email": p["professor_email"],
                               "subject": p["subject"], "body": p["body"]}
            await _send_emails_bg(context, chat_id, uid, snap["profs"], snap["client"], snap, approved_first)
        await asyncio.to_thread(db_delete_preview_snapshot, pid)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"_preview_timeout crashed for pid={pid}: {e}")
        traceback.print_exc()

async def _handle_preview(update, context):
    ud   = context.user_data
    text = (update.message.text or "").strip()
    pid  = ud.get(S_PREVIEW_ID)
    p    = await asyncio.to_thread(db_get_preview, pid) if pid else None

    if text == "✅ تایید و ارسال (بقیه خودکار ادامه پیدا می‌کنه)":
        uid = update.effective_user.id
        if _has_active_send(uid):
            await safe(update.message,
                "⏳ یک عملیات ارسال دیگر همین الان برای شما در حال اجراست — "
                "صبر کنید تمام شود، یا با /cancel لغوش کنید.", reply_markup=BACK_KB)
            return 0
        if p:
            await asyncio.to_thread(db_decide_preview, pid, "approved")
        # snapshot دیگر لازم نیست — کاربر خودش تایید کرد؛ پاکش می‌کنیم
        # تا نه در bot_data و نه در دیتابیس (preview_snapshots) بی‌نهایت
        # جمع نشه.
        context.application.bot_data.pop(f"preview_snapshot_{pid}", None)
        await asyncio.to_thread(db_delete_preview_snapshot, pid)
        await safe(update.message,
            "🚀 ارسال ایمیل‌ها شروع شد!\n"
            "بات خودش به‌ترتیب می‌ره سراغ تک‌تک استادهای باقی‌مونده — لازم نیست چیزی بزنید یا منتظر "
            "بمونید. نتیجه‌ی هر استاد (ارسال شد/رد شد) همین‌جا براتون میاد تا کل لیست تموم بشه.",
            reply_markup=BACK_KB)
        profs  = ud.get(S_PROFS, [])
        client = ud.get(S_CLIENT, {})
        # همین متنی که کاربر الان تایید کرد (شامل هر ویرایش/بازنویسی‌ای که
        # انجام داده) رو دقیقاً برای همون استاد اول می‌فرستیم، نه یک نسخه‌ی
        # تازه‌ی دیگه.
        approved_first = {"professor_email": p["professor_email"],
                           "subject": p["subject"], "body": p["body"]} if p else None
        _t = asyncio.create_task(
            _run_bg_task(_send_emails_bg(context, update.effective_chat.id,
                            uid, profs, client, ud, approved_first),
                         context, update.effective_chat.id, "ارسال ایمیل"))
        _register_send_task(uid, _t)
        ud[S] = ST_SENDING
        return 0

    if text == "⏭ بررسی استاد بعدی":
        profs = ud.get(S_PROFS, [])
        idx = ud.get("preview_idx", 0)
        # فیکس: قبلاً next_idx = (idx + 1) % len(profs) بود — یعنی نزدیک
        # انتهای لیست، دور می‌زد و به استاد اول (که قبلاً دیده شده بود)
        # برمی‌گشت و اون رو دوباره به‌عنوان «استاد بعدی» نشون می‌داد. الان
        # هیچ‌وقت به عقب برنمی‌گرده — اگه استاد بعدی‌ای در ادامه‌ی لیست
        # نمونده باشه، صادقانه می‌گیم و نه Loop می‌شه نه به منوی اصلی می‌ره.
        if len(profs) <= 1 or idx + 1 >= len(profs):
            await safe(update.message,
                "✅ به آخر لیست اساتید یافت‌شده رسیدید — استاد دیگری برای نمایش وجود ندارد.\n"
                "می‌توانید همین پیش‌نمایش فعلی را تایید و ارسال کنید، یا از منوی زیر گزینه‌ی "
                "دیگری انتخاب کنید:",
                reply_markup=PREVIEW_KB)
            return 0
        next_idx = idx + 1
        await safe(update.message, "✍️ در حال آماده‌سازی پیش‌نمایش استاد بعدی...")
        await _render_prof_preview(update, context, next_idx)
        return 0

    if text == "⚙️ تغییر تنظیمات (رشته/کشور/رزومه)":
        ud[S] = ST_PREVIEW_SETTINGS
        client = ud.get(S_CLIENT, {})
        await safe(update.message,
            f"📚 رشته فعلی: {client.get('field','—')}\n🌍 کشور فعلی: {client.get('countries','—')}\n\n"
            "چه چیزی را می‌خواهید تغییر دهید؟ (پیش‌نمایش فعلی و لیست اساتید شما نگه داشته می‌شود تا "
            "برگردید)",
            reply_markup=PREVIEW_SETTINGS_KB)
        return 0

    if text == "✏️ ویرایش موضوع":
        await safe(update.message, "موضوع جدید را بنویسید:")
        ud["edit_subject"] = True
        return 0

    if ud.get("edit_subject"):
        ud.pop("edit_subject")
        if p:
            await asyncio.to_thread(db_update_preview_text, pid, subject=text)
        await safe(update.message, f"✅ موضوع آپدیت شد: {text}\n\nاکنون چه کار کنیم?",
            reply_markup=PREVIEW_KB)
        return 0

    # ── TASK 7 — ویرایش هوشمند متن ایمیل با دستور آزاد کاربر ─────────
    if text == "✏️ ویرایش متن ایمیل":
        await safe(update.message,
            "چه چیزی را می‌خواهید تغییر دهید؟ یکی از گزینه‌های زیر را بزنید یا خودتان بنویسید:",
            reply_markup=EDIT_BODY_SUGGEST_KB)
        ud["edit_body_instruction"] = True
        return 0

    if ud.get("edit_body_instruction"):
        ud.pop("edit_body_instruction")
        if text == "🔙 برگشت":
            await safe(update.message, "باشه، لغو شد.", reply_markup=PREVIEW_KB)
            return 0
        if not p:
            await safe(update.message, "پیش‌نمایشی برای ویرایش پیدا نشد.", reply_markup=PREVIEW_KB)
            return 0
        profs = ud.get(S_PROFS, [])
        first_prof = profs[0] if profs else {"name": p.get("professor_name", "")}
        await safe(update.message, "✍️ در حال اعمال تغییرات...")
        await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
        new_subj, new_body = await _revise_prof_email(
            ud.get(S_CLIENT, {}), first_prof, p["subject"], p["body"], text)
        await asyncio.to_thread(db_update_preview_text, pid, subject=new_subj, body=new_body)
        await safe(update.message,
            f"📧 نسخه ویرایش‌شده:\n\n📌 {new_subj}\n\n"
            f"{new_body[:1200]}{'...' if len(new_body)>1200 else ''}\n\n"
            "اگر باز هم می‌خواهید چیزی تغییر کند، دوباره «✏️ ویرایش متن ایمیل» را بزنید:",
            reply_markup=PREVIEW_KB)
        return 0

    if text == "🔄 نسخه جدید بنویسید":
        client = ud.get(S_CLIENT, {})
        profs  = ud.get(S_PROFS, [])
        if profs:
            await safe(update.message, "✍️ در حال نوشتن نسخه جدید...")
            await context.bot.send_chat_action(chat_id=update.effective_chat.id, action=ChatAction.TYPING)
            subj, body, _review = await generate_and_review_email(
                client, profs[0], papers=profs[0].get("papers") or [])
            if pid:
                await asyncio.to_thread(db_update_preview_text, pid, subject=subj, body=body)
            await safe(update.message,
                f"📧 نسخه جدید:\n\n📌 {subj}\n\n{body[:1000]}",
                reply_markup=PREVIEW_KB)
        return 0

    await safe(update.message, "یکی از گزینه‌های زیر را انتخاب کنید:", reply_markup=PREVIEW_KB)
    return 0

def _parse_prof_list(text: str) -> list[dict]:
    profs = []
    for line in text.split("\n"):
        line = line.strip()
        if not line: continue
        emails = re.findall(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", line)
        urls   = re.findall(r"https?://\S+", line)
        name   = re.split(r"[|,\t]", line)[0].strip()
        name   = re.sub(r"https?://\S+", "", name)
        name   = re.sub(r"[a-zA-Z0-9._%+\-]+@\S+", "", name).strip()
        if emails or urls:
            profs.append({
                "email": emails[0] if emails else "",
                "url":   urls[0]   if urls   else "",
                "name":  name[:60] if name   else "",
                "papers": [],
                "_manual_entry": True,  # لیست خودِ کاربره — رد نکن حتی اگه صفحه‌ی دانشکده پیدا نشه
            })
    return profs[:EMAIL_QUOTA]

# ================================================================
# BACKGROUND — ارسال ایمیل
# ================================================================

async def _notify_no_email_found(context, chat_id, prof: dict, verdict: dict | None = None):
    """وقتی pipeline ۱۱مرحله‌ای ایمیل استاد رو پیدا نکرد، ولی اسم/موقعیت
    استاد رو داریم — به‌جای این‌که این استاد بی‌سروصدا فقط توی email_jobs
    با status='SKIPPED' گم بشه، همین الان اطلاعات کاملی که پیدا کردیم
    (اسم، دانشگاه/دپارتمان، لینک صفحه‌ی دانشکده/آزمایشگاه، دلیل تطابق
    پژوهشی) رو مستقیم برای کاربر می‌فرستیم تا خودش دستی پیگیری کنه —
    بهتر از این‌که این سرنخ کاملاً از دست بره."""
    name = (prof.get("name") or "").strip()
    if not name:
        return  # بدون اسم، این اطلاعات برای کاربر قابل استفاده نیست
    try:
        lines = [f"⚠️ ایمیل این استاد رو هر چی گشتیم پیدا نکردیم — ولی اطلاعاتی که پیدا کردیم:",
                  f"👨‍🏫 {name}"]
        if prof.get("university"):
            lines.append(f"🏫 {prof['university']}")
        if prof.get("department"):
            lines.append(f"🏢 دپارتمان: {prof['department']}")
        if prof.get("title"):
            lines.append(f"🎓 {prof['title']}")
        if prof.get("url"):
            lines.append(f"🔗 {prof['url']}")
        if verdict and verdict.get("reason"):
            lines.append(f"💡 دلیل تطابق با رزومه‌تون: {verdict['reason'][:150]}")
        if prof.get("accepting_students"):
            lines.append(f"🎓 احتمالاً در حال پذیرش دانشجو ({prof.get('accepting_evidence','')[:100]})")
        if prof.get("has_funding"):
            lines.append(f"💰 سیگنال بودجه/گرنت: {prof.get('funding_evidence','')[:100]}")
        lines.append("💡 خودتون می‌تونید از طریق صفحه‌ی دانشکده/دپارتمان بالا ایمیلش رو پیدا کنید.")
        await context.bot.send_message(chat_id=chat_id, text="\n".join(lines))
    except Exception as e:
        logger.debug(f"_notify_no_email_found: {e}")

async def _send_emails_bg(context, chat_id, uid, profs, client, ud_snap, approved_first=None):
    """approved_first: دیکشنری {"professor_email","subject","body"} از preview_queue —
    قبلاً وقتی کاربر پیش‌نمایش ایمیل اولین استاد را تایید/ویرایش می‌کرد، این
    متن اصلاً این‌جا استفاده نمی‌شد و ایمیل واقعی هر بار از صفر با AI بازنویسی
    می‌شد؛ یعنی ویرایش کاربر عملاً بی‌اثر بود. الان اگر این پارامتر پر باشد،
    برای همان استاد (بر اساس ایمیل) دقیقاً همان متنی که کاربر دیده/تایید کرده
    ارسال می‌شود، نه یک نسخه‌ی تازه."""
    sent_ok = sent_fail = skipped = 0
    total   = min(len(profs), EMAIL_QUOTA)
    # ── لاگ funnel «مرحله‌ی تحلیل» — جدا از search_metrics (که فقط
    # مرحله‌ی کشف رو اندازه می‌گیره). هر gate که این‌جا یک استاد رو رد
    # می‌کنه، یک شمارنده‌ی مجزا داره تا در پایان batch دقیقاً معلوم بشه
    # بیشترین ریزش کجاست — نه فقط عدد نهایی کم.
    funnel = {
        "duplicate": 0, "batch_time_budget": 0, "no_email_found": 0,
        "invalid_name": 0, "invalid_email": 0, "unverified_faculty": 0,
        "unverified_signals": 0, "no_match": 0, "send_failed": 0,
    }
    smtp_e  = ud_snap.get(S_SMTP_E, "")
    smtp_p  = ud_snap.get(S_SMTP_P, "")
    smtp_h  = ud_snap.get(S_SMTP_H, "smtp.gmail.com")
    smtp_pt = ud_snap.get(S_SMTP_PORT, 587)
    sub_id  = ud_snap.get(S_SUB)
    used    = 0  # مصرف quota از دیتابیس (مقاوم در برابر ری‌استارت)

    # اگر بات وسط ارسال ری‌استارت شده باشه و این تابع دوباره از اول صدا زده
    # بشه، بدون این چک همون اساتید قبلی دوباره ایمیل می‌گرفتن. با این مجموعه
    # هر کسی که قبلاً واقعاً ایمیل موفق گرفته، رد می‌شه.
    already_sent = await asyncio.to_thread(db_get_sent_email_set, uid)
    batch_start  = time.monotonic()

    # ── Queue واقعی: قبل از شروع، همه‌ی استادهای این batch رو PENDING
    # ثبت می‌کنیم تا اگه بات وسط کار مرد، دقیقاً معلوم باشه کدوم استاد
    # کجا مونده — نه فقط یک لیست گم‌شده‌ی توی حافظه.
    if sub_id:
        await asyncio.to_thread(db_enqueue_email_jobs, uid, sub_id, profs, total)

    for idx, prof in enumerate(profs[:total]):
        job_id = None
        try:
            if sub_id:
                job_info = await asyncio.to_thread(db_get_job, uid, sub_id, idx)
                if job_info:
                    job_id = job_info["id"]
                    if job_info["status"] == "SENT":
                        # قبلاً موفق ارسال شده (مثلاً این batch بعد از ری‌استارت
                        # دوباره از اول صدا زده شده) — دوباره کاری نکن.
                        continue
                    if job_info["status"] == "UNKNOWN":
                        # این job وسط یک تلاش قبلی بود که process‌اش (نه این
                        # یکی) وسط کار مرد — معلوم نیست SMTP واقعاً موفق شده
                        # بود یا نه (نگاه کن به db_reconcile_stale_sending_jobs).
                        # عمداً خودکار دوباره نمی‌فرستیم؛ خطر یک ایمیل تکراری
                        # به همون استاد از یک ایمیل معلق/گم‌شده بدتره. ادمین
                        # این تعداد رو جدا می‌بینه و دستی تصمیم می‌گیره.
                        continue
                    await asyncio.to_thread(db_mark_job_sending, job_id)

            # اگر ایمیل ندارد و URL دارد، scrape کن (ارزون، همیشه اول امتحان می‌شه)
            if not prof.get("email") and prof.get("url"):
                raw = await asyncio.to_thread(scrape, prof["url"])
                emails = extract_emails(raw)
                if emails: prof["email"] = emails[0]

            # اگه از قبل ایمیل داریم، همین الان (ارزون) چک تکراری رو بزن —
            # قبل از AI judge/ایمیل‌یابی سنگین. برای اساتید بدون ایمیل این
            # چک بعداً، بعد از پیدا شدن ایمیل، دوباره انجام می‌شه.
            if prof.get("email") and prof["email"] in already_sent:
                funnel["duplicate"] += 1
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"], "duplicate")
                continue

            # ── سقف زمانی batch ────────────────────────────────────
            # اگه استاد از قبل ایمیل نداره و پیدا کردنش (pipeline ۷مرحله‌ای،
            # کندترین بخش کل فرایند) دیگه توی بودجه‌ی زمانی batch جا
            # نمی‌شه، به‌جای معطل موندن کل ارسال، از این استاد می‌گذریم —
            # هیچ‌وقت کل فرایند به‌خاطر کند/خراب بودن یک سرویس خارجی
            # (مثلاً DuckDuckGo) متوقف نمی‌مونه.
            if not prof.get("email") and (time.monotonic() - batch_start) > MAX_SCAN_SECONDS:
                skipped += 1
                funnel["batch_time_budget"] += 1
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", "", "batch_time_budget_exceeded")
                continue

            # ── تشخیص match واقعی + پیدا کردن ایمیل (اگه هنوز نداریم) ──
            # نکته‌ی مهم: قبلاً این‌جا اگه prof از قبل ایمیل نداشت (که برای
            # اکثر اساتیدی که از OpenAlex/Semantic Scholar پیدا می‌شن
            # همینطوره) بدون تلاش برای پیدا کردنش رد می‌شد — یعنی
            # pipeline ۷مرحله‌ای ایمیل‌یابی (که داخل get_professor_relevance
            # صدا زده می‌شه) عملاً هیچ‌وقت به این استادها نمی‌رسید. الان
            # این تابع همیشه اول اجرا می‌شه، بعد چک می‌کنیم ایمیل پیدا شد
            # یا نه.
            verdict, papers = await get_professor_relevance(client, prof)
            prof_score  = verdict.get("score", 0)
            prof_reason = verdict.get("reason", "")

            if not prof.get("email"):
                skipped += 1
                funnel["no_email_found"] += 1
                await _notify_no_email_found(context, chat_id, prof, verdict)
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", "", "no_email_found")
                continue

            # ── اسم واقعی اجباری — دقیقاً هم‌سطح چک بالا برای ایمیل ────
            # بدون این چک، اگه prof["name"] خالی یا ناقص بود (مثلاً از
            # OpenAlex/Web Discovery که گاهی فقط ایمیل پیدا می‌کنه ولی
            # اسم درست استخراج نمی‌شه)، _fallback_email و _build_salutation
            # به‌جای رد کردن، بی‌سروصدا از یک اسم جایگزین عمومی («Professor»)
            # استفاده می‌کردن — یعنی ایمیلی با خطاب کلی مثل «Dear Professor»
            # به‌جای اسم واقعی فرستاده می‌شد. الان به‌جای این fallback خاموش،
            # همچون ایمیل نامعتبر، این استاد رد می‌شه.
            prof_name_clean = (prof.get("name") or "").strip()
            if not prof_name_clean or len(prof_name_clean.split()) < 2:
                skipped += 1
                funnel["invalid_name"] += 1
                await asyncio.to_thread(db_log_email, uid,
                    prof_name_clean, prof.get("email", ""), "", "skipped:missing_or_invalid_name")
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED",
                    prof.get("email", ""), "missing_or_invalid_name")
                continue

            if prof["email"] in already_sent:
                funnel["duplicate"] += 1
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"], "duplicate")
                continue
            already_sent.add(prof["email"])

            # ── heartbeat — هر ۱۵ استاد یک پیام کوتاه که کاربر مطمئن بشه
            # بات هنوز زنده و در حال کاره، نه اینکه انگار «هنگ» کرده ────
            if (idx + 1) % 15 == 0:
                try:
                    await context.bot.send_message(chat_id=chat_id,
                        text=f"🔄 هنوز در حال بررسی لیست اساتیدم...\n"
                             f"{idx+1}/{total} بررسی شد — {sent_ok} ایمیل تا الان ارسال شده.")
                except Exception:
                    pass

            # ── اعتبارسنجی ایمیل قبل از ارسال: Regex → MX → Disposable ──
            # این‌جوری به‌جای اینکه یک ایمیل واضحاً خراب/جعلی رو بفرستیم
            # و فقط bounce بگیریم، از همون اول رد می‌شه.
            valid, valid_reason = await asyncio.to_thread(validate_email_deliverability, prof["email"])
            if not valid:
                skipped += 1
                funnel["invalid_email"] += 1
                await asyncio.to_thread(db_log_email, uid,
                    prof.get("name",""), prof.get("email",""), "",
                    f"skipped:invalid_email:{valid_reason[:80]}")
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"], f"invalid_email:{valid_reason[:100]}")
                continue

            # ── University Domain confidence ────────────────────────
            # مثلاً professor@gmail.com معمولاً ایمیل رسمی استاد نیست —
            # رد نمی‌شه (بعضی وقتا واقعیه) ولی امتیازش پایین میاد.
            domain_info = classify_email_domain(prof["email"])
            if domain_info["category"] == "personal":
                prof_score = max(0, prof_score - 20)
                verdict["score"] = prof_score
                prof_reason += " [⚠️ ایمیل شخصی/عمومی، نه دامنه‌ی دانشگاهی]"

            # ── Professor Verification ──────────────────────────────
            # اگر هیچ نشونه‌ای (صفحه‌ی دانشکده/آزمایشگاه/دایرکتوری، فعالیت
            # اخیر، پذیرش دانشجو/بودجه) پیدا نشد، ارسال نکن — به‌جز
            # لیست‌های دستی خود کاربر.
            verified, verify_reason = _professor_verification_passed(prof)
            if not verified:
                skipped += 1
                funnel["unverified_faculty"] += 1
                await asyncio.to_thread(db_log_email, uid,
                    prof.get("name",""), prof.get("email",""), "",
                    f"skipped:unverified_faculty:{verify_reason[:100]}")
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"], f"unverified_faculty:{verify_reason[:100]}")
                continue

            # ── Confidence Score — سه متریک واقعاً مجزا، نه یک عدد با
            # آفست تزئینی ±۳ (که نسخه‌ی قبلی این‌جا داشت) ─────────────
            # Research Match: تطابق پژوهشی رزومه با کار استاد (prof_score)
            # Email Confidence: چقدر مطمئنیم خودِ آدرس ایمیل واقعی/فعاله
            # Faculty Verification: تا این‌جا رسیدیم یعنی حتماً Yes است
            # (چون verified=False بالاتر همین حالا continue کرده)
            email_confidence = compute_email_confidence(prof, domain_info)

            # ── Research Distance + Fit Matrix + Advisor Fit ─────────
            # طبق درخواست کاربر: به‌جای یک عدد تکی، هم فاصله‌ی پژوهشی
            # (VERY_CLOSE/ADJACENT/MODERATE/FAR — تا استادهای
            # interdisciplinary از دست نرن)، هم یک ماتریس ۹معیاره‌ی
            # قابل‌توضیح، هم Advisor Fit (جدا از Research Fit، فقط لیبل
            # کمکی، هیچ‌جا فیلتر نیست) محاسبه می‌شه. کاملاً deterministic
            # بجز topic_fit/methods_fit/publications_fit/distance که از
            # همون خروجی ساختاریافته‌ی judge_relevance میان.
            fit_matrix = compute_fit_matrix(prof, verdict, email_confidence,
                                             similarity=verdict.get("similarity_score"))
            advisor_fit = compute_advisor_fit(prof)

            # نمایش عادی: selectively (که چت پر نشه). ولی اگه Email
            # Confidence پایین بود (یعنی خودِ ایمیل، نه فقط تطابق پژوهشی،
            # مشکوکه)، هشدار همیشه نشون داده می‌شه — کاربر نباید بی‌خبر
            # بمونه که این یکی ریسک bounce بالاتری داره.
            LOW_EMAIL_CONFIDENCE = 55
            show_prof_score = (
                (sent_ok + skipped) < 3 or
                (45 <= prof_score <= 75) or
                (sent_ok + skipped) % 25 == 0 or
                email_confidence < LOW_EMAIL_CONFIDENCE
            )
            if show_prof_score and prof.get("name"):
                res_icon = "✅" if verdict["match"] else "⚠️"
                warn_line = (f"⚠️ Email Confidence پایینه ({email_confidence}%) — این آدرس رو با احتیاط "
                             f"در نظر بگیرید، ریسک برگشت‌خوردن (bounce) بیشتره.\n"
                             if email_confidence < LOW_EMAIL_CONFIDENCE else "")
                extra_lines = ""
                if prof.get("last_pub_year"):
                    activity_icon = "🟢" if prof.get("is_active") else "🟡"
                    extra_lines += f"{activity_icon} آخرین مقاله: {prof['last_pub_year']}\n"
                if prof.get("accepting_students"):
                    extra_lines += f"🎓 احتمالاً در حال پذیرش دانشجو ({prof.get('accepting_evidence','')})\n"
                if prof.get("has_funding"):
                    extra_lines += f"💰 سیگنال بودجه/گرنت: {prof.get('funding_evidence','')}\n"
                if domain_info["category"] == "university":
                    extra_lines += f"🏫 ایمیل دانشگاهی تأییدشده ({domain_info['domain']})\n"
                elif domain_info["category"] == "personal":
                    extra_lines += f"⚠️ ایمیل شخصی/عمومی ({domain_info['domain']})\n"
                lab = prof.get("lab_analysis")
                if lab:
                    if lab.get("members_count"):
                        extra_lines += f"👥 اعضای آزمایشگاه: {lab['members_count']}\n"
                    if lab.get("industry_partners"):
                        extra_lines += f"🏭 همکاری صنعتی: {', '.join(lab['industry_partners'][:3])}\n"
                # Availability Detection — از v9 به بعد برای همه‌ی مسیرها
                # پر می‌شه (نه فقط University Discovery Mode)، چون از همون
                # lab_page_text ای استفاده می‌کنه که برای ایمیل‌یابی هرحال
                # گرفته می‌شه. اگه صفحه‌ای گرفته نشده باشه (مثلاً همه‌چیز از
                # cache/API بدون صفحه‌ی خام اومده)، ۰ می‌مونه و این بخش رد
                # می‌شه — لیبل، نه فیلتر.
                if prof.get("availability_score"):
                    ev = " | ".join(prof.get("availability_evidence") or [])
                    extra_lines += f"⭐ Supervisor Availability: {prof['availability_score']}/85{(' — ' + ev) if ev else ''}\n"
                # Funding Intelligence — فقط وقتی has_funding=True بوده و AI
                # تونسته منبع/مبلغ/اعتبار مشخصی دربیاره (نه یک حدس مبهم).
                fi = prof.get("funding_intelligence")
                if fi:
                    fi_line = "💰 Funding Intelligence: "
                    parts = []
                    if fi.get("source"):
                        parts.append(f"Source: {fi['source']}")
                    parts.append(f"Amount: {fi.get('amount') or 'Unknown'}")
                    if fi.get("valid_until"):
                        parts.append(f"Valid until: {fi['valid_until']}")
                    parts.append(f"Probability: {fi.get('probability','Medium')}")
                    extra_lines += fi_line + " | ".join(parts) + "\n"
                # Publication Trend — از papers واقعی همین استاد (نه فقط
                # صفحه‌ی دانشگاه) — لیبل کوتاه، فقط وقتی papers کافی داشتیم.
                trend = prof.get("publication_trend")
                if trend and trend.get("recent"):
                    years_str = ", ".join(str(p["year"]) for p in trend["recent"])
                    trend_icon = {"Growing": "📈", "Stable": "➖", "Declining": "📉"}.get(trend["trend"], "❔")
                    extra_lines += f"{trend_icon} Publication Trend: {trend['trend']} (آخرین سال‌ها: {years_str})\n"
                # ── Research Distance block ──────────────────────────
                distance_key = verdict.get("distance", "MODERATE")
                distance_label = DISTANCE_LABELS.get(distance_key, distance_key)
                distance_icon = {"VERY_CLOSE": "🎯", "ADJACENT": "🧩",
                                  "MODERATE": "🔗", "FAR": "🚫"}.get(distance_key, "🔗")
                distance_block = (
                    f"{distance_icon} Research Distance: {distance_label}\n"
                    f"   Potential thesis overlap: {verdict.get('thesis_overlap','Medium')}\n\n"
                )

                # ── Fit Matrix block (۹ معیار + Final Recommendation) ──
                fit_matrix_block = format_fit_matrix_block(fit_matrix) + "\n"

                # ── Paper-to-CV Matching block (موضوع‌به‌موضوع) + جزئیات
                # Hybrid Search — طبق درخواست صریح کاربر، نشون می‌ده Research
                # Fit از کجا اومده (نه فقط یک عدد نهایی) ────────────────────
                paper_cv_block = ""
                pcm = verdict.get("paper_cv_match")
                if pcm and pcm.get("topics"):
                    paper_cv_block = format_paper_cv_match_block(pcm) + "\n"
                hybrid = verdict.get("hybrid_fit")
                if hybrid:
                    emb_display = f"{hybrid['embedding']}%" if hybrid.get("embedding") is not None else "N/A"
                    paper_cv_block += (
                        f"🧬 Hybrid Search: Keyword {hybrid['keyword']}% | Embedding {emb_display} | "
                        f"Paper-sim {hybrid['paper_similarity']}% | LLM {hybrid['llm']}%\n"
                    )

                # ── Advisor Fit — کمکی، هرگز فیلتر نیست (طبق درخواست کاربر) ──
                top_factors = "؛ ".join(advisor_fit["factors"][:3])
                advisor_block = (
                    f"🧭 Advisor Fit: {advisor_fit['score']}% "
                    f"(راهنما، نه فیلتر — تفاوتش با Research Fit)\n"
                    f"   {top_factors}\n"
                )

                try:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=(
                            f"{res_icon} تطابق با استاد:\n"
                            f"👨‍🏫 {prof.get('name','?')}\n\n"
                            f"{distance_block}"
                            f"{fit_matrix_block}\n"
                            f"{paper_cv_block}\n"
                            f"{advisor_block}\n"
                            f"{warn_line}"
                            f"{extra_lines}"
                            f"💬 {prof_reason}"
                        ))
                except Exception as _e:
                    logger.debug(f"prof match score display skipped: {_e}")


            if not (verdict["match"] and prof_score >= RELEVANCE_MIN_SCORE):
                skipped += 1
                funnel["no_match"] += 1
                await asyncio.to_thread(db_log_email, uid,
                    prof.get("name",""), prof.get("email",""), "",
                    f"skipped:no_match(src={verdict.get('source','?')},"
                    f"score={prof_score}):{verdict['reason'][:80]}")
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"],
                    f"no_match(score={prof_score}):{verdict['reason'][:100]}")
                # پیام جداگانه برای هر skip نمی‌فرستیم (سرعت مهم‌تره)؛ فقط
                # هر چند تا یکبار خلاصه می‌دیم که کاربر بی‌خبر نمونه.
                if skipped % 10 == 0:
                    await context.bot.send_message(chat_id=chat_id,
                        text=f"⏭ تا الان {skipped} استاد به‌خاطر عدم تطابق واقعی با رزومه‌تون رد شدن "
                             f"(برای جلوگیری از ایمیل بی‌ربط).")
                continue

            # ── Professor Verification ────────────────────────────
            # match مرتبط بودن رزومه رو تایید می‌کنه، ولی این جدا از اینه که
            # آیا خود استاد/آزمایشگاه واقعاً «تایید»شده و قابل‌اعتماده. اگه
            # ایمیل فقط از یه منبع ضعیف/عمومی (مثلاً یه نتیجه‌ی جستجوی
            # نامطمئن به‌جای صفحه‌ی رسمی دانشکده/آزمایشگاه) اومده باشه، و
            # هیچ سیگنال دیگه‌ای (فعالیت اخیر، آزمایشگاه، پذیرش دانشجو) هم
            # نداشته باشیم، یعنی داریم به یه ایمیل تقریباً بی‌پشتوانه ایمیل
            # می‌زنیم — این ریسک bounce/spam رو بالا می‌بره، بهتره ردش کنیم.
            verification = {
                "faculty_page_exists": prof.get("email_source") in HIGH_CONFIDENCE_EMAIL_SOURCES,
                "last_updated":  bool(prof.get("last_pub_year")),
                "lab":           bool(prof.get("lab_analysis")),
                "open_position": bool(prof.get("accepting_students")),
            }
            if not any(verification.values()):
                skipped += 1
                funnel["unverified_signals"] += 1
                await asyncio.to_thread(db_log_email, uid,
                    prof.get("name",""), prof.get("email",""), "",
                    f"skipped:unverified(email_source={prof.get('email_source','?')})")
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"],
                    f"unverified(email_source={prof.get('email_source','?')})")
                continue

            # ── match تایید شد — کاربر رو در جریان بذاریم چی داره ارسال می‌شه ──
            best_paper_title = papers[0]["title"] if papers else ""

            # رزومه را برای این استاد بهینه می‌کنیم — این کار رو هم‌زمان با
            # فرستادن پیام اطلاع‌رسانی انجام می‌دیم تا زمان اضافی نگیره.
            # اگه AI جواب نداد یا خطا داد، adapt_resume_for_target خودش
            # fallback رزومه‌ی خام رو برمی‌گردونه — loop هیچ‌وقت متوقف نمی‌شه.
            prof_with_pub = dict(prof)
            prof_with_pub["pub_list"] = papers  # لیست مقالات واقعی رو پاس می‌دیم
            if papers:
                prof["papers"] = papers  # تا _fallback_email هم (که مستقیم prof['papers'] رو می‌خونه) بهش دسترسی داشته باشه

            adapted_resume_task = asyncio.create_task(
                adapt_resume_for_target(client, prof_with_pub, "professor"))
            priority_badge = ""
            if prof.get("has_funding"):
                priority_badge = f"🔥 High Priority — سیگنال بودجه/گرنت پیدا شد: {prof.get('funding_evidence','')}\n"
            notify_task = asyncio.create_task(context.bot.send_message(
                chat_id=chat_id,
                text=f"🎯 تطابق پیدا شد: {prof.get('name','?')}\n"
                     f"{priority_badge}"
                     f"📄 پژوهش مرتبط: {best_paper_title[:90]}\n"
                     f"💡 دلیل: {verdict['reason'][:120]}\n"
                     f"✍️ در حال شخصی‌سازی رزومه و نوشتن ایمیل..."))

            # هر دو task رو منتظر می‌مونیم — notify معمولاً سریع‌تر تموم می‌شه
            adapted_resume, _ = await asyncio.gather(
                adapted_resume_task, notify_task, return_exceptions=True)
            if isinstance(adapted_resume, Exception):
                adapted_resume = ""  # fallback — نباید کرش کنه

            if approved_first and prof.get("email") == approved_first.get("professor_email"):
                subj = approved_first.get("subject") or ""
                body = approved_first.get("body") or ""
                if not subj or not body:
                    ab = papers[0]["abstract"] if papers else ""
                    subj, body, _review = await generate_and_review_email(client, prof, ab, adapted_resume, papers=papers)
            else:
                ab = papers[0]["abstract"] if papers else ""
                subj, body, _review = await generate_and_review_email(client, prof, ab, adapted_resume, papers=papers)

            ok, err = await _deliver_email(
                context.bot, chat_id, smtp_e, smtp_p, smtp_h, smtp_pt,
                prof["email"], subj, body,
                f"{client.get('first_name','Applicant')} via Manifest Apply")

            if ok:
                sent_ok += 1
                await asyncio.to_thread(db_log_email, uid,
                    prof.get("name",""), prof["email"], subj, "sent")
                await asyncio.to_thread(db_mark_job_result, job_id, "SENT", prof["email"])
                if sub_id:
                    used = await asyncio.to_thread(db_increment_sub_count, sub_id, "email") or sent_ok
                else:
                    used = sent_ok
                progress_label = "📋 آماده شد" if err == "manual" else "📧 ایمیل"
                next_line = (f"\n▶️ می‌رم سراغ استاد بعدی... ({total - (idx+1)} استاد دیگه مونده)"
                             if idx + 1 < total else "")
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"{progress_label} {sent_ok}/{total}\n"
                         f"👨‍🏫 {prof.get('name','?')}\n"
                         f"📮 {prof['email']}\n"
                         f"📌 {subj[:60]}\n"
                         f"📊 مصرف کل: {used}/{EMAIL_QUOTA}"
                         f"{next_line}")
                if sent_ok % 50 == 0:
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"📊 پیشرفت: {sent_ok}/{total} ({int(sent_ok/total*100)}%)\n❌ ناموفق: {sent_fail}")
            else:
                sent_fail += 1
                funnel["send_failed"] += 1
                await asyncio.to_thread(db_log_email, uid,
                    prof.get("name",""), prof["email"], "", f"failed:{err}")
                await asyncio.to_thread(db_mark_job_result, job_id, "FAILED", prof["email"], err or "unknown_error")
                if sent_fail > 0 and sent_fail % 20 == 0:
                    await context.bot.send_message(chat_id=chat_id,
                        text=f"⚠️ {sent_fail} ایمیل ناموفق.\nتیم @manifestapply مطلع شد.")
                    await notify_admin(context.bot,
                        f"⚠️ خطا در ارسال ایمیل\nکاربر: {uid}\nارسال: {sent_ok}\nناموفق: {sent_fail}\nخطا: {err}")

            await asyncio.sleep(SEND_DELAY_SEC)
        except asyncio.CancelledError:
            # اگه بات داره خاموش/ری‌استارت می‌شه، این job رو به PENDING
            # برگردون (نه SENDING بمونه برای همیشه) تا دفعه‌ی بعد که این
            # batch دوباره اجرا بشه، خودش دوباره امتحان بشه.
            if job_id:
                try:
                    await asyncio.to_thread(db_mark_job_result, job_id, "FAILED", prof.get("email",""), "cancelled_mid_run")
                except Exception:
                    pass
            break
        except Exception as e:
            logger.error(f"email loop: {e}")
            if job_id:
                try:
                    await asyncio.to_thread(db_mark_job_result, job_id, "FAILED", prof.get("email",""), str(e)[:200])
                except Exception:
                    pass
            await asyncio.sleep(5)

    # ── لاگ funnel «مرحله‌ی تحلیل» — یک ردیف در analysis_funnel_metrics
    # + یک خط لاگ خوانا، دقیقاً به شکلی که معلوم باشه بیشترین ریزش سر
    # کدوم gate بوده، نه فقط این‌که عدد نهایی کم بود. ──
    field_label = (client or {}).get("field", "") or ""
    try:
        await asyncio.to_thread(db_log_analysis_funnel, uid, sub_id, field_label, total, funnel, sent_ok)
    except Exception as e:
        logger.debug(f"funnel logging skipped: {e}")
    funnel_lines = "\n".join(
        f"  → {label}: {funnel[key]}"
        for key, label in [
            ("duplicate", "قبلاً ایمیل شده (duplicate)"),
            ("batch_time_budget", "سقف زمانی batch"),
            ("no_email_found", "ایمیل پیدا نشد"),
            ("invalid_name", "اسم نامعتبر/ناقص"),
            ("invalid_email", "ایمیل نامعتبر (MX/format)"),
            ("unverified_faculty", "تأیید نشد (faculty verification)"),
            ("no_match", "عدم تطابق پژوهشی (AI judge/prescore)"),
            ("unverified_signals", "بدون سیگنال پشتیبان (verification دوم)"),
            ("send_failed", "ارسال SMTP ناموفق"),
        ] if funnel.get(key)
    )
    logger.info(
        f"📊 Analysis funnel | field='{field_label[:60]}' | {total} استاد ورودی → {sent_ok} ارسال موفق\n"
        + (funnel_lines or "  (هیچ gate ای رد نکرد)")
    )

    # ── گزارش نهایی تفصیلی (برای جلوگیری از هر اعتراض بعدی) ─────────
    recent = await asyncio.to_thread(db_get_recent_emails, uid, sent_ok)
    prof_lines = "\n".join(f"• {r['name']} — {r['subject'][:60]}" for r in recent[:50])
    more_note = f"\n... و {len(recent)-50} استاد دیگر" if len(recent) > 50 else ""
    final_report = (
        f"📋 گزارش نهایی ارسال ایمیل — کاربر {uid}\n"
        f"✅ موفق: {sent_ok} | ❌ ناموفق: {sent_fail} | ⏭ رد‌شده (عدم تطابق): {skipped}\n"
        f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"📊 Funnel تحلیل (کجا رد شدن):\n{funnel_lines or '  (هیچ gate ای رد نکرد)'}\n\n"
        f"لیست اساتید:\n{prof_lines}{more_note}"
    )
    await notify_admin(context.bot, final_report)

    is_done = sub_id and used >= EMAIL_QUOTA
    if is_done:
        # فقط وقتی واقعاً به quota ۵۰۰ رسیده باشیم subscription رو ببند —
        # قبلاً این‌جا فارغ از مقدار used همیشه صدا زده می‌شد، یعنی هر batch
        # (حتی وقتی فقط چند استاد پیدا/تأیید شده بودن) کل subscription رو
        # completed می‌کرد و مانیتور دوره‌ای رو غیرفعال می‌کرد — یعنی کاربر
        # قبل از رسیدن به ۵۰۰ ایمیل عملاً از سرویس خارج می‌شد و هیچ‌وقت
        # مانیتور (که قرار بود دوره‌ای اساتید جدید پیدا کنه) فرصت اجرا پیدا
        # نمی‌کرد. الان فقط با رسیدن واقعی به quota بسته می‌شه؛ در غیر این
        # صورت subscription/مانیتور فعال می‌مونه تا batchهای بعدی/دوره‌ای
        # ادامه بدن تا used واقعاً به EMAIL_QUOTA برسه.
        await asyncio.to_thread(db_mark_sub_completed, sub_id)

    ud_snap[S] = "closed_feedback"
    # نکته: وقتی این تابع از مسیر auto-timeout (۱۵ دقیقه) صدا زده می‌شه،
    # ud_snap ممکنه یک snapshot جدا از bot_data باشه نه دیکشنری زنده‌ی
    # کاربر — پس مستقیم هم روی دیکشنری زنده‌اش state رو ست می‌کنیم تا
    # پیام بعدی‌اش (فیدبک/گزارش خطا) درست مسیریابی بشه.
    live_ud = context.application.user_data.get(uid)
    if live_ud is not None:
        live_ud[S] = "closed_feedback"

    # دکمه‌های رضایت‌سنجی inline (نه Reply keyboard) — چون بعد از اینجا
    # MAIN_KB نمایش داده می‌شه و می‌خوایم رأی رو جدا از ادامه‌ی مکالمه
    # ثبت کنیم، بدون اینکه کاربر مجبور باشه چیزی تایپ کنه.
    sat_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("👍 راضی بودم", callback_data=f"sat_like_email_{uid}_{sub_id or 0}"),
        InlineKeyboardButton("👎 ناراضی بودم", callback_data=f"sat_dislike_email_{uid}_{sub_id or 0}"),
    ]])
    await context.bot.send_message(
        chat_id=chat_id,
        text=f"🎉 ارسال ایمیل‌ها کامل شد!\n\n"
             f"✅ ارسال‌شده: {sent_ok}\n"
             f"❌ ناموفق: {sent_fail}\n"
             f"⏭ رد‌شده (بدون تطابق واقعی با رزومه‌تون، برای جلوگیری از ایمیل بی‌ربط): {skipped}\n"
             f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
             f"💡 ایمیل خود را در روزهای آینده چک کنید. اگر استادی پاسخ داد، سریع جواب دهید.\n\n"
             f"🙏 ممنونیم از همکاری‌تان. امیدواریم موفقیت تحصیلی خوبی داشته باشید!\n\n"
             f"اگر نقد یا اعتراضی دارید همینجا بنویسید — مستقیم برای تیم ارسال می‌شود.\n"
             f"اگر ایمیلی اشتباه رفت اسم استاد را بگید تا برایش پیام اصلاحی بفرستیم.\n\n"
             f"📩 مشاوره بیشتر: @manifestapply\n\n"
             f"از سرویس ما راضی بودید؟",
        reply_markup=sat_kb)
    await notify_admin(context.bot,
        f"✅ ارسال ایمیل تمام شد\nکاربر: {uid}\n{sent_ok}/{total} (رد‌شده: {skipped})"
        + ("\n🔒 quota ۵۰۰ تکمیل شد — دسترسی بسته شد." if is_done else ""))

# ================================================================
# BACKGROUND — Time-Budget Rescue Job
# ================================================================
# استادهایی که توی _send_emails_bg اصلی به‌خاطر سقف MAX_SCAN_SECONDS رد
# شدن (status='SKIPPED', last_error='batch_time_budget_exceeded') — یعنی
# نه چون منبعی نداشتیم، فقط چون وقت کل batch تموم شد و اصلاً pipeline
# ۱۱مرحله‌ای براشون اجرا نشد. این job جدا و پس‌زمینه (نه توی مسیر ارسال
# اصلی، پس هیچ ارسالی رو کند نمی‌کنه) دوره‌ای این استادها رو برمی‌داره،
# همون pipeline رو بدون فشار زمانی batch اصلی دوباره روشون اجرا می‌کنه،
# و اگه ایمیل پیدا شد واقعاً می‌فرسته.
#
# چرا تابع جدا به‌جای صدا زدن مستقیم _send_emails_bg: اون تابع برای یک
# batch با ترتیب پشت‌سرهم (idx پیوسته، یک chat_id واحد) طراحی شده و
# مستقیم روی db_get_job(tid, sid, batch_idx) کار می‌کنه؛ این‌جا داریم
# جاب‌های پراکنده از batchها/کاربرهای مختلف رو یکی‌یکی rescue می‌کنیم،
# پس منطق پردازش تک‌استاد (همون بررسی‌های verified/valid/relevance که
# توی _send_emails_bg هست) این‌جا هم دقیقاً تکرار می‌شه — منتها بدون
# پیام‌های heartbeat/پیشرفت مخصوص یک batch بزرگ.
RESCUE_BATCH_LIMIT = 300  # هر بار اجرای job حداکثر همین‌قدر جاب رو rescue می‌کنه

async def _rescue_one_skipped_professor(context, job_id, tid, sub_id, prof, client,
                                         smtp_e, smtp_p, smtp_h, smtp_pt, already_sent) -> str:
    """یک استاد رد‌شده به‌خاطر سقف زمانی رو دوباره امتحان می‌کنه.
    برمی‌گردونه: 'sent' / 'skipped' / 'failed' — فقط برای شمارش نهایی."""
    try:
        # اگه بین این مدت quota کاربر تکمیل شده (مثلاً از یه batch دیگه)،
        # دیگه لازم نیست حتی pipeline ایمیل‌یابی رو اجرا کنیم.
        if sub_id:
            sub = await asyncio.to_thread(db_get_sub, sub_id)
            if sub and sub.get("emails_sent", 0) >= EMAIL_QUOTA:
                await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", "", "quota_reached")
                return "skipped"

        verdict, papers = await get_professor_relevance(client, prof)
        prof_score  = verdict.get("score", 0)
        prof_reason = verdict.get("reason", "")

        if not prof.get("email"):
            await _notify_no_email_found(context, tid, prof, verdict)
            await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", "", "no_email_found_on_rescue")
            return "skipped"

        prof_name_clean = (prof.get("name") or "").strip()
        if not prof_name_clean or len(prof_name_clean.split()) < 2:
            await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED",
                prof.get("email", ""), "missing_or_invalid_name")
            return "skipped"

        if prof["email"] in already_sent:
            await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"], "duplicate")
            return "skipped"

        valid, valid_reason = await asyncio.to_thread(validate_email_deliverability, prof["email"])
        if not valid:
            await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"],
                f"invalid_email:{valid_reason[:100]}")
            return "skipped"

        domain_info = classify_email_domain(prof["email"])
        if domain_info["category"] == "personal":
            prof_score = max(0, prof_score - 20)
            verdict["score"] = prof_score

        verified, verify_reason = _professor_verification_passed(prof)
        if not verified:
            await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED",
                prof["email"], f"unverified_faculty:{verify_reason[:100]}")
            return "skipped"

        if not (verdict["match"] and prof_score >= RELEVANCE_MIN_SCORE):
            await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"],
                f"no_match(score={prof_score}):{prof_reason[:100]}")
            return "skipped"

        verification = {
            "faculty_page_exists": prof.get("email_source") in HIGH_CONFIDENCE_EMAIL_SOURCES,
            "last_updated":  bool(prof.get("last_pub_year")),
            "lab":           bool(prof.get("lab_analysis")),
            "open_position": bool(prof.get("accepting_students")),
        }
        if not any(verification.values()):
            await asyncio.to_thread(db_mark_job_result, job_id, "SKIPPED", prof["email"],
                f"unverified(email_source={prof.get('email_source','?')})")
            return "skipped"

        prof_with_pub = dict(prof)
        prof_with_pub["pub_list"] = papers
        if papers:
            prof["papers"] = papers
        adapted_resume = await adapt_resume_for_target(client, prof_with_pub, "professor")
        ab = papers[0]["abstract"] if papers else ""
        subj, body, _review = await generate_and_review_email(client, prof, ab, adapted_resume, papers=papers)

        # قبل از خود SMTP send، وضعیت رو از SKIPPED به SENDING می‌بریم — دقیقاً
        # همون دلیل حلقه‌ی اصلی ارسال (بالاتر): اگه درست بین موفقیت SMTP و ثبت
        # نتیجه crash کنیم، db_get_time_budget_skipped_jobs (که فقط status='SKIPPED'
        # رو برمی‌گردونه) این job رو بی‌سروصدا دوباره rescue می‌کرد و دوباره
        # می‌فرستاد. با این خط، بدترین حالت اینه که بعد از استارتاپ بعدی به
        # UNKNOWN تبدیل بشه (نه دوباره‌ارسال خودکار) — همون رفتار امن حلقه‌ی اصلی.
        await asyncio.to_thread(db_mark_job_sending, job_id)

        ok, err = await _deliver_email(
            context.bot, tid, smtp_e, smtp_p, smtp_h, smtp_pt,
            prof["email"], subj, body,
            f"{client.get('first_name','Applicant')} via Manifest Apply")

        if ok:
            await asyncio.to_thread(db_log_email, tid, prof.get("name",""), prof["email"], subj, "sent")
            await asyncio.to_thread(db_mark_job_result, job_id, "SENT", prof["email"])
            if sub_id:
                await asyncio.to_thread(db_increment_sub_count, sub_id, "email")
            already_sent.add(prof["email"])
            try:
                await context.bot.send_message(
                    chat_id=tid,
                    text=f"📧 یک ایمیل دیگه فرستاده شد (از لیست استادهایی که وقت "
                         f"کافی توی دور اول نرسید بهشون):\n"
                         f"👨‍🏫 {prof.get('name','?')}\n📮 {prof['email']}\n📌 {subj[:60]}")
            except Exception:
                pass
            return "sent"
        else:
            await asyncio.to_thread(db_log_email, tid, prof.get("name",""), prof["email"], "", f"failed:{err}")
            await asyncio.to_thread(db_mark_job_result, job_id, "FAILED", prof["email"], err or "unknown_error")
            return "failed"

    except Exception as e:
        logger.error(f"_rescue_one_skipped_professor job_id={job_id}: {e}")
        try:
            await asyncio.to_thread(db_mark_job_result, job_id, "FAILED", prof.get("email",""), str(e)[:200])
        except Exception:
            pass
        return "failed"

async def _rescue_time_budget_skipped_bg(context: ContextTypes.DEFAULT_TYPE):
    """هر چند ساعت یک‌بار صدا زده می‌شه (پایین‌تر در job_queue ثبت شده).
    SMTP creds و client (پروفایل کاربر) دقیقاً مثل _check_bounces_daily از
    context.application.user_data خونده می‌شه — همون session در-حافظه‌ای
    که SQLiteUserDataPersistence نگه می‌داره و بعد از ری‌استارت هم دوباره
    از دیسک لود می‌شه، پس نیازی به ذخیره‌ی جداگانه‌ی SMTP creds نیست.
    اگه یک کاربر دیگه session فعالی نداشته باشه (مثلاً به‌طور کامل از
    ربات خارج شده)، جاب‌هاش این دور rescue نمی‌شن و برای اجرای بعدی
    می‌مونن — هیچ‌وقت crash نمی‌کنه یا جاب رو گم نمی‌کنه."""
    try:
        jobs = await asyncio.to_thread(db_get_time_budget_skipped_jobs, RESCUE_BATCH_LIMIT)
    except Exception as e:
        logger.error(f"db_get_time_budget_skipped_jobs: {e}")
        return
    if not jobs:
        return

    logger.info(f"⏳ Time-Budget Rescue: {len(jobs)} استاد رد‌شده به‌خاطر سقف زمانی برای بررسی مجدد")

    sent = failed = skipped = 0
    already_sent_cache: dict[int, set] = {}

    for j in jobs:
        tid, sub_id, prof, job_id = j["telegram_id"], j["sub_id"], j["prof"], j["job_id"]
        ud = context.application.user_data.get(tid)
        if not ud:
            # session این کاربر الان در دسترس نیست — بی‌خیال این جاب برای
            # این دور، دور بعدی دوباره امتحان می‌شه (وضعیتش هنوز SKIPPED
            # مونده، هیچی گم نشده).
            continue
        smtp_e  = ud.get(S_SMTP_E, "")
        smtp_p  = ud.get(S_SMTP_P, "")
        smtp_h  = ud.get(S_SMTP_H, "smtp.gmail.com")
        smtp_pt = ud.get(S_SMTP_PORT, 587)
        client  = ud.get(S_CLIENT, {})
        if not smtp_e or not client:
            continue

        if tid not in already_sent_cache:
            already_sent_cache[tid] = await asyncio.to_thread(db_get_sent_email_set, tid)

        result = await _rescue_one_skipped_professor(
            context, job_id, tid, sub_id, prof, client,
            smtp_e, smtp_p, smtp_h, smtp_pt, already_sent_cache[tid])
        if result == "sent":
            sent += 1
        elif result == "failed":
            failed += 1
        else:
            skipped += 1
        await asyncio.sleep(SEND_DELAY_SEC)

    if sent or failed:
        await notify_admin(context.bot,
            f"⏳ Time-Budget Rescue Job تمام شد\n"
            f"✅ ارسال‌شده: {sent} | ❌ ناموفق: {failed} | ⏭ رد‌شده: {skipped}\n"
            f"از {len(jobs)} استادی که به‌خاطر سقف زمانی batch اصلی رد شده بودن.")

# ================================================================
# BACKGROUND — اپلای کار
# ================================================================

async def _send_jobs_bg(context, chat_id, uid, jobs, client, sub_id=None,
                         smtp_e=None, smtp_p=None, smtp_h=None, smtp_pt=587):
    applied = failed = skipped = 0
    # ── تفکیک صریح «اپلای واقعی» از «فرصت/لینک پورتال» ──────────────
    # قبلاً applied یک شمارنده‌ی واحد بود که هم ایمیل واقعاً ارسال‌شده
    # (method='email', status='sent') و هم صرفاً لینک پورتالی که خود
    # کاربر باید دستی پر کنه (method='portal_link', status='link_provided')
    # رو یکسان می‌شمرد — یعنی گزارش نهایی و پیام‌های پیشرفت می‌گفتن
    # «اپلای X» درحالی‌که ممکن بود X تا از X تا فقط لینک باشن، نه اپلای
    # واقعی. applied/used همچنان کل quota (شامل هر دو) رو نشون می‌ده —
    # چون تعریف محصول «۲۰۰ فرصت شغلی واجد شرایط + اپلای ایمیلی خودکار
    # در صورت امکان» است، نه «۲۰۰ اپلای واقعی» — ولی گزارش نهایی و آمار
    # پایین همیشه این دو رو جدا نشون می‌دن تا هیچ‌جا ادعای اپلای‌شدن برای
    # چیزی که کاربر خودش باید دستی اپلای کنه مطرح نشه.
    real_applied_email = 0
    portal_opportunities = 0
    used = 0

    # ── امتیاز ATS رزومه (اگه قبلاً اسکن شده) — یک‌بار همین‌جا می‌خونیم، نه
    # داخل حلقه‌ی هر شغل، چون برای کل این batch ثابته و صرفاً یک مقدار
    # profile-level هست، نه چیزی که به‌ازای هر آگهی فرق کنه.
    try:
        _ats_row = await asyncio.to_thread(db_get_resume_ats_scan, uid)
        ats_score = _ats_row["score"] if _ats_row else None
    except Exception as e:
        logger.warning(f"db_get_resume_ats_scan (job batch): {e}")
        ats_score = None

    # ── مانیتورینگ مداوم ─────────────────────────────────────────
    # این تابع تنها نقطه‌ی مشترکیه که هر سه شاخه‌ی جستجوی موفق شغل
    # (جستجوی اولیه، «دوباره با همین تنظیمات»، تغییر کشور) بهش می‌رسن —
    # پس همینجا یک‌بار مانیتور رو می‌سازیم/تازه می‌کنیم. تا وقتی این
    # subscription فعاله، از این به بعد هر ۲۴ ساعت خودکار دوباره جستجو
    # می‌شه و فقط فرصت‌های *جدید* (که قبلاً برای همین کاربر اپلای نشده)
    # گزارش می‌شن.
    try:
        await asyncio.to_thread(
            db_create_or_refresh_monitor, uid, sub_id, "job",
            client.get("field", ""), client.get("countries", ""))
    except Exception as e:
        logger.error(f"db_create_or_refresh_monitor (job): {e}")

    # اگر کاربر مسیر «ایمیل مستقیم به HR» را انتخاب کرده باشد، smtp_e/smtp_p
    # از قبل تست‌شده پاس داده می‌شوند و برای آگهی‌هایی که ایمیل مستقیم دارند
    # واقعاً استفاده می‌شوند. اگر کاربر مسیر «لینک پورتال» را انتخاب کرده
    # باشد، این مقادیر None می‌مانند و همه‌چیز طبیعتاً به لینک پورتال می‌رود.

    # ── بارگذاری فیلترهای کاربر (blacklist / remote / visa / age) ──
    raw_filters = client.get("job_filters", "")
    user_filters = _parse_job_filters(raw_filters) if raw_filters else {}

    # ── AI Memory: مشاغل/شرکت‌هایی که کاربر قبلاً (این بار یا بار قبل)
    # گفته دوست ندارد، خودکار به فیلترها اضافه می‌شود — کاربر مجبور نیست
    # هر بار دوباره بگوید، بات خودش تصمیم می‌گیرد و رد می‌کند.
    try:
        disliked_raw = await asyncio.to_thread(db_get_disliked_jobs, uid)
        disliked_kw = [x for x in disliked_raw.split("|") if x]
        if disliked_kw:
            user_filters.setdefault("disliked_keywords", [])
            user_filters["disliked_keywords"].extend(disliked_kw)
    except Exception as e:
        logger.error(f"[uid={uid}] loading disliked_jobs memory failed: {e}")

    # ── Application Brain: یادگرفته‌های ساختاریافته از رد کردن‌های قبلی
    # کاربر (work_mode/seniority/company_stage/sponsorship) — خودکار به
    # فیلترهای همین جستجو اضافه می‌شود، بدون این‌که کاربر دوباره چیزی
    # بگوید.
    try:
        learned = await asyncio.to_thread(db_get_learned_filter_summary, uid)
        if learned:
            user_filters = _apply_learned_filters(user_filters, learned)
    except Exception as e:
        logger.error(f"[uid={uid}] applying learned filters (Application Brain) failed: {e}")

    # ── Job Fingerprint: همون شغل که روی چند منبع دیده شده (LinkedIn/
    # سایت شرکت/Indeed/برد دیگر) رو زیر یک آگهی ادغام می‌کنه و لیست
    # منابع رو نگه می‌داره («یافت‌شده در N منبع») — قبل از dedup شرکتی،
    # چون اون یکی هدفش محدود کردن به یک آگهی در هر شرکته، نه ادغام
    # کپی‌های همون آگهی.
    jobs = merge_jobs_by_fingerprint(jobs)

    # ── dedup: فقط بهترین آگهی هر شرکت ────────────────────────────
    jobs = deduplicate_by_company(jobs, max_per_company=1)
    logger.info(f"[uid={uid}] after fingerprint-merge + dedup: {len(jobs)} jobs remain")

    already_applied_urls, already_applied_pairs = await asyncio.to_thread(db_get_applied_job_keys, uid)
    logger.info(f"[uid={uid}] idempotency: {len(already_applied_urls)+len(already_applied_pairs)} jobs already applied, duplicates will be skipped")

    filtered_old = filtered_bl = 0

    for idx, job in enumerate(jobs):
        try:
            title   = job.get("title","")
            company = job.get("company","")
            url     = job.get("url","")
            email   = job.get("email","")

            # ── تازگی/منبع همین‌جا (یک‌بار برای کل این آیتم) محاسبه می‌شه —
            # چه این آگهی نهایتاً match بشه چه skip بشه، «کِی خودمون این
            # منبع رو چک کردیم» باید همون لحظه‌ی واقعی پردازش باشه، نه یک
            # timestamp جدا که فقط برای آگهی‌های موفق ساخته می‌شه.
            freshness  = _job_freshness(job)
            age_badge  = freshness["badge"]
            checked_at = freshness["checked_at"]

            # ── Job Fingerprint metadata (از merge_jobs_by_fingerprint بالای
            # تابع) — یک‌بار اینجا استخراج می‌شه تا هر سه db_log_job call
            # پایین‌تر (skip/no_match، skip/no_email_or_url، موفق) همینا رو
            # واقعاً در دیتابیس ذخیره کنن، نه این‌که فقط در حافظه بمونن و
            # migration ۱۱ (fingerprint/sources_json) همیشه خالی بمونه.
            job_fp = job.get("fingerprint", "")
            job_sources_json = json.dumps(job.get("sources", [])) if job.get("sources") else ""

            if (url and url in already_applied_urls) or (title, company) in already_applied_pairs:
                continue

            # ── فیلتر آگهی قدیمی + blacklist + remote + visa ──────────
            passes, filter_reason = _passes_job_filters(job, user_filters)
            if not passes:
                if "قدیمی" in filter_reason:
                    filtered_old += 1
                else:
                    filtered_bl += 1
                logger.debug(f"job filtered: {title} — {filter_reason}")
                continue

            # ── مچینگ واقعی رزومه با این آگهی — قبلاً این چک اصلاً وجود
            # نداشت و بات هر آگهی‌ای که جستجوی کلیدواژه‌ای پیدا می‌کرد رو
            # مستقیم اپلای می‌کرد، حتی اگه با رزومه‌ی کاربر هیچ ربطی نداشت.
            verdict = await judge_job_relevance(client, job, user_filters)
            if not (verdict["match"] and verdict["score"] >= JOB_RELEVANCE_MIN_SCORE):
                skipped += 1
                await asyncio.to_thread(db_log_job, uid, title, company, "skipped",
                    f"skipped:no_match(score={verdict['score']}):{verdict['reason'][:80]}", url,
                    checked_at, job_fp, job_sources_json)
                if skipped % 10 == 0:
                    await context.bot.send_message(chat_id=chat_id,
                        text=f"⏭ تا الان {skipped} فرصت شغلی به‌خاطر عدم تطابق واقعی با رزومه‌تون رد شدن "
                             f"(برای جلوگیری از اپلای بی‌ربط).")
                await asyncio.sleep(1.0)
                continue

            # ── Smart Apply Ranking: به کاربر می‌گیم این آگهی چرا انتخاب شد ──
            score_bar = "🟢" if verdict["score"] >= 80 else "🟡" if verdict["score"] >= 65 else "🟠"
            # age_badge/checked_at از بالای حلقه (محاسبه‌ی مشترک برای match/skip) موجودن

            # ── Job Fingerprint: اگه merge_jobs_by_fingerprint این آگهی رو از
            # چند منبع مستقل (مثلاً هم LinkedIn هم سایت خودِ شرکت) دیده،
            # به کاربر می‌گیم — سیگنال اعتماد قوی‌تریه، نه صرفاً یک منبع تنها.
            sources_line = ""
            if job.get("source_count", 1) > 1:
                sources_line = f"🔗 {format_job_sources_label(job)}\n"

            # ── احتمال قبولی تخمینی برای همین آگهی خاص (تطابق شغلی + سلامت
            # ATS رزومه). اگه هنوز اسکن ATS انجام نشده (ats_score=None)، فقط
            # روی تطابق شغلی حساب می‌شه — هیچ‌وقت اسکن ATS رو اجباری‌تر از
            # چیزی که واقعاً هست نشون نمی‌دیم.
            accept = job_acceptance_probability(verdict["score"], ats_score)
            accept_line = f"{accept['emoji']} شانس قبولی تخمینی: {accept['probability']}% ({accept['label']})"
            if ats_score is None:
                accept_line += "\n   ⓘ برای تخمین دقیق‌تر، رزومه‌تون رو با «🔎 اسکن ATS رزومه» بررسی کنید."

            # بهینه‌سازی رزومه برای این شغل — هم‌زمان با نوشتن پیام اطلاع‌رسانی
            adapted_resume_task = asyncio.create_task(
                adapt_resume_for_target(client, job, "job"))
            notify_task = asyncio.create_task(context.bot.send_message(
                chat_id=chat_id,
                text=f"🎯 تطابق پیدا شد:\n"
                     f"💼 {title}\n"
                     f"🏢 {company} | 📍 {job.get('location','')}\n"
                     f"📅 {age_badge}\n"
                     f"🔍 Source checked: {checked_at}\n"
                     f"{sources_line}"
                     f"{score_bar} امتیاز کلی تطابق: {verdict['score']}%\n"
                     f"   • مهارت‌ها: {verdict['skills_match']}%\n"
                     f"   • سابقه کار: {verdict['experience_match']}%\n"
                     f"   • ویزا/مجوز کار: {verdict['visa_match']}%\n"
                     f"   • موقعیت مکانی: {verdict['location_match']}%\n"
                     f"{accept_line}\n"
                     f"💡 {verdict['reason'][:120]}\n"
                     f"✍️ در حال شخصی‌سازی رزومه و اپلای..."))

            adapted_resume, _ = await asyncio.gather(
                adapted_resume_task, notify_task, return_exceptions=True)
            if isinstance(adapted_resume, Exception):
                adapted_resume = ""

            if email and smtp_e:
                # اپلای ایمیلی
                subj, body = await write_cover_letter(client, job, adapted_resume)
                ok, err = await _deliver_email(
                    context.bot, chat_id, smtp_e, smtp_p, smtp_h, smtp_pt,
                    email, subj, body)
                method = "email"
                status = "sent" if ok else f"failed:{err}"
            elif url:
                # لینک پورتال — کاربر باید خودش apply کند
                method = "portal_link"
                status = "link_provided"
                ok = True
            else:
                # نه ایمیل معتبر داریم نه لینک آگهی — قبلاً این حالت هم به
                # شاخه‌ی «لینک پورتال» می‌رفت و ok=True می‌شد، یعنی به کاربر
                # پیام «✅ فرصت پیدا شد» با یک لینک خالی/معتبر نبود نشون داده
                # می‌شد. الان به‌جای این ادعای دروغ، این آگهی رد می‌شه — کاربر
                # هرگز پیامی نمی‌گیره که نه ایمیل واقعی داره نه لینک واقعی.
                skipped += 1
                await asyncio.to_thread(db_log_job, uid, title, company, "skipped",
                    "skipped:no_email_or_url", url, checked_at, job_fp, job_sources_json)
                continue

            if ok:
                applied += 1
                if method == "email":
                    real_applied_email += 1
                else:
                    portal_opportunities += 1
                await asyncio.to_thread(db_log_job, uid, title, company, method, status, url,
                    checked_at, job_fp, job_sources_json)
                if sub_id:
                    used = await asyncio.to_thread(db_increment_sub_count, sub_id, "job") or applied
                else:
                    used = applied
                if method == "email":
                    next_line = (f"\n▶️ می‌رم سراغ فرصت بعدی... ({len(jobs) - (idx+1)} فرصت دیگه مونده)"
                                 if idx + 1 < len(jobs) else "")
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"✅ اپلای ایمیلی {real_applied_email}: {title} — {company}\n📮 ایمیل ارسال شد\n"
                             f"📊 مصرف کل (فرصت‌ها): {used}/{JOB_QUOTA}"
                             f"{next_line}")
                else:
                    next_line = (f"\n▶️ می‌رم سراغ فرصت بعدی... ({len(jobs) - (idx+1)} فرصت دیگه مونده)"
                                 if idx + 1 < len(jobs) else "")
                    await context.bot.send_message(
                        chat_id=chat_id,
                        text=f"🔗 فرصت {portal_opportunities}: {title} — {company}\n"
                             f"📍 {job.get('location','')}\n"
                             f"🌐 {url[:60]}\n"
                             f"منبع: {job.get('source','')}\n"
                             f"⚠️ این یک لینک پورتاله — خودتون باید فرم رو دستی پر و ارسال کنید، هنوز اپلای نشده.\n"
                             f"📊 مصرف کل (فرصت‌ها): {used}/{JOB_QUOTA}"
                             f"{next_line}")
            else:
                failed += 1

            if applied % 25 == 0 and applied > 0:
                await context.bot.send_message(
                    chat_id=chat_id,
                    text=f"📊 پیشرفت: {applied}/{len(jobs)} فرصت "
                         f"(✅ اپلای ایمیلی: {real_applied_email} | 🔗 لینک پورتال: {portal_opportunities})")

            await asyncio.sleep(3)
        except Exception as e:
            logger.error(f"job loop: {e}")
            await asyncio.sleep(5)

    recent = await asyncio.to_thread(db_get_recent_jobs, uid, applied)
    job_lines = "\n".join(f"• {r['title']} — {r['company']}" for r in recent[:50])
    more_note = f"\n... و {len(recent)-50} فرصت دیگر" if len(recent) > 50 else ""
    await notify_admin(context.bot,
        f"📋 گزارش نهایی اپلای کار — کاربر {uid}\n"
        f"✅ اپلای ایمیلی واقعی: {real_applied_email} | 🔗 فرصت/لینک پورتال (کاربر باید دستی اپلای کنه): {portal_opportunities}\n"
        f"❌ ناموفق: {failed} | ⏭ رد‌شده (عدم تطابق): {skipped}\n"
        f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        f"لیست فرصت‌ها:\n{job_lines}{more_note}")

    is_done = sub_id and used >= JOB_QUOTA
    if is_done:
        # همون فیکس مسیر ایمیل استادها: قبلاً این‌جا هیچ چک quota ای نبود —
        # هر batch (حتی وقتی فقط چند فرصت شغلی پیدا شده بود، نه ۲۰۰ تا)
        # بی‌قید‌وشرط subscription رو completed می‌کرد و مانیتور دوره‌ای رو
        # خاموش می‌کرد. الان فقط وقتی used واقعاً به JOB_QUOTA برسه بسته
        # می‌شه؛ در غیر این صورت مانیتور فعال می‌مونه تا دوره‌ای دنبال
        # فرصت‌های جدید بگرده تا quota واقعی تکمیل بشه.
        await asyncio.to_thread(db_mark_sub_completed, sub_id)

    live_ud = context.application.user_data.get(uid)
    if live_ud is not None:
        live_ud[S] = "closed_feedback"

    filter_note = ""
    if filtered_old > 0 or filtered_bl > 0:
        filter_note = (
            f"🔍 فیلترهای اعمال‌شده:\n"
            f"   ⏭ آگهی قدیمی (بیش از {JOB_MAX_AGE_DAYS} روز): {filtered_old}\n"
            f"   ⛔ Blacklist/Remote/Visa فیلتر: {filtered_bl}\n\n"
        )

    sat_kb_job = InlineKeyboardMarkup([[
        InlineKeyboardButton("👍 راضی بودم", callback_data=f"sat_like_job_{uid}_{sub_id or 0}"),
        InlineKeyboardButton("👎 ناراضی بودم", callback_data=f"sat_dislike_job_{uid}_{sub_id or 0}"),
    ]])
    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            f"🎉 اپلای کار کامل شد!\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"✅ اپلای ایمیلی خودکار: {real_applied_email}\n"
            f"🔗 فرصت شغلی (لینک پورتال — نیاز به اپلای دستی خودتون): {portal_opportunities}\n"
            f"❌ ناموفق: {failed}\n"
            f"⏭ رد‌شده (عدم تطابق رزومه): {skipped}\n"
            f"{filter_note}"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"⚠️ توجه: فرصت‌های «لینک پورتال» یعنی خود شرکت فقط از طریق سایت خودش اپلای قبول می‌کنه "
            f"و ما نمی‌تونیم به‌جای شما فرم‌شون رو پر کنیم — این‌ها هنوز اپلای نشدن، فقط پیدا و رزومه‌تون "
            f"براشون بهینه شده. لطفاً خودتون از روی لینکی که فرستادیم اپلای کنید.\n\n"
            f"💡 LinkedIn و email خود را چک کنید.\n"
            f"پروفایل LinkedIn را آپدیت نگه دارید.\n\n"
            f"🙏 ممنونیم از همکاری‌تان. امیدواریم موفقیت شغلی خوبی داشته باشید!\n\n"
            f"اگر نقد یا اعتراضی دارید همینجا بنویسید — مستقیم برای تیم ارسال می‌شود.\n\n"
            f"📩 مشاوره بیشتر: @manifestapply\n\n"
            f"از سرویس ما راضی بودید؟"
        ),
        reply_markup=sat_kb_job)

# ================================================================
# GOOGLE OAUTH — سرور کوچک برای دریافت callback مرورگر
# ================================================================
# چرا یک http.server ساده و نه یک فریم‌ورک وب سنگین؟ چون این بات فقط با
# polling کار می‌کند (نه webhook) و تنها کاری که این سرور باید بکند گرفتن
# یک GET request از گوگل و برگرداندن یک صفحه‌ی تشکر است — یک ترد جدا با
# کتابخانه‌ی استاندارد پایتون، بدون هیچ وابستگی اضافه، کمترین ریسک باگ را دارد.

MAIN_LOOP = None  # با post_init پر می‌شود (بعد از این‌که event loop واقعاً بالا آمد)

async def _complete_google_login(tid: int, ok: bool, result: str, email: str):
    """بعد از برگشت کاربر از صفحه‌ی تایید گوگل (چه موفق چه ناموفق) صدا زده
    می‌شود و دقیقاً از همان نقطه‌ای که قطع شده بود ادامه می‌دهد — بدون این‌که
    کاربر مجبور باشد /start بزند یا چیزی از دست بدهد."""
    bot = app.bot
    if not ok:
        try:
            await safe_send(bot, tid,
                f"❌ اتصال به گوگل ناموفق بود: {result}\n\n"
                "می‌توانید دوباره تلاش کنید یا از App Password استفاده کنید:",
                reply_markup=auth_choice_kb())
        except Exception as e:
            logger.error(f"google login fail notify: {e}")
        return

    target_ud = app.user_data.get(tid)
    if target_ud is None or target_ud.get(S) != ST_WAIT_GOOGLE:
        # کاربر خیلی دیر برگشته یا بات بین این مدت ری‌استارت شده — چیزی از
        # اطلاعاتش گم نمی‌شود، فقط باید یک پیام (حتی /start) بفرستد تا از
        # سر بگیریم؛ اتصال گوگلش هم موفق بوده و در قدم بعد استفاده می‌شود.
        try:
            await safe_send(bot, tid,
                f"✅ ورود با گوگل ({email}) موفق بود!\n\n"
                "برای ادامه فقط یک پیام (مثلاً /start) به ربات بفرستید.")
        except Exception as e:
            logger.error(f"google login late notify: {e}")
        return

    target_ud[S_SMTP_E]    = email
    target_ud[S_SMTP_P]    = GOOGLE_AUTH_PREFIX + result
    target_ud[S_SMTP_H]    = "gmail-api"
    target_ud[S_SMTP_PORT] = 0
    target_ud[S_AUTH_METHOD] = "google"

    if target_ud.get(S_SRV) == "job":
        client = target_ud.get(S_CLIENT, {})
        target_ud[S] = ST_JOB_FIELD
        await safe_send(bot, tid,
            f"✅ با گوگل ({email}) وارد شدید — بدون پسورد!\n\n"
            f"رشته جستجو: {client.get('field','')}\n"
            f"کشور: {client.get('countries','')}\n\n"
            "رشته دقیق برای جستجو را تایید یا ویرایش کنید:",
            reply_markup=SEARCH_CONFIRM_KB)
        return

    target_ud[S] = ST_PROF_LIST
    await safe_send(bot, tid,
        f"✅ با گوگل ({email}) وارد شدید — بدون پسورد!\n\n"
        "حالا لیست اساتید:\n\n"
        "1️⃣ «🤖 بات پیدا کند» — بات خودش جستجو می‌کند\n"
        "2️⃣ لینک/اسم استاد — یا چند خط با ایمیل اساتید\n\n"
        "انتخاب کنید:",
        reply_markup=mk(["🤖 بات خودش پیدا کند"],["📋 لیست خودم دارم"],["🔙 برگشت"]))

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

# نکته: این سرور همیشه بالا میاد (نه فقط وقتی GOOGLE_LOGIN_ENABLED باشه) —
# چون بیشتر PaaSها (Railway و مشابه) برای health-check انتظار دارن پروسه
# روی $PORT گوش بده؛ قبلاً اگه گوگل لاگین غیرفعال بود، اصلاً هیچ پورتی باز
# نمی‌شد و پلتفرم می‌تونست بات رو (غلط) "down" تشخیص بده با اینکه بات تلگرام
# کاملاً سالم داشت کار می‌کرد.
_BOT_START_TIME = time.time()

class _LocalHTTPHandler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # جلوگیری از شلوغ شدن لاگ بات با هر hit این سرور

    def _html(self, msg: str, code: int = 200):
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write((
            "<html><body style='font-family:sans-serif;text-align:center;"
            f"padding-top:60px;direction:rtl'><h2>{msg}</h2></body></html>"
        ).encode("utf-8"))

    def _json(self, obj: dict, code: int = 200):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            if parsed.path in ("/healthz", "/health"):
                self._handle_health()
                return
            if GOOGLE_LOGIN_ENABLED and parsed.path == "/oauth2callback":
                self._handle_oauth_callback(parsed)
                return
            self._html("صفحه یافت نشد.", 404)
        except Exception as e:
            logger.error(f"Local HTTP handler error: {e}")
            try:
                self._html("❌ خطای غیرمنتظره.", 500)
            except Exception:
                pass

    def _handle_health(self):
        """این endpoint فقط برای uptime monitoring (مثلاً Railway health
        check) هست، نه یک liveness کامل — عمداً یک 200 دروغین برنمی‌گردونه
        صرفاً به این خاطر که پروسه هنوز zombie زنده‌ست. دو چیز رو واقعاً
        چک می‌کنه:
          ۱) دیتابیس واقعاً query-پذیره (نه فقط فایلش وجود داره — مثلاً اگه
             لاک شده یا فایلش خراب شده باشه، اینجا fail می‌شه)
          ۲) event loop اصلی بات (که همه‌ی handlerها روش اجرا می‌شن) بالا
             و در حال اجراست
        اگه هرکدوم fail بشه، 503 برمی‌گردونه تا پلتفرم واقعاً بفهمه مشکلی
        هست، نه این‌که فقط "پورت باز است" رو با "بات سالم است" اشتباه بگیره."""
        checks = {}
        healthy = True
        try:
            with _db() as c:
                c.execute("SELECT 1").fetchone()
            checks["database"] = "ok"
        except Exception as e:
            checks["database"] = f"error: {e}"
            healthy = False
        loop_ok = MAIN_LOOP is not None and MAIN_LOOP.is_running()
        checks["telegram_loop"] = "ok" if loop_ok else "not_running"
        if not loop_ok:
            healthy = False
        self._json({
            "status": "ok" if healthy else "degraded",
            "uptime_seconds": int(time.time() - _BOT_START_TIME),
            "checks": checks,
        }, code=200 if healthy else 503)

    def _handle_oauth_callback(self, parsed):
        qs    = parse_qs(parsed.query)
        code  = (qs.get("code")  or [""])[0]
        state = (qs.get("state") or [""])[0]
        error = (qs.get("error") or [""])[0]
        entry = _pending_google_states.pop(state, None)
        tid   = entry[0] if entry else None
        # چک صریح انقضا در همون لحظه‌ی مصرف — قبلاً فقط cleanup
        # ساعتی این کار رو می‌کرد (نگاه کن به کامنت بالای
        # GOOGLE_STATE_MAX_AGE_SEC)؛ یک state قدیمی نباید فقط به
        # این خاطر که هنوز پاک نشده معتبر حساب بشه.
        if entry and (time.time() - entry[1] > GOOGLE_STATE_MAX_AGE_SEC):
            logger.warning(f"OAuth callback: state منقضی‌شده رد شد (uid={tid})")
            tid = None

        if error or not code or not tid:
            self._html("❌ ورود ناموفق یا لینک منقضی شده. به تلگرام برگردید و دوباره تلاش کنید.")
            return

        ok, result, email = exchange_google_code(code)
        if MAIN_LOOP is not None and MAIN_LOOP.is_running():
            asyncio.run_coroutine_threadsafe(
                _complete_google_login(tid, ok, result, email), MAIN_LOOP)
        else:
            logger.error("MAIN_LOOP در دسترس نیست — نتیجه‌ی ورود گوگل به کاربر اطلاع داده نشد.")

        if ok:
            self._html(f"✅ ورود با گوگل ({email}) موفق بود! می‌توانید به تلگرام برگردید.")
        else:
            self._html("❌ اتصال به گوگل ناموفق بود. به تلگرام برگردید و دوباره تلاش کنید.")

def _start_local_http_server():
    try:
        server = ThreadingHTTPServer(("0.0.0.0", OAUTH_LOCAL_PORT), _LocalHTTPHandler)
        t = threading.Thread(target=server.serve_forever, daemon=True, name="local-http-server")
        t.start()
        extra = " + OAuth callback" if GOOGLE_LOGIN_ENABLED else ""
        logger.info(f"✅ HTTP server (health-check{extra}) روی پورت {OAUTH_LOCAL_PORT} در حال گوش‌دادن است.")
    except Exception as e:
        # اگر این سرور بالا نیاد، بقیه‌ی بات (از جمله روش App Password)
        # باید بدون مشکل کار کند — فقط دکمه‌ی گوگل و health-check پلتفرم
        # عملاً جواب نمی‌دهند.
        logger.error(f"❌ راه‌اندازی HTTP server ناموفق: {e} — health-check "
                     f"{'و ورود با گوگل ' if GOOGLE_LOGIN_ENABLED else ''}کار نخواهد کرد.")

async def _periodic_memory_cleanup():
    """این‌ها همه دیکشنری‌های درون‌حافظه‌ای هستن (نه دیتابیس) که اگه هیچ‌وقت
    پاک نشن، بعد از چند هفته کار پیوسته کم‌کم رشد می‌کنن. هر یک ساعت یک‌بار
    ورودی‌های قدیمی/بلااستفاده رو جمع می‌کنیم — سبک و بی‌خطر، چون فقط روی
    خود دیکشنری‌ها کار می‌کنه و به هیچ Task یا کاربر فعالی دست نمی‌زنه."""
    while True:
        try:
            await asyncio.sleep(3600)  # هر ۱ ساعت
            now = time.time()

            # پیام‌های rate-limit قدیمی‌تر از پنجره‌ی زمانی رو حذف کن؛ اگه
            # لیست یه کاربر کاملاً خالی شد، خود کلید رو هم حذف کن (وگرنه
            # کلید برای هر کاربری که تا حالا پیام داده تا ابد می‌مونه).
            stale_users = []
            for uid, hits in list(_rate_limit_hits.items()):
                fresh = [t for t in hits if now - t <= RATE_LIMIT_WINDOW_S]
                if fresh:
                    _rate_limit_hits[uid] = fresh
                else:
                    stale_users.append(uid)
            for uid in stale_users:
                _rate_limit_hits.pop(uid, None)

            # حالت‌های OAuth گوگل که کاربر هیچ‌وقت تکمیلش نکرده (بیشتر از
            # ۳۰ دقیقه معلق مونده) — لینکشون هم تو گوگل منقضی شده، بی‌فایده‌ست.
            stale_states = [s for s, (_, ts) in _pending_google_states.items() if now - ts > 1800]
            for s in stale_states:
                _pending_google_states.pop(s, None)

            if stale_users or stale_states:
                logger.info(f"🧹 پاکسازی حافظه: {len(stale_users)} rate-limit، {len(stale_states)} google-state")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"periodic cleanup error: {e}")

async def _on_bot_startup(application):
    """بعد از این‌که event loop واقعی بات بالا اومد صدا زده می‌شود (post_init) —
    اینجا و نه قبل از run_polling، چون قبل از آن هنوز loop در حال اجرا نیست
    و asyncio.run_coroutine_threadsafe از ترد سرور OAuth کار نمی‌کرد."""
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    _start_local_http_server()  # همیشه اجرا می‌شه (health-check + اگه فعال باشه OAuth callback)
    asyncio.create_task(_periodic_memory_cleanup())

    # ── هشدار درباره‌ی batchهایی که با ری‌استارت/کرش وسط کار موندن ──
    # قبلاً هیچ اثری از این‌که یک batch نیمه‌کاره مونده وجود نداشت — الان
    # چون وضعیت هر ایمیل توی email_jobs ثبت می‌شه، همین اول کار می‌شه
    # فهمید و به ادمین خبر داد (به‌جای اینکه بی‌سروصدا فراموش بشه).
    #
    # قبل از گزارش، هر job که با status='SENDING' از process قبلی (که دیگه
    # زنده نیست) باقی مونده رو به 'UNKNOWN' تبدیل می‌کنیم — یعنی «واقعاً
    # معلوم نیست SMTP موفق شده بود یا نه، پس خودکار دوباره فرستاده نمی‌شه».
    # جزئیات دلیل توی db_reconcile_stale_sending_jobs.
    try:
        n_unknown = await asyncio.to_thread(db_reconcile_stale_sending_jobs)
        if n_unknown:
            logger.warning(f"⚠️ {n_unknown} email job(s) were mid-SENDING at last shutdown/crash — marked UNKNOWN.")
    except Exception as e:
        logger.error(f"startup db_reconcile_stale_sending_jobs: {e}")

    try:
        incomplete = await asyncio.to_thread(db_get_incomplete_batches)
        if incomplete:
            total_unknown = sum(b["unknown"] for b in incomplete)
            lines = "\n".join(
                f"• کاربر {b['telegram_id']} (sub {b['sub_id']}): "
                f"{b['pending']} در صف، {b['unknown']} نامشخص (شاید ارسال شده)، "
                f"{b['sent']} ارسال‌شده، {b['failed']} ناموفق"
                for b in incomplete[:15])
            unknown_line = (
                f"\n\n⚠️ {total_unknown} ایمیل ممکن است ارسال شده باشند ولی تایید ثبت نشده "
                f"(وسط SMTP، سرور قبلاً crash کرده) — این‌ها را ما دیگر خودکار دوباره نمی‌فرستیم.\n"
                f"برای هرکدوم که مطمئن نیستید، صندوق Sent همون ایمیل کاربر رو چک کنید یا دستی دوباره برای"
                f" همون استاد enqueue کنید."
            ) if total_unknown else ""
            await notify_admin(application.bot,
                f"⚠️ {len(incomplete)} batch ارسال ایمیل به‌خاطر ری‌استارت/کرش قبلی نیمه‌کاره مونده:\n\n{lines}\n\n"
                f"جاهای «در صف» و «ناموفق» الان دوباره ارسال نمی‌شن (برای جلوگیری از اجرای ناخواسته) — "
                f"برای ادامه‌شون لازمه سرویس مربوطه دوباره برای همون کاربر تریگر بشه؛ "
                f"چون وضعیت قبلی توی دیتابیس ثبته، استادهایی که قبلاً واقعاً SENT (یا حتی نامشخص) شدن "
                f"دوباره ایمیل نمی‌گیرن.{unknown_line}")
    except Exception as e:
        logger.error(f"startup incomplete-batch check: {e}")

    # ── Reconciliation صریح previewهای pending بعد از ری‌استارت ──────
    # قبلاً تسک _preview_timeout صرفاً یک asyncio task در حافظه بود —
    # با هر ری‌استارت (حتی یک ری‌استارت عادی، نه فقط crash)، این تسک و
    # snapshot مربوطه‌اش (که در bot_data غیرpersistent بود) کاملاً از
    # بین می‌رفتن: نه فقط auto-send انجام نمی‌شد، حتی همون پیام خطای
    # «اطلاعات ارسال یافت نشد» هم فرستاده نمی‌شد، چون خود تایمر دیگه
    # وجود نداشت که بعد از ۱۵ دقیقه بیدار بشه. کاربر برای همیشه با یک
    # preview روی حالت pending می‌موند، بدون هیچ پیگیری.
    #
    # الان اینجا: هر preview که هنوز status='pending' هست رو بر اساس
    # created_at دوباره زمان‌بندی می‌کنیم — یا با باقی‌مونده‌ی واقعی مهلت
    # (اگه هنوز نرسیده)، یا فوری (اگه مهلتش قبل از این ری‌استارت گذشته
    # بود، همون لحظه با delay=0 اجرا می‌شه). snapshot لازم برای auto-send
    # از preview_snapshots (نگاه کن به db_save_preview_snapshot) خونده
    # می‌شه؛ _preview_timeout خودش این کار رو انجام می‌ده، پس این‌جا فقط
    # لازمه تسک رو با chat_id/uid درست دوباره بسازیم.
    try:
        pending_previews = await asyncio.to_thread(db_get_all_pending_previews)
        for pv in pending_previews:
            pid, tid = pv["id"], pv["telegram_id"]
            try:
                created = datetime.fromisoformat(pv["created_at"])
                elapsed = (datetime.now() - created).total_seconds()
            except Exception:
                elapsed = 0.0
            remaining = max(PREVIEW_TIMEOUT - elapsed, 0)
            # chat_id همیشه با telegram_id یکیه (این بات فقط توی چت خصوصی
            # کار می‌کنه، نه گروه) — دقیقاً همون فرضی که جاهای دیگه‌ی این
            # کدبیس (مثلاً مانیتورینگ مداوم) هم می‌کنن.
            asyncio.create_task(_preview_timeout(
                _StartupCtx(application), tid, tid, pid, delay=remaining))
        if pending_previews:
            logger.info(f"🔄 {len(pending_previews)} preview pending از قبل ری‌استارت دوباره زمان‌بندی شد")
    except Exception as e:
        logger.error(f"startup preview reconciliation: {e}")

    if application.job_queue is not None:
        # هر ساعت چک می‌کنیم کدوم جستجوهای معلق به ۲۴ ساعت رسیدن — نه
        # این‌که خودش هر ۲۴ ساعت شلیک کنه، چون این‌جوری اگه بات وسط این
        # مدت ری‌استارت بشه، هیچی گم نمی‌شه (وضعیت روی دیسکه، نه حافظه).
        application.job_queue.run_repeating(_check_pending_searches, interval=3600, first=120)
        # مانیتورینگ مداوم: هر ساعت چک می‌کنیم کدوم مانیتور فعال به ۲۴ ساعت
        # رسیده (نه این‌که خودش هر ۲۴ ساعت شلیک کنه — همون منطق ری‌استارت‌
        # مقاوم بالا، وضعیت روی دیسکه نه حافظه). first=180 تا بعد از
        # _check_pending_searches (first=120) اجرا بشه و فشار هم‌زمان روی
        # APIهای بیرونی کمتر بشه.
        application.job_queue.run_repeating(_check_active_monitors, interval=3600, first=180)
        # IMAP Bounce Watcher: فقط یک‌بار در روز (نه هر ساعت مثل بقیه) —
        # چون هدفش فقط تشخیص آدرس‌های مرده‌ست، نه چیزی real-time، و باز
        # کردن صندوق ایمیل کاربر هر ساعت هم غیرضروریه هم بیشتر در معرض
        # rate-limit ارائه‌دهنده‌های IMAP قرار می‌گیره.
        application.job_queue.run_repeating(_check_bounces_daily, interval=86400, first=300)
        # Time-Budget Rescue: هر ۶ ساعت دنبال استادهایی می‌گرده که فقط
        # به‌خاطر تموم شدن سقف زمانی کل batch (نه چون منبعی نداشتیم) رد
        # شدن، و pipeline ایمیل‌یابی رو خارج از فشار زمانی batch اصلی
        # دوباره براشون اجرا می‌کنه. first=420 تا بعد از بقیه‌ی jobهای
        # startup اجرا بشه و فشار هم‌زمان روی APIهای بیرونی کمتر بشه.
        application.job_queue.run_repeating(_rescue_time_budget_skipped_bg, interval=21600, first=420)
        # Personal Visa Agent: دقیقاً همون الگوی بالا (هر ساعت چک می‌کنیم
        # کی «سررسیده»، نه این‌که خودش هر ۷ روز یک‌بار شلیک کنه) — چون
        # وضعیت (visa_agent_last_sent) روی دیسکه، نه حافظه، پس با ری‌استارت
        # سرور هیچ کاربری دیر/زودتر از موعدش پیام نمی‌گیره یا فراموش نمی‌شه.
        application.job_queue.run_repeating(_send_visa_agent_weekly_digest, interval=3600, first=240)
        # بک‌آپ هفتگی دیتابیس برای ادمین: هر ساعت چک می‌کنیم آیا یک هفته از
        # آخرین بک‌آپ گذشته یا نه (همون الگوی resilient بالا). first=480 تا
        # آخرین job این دسته باشه.
        application.job_queue.run_repeating(_send_weekly_db_backup, interval=3600, first=480)
    else:
        logger.warning("⚠️ job_queue در دسترس نیست — قابلیت «هر ۲۴ ساعت خودت دوباره بگرد» غیرفعاله. "
                        "برای فعال‌سازی: pip install \"python-telegram-bot[job-queue]\"")

async def _check_pending_searches(context: ContextTypes.DEFAULT_TYPE):
    """هر ساعت صدا زده می‌شه، ولی فقط جستجوهایی که واقعاً ۲۴ ساعت ازشون
    گذشته رو دوباره امتحان می‌کنه. هر خطایی (چه توی خود جستجو چه توی
    ارسال پیام) کاملاً محلی می‌مونه — یه کاربر با مشکل باعث نمی‌شه بقیه‌ی
    کاربرهای در صف چک نشن."""
    try:
        due = await asyncio.to_thread(db_get_due_pending_searches, 24)
    except Exception as e:
        logger.error(f"db_get_due_pending_searches: {e}")
        return
    for item in due:
        try:
            await asyncio.to_thread(db_mark_pending_search_tried, item["id"])
            if item["search_type"] == "professor":
                results = await asyncio.to_thread(
                    search_professors, item["field"], item["country"], EMAIL_QUOTA, {})
                kind_fa = "استاد"
            else:
                results = await asyncio.to_thread(
                    search_jobs_realtime, item["field"], item["country"], JOB_QUOTA)
                kind_fa = "فرصت شغلی"

            if results:
                await asyncio.to_thread(db_deactivate_pending_search, item["id"])
                await context.bot.send_message(
                    chat_id=item["telegram_id"],
                    text=f"🎉 خبر خوب! {len(results)} {kind_fa} مرتبط با «{item['field']}» "
                         f"در «{item['country']}» پیدا شد.\n\n"
                         f"برای ادامه، از منوی اصلی سرویس مربوطه رو دوباره انتخاب کنید — "
                         f"این‌بار نتیجه داره.")
            elif item["attempts"] >= 6:
                # بعد از ۶ بار تلاش (~۶ روز)، دیگه بی‌خبر ادامه نمی‌دیم —
                # به کاربر خبر می‌دیم که هنوز چیزی پیدا نشده، خودش تصمیم بگیره.
                await asyncio.to_thread(db_deactivate_pending_search, item["id"])
                await context.bot.send_message(
                    chat_id=item["telegram_id"],
                    text=f"ℹ️ بعد از چند روز جستجوی خودکار، هنوز {kind_fa}ی برای «{item['field']}» "
                         f"در «{item['country']}» پیدا نکردیم. می‌تونید با یه رشته یا کشور دیگه دوباره امتحان کنید.")
        except Exception as e:
            logger.error(f"_check_pending_searches item {item.get('id')}: {e}")
            continue

# ── مانیتورینگ مداوم (Active Monitors) ──────────────────────────────
# فرق این تابع با _check_pending_searches بالا: اون فقط برای «جستجوی
# اول ۰ نتیجه داد» است و همین که یک بار چیزی پیدا کرد خاموش می‌شه.
# این تابع برعکسه — تا وقتی subscription فعاله (حداکثر
# MONITOR_DURATION_DAYS روز)، صرف‌نظر از این‌که جستجوی قبلی چند نتیجه
# داشته، هر MONITOR_CHECK_INTERVAL_HOURS ساعت دوباره جستجو می‌کنه و فقط
# نتایج *جدید* (که قبلاً برای همین کاربر ارسال/اپلای نشده) رو گزارش
# می‌ده — چون هم استادهای جدید و هم فرصت‌های شغلی جدید هر روز اضافه
# می‌شن، یک اسکن یک‌باره برای «۱۰۰٪ به نتیجه رسوندن مشتری» کافی نیست.
async def _check_active_monitors(context: ContextTypes.DEFAULT_TYPE):
    """هر ساعت صدا زده می‌شه، ولی فقط مانیتورهایی که واقعاً
    MONITOR_CHECK_INTERVAL_HOURS ساعت ازشون گذشته رو دوباره جستجو
    می‌کنه. هر خطا (چه توی جستجو چه توی ارسال پیام) کاملاً محلی می‌مونه —
    یه کاربر مشکل‌دار باعث نمی‌شه بقیه‌ی مانیتورهای فعال چک نشن."""
    try:
        due = await asyncio.to_thread(db_get_due_monitors, MONITOR_CHECK_INTERVAL_HOURS)
    except Exception as e:
        logger.error(f"db_get_due_monitors: {e}")
        return

    for item in due:
        mid, tid, sid = item["id"], item["telegram_id"], item["sub_id"]
        try:
            await asyncio.to_thread(db_mark_monitor_checked, mid)

            # اگه subscription دیگه approved نیست (رد شده/لغو/منقضی/تکمیل
            # شده یا به هر دلیلی وضعیتش عوض شده)، مانیتور رو خاموش کن —
            # نیازی نیست برای مشترکی که دیگه فعال نیست جستجوی جدید انجام بشه.
            # نکته‌ی مهم: قبلاً 'pending' هم جزو حالت‌های فعال حساب می‌شد؛
            # این یعنی یک subscription که ادمین صراحتاً رد کرده بود (و قبل از
            # فیکس db_reject_sub، هیچ‌وقت واقعاً از 'pending' خارج نمی‌شد)
            # می‌تونست مانیتورش برای همیشه فعال بمونه و مدام جستجوی خودکار
            # اجرا کنه. مانیتور فقط برای subscription واقعاً approved معنی
            # داره؛ pending اصلاً نباید اینجا فعال حساب بشه.
            sub = await asyncio.to_thread(db_get_sub, sid) if sid else None
            if not sub or sub["status"] != "approved":
                await asyncio.to_thread(db_deactivate_monitor, mid)
                continue
            if item["monitor_type"] == "professor" and sub.get("emails_sent", 0) >= EMAIL_QUOTA:
                await asyncio.to_thread(db_deactivate_monitor, mid)
                continue
            if item["monitor_type"] == "job" and sub.get("jobs_applied", 0) >= JOB_QUOTA:
                await asyncio.to_thread(db_deactivate_monitor, mid)
                continue

            if item["monitor_type"] == "professor":
                # عمداً از کش رد می‌شیم (client={} یعنی pool خام برمی‌گرده،
                # نه رتبه‌بندی‌شده‌ی مخصوص این کاربر) — چون هدف اینجا فقط
                # کشف «آیا اصلاً استاد تازه‌ای اضافه شده» است؛ dedupe واقعی
                # پایین‌تر روی ایمیل انجام می‌شه، نه اینجا.
                pool = await asyncio.to_thread(search_professors, item["field"], item["country"], EMAIL_QUOTA, {})
                already = await asyncio.to_thread(db_get_sent_email_set, tid)
                new_items = [p for p in pool if p.get("email") and p["email"] not in already]
                kind_fa, label = "استاد", "professor"
            else:
                pool = await asyncio.to_thread(search_jobs_realtime, item["field"], item["country"], JOB_QUOTA)
                already_urls, already_pairs = await asyncio.to_thread(db_get_applied_job_keys, tid)
                new_items = [
                    j for j in pool
                    if (j.get("url") and j["url"] not in already_urls)
                    or (not j.get("url") and (j.get("title", ""), j.get("company", "")) not in already_pairs)
                ]
                kind_fa, label = "فرصت شغلی", "job"

            if new_items:
                await asyncio.to_thread(db_add_monitor_new_matches, mid, len(new_items))
                await context.bot.send_message(
                    chat_id=tid,
                    text=f"🎉 {len(new_items)} {kind_fa} *جدید* مرتبط با «{item['field']}» "
                         f"در «{item['country']}» پیدا شد (که قبلاً براتون نداشتیم).\n\n"
                         f"برای دیدن و ادامه، از منوی اصلی سرویس مربوطه رو دوباره انتخاب کنید.")
                logger.info(f"monitor {mid} ({label}) uid={tid}: {len(new_items)} new of {len(pool)} total")
            # اگه چیز جدیدی نبود، هیچ پیامی نمی‌فرستیم — مشتری با هر بار
            # «هیچی جدید نیست» اسپم نمی‌شه؛ فقط last_checked_at آپدیت شد و
            # ساعت بعدی دوباره امتحان می‌شه (تا وقتی مانیتور فعاله).
        except Exception as e:
            logger.error(f"_check_active_monitors item {mid}: {e}")
            continue

# ── Personal Visa Agent — پیام هفتگی «۳ کار ضروری» ─────────────────────
# فقط برای کاربرهایی که صریحاً از منوی «پرونده مهاجرتی من» روشنش کردن
# (visa_agent_optin=1). دو قانون امنیتی سخت‌گیرانه اینجا رعایت می‌شه:
#  ۱) AI هرگز تاریخ/شمار روز اختراع نمی‌کنه — پرامپت صراحتاً می‌گه فقط
#     کار پیشنهاد بده، نه ددلاین. شمار روزهای واقعی (اگه کاربر ثبت کرده
#     باشه) کاملاً جدا و با datetime پایتون محاسبه و به متن پیام اضافه
#     می‌شه، نه از خروجی AI.
#  ۲) اگه پرونده‌ی مهاجرتی کاربر کاملاً خالیه (هیچ فیلد weak_points/
#     documents_status/strategy/visa_goal ای پر نشده)، پیام اصلاً فرستاده
#     نمی‌شه — چون AI روی متن خالی فقط سه‌تا توصیه‌ی جنریک و بی‌ربط می‌سازه
#     که هیچ ارزشی نداره و فقط اسپمه.
_VISA_AGENT_INTERVAL_DAYS = 7

def _visa_agent_profile_has_content(imm: dict) -> bool:
    keys = ("visa_goal", "previous_refusal", "documents_status", "weak_points", "strategy")
    return any((imm.get(k) or "").strip() for k in keys)

async def _generate_visa_agent_tasks(imm: dict) -> list[str] | None:
    """فقط از روی متن پرونده (نه دانش عمومی خودش، نه هیچ تاریخی) دقیقاً
    ۳ کار عملی این هفته پیشنهاد می‌ده. اگه AI جواب معتبر نده یا JSON
    پارس نشه، None برمی‌گرده — caller در این حالت هیچ پیامی نمی‌فرسته،
    به‌جای این‌که یه چیز نامطمئن/جنریک به کاربر نشون بده."""
    profile_block = _format_immigration_profile_block(imm)
    prompt = f"""{profile_block}

با توجه *فقط* به همین پرونده (نه دانش عمومی درباره‌ی مهاجرت به‌طور کلی)، دقیقاً ۳ کار عملی و
مشخص پیشنهاد بده که این کاربر این هفته باید انجام بده تا پرونده‌اش قوی‌تر بشه.
مهم: هیچ تاریخ، ددلاین، یا شمار روز اختراع نکن — فقط خودِ کار رو بگو، بدون زمان‌بندی.
اگه پرونده اطلاعات کافی نداره که ۳ کار معنادار پیشنهاد بشه، آرایه‌ی خالی برگردون.
فقط JSON خالص، دقیقاً این ساختار:
{{"tasks": ["کار اول، کوتاه و عملی", "کار دوم", "کار سوم"]}}"""
    try:
        ai = await call_ai(prompt, min_length=10)
        data = _extract_json_object(ai)
        if not data:
            return None
        tasks = [str(t).strip()[:200] for t in (data.get("tasks") or []) if str(t).strip()]
        return tasks[:3] if tasks else None
    except Exception as e:
        logger.warning(f"_generate_visa_agent_tasks: {e}")
        return None

async def _send_visa_agent_weekly_digest(context: ContextTypes.DEFAULT_TYPE):
    """هر ساعت صدا زده می‌شه، ولی فقط کاربرهایی که واقعاً
    _VISA_AGENT_INTERVAL_DAYS روز ازشون گذشته (یا هنوز هیچ‌وقت پیام
    نگرفتن) رو پردازش می‌کنه. هر خطا کاملاً محلی می‌مونه — یک کاربر
    مشکل‌دار بقیه رو متوقف نمی‌کنه."""
    try:
        subs = await asyncio.to_thread(db_get_visa_agent_subscribers)
    except Exception as e:
        logger.error(f"db_get_visa_agent_subscribers: {e}")
        return

    cutoff = datetime.now() - timedelta(days=_VISA_AGENT_INTERVAL_DAYS)
    for sub in subs:
        tid = sub["telegram_id"]
        try:
            imm = await asyncio.to_thread(db_get_immigration_profile, tid)
        except Exception as e:
            logger.error(f"[visa_agent uid={tid}] db_get_immigration_profile: {e}")
            continue
        if not _visa_agent_profile_has_content(imm):
            continue  # پرونده‌ی خالی — چیزی برای پیشنهاد نیست، اسپم نکن

        # last_sent رو مستقیم از دیتابیس می‌خونیم (نه از خروجی
        # db_get_visa_agent_subscribers) چون اون تابع فقط خواندنی و سبکه
        # و last_sent رو برنمی‌گردونه — این‌جوری هم whitelist ستون‌ها ساده
        # می‌مونه هم منطق «سررسیده یا نه» یک‌جا (همین‌جا) متمرکزه.
        try:
            with _db() as c:
                row = c.execute("SELECT visa_agent_last_sent FROM career_profiles WHERE telegram_id=?",
                                 (tid,)).fetchone()
            last_sent_raw = row[0] if row else ""
        except Exception as e:
            logger.error(f"[visa_agent uid={tid}] last_sent lookup: {e}")
            continue
        if last_sent_raw:
            try:
                if datetime.strptime(last_sent_raw[:19], "%Y-%m-%d %H:%M:%S") > cutoff:
                    continue  # هنوز یک هفته نگذشته
            except ValueError:
                pass  # فرمت غیرمنتظره — به نفع کاربر فرض می‌کنیم سررسیده

        tasks = await _generate_visa_agent_tasks(imm)
        if not tasks:
            continue  # AI جواب قابل‌اتکا نداد یا پرونده کافی نبود — این هفته چیزی نفرست

        lines = ["🧭 دستیار هفتگی مهاجرت — پرونده‌تون رو بررسی کردم:\n", "کارهای ضروری این هفته:"]
        for i, t in enumerate(tasks, 1):
            lines.append(f"{i}. {t}")

        # شمار روز فقط از تاریخ واقعی‌ای که خودِ کاربر ثبت کرده محاسبه
        # می‌شه — با کد پایتون، نه AI.
        if sub["deadline_date"]:
            try:
                days_left = (datetime.strptime(sub["deadline_date"], "%Y-%m-%d").date()
                             - datetime.now().date()).days
                dl_label = sub["deadline_label"] or "ددلاین شما"
                if days_left >= 0:
                    lines.append(f"\n⏰ {dl_label}: {days_left} روز مونده ({sub['deadline_date']})")
                else:
                    lines.append(f"\n⚠️ تاریخ ثبت‌شده برای «{dl_label}» ({sub['deadline_date']}) گذشته — "
                                  "اگه هنوز معتبره، لطفاً از منو به‌روزش کنید.")
            except ValueError:
                pass

        lines.append("\nاین پیشنهادها فقط بر اساس پرونده‌ی ثبت‌شده‌ی خودتونه، نه یک مشاوره‌ی حقوقی رسمی.")
        try:
            await safe_send(context.bot, tid, "\n".join(lines))
            await asyncio.to_thread(db_mark_visa_agent_sent, tid)
        except Exception as e:
            logger.error(f"[visa_agent uid={tid}] send failed: {e}")

# ── بک‌آپ هفتگی دیتابیس برای ادمین (تلگرام) ────────────────────────────
# فایل کنار خودِ اسکریپت که آخرین زمان ارسال موفق بک‌آپ رو نگه می‌داره —
# دقیقاً همون الگوی سایدکار-فایل که این پروژه از قبل برای .bot.lock و
# .session.key استفاده می‌کنه؛ نیازی به ستون جدید توی دیتابیس یا جدول
# تنظیمات جداگانه نیست چون این یه وضعیت تک‌مقداره‌ی سراسریه (نه per-user).
_DB_BACKUP_STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".last_db_backup")
DB_BACKUP_INTERVAL_DAYS = 7

async def _send_weekly_db_backup(context: ContextTypes.DEFAULT_TYPE):
    """هر ساعت صدا زده می‌شه (دقیقاً همون الگوی بقیه‌ی jobهای بالا: چک کن
    سررسیده یا نه، نه این‌که خودت با interval هفتگی شلیک کنی) — چون
    وضعیت (زمان آخرین بک‌آپ) روی دیسکه، پس با ری‌استارت بات نه بک‌آپی گم
    می‌شه نه زودتر/دیرتر از موعد ارسال می‌شه.

    از sqlite3 Backup API استفاده می‌کنه (نه کپی خام فایل) چون دیتابیس در
    WAL mode با connectionهای per-thread فعاله؛ کپی مستقیم فایل ممکنه
    وسط یک write ناقص گیر بیفته و فایل بک‌آپ خراب باشه — Backup API این
    مشکل رو تضمینی حل می‌کنه (consistent snapshot می‌گیره حتی اگه هم‌زمان
    نوشتن در جریان باشه)."""
    if not ADMIN_CHAT_ID:
        return  # جایی برای فرستادن نیست — بی‌سروصدا رد شو (مثل بقیه‌ی jobهای ادمین)
    try:
        last_ts = 0.0
        if os.path.exists(_DB_BACKUP_STATE_FILE):
            with open(_DB_BACKUP_STATE_FILE) as f:
                last_ts = float((f.read() or "0").strip() or 0)
    except Exception as e:
        logger.warning(f"db backup: خواندن state file شکست خورد ({e}) — محافظه‌کارانه فرض می‌کنیم سررسیده")
        last_ts = 0.0
    if time.time() - last_ts < DB_BACKUP_INTERVAL_DAYS * 86400:
        return  # هنوز یک هفته نگذشته

    backup_path = None
    try:
        backup_path = os.path.join(tempfile.gettempdir(), f"services_bot_backup_{int(time.time())}.db")

        def _make_snapshot():
            src = sqlite3.connect(DB_FILE)
            try:
                dst = sqlite3.connect(backup_path)
                try:
                    src.backup(dst)
                finally:
                    dst.close()
            finally:
                src.close()

        await asyncio.to_thread(_make_snapshot)
        size_mb = os.path.getsize(backup_path) / (1024 * 1024)
        # محدودیت Bot API تلگرام برای send_document حدود ۵۰ مگابایته؛ اگه
        # دیتابیس از این بزرگ‌تر شد، اون بالاتر یه هشدار صریح می‌فرستیم
        # به‌جای این‌که send_document بی‌صدا/با خطای مبهم fail کنه.
        if size_mb > 49:
            logger.error(f"❌ db backup: حجم دیتابیس ({size_mb:.1f}MB) از سقف ~۵۰MB ارسال فایل تلگرام "
                         "بیشتره — بک‌آپ این هفته ارسال نشد. باید یا strategy فشرده‌سازی اضافه بشه، "
                         "یا مسیر دیگه‌ای (مثلاً آپلود به یک storage خارجی) جایگزین بشه.")
            return
        with open(backup_path, "rb") as f:
            await context.bot.send_document(
                chat_id=ADMIN_CHAT_ID,
                document=f,
                filename=f"services_bot_backup_{datetime.now():%Y-%m-%d}.db",
                caption=f"📦 بک‌آپ هفتگی دیتابیس — {size_mb:.1f}MB")
        with open(_DB_BACKUP_STATE_FILE, "w") as f:
            f.write(str(time.time()))
        logger.info(f"✅ بک‌آپ هفتگی دیتابیس ({size_mb:.1f}MB) برای ادمین ارسال شد")
    except Exception as e:
        logger.error(f"❌ ارسال بک‌آپ هفتگی دیتابیس شکست خورد: {e}")
    finally:
        if backup_path and os.path.exists(backup_path):
            try:
                os.remove(backup_path)
            except Exception:
                pass

# ── IMAP Bounce Watcher — اجرای روزانه برای کاربرهای opt-in ────────────
async def _check_bounces_daily(context: ContextTypes.DEFAULT_TYPE):
    """هر روز یک‌بار صدا زده می‌شه. فقط کاربرهایی که db_set_imap_optin
    صریحاً روشنش کرده چک می‌شن. SMTP creds از context.application.user_data
    خونده می‌شه (همون session در-حافظه‌ی plaintext که SQLiteUserDataPersistence
    نگه می‌داره) — نه از دیسک، چون فقط نسخه‌ی روی دیسک رمزنگاری‌شده و اینجا
    نیازی به رمزگشایی دستی نیست."""
    try:
        tids = await asyncio.to_thread(db_get_imap_optin_users)
    except Exception as e:
        logger.error(f"db_get_imap_optin_users: {e}")
        return
    for tid in tids:
        try:
            ud = context.application.user_data.get(tid) or {}
            smtp_e = ud.get(S_SMTP_E)
            smtp_p = ud.get(S_SMTP_P)
            smtp_h = ud.get(S_SMTP_H, "smtp.gmail.com")
            n = await check_bounces_for_user(tid, smtp_e, smtp_p, smtp_h)
            if n:
                await context.bot.send_message(
                    chat_id=tid,
                    text=f"ℹ️ {n} تا از ایمیل‌های قبلی‌تون به آدرس‌هایی برگشت خورده (bounce) که "
                         f"دیگه فعال نیستن — این آدرس‌ها رو دیگه دوباره امتحان نمی‌کنیم.")
        except Exception as e:
            logger.error(f"_check_bounces_daily uid={tid}: {e}")
            continue

# ================================================================
# ADMIN COMMANDS
# ================================================================

async def _resume_user_after_approval(context, sid, tid):
    """بعد از تایید پرداخت — کاربر رو مستقیم به مرحله‌ی بعد می‌بریم.

    نکته‌ی حیاتی: ادمین معمولاً همون لحظه‌ای که اسکرین‌شات رو می‌بینه تایید
    می‌کنه — یعنی خیلی وقت‌ها کاربر هنوز داره نام/شماره/سوالات (رشته،
    رزومه، ...) رو پر می‌کنه و هنوز به «wait_approval» نرسیده. قبلاً این
    تابع در اون حالت هم کاربر رو مستقیم به مرحله‌ی بعد (جستجوی استاد/کار)
    می‌فرستاد — یعنی رزومه/رشته/... که هنوز نگرفته بودیم خالی می‌موند و
    مرحله‌ی بعد عملاً با پروفایل ناقص/خالی اجرا می‌شد (یا هیچ خروجی
    درستی نمی‌داد). الان: فقط وقتی واقعاً به «wait_approval» رسیده باشه
    (یعنی همه‌چیز رو داده و فقط منتظر تاییده) بلافاصله ادامه می‌دیم؛ در
    غیر این صورت فقط توی DB approved ثبت می‌مونه (قبلاً توسط caller انجام
    شده) و کاربر همون‌طور که داشت جواب می‌داد ادامه می‌ده — و وقتی خودش
    تموم کرد، _finish_collect خودش می‌بینه db_is_approved=True هست و بدون
    کوچک‌ترین مکث یا تاییدیه‌ی اضافه ادامه می‌ده.
    """
    target_ud = context.application.user_data.get(tid)

    if target_ud is not None:
        svc = target_ud.get(S_SRV) or target_ud.get("svc_service")
        if target_ud.get(S) == "wait_approval":
            await safe_send(context.bot, tid,
                "🎉 پرداخت شما تایید شد! ادامه‌ی روند به‌صورت خودکار شروع می‌شود...")
            await _advance_after_approval(context.bot, tid, target_ud, svc)
            await asyncio.to_thread(db_delete_pending_snapshot, sid)
            return True
        # هنوز وسط پر کردن اطلاعاته — نپریم جلو، فقط خبر بدیم.
        await safe_send(context.bot, tid,
            "🎉 پرداخت شما تایید شد!\n\n"
            "لطفاً به پر کردن اطلاعات ادامه بدید — به محض تموم شدن، بدون هیچ "
            "توقف یا تاییدیه‌ی اضافه‌ای، روند به‌صورت خودکار شروع می‌شود.")
        return True

    # ── مسیر ۲: بازیابی از snapshot دیتابیس ────────────────────
    # اگه user_data خالیه (بات ری‌استارت کرده)، snapshot رو که موقع
    # wait_approval ذخیره کردیم بازیابی می‌کنیم. نکته: snapshot فقط وقتی
    # قابل‌اعتماده که واقعاً حاوی پروفایل تکمیل‌شده باشه (یعنی مقدار
    # field پر باشه) — وگرنه (مثلاً snapshot اولیه‌ی خالی VIP قبل از
    # collect) نباید کاربر رو جلو بفرستیم.
    snap = await asyncio.to_thread(db_get_pending_snapshot, sid)
    if snap and (snap.get(S_CLIENT) or {}).get("field"):
        svc = snap.get(S_SRV) or snap.get("svc_service")
        restored_ud = {
            S: "wait_approval",
            S_CLIENT: snap.get(S_CLIENT, {}),
            S_SRV: svc,
            S_TYPE: snap.get(S_TYPE, "normal"),
            S_SUB: sid,
        }
        await safe_send(context.bot, tid,
            "🎉 پرداخت شما تایید شد! ادامه‌ی روند به‌صورت خودکار شروع می‌شود...")
        await _advance_after_approval(context.bot, tid, restored_ud, svc)
        await asyncio.to_thread(db_delete_pending_snapshot, sid)
        return True

    # ── مسیر ۳: داده‌ای وجود نداره ─────────────────────────────
    await safe_send(context.bot, tid,
        "🎉 دسترسی شما تایید شد!\n\n"
        "برای ادامه لطفاً /start بزنید — سرویس آماده‌ی استفاده است.")
    return False

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    text = update.message.text or ""

    if ADMIN_CHAT_ID and uid != ADMIN_CHAT_ID:
        return

    admin_name = _admin_display_name(update)

    # /auditlog [N] — نمایش N ردیف آخر audit log (پیش‌فرض ۱۵)
    # /auditlog_<user_id> — فقط ردیف‌های مربوط به یک کاربر خاص
    m = re.match(r"/auditlog(?:_(\d+))?(?:\s+(\d+))?", text)
    if m:
        target_uid = int(m.group(1)) if m.group(1) else None
        try:
            limit = int(m.group(2)) if m.group(2) else 15
        except ValueError:
            limit = 15
        try:
            rows = await asyncio.to_thread(db_get_audit_log, limit, target_uid)
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Viewed audit log",
                target_uid, "Success", detail=f"limit={limit}")
            if not rows:
                await update.message.reply_text("ℹ️ هنوز هیچ رکوردی توی audit log ثبت نشده.")
                return
            header = (f"📜 Audit Log — آخرین {len(rows)} رکورد"
                      + (f" (کاربر {target_uid})" if target_uid else "") + "\n"
                      "━━━━━━━━━━━━━━━━━━━")
            blocks = [header] + [_format_audit_entry(r) for r in rows]
            # تلگرام هر پیام رو حداکثر ۴۰۹۶ کاراکتر قبول می‌کنه — اگه از این
            # بیشتر شد، توی چند پیام جدا می‌فرستیم به‌جای کات‌شدن یا خطا.
            chunk = ""
            for b in blocks:
                piece = ("\n\n━━━━━━━━━━━━━━━━━━━\n\n" if chunk else "") + b
                if len(chunk) + len(piece) > 3800:
                    await update.message.reply_text(chunk)
                    chunk = b
                else:
                    chunk += piece
            if chunk:
                await update.message.reply_text(chunk)
            await update.message.reply_text(
                "برای دریافت فایل کامل: /auditlog_export"
                + (f"_{target_uid}" if target_uid else ""))
        except Exception as e:
            logger.error(f"cmd_admin /auditlog: {e}")
            await update.message.reply_text(f"❌ خطا در دریافت audit log: {e}")
        return

    # /auditlog_export یا /auditlog_export_<user_id> — کل تاریخچه به‌صورت فایل txt
    m = re.match(r"/auditlog_export(?:_(\d+))?", text)
    if m:
        target_uid = int(m.group(1)) if m.group(1) else None
        try:
            rows = await asyncio.to_thread(db_get_audit_log, 5000, target_uid)
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Exported audit log",
                target_uid, "Success", detail=f"rows={len(rows)}")
            if not rows:
                await update.message.reply_text("ℹ️ هنوز هیچ رکوردی توی audit log ثبت نشده.")
                return
            content = "\n\n━━━━━━━━━━━━━━━━━━━\n\n".join(_format_audit_entry(r) for r in rows)
            buf = io.BytesIO(content.encode("utf-8"))
            buf.name = "audit_log.txt"
            fname = f"audit_log_user_{target_uid}.txt" if target_uid else "audit_log_full.txt"
            await context.bot.send_document(
                chat_id=uid, document=buf, filename=fname,
                caption=f"📜 Audit Log کامل — {len(rows)} رکورد")
        except Exception as e:
            logger.error(f"cmd_admin /auditlog_export: {e}")
            await update.message.reply_text(f"❌ خطا در ساخت فایل audit log: {e}")
        return

    # /stats — داشبورد ادمین
    if "/stats" in text:
        try:
            s = await asyncio.to_thread(db_get_dashboard_stats)
            sat = f"{s['satisfaction_rate']}% ({s['likes']}👍 {s['dislikes']}👎)" \
                  if s['satisfaction_rate'] is not None else "هنوز رأیی ثبت نشده"
            recent = "\n".join(
                f"  • {r['name'] or '?'} — {r['svc'] or '?'} — {r['status'] or '?'}"
                for r in s["recent_users"]) or "  (هیچ‌کس)"
            await update.message.reply_text(
                f"📊 داشبورد Manifest Apply\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"👥 کاربران: {s['total_users']} کل | {s['approved_users']} تایید | {s['pending_users']} انتظار\n"
                f"📋 اشتراک‌ها: {s['email_subs']} ایمیل | {s['job_subs']} کار | {s['completed_subs']} تکمیل\n\n"
                f"📧 ایمیل‌ها\n"
                f"  ✅ ارسال: {s['total_emails']} | ❌ خطا: {s['failed_emails']}\n"
                f"  ⏭ رد‌شده: {s['skipped_emails']} | ✉️ دستی: {s['manual_emails']}\n"
                f"  📈 نرخ موفقیت: {s['email_success_rate']}%\n\n"
                f"💼 اپلای کار\n"
                f"  ✅ اپلای ایمیلی: {s['jobs_applied_email']} | 🔗 فرصت/لینک پورتال: {s['jobs_portal_link']} | ❌ خطا: {s['failed_jobs']}\n"
                f"  ⏭ رد‌شده: {s['skipped_jobs']}\n"
                f"  📈 نرخ موفقیت: {s['job_success_rate']}%\n\n"
                f"⭐ رضایت: {sat}\n\n"
                f"📅 آخرین کاربران:\n{recent}\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}"
            )
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Viewed dashboard stats", None, "Success")
        except Exception as e:
            logger.error(f"cmd_admin /stats: {e}")
            await update.message.reply_text(f"❌ خطا در دریافت آمار: {e}")
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Viewed dashboard stats", None, "Failure", detail=str(e)[:200])
        return

    # /security — داشبورد امنیتی (رویدادهای ۲۴ ساعت اخیر)
    if "/security" in text:
        try:
            sec = await asyncio.to_thread(db_get_security_summary, 24)
            n_blocked_now = len(_blocked_until)
            await update.message.reply_text(
                f"🛡 داشبورد امنیتی — ۲۴ ساعت اخیر\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"🚫 درخواست بلاک‌شده (rate limit): {sec.get('blocked_request', 0)}\n"
                f"🚨 رفتار مشکوک (temp-block): {sec.get('suspicious_activity', 0)}\n"
                f"⛔️ در حال حاضر مسدود: {n_blocked_now} کاربر\n"
                f"━━━━━━━━━━━━━━━━━━━\n"
                f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
                f"ℹ️ این جدول جدیده — فقط از الان به بعد رویدادها ثبت می‌شن، "
                f"تاریخچه‌ی قبل از این آپدیت توش نیست."
            )
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Viewed security dashboard", None, "Success")
        except Exception as e:
            logger.error(f"cmd_admin /security: {e}")
            await update.message.reply_text(f"❌ خطا در دریافت آمار امنیتی: {e}")
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Viewed security dashboard", None, "Failure", detail=str(e)[:200])
        return

    # /approve_<sid>_<tid>
    m = re.search(r"/(approve|vip_approve)_(\d+)_(\d+)", text)
    if m:
        sid, tid = int(m.group(2)), int(m.group(3))
        just_approved = await asyncio.to_thread(db_approve_sub, sid)
        if not just_approved:
            # قبلاً تایید شده بود (مثلاً ادمین دوبار روی /approve زده) —
            # دوباره workflow (پیام به کاربر، شروع مجدد فرایند) رو اجرا
            # نمی‌کنیم که دوبار ایمیل نره یا دوبار Job ساخته نشه.
            await update.message.reply_text(f"ℹ️ Sub:{sid} از قبل تایید شده بود — دوباره اجرا نشد.")
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Approved subscription (command)", tid,
                "No-op (already approved)", detail=f"sub_id={sid}")
            return
        await asyncio.to_thread(db_set_user, tid, status="approved")
        await _resume_user_after_approval(context, sid, tid)
        await update.message.reply_text(f"✅ تایید شد. Sub:{sid} User:{tid}")
        await asyncio.to_thread(
            db_log_audit, uid, admin_name, "Approved subscription (command)", tid,
            "Success", detail=f"sub_id={sid}")
        return

    # /reject_<sid>_<tid>
    m = re.search(r"/(reject|vip_reject)_(\d+)_(\d+)", text)
    if m:
        sid, tid = int(m.group(2)), int(m.group(3))
        just_rejected = await asyncio.to_thread(db_reject_sub, sid)
        if not just_rejected:
            await update.message.reply_text(f"ℹ️ Sub:{sid} از قبل رد/تایید شده بود — دوباره اجرا نشد.")
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Rejected subscription (command)", tid,
                "No-op (already decided)", detail=f"sub_id={sid}")
            return
        try:
            await context.bot.send_message(
                chat_id=tid,
                text="⚠️ درخواست شما تایید نشد.\n@manifestapply تماس بگیرید.")
        except Exception: pass
        await update.message.reply_text(f"❌ رد شد. Sub:{sid} User:{tid}")
        await asyncio.to_thread(
            db_log_audit, uid, admin_name, "Rejected subscription (command)", tid,
            "Success", detail=f"sub_id={sid}")
        return

    # /reply_<tid> <متن> — پاسخ ادمین به فیدبک/اعتراض کاربر
    m = re.match(r"/reply_(\d+)\s+([\s\S]+)", text)
    if m:
        tid, reply_text = int(m.group(1)), m.group(2).strip()
        try:
            await context.bot.send_message(
                chat_id=tid,
                text=f"📩 پاسخ تیم Manifest Apply:\n\n{reply_text}")
            await update.message.reply_text(f"✅ پاسخ برای کاربر {tid} ارسال شد.")
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Replied to user", tid, "Success",
                detail=reply_text[:200])
        except Exception as e:
            await update.message.reply_text(f"❌ ارسال پاسخ ناموفق: {e}")
            await asyncio.to_thread(
                db_log_audit, uid, admin_name, "Replied to user", tid, "Failure",
                detail=str(e)[:200])
        return

# ================================================================
# INLINE CALLBACK — دکمه‌های تایید/رد ادمین
# ================================================================
#
# ── جلوگیری از اجرای تکراری/هم‌زمان یک Callback ────────────────────
# دو ریسک جدا رو پوشش می‌ده:
#  ۱) دوتاپ سریع کاربر روی همون دکمه، یا دلیوری تکراری همون Update توسط
#     خود تلگرام (قطعی شبکه‌ی موقت وسط polling) — با id خودِ
#     callback_query تشخیص داده و نادیده گرفته می‌شه؛ id هر تپ واقعی
#     همیشه یکتاست، پس این فقط دلیوری تکراری *همون* رویداد رو می‌گیره.
#  ۲) دو تپ *متفاوت* اما پشت‌سرهم از همون کاربر (id های متفاوت،
#     رویدادهای واقعاً جدا) — با همون قفل per-user که dispatch() هم
#     استفاده می‌کنه (تعریف‌شده کنار _rate_limited) سریالایز می‌شن؛
#     Application اینجا با concurrent_updates=False (پیش‌فرض) اجرا
#     می‌شه یعنی الان هم پردازش Update ها به‌صورت سراسری صف می‌شه، ولی
#     این قفل صریح باعث می‌شه رفتار حتی اگه بعداً concurrent_updates یا
#     دیپلوی چند-Worker/وبهوک فعال بشه هم درست بمونه — بدون قفل صریح،
#     آماده‌سازی برای اون حالت‌ها یعنی هر Callback هندلر باید جداگانه
#     race-safe باشه که نگه‌داشتنش سخت‌تره.
_processed_callback_ids: dict[str, float] = {}
_CALLBACK_ID_TTL_S = 300  # تلگرام دیرتر از چند دقیقه همون Update رو دوباره دلیوری نمی‌کنه

def _is_duplicate_callback(query_id: str) -> bool:
    now = time.time()
    if len(_processed_callback_ids) > 2000:  # پاکسازی تنبل، بدون Task جدا
        for k, ts in list(_processed_callback_ids.items()):
            if now - ts > _CALLBACK_ID_TTL_S:
                _processed_callback_ids.pop(k, None)
    if query_id in _processed_callback_ids:
        return True
    _processed_callback_ids[query_id] = now
    return False

async def inline_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Wrapper نازک: قبل از رسیدن به منطق واقعی، دلیوری تکراری همون
    Callback رو رد می‌کنه و بقیه رو پشت قفل per-user سریالایز می‌کنه."""
    query = update.callback_query
    if not query or not query.data:
        return
    if _is_duplicate_callback(query.id):
        logger.info(f"[uid={query.from_user.id}] callback تکراری نادیده گرفته شد: {query.data[:40]}")
        try:
            await query.answer()
        except Exception:
            pass
        return
    async with _get_user_action_lock(query.from_user.id):
        await _inline_callback_impl(update, context)

async def _inline_callback_impl(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.data:
        return

    data = query.data
    uid  = query.from_user.id

    # ── رضایت‌سنجی (👍/👎) — هر کاربر فقط رأی خودش رو می‌تونه ثبت کنه ──
    # نکته: این callback باید قبل از چک ADMIN_CHAT_ID بیاد چون کاربر عادی
    # باید بتونه رأی بده، نه فقط ادمین.
    m = re.match(r"sat_(like|dislike)_(email|job)_(\d+)_(\d+)", data)
    if m:
        rating, service, target_uid, sub_id = m.group(1), m.group(2), int(m.group(3)), int(m.group(4))
        if uid != target_uid:
            await query.answer("این دکمه برای شما نیست.", show_alert=True)
            return
        try:
            newly_recorded = await asyncio.to_thread(db_log_satisfaction, uid, rating, service,
                                    sub_id if sub_id else None)
        except Exception as e:
            logger.error(f"db_log_satisfaction: {e}")
            # اگه خود دیتابیس خطا داد (نه این‌که قبلاً ثبت شده)، به نفع
            # کاربر فرض می‌کنیم اولین‌باره تا لااقل پیام معمولی رو ببینه.
            newly_recorded = True

        if not newly_recorded:
            # قبلاً برای همین (کاربر, سرویس, sub) رأی ثبت شده بود — دوتاپ
            # سریع روی دکمه یا دلیوری تکراری خودِ تلگرام. فقط دکمه‌ها رو
            # (اگه هنوز موجودن) پاک می‌کنیم و یک alert کوتاه می‌دیم؛ نه
            # ردیف تکراری توی آمار می‌ره، نه پیام تشکر دوباره می‌فرستیم.
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            await query.answer("رأی شما قبلاً ثبت شده — ممنون از بازخوردتون! 🙏", show_alert=False)
            return

        emoji = "👍" if rating == "like" else "👎"
        msg = "ممنون از بازخوردتون! 🙏" if rating == "like" else "متأسفیم که راضی نبودید. تیم @manifestapply در خدمت شماست."
        await query.answer(msg, show_alert=False)
        try:
            await query.edit_message_reply_markup(reply_markup=None)
            await context.bot.send_message(chat_id=uid, text=f"{emoji} بازخورد شما ثبت شد. {msg}")
        except Exception: pass
        await notify_admin(context.bot,
            f"{emoji} رضایت‌سنجی\nکاربر: {uid}\nسرویس: {service}\nنظر: {'راضی' if rating=='like' else 'ناراضی'}")
        return

    # ── AI Memory: ثبت وضعیت پاسخ استاد — هر کاربر فقط برای خودش ────
    m = re.match(r"replyset_(positive|negative|none)_(\d+)_(\d+)", data)
    if m:
        status, email_id, target_uid = m.group(1), int(m.group(2)), int(m.group(3))
        if uid != target_uid:
            await query.answer("این دکمه برای شما نیست.", show_alert=True)
            return
        try:
            await asyncio.to_thread(db_set_reply_status, uid, email_id, status)
        except Exception as e:
            logger.error(f"db_set_reply_status: {e}")
        label = {"positive": "✅ پاسخ مثبت", "negative": "❌ پاسخ منفی", "none": "🚫 بدون پاسخ"}[status]
        await query.answer(f"ثبت شد: {label}")
        try:
            orig = query.message.text or ""
            await query.edit_message_text(orig + f"\n\n📌 وضعیت ثبت‌شده: {label}", reply_markup=None)
        except Exception:
            pass
        return

    # ── دکمه‌های تایید/رد پرداخت — فقط ادمین ────────────────────────
    if ADMIN_CHAT_ID and uid != ADMIN_CHAT_ID:
        await query.answer("فقط ادمین می‌تواند این عمل را انجام دهد.", show_alert=True)
        return

    await query.answer()

    m = re.match(r"(pay_ok|pay_no)_(\d+)_(\d+)", data)
    if not m:
        return

    action, sid, tid = m.group(1), int(m.group(2)), int(m.group(3))
    admin_name = _admin_display_name(update)

    if action == "pay_ok":
        just_approved = await asyncio.to_thread(db_approve_sub, sid)
        if not just_approved:
            # قبلاً تایید شده (مثلاً ادمین دوبار روی دکمه تپ کرده یا تلگرام
            # callback رو دوبار فرستاده) — دوباره workflow اجرا نمی‌شه.
            result = "ℹ️ قبلاً تایید شده بود (دوباره اجرا نشد)"
            audit_result = "No-op (already approved)"
        else:
            await asyncio.to_thread(db_set_user, tid, status="approved")
            result = "✅ تایید شد"
            audit_result = "Success"
            try:
                await _resume_user_after_approval(context, sid, tid)
            except Exception as e:
                logger.error(f"inline approve notify: {e}")
        await asyncio.to_thread(
            db_log_audit, uid, admin_name, "Approved subscription (button)", tid,
            audit_result, detail=f"sub_id={sid}")
    else:
        just_rejected = await asyncio.to_thread(db_reject_sub, sid)
        if not just_rejected:
            # قبلاً رد شده بود (دوبار تپ روی دکمه) یا اصلاً pending نبوده —
            # دوباره پیام به کاربر نمی‌فرستیم که دوبار مزاحمش نشیم.
            result = "ℹ️ قبلاً رد شده بود (دوباره اجرا نشد)"
            audit_result = "No-op (already decided)"
        else:
            result = "❌ رد شد"
            audit_result = "Success"
            try:
                await context.bot.send_message(
                    chat_id=tid,
                    text=(
                        "⚠️ رسید پرداخت تایید نشد.\n\n"
                        "لطفاً با پشتیبانی تماس بگیرید:\n"
                        "@manifestapply"
                    )
                )
            except Exception as e:
                logger.error(f"inline reject notify: {e}")
        await asyncio.to_thread(
            db_log_audit, uid, admin_name, "Rejected subscription (button)", tid,
            audit_result, detail=f"sub_id={sid}")

    # ویرایش caption پیام ادمین
    try:
        orig = query.message.caption or ""
        await query.edit_message_caption(
            caption=orig + f"\n\n━━━━━━━━━━\n{result} توسط ادمین",
            reply_markup=None
        )
    except Exception as e:
        logger.error(f"inline edit caption: {e}")

# ================================================================
# ERROR HANDLER
# ================================================================
#
# ── اطلاع‌رسانی کرش/باگ به ادمین ────────────────────────────────
# قبلاً خطاهای handle‌نشده فقط توی لاگ سرور می‌رفتن — ادمین هیچ‌وقت
# نمی‌فهمید کاربری با باگ مواجه شده مگر خودش شکایت می‌کرد. حالا هر خطای
# غیرمنتظره (نه NetworkError/RetryAfter که موقتی و بی‌ضررن) به ادمین هم
# گزارش می‌شه. عمداً throttle شده: اگه یه خطای تکراری پشت‌سرهم (مثلاً یه
# باگ که با هر پیام یه کاربر خاص دوباره trigger می‌شه) چندین بار در چند
# ثانیه بیفته، فقط بار اول به ادمین اطلاع می‌ده و بقیه رو فقط لاگ می‌کنه —
# وگرنه یه کرش‌لوپ می‌تونه صدها پیام پشت‌سرهم به ادمین بفرسته و خودش یه
# مشکل جدید (Telegram flood control) بسازه.
_ERROR_NOTIFY_THROTTLE_SEC = 120
_last_error_notify: dict[str, float] = {}

def _should_notify_admin_for_error(err: BaseException) -> bool:
    key = f"{type(err).__name__}:{str(err)[:200]}"
    now = time.time()
    last = _last_error_notify.get(key, 0)
    if now - last < _ERROR_NOTIFY_THROTTLE_SEC:
        return False
    _last_error_notify[key] = now
    if len(_last_error_notify) > 500:  # پاکسازی تنبل، بدون Task جدا
        cutoff = now - _ERROR_NOTIFY_THROTTLE_SEC
        for k, ts in list(_last_error_notify.items()):
            if ts < cutoff:
                _last_error_notify.pop(k, None)
    return True

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, (NetworkError, TimedOut)):
        logger.warning(f"Network error: {context.error}")
        return
    if isinstance(context.error, RetryAfter):
        # Flood control تلگرام — خطای واقعی نیست، فقط باید کندتر ارسال کنیم
        logger.warning(f"Telegram flood control hit: retry_after={context.error.retry_after}s")
        return
    logger.error(f"Unhandled error: {context.error}")
    traceback.print_exc()
    if isinstance(update, Update) and update.effective_message:
        try:
            # قبلاً این پیام می‌گفت «دوباره /start بزنید» — که حس می‌داد
            # کاربر کاملاً از اول انداخته می‌شه بیرون و همه‌ی پیشرفتش (نام،
            # پرداخت، رزومه و...) از دست می‌ره. اطلاعات کاربر (user_data)
            # واقعاً پاک نمی‌شه؛ فقط همون یک قدم به مشکل خورده. پس به‌جای
            # وادار کردن به /start کامل، از همینجا با یه پیام دلگرم‌کننده
            # می‌گیم ادامه بده — چون واقعاً می‌تونه ادامه بده.
            await update.effective_message.reply_text(
                "⚠️ یه مشکل موقت پیش اومد — نگران نباشید، اطلاعاتتون گم نشده.\n"
                "لطفاً چند لحظه صبر کنید و دوباره امتحان کنید (همون دکمه یا پیام قبلی رو دوباره بفرستید).")
        except Exception: pass
    try:
        if _should_notify_admin_for_error(context.error):
            uid = None
            if isinstance(update, Update) and update.effective_user:
                uid = update.effective_user.id
            tb = "".join(traceback.format_exception(
                type(context.error), context.error, context.error.__traceback__))[-1200:]
            await notify_admin(context.bot,
                f"🐞 خطای غیرمنتظره در بات\n"
                f"کاربر: {uid or 'نامشخص'}\n"
                f"نوع خطا: {type(context.error).__name__}\n"
                f"پیام: {str(context.error)[:300]}\n\n"
                f"Traceback (آخرین بخش):\n{tb}")
    except Exception as e:
        # اطلاع‌رسانی به ادمین هیچ‌وقت نباید خودش باعث خطای جدید بشه
        logger.error(f"error_handler → notify_admin failed: {e}")

# ================================================================
# TASK 2 — Resume بعد از ری‌استارت (Session کامل توی دیتابیس)
# ================================================================
# قبلاً context.user_data فقط توی RAM بود — با ری‌استارت سرور، بسته شدن
# پروسه توسط PythonAnywhere، یا کرش، کل مکالمه (نام، SMTP، لیست اساتید،
# استادِ در حال بررسی، هر state ای که بود) از بین می‌رفت و کاربر مجبور
# می‌شد از اول شروع کنه. این کلاس همون user_data رو با هر تغییر توی
# جدول bot_sessions می‌نویسه و موقع بالا اومدن بات، همه رو از همون‌جا
# برمی‌گردونه — بدون نیاز به این‌که کاربر دوباره /start بزنه؛ همین که
# اولین پیامش بعد از ری‌استارت برسه، دقیقاً از همون step/همون استاد ادامه
# پیدا می‌کنه.
class SQLiteUserDataPersistence(BasePersistence):
    def __init__(self, update_interval: float = 1.0):
        super().__init__(
            store_data=PersistenceInput(
                bot_data=False, chat_data=False, user_data=True, callback_data=False
            ),
            update_interval=update_interval,
        )
        self._user_data: dict[int, dict] = {}

    async def get_user_data(self) -> dict:
        if not self._user_data:
            raw = await asyncio.to_thread(db_load_all_sessions)
            # پسورد SMTP از این به بعد روی دیسک رمزنگاری‌شده ذخیره می‌شه؛
            # اینجا موقع بازیابی، فقط برای همون sessionهایی که رمزنگاری‌شده
            # هستن رمزگشایی می‌کنیم. sessionهای قدیمی‌تر از این فیکس که هنوز
            # plaintext ذخیره شدن هم دست‌نخورده کار می‌کنن (_decrypt_secret
            # روی مقدار plaintext هیچ کاری نمی‌کنه) — از اولین بار که همون
            # کاربر دوباره پسوردش رو وارد کنه یا session‌اش آپدیت بشه،
            # نسخه‌ی روی دیسک هم رمزنگاری‌شده می‌شه.
            self._user_data = {}
            for uid, d in raw.items():
                d = dict(d)
                if S_SMTP_P in d:
                    d[S_SMTP_P] = _decrypt_secret(d[S_SMTP_P], field_label=f"smtp_p (uid={uid})")
                self._user_data[uid] = d
            logger.info(f"🔄 {len(self._user_data)} session از دیتابیس بازیابی شد")
        # PTB روی دیکشنری‌های داخلی deepcopy می‌کنه، خودمون نیازی نداریم
        return {uid: dict(d) for uid, d in self._user_data.items()}

    async def update_user_data(self, user_id: int, data: dict) -> None:
        # نسخه‌ی in-memory عمداً plaintext می‌مونه — همینه که حین اجرا برای
        # لاگین واقعی SMTP استفاده می‌شه؛ فقط چیزی که روی دیسک می‌ره رمزنگاری
        # می‌شه، تا اگه فایل دیتابیس لو بره پسورد ایمیل کاربرها لو نره.
        self._user_data[user_id] = dict(data)
        try:
            to_store = dict(data)
            if S_SMTP_P in to_store:
                to_store[S_SMTP_P] = _encrypt_secret(to_store[S_SMTP_P])
            await asyncio.to_thread(
                db_save_session, user_id, json.dumps(to_store, ensure_ascii=False, default=str)
            )
        except Exception as e:
            # هیچ‌وقت نباید بذاریم یک خطای serialize کل پردازش پیام رو خراب
            # کنه — فقط لاگ می‌کنیم؛ نسخه‌ی قبلی توی دیتابیس دست‌نخورده می‌مونه.
            logger.error(f"⚠️ ذخیره‌ی session برای uid={user_id} ناموفق بود: {e}")

    async def drop_user_data(self, user_id: int) -> None:
        self._user_data.pop(user_id, None)
        await asyncio.to_thread(db_delete_session, user_id)

    async def refresh_user_data(self, user_id: int, user_data: dict) -> None:
        pass  # چیزی برای refresh کردن جدا از خودِ user_data نداریم

    # ── این پروژه فقط از user_data استفاده می‌کنه؛ بقیه‌ی متدهای
    # BasePersistence (بات/چت/callback_data/conversations) عمداً no-op ان.
    async def get_bot_data(self) -> dict:
        return {}

    async def update_bot_data(self, data: dict) -> None:
        pass

    async def refresh_bot_data(self, bot_data: dict) -> None:
        pass

    async def get_chat_data(self) -> dict:
        return {}

    async def update_chat_data(self, chat_id: int, data: dict) -> None:
        pass

    async def refresh_chat_data(self, chat_id: int, chat_data: dict) -> None:
        pass

    async def drop_chat_data(self, chat_id: int) -> None:
        pass

    async def get_callback_data(self):
        return None

    async def update_callback_data(self, data) -> None:
        pass

    async def get_conversations(self, name: str):
        return {}

    async def update_conversation(self, name: str, key, new_state) -> None:
        pass

    async def flush(self) -> None:
        pass

# ================================================================
# RUN
# ================================================================

# ================================================================
# RUN
# ================================================================
# نکته‌ی تستیبیلیتی: قبلاً ساخت Application و ثبت همه‌ی handlerها این‌جا،
# بیرون از `if __name__ == "__main__":`، بی‌قید و شرط در همون لحظه‌ی
# import ماژول اجرا می‌شد — یعنی حتی صرفِ `import manifest_services_bot`
# (مثلاً برای تست یک تابع منطق محض مثل _is_unsafe_ip) کافی بود که یک
# Application واقعی تلگرام ساخته و کامل handler-registration بشه. الان
# این کار توی `main()` جمع شده و فقط وقتی فایل مستقیم اجرا بشه (نه وقتی
# import بشه) انجام می‌شه — دقیقاً همون چیزی که `if __name__` قرار بود
# از اول تضمین کنه ولی این تیکه ازش بیرون مونده بود.
app = None  # توسط main() قبل از اولین استفاده مقداردهی می‌شه (نگاه کن به
            # نکته‌ی بالا: خط ۱۵۱۴۵/۱۵۱۵۶ به این global وابسته‌ان — چون
            # این توابع فقط از داخل یک هندلر/ترد پس‌زمینه‌ی زنده صدا زده
            # می‌شن، و آن‌ها فقط زمانی اجرا می‌شن که بات واقعاً از طریق
            # main() بالا اومده باشه، ترتیب همیشه درسته)

def _build_app():
    req = HTTPXRequest(connect_timeout=30, read_timeout=60,
                       write_timeout=60, pool_timeout=60)
    a = (Application.builder()
           .token(BOT_TOKEN)
           .request(req)
           .persistence(SQLiteUserDataPersistence())
           .post_init(_on_bot_startup)
           .build())

    a.add_handler(CommandHandler("start",  cmd_start))
    a.add_handler(CommandHandler("cancel", cmd_cancel))
    # نکته‌ی مهم: CommandHandler("approve", ...) فقط دقیقاً روی "/approve" مچ
    # می‌شه، نه روی "/approve_123_456" — چون تلگرام کل "approve_123_456" رو
    # یک توکن دستور واحد در نظر می‌گیره (زیرخط و عدد جزو حروف مجاز دستورن).
    # یعنی این خط‌ها همیشه بی‌اثر بودن و دستورات تایید/رد ادمین هیچ‌وقت واقعاً
    # اجرا نمی‌شدن! به‌جاش با یک MessageHandler روی filters.COMMAND می‌گیریم
    # و خود cmd_admin با regex داخلی‌اش دستور دقیق رو تشخیص می‌ده.
    a.add_handler(MessageHandler(filters.COMMAND, cmd_admin))
    a.add_handler(MessageHandler(filters.PHOTO, photo_handler))
    a.add_handler(MessageHandler(filters.Document.ALL, document_handler))
    a.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, dispatch))
    a.add_handler(CallbackQueryHandler(inline_callback))
    a.add_error_handler(error_handler)
    return a

def run():
    logger.info("🚀 Services Bot starting...")
    app.run_polling(
        drop_pending_updates=True,
        poll_interval=1.0,
        timeout=30, read_timeout=30,
        write_timeout=30, connect_timeout=30,
        close_loop=False)

def main():
    global app
    app = _build_app()
    while True:
        try:
            run()
        except KeyboardInterrupt:
            logger.info("🛑 Stopped.")
            sys.exit(0)
        except Exception as e:
            logger.critical(f"Top-level crash: {e}")
            traceback.print_exc()
            logger.info("🔄 Restarting in 5s...")
            time.sleep(5)

if __name__ == "__main__":
    main()
