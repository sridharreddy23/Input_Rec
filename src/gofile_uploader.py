"""
GoFile uploader — token-friendly, robust, progress + debug saving.

Usage examples:
    # env var recommended
    import os
    TOKEN = os.environ.get("GOFILE_TOKEN")
    link = upload_to_gofile("/path/to/dump.ts", api_token=TOKEN)

    # or pass token directly (not recommended for production)
    link = upload_to_gofile("/path/to/dump.ts", api_token="your-token", show_progress=True)
"""
import os
import time
import logging
import random
import re
from typing import Optional, Callable, Tuple, Dict, Any
from contextlib import contextmanager
import requests

log = logging.getLogger("gofile")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

UPLOAD_URL = "https://upload.gofile.io/uploadfile"
DEFAULT_RETRIES = int(os.environ.get("GOFILE_RETRIES", "3"))
DEFAULT_TIMEOUT = int(os.environ.get("GOFILE_TIMEOUT", "900"))  # seconds for large uploads
MAX_BACKOFF = int(os.environ.get("GOFILE_MAX_BACKOFF", "30"))  # seconds
DEBUG_DIR = os.environ.get("GOFILE_DEBUG_DIR", ".")  # directory for debug files


# optional dependencies
try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor
except Exception:
    MultipartEncoder = None
    MultipartEncoderMonitor = None


class ProgressFile:
    """Fallback file wrapper updating tqdm when read() is called."""
    def __init__(self, fh: Any, pbar: Optional[Any] = None):
        self._fh = fh
        self._pbar = pbar

    def read(self, size: int = -1) -> bytes:
        chunk = self._fh.read(size)
        if chunk and self._pbar:
            try:
                self._pbar.update(len(chunk))
            except Exception:
                pass
        return chunk

    def __getattr__(self, name: str) -> Any:
        return getattr(self._fh, name)


def _make_monitor_callback(pbar: Optional["tqdm"]) -> Callable[[Any], None]:
    """
    Returns callback for MultipartEncoderMonitor that updates tqdm or logs progress.
    """
    if pbar is None:
        last_logged: Dict[str, int] = {"pct": -1}

        def _log_cb(monitor: Any) -> None:
            pos = getattr(monitor, "bytes_read", 0)
            total = getattr(monitor, "len", 0)
            pct = int(100 * pos / total) if total else 0
            if pos == total or pct - last_logged["pct"] >= 5:
                last_logged["pct"] = pct
                log.info("Upload progress: %d%% (%d/%d)", pct, pos, total)
        return _log_cb

    def _tqdm_cb(monitor: Any) -> None:
        try:
            new_pos = monitor.bytes_read
            delta = new_pos - getattr(_tqdm_cb, "last", 0)
            if delta > 0:
                pbar.update(delta)
            _tqdm_cb.last = new_pos
        except Exception:
            pass

    _tqdm_cb.last = 0
    return _tqdm_cb


def _should_retry_on_status(status_code: int) -> bool:
    """Retry for server errors (5xx) and 429 Too Many Requests."""
    if status_code >= 500:
        return True
    if status_code == 429:
        return True
    return False


def _sleep_backoff(attempt: int, max_backoff: int = MAX_BACKOFF):
    """Exponential backoff with full jitter."""
    base = min(max_backoff, (2 ** (attempt - 1)))
    sleep_for = random.uniform(base / 2, base)
    log.info("Backing off for %.1f seconds before retry", sleep_for)
    time.sleep(sleep_for)


def _sanitize_filename(filename: str) -> str:
    """Sanitize filename to prevent path traversal and invalid characters."""
    # Remove path separators and directory navigation
    safe = os.path.basename(filename)
    # Replace invalid filesystem characters with underscore
    safe = re.sub(r'[<>:"|?*\x00-\x1f]', '_', safe)
    # Limit length to prevent filesystem issues
    if len(safe) > 200:
        name, ext = os.path.splitext(safe)
        safe = name[:200] + ext
    return safe or "unknown_file"


def _save_failure_debug(filename: str, resp: requests.Response) -> Tuple[str, str]:
    """Save server response to disk for post-mortem."""
    ts = int(time.time())
    safe_name = _sanitize_filename(filename)
    
    # Ensure debug directory exists
    debug_dir = os.path.abspath(DEBUG_DIR)
    os.makedirs(debug_dir, exist_ok=True)
    
    json_path = os.path.join(debug_dir, f"upload-failure-{safe_name}-{ts}.debug.json")
    raw_path = os.path.join(debug_dir, f"upload-failure-{safe_name}-{ts}.debug.txt")
    
    json_path_written = False
    raw_path_written = False
    
    try:
        # try to save text (often JSON)
        with open(json_path, "w", encoding="utf-8") as fh:
            fh.write(resp.text if hasattr(resp, "text") else "")
        log.warning("Saved server response body to %s", json_path)
        json_path_written = True
    except Exception as ex:
        log.warning("Failed to save JSON debug file: %s", ex)

    try:
        with open(raw_path, "wb") as fh:
            content = resp.content if hasattr(resp, "content") and resp.content else (
                (resp.text or "").encode("utf-8", "replace") if hasattr(resp, "text") else b""
            )
            fh.write(content)
        log.warning("Saved raw server response to %s", raw_path)
        raw_path_written = True
    except Exception as ex:
        log.warning("Failed to save raw debug file: %s", ex)

    return (json_path if json_path_written else "", raw_path if raw_path_written else "")


@contextmanager
def _progress_bar_context(total: int, filename: str, show_progress: bool):
    """Context manager for progress bar cleanup."""
    pbar = None
    try:
        if show_progress and tqdm is not None:
            pbar = tqdm(total=total, unit="B", unit_scale=True, desc=filename)
        yield pbar
    finally:
        if pbar:
            try:
                pbar.close()
            except Exception:
                pass


def upload_to_gofile(
    file_path: str,
    api_token: Optional[str] = None,  # prefer Authorization header
    token: Optional[str] = None,      # backward-compatible form field fallback
    folder_id: Optional[str] = None,
    retries: int = DEFAULT_RETRIES,
    timeout: int = DEFAULT_TIMEOUT,
    show_progress: bool = True,
) -> str:
    """
    Upload file to GoFile and return downloadPage link.
    
    Args:
        file_path: Path to the file to upload
        api_token: API token sent as Authorization: Bearer header (preferred)
        token: Legacy token sent as form field (fallback for backward compatibility)
        folder_id: Optional GoFile folder ID to upload to
        retries: Number of retry attempts (default: 3)
        timeout: Request timeout in seconds (default: 900)
        show_progress: Whether to show upload progress (default: True)
    
    Returns:
        The download page URL from GoFile
    
    Raises:
        FileNotFoundError: If the file doesn't exist
        RuntimeError: If upload fails after all retries
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    filename = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    last_exception: Optional[Exception] = None

    for attempt in range(1, retries + 1):
        try:
            log.info("Uploading (attempt %d/%d): %s (%.2f MB)", attempt, retries, filename, file_size / (1024 * 1024))

            # prepare headers
            headers: Dict[str, str] = {}
            if api_token:
                headers["Authorization"] = f"Bearer {api_token}"
            elif token:
                # If only token is provided, use it in Authorization header too
                headers["Authorization"] = f"Bearer {token}"

            # --- Use requests-toolbelt MultipartEncoder if available (accurate progress) ---
            if MultipartEncoder is not None:
                with open(file_path, "rb") as f:
                    fields: Dict[str, Any] = {"file": (filename, f)}
                    # Include older token field if provided (for backward compatibility)
                    if token and not api_token:
                        fields["token"] = str(token)
                    if folder_id:
                        fields["folderId"] = str(folder_id)

                    encoder = MultipartEncoder(fields=fields)
                    monitor_cb = None

                    with _progress_bar_context(encoder.len, filename, show_progress) as pbar:
                        if show_progress:
                            if pbar is not None:
                                monitor_cb = _make_monitor_callback(pbar)
                            else:
                                log.info("tqdm not installed; progress will be logged.")
                                monitor_cb = _make_monitor_callback(None)

                        monitor = MultipartEncoderMonitor(
                            encoder, 
                            monitor_cb if monitor_cb else lambda m: None
                        )
                        # ensure content-type included alongside Authorization header
                        all_headers = dict(headers)
                        all_headers["Content-Type"] = monitor.content_type

                        resp = requests.post(UPLOAD_URL, data=monitor, headers=all_headers, timeout=timeout)

            else:
                # fallback: requests.files with ProgressFile wrapper
                if show_progress and tqdm is None:
                    log.info("requests-toolbelt not installed; continuing without visual progress bar.")

                with open(file_path, "rb") as fh, \
                     _progress_bar_context(file_size, filename, show_progress) as pbar:
                    wrapped_fh = ProgressFile(fh, pbar=pbar if show_progress else None)
                    files = {"file": (filename, wrapped_fh)}
                    data: Dict[str, str] = {}
                    # include token as form field if provided (fallback); api_token still sent as header
                    if token and not api_token:
                        data["token"] = token
                    if folder_id:
                        data["folderId"] = folder_id

                    resp = requests.post(UPLOAD_URL, files=files, data=data, headers=headers, timeout=timeout)

            # --- handle response ---
            status = getattr(resp, "status_code", None)
            if status is None:
                raise RuntimeError("No status code in response")
                
            try:
                text = resp.text
            except Exception:
                text = "<no-body>"

            if not (200 <= status < 300):
                log.warning("Upload returned non-2xx status: %d", status)
                log.debug("Response headers: %s", dict(resp.headers))
                log.debug("Response body (first 500 chars): %s", text[:500])
                
                json_path, raw_path = _save_failure_debug(filename, resp)

                # honor Retry-After for 429
                if status == 429:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        try:
                            ra_val = int(ra)
                            log.info("Server requested Retry-After: %s seconds", ra_val)
                            time.sleep(ra_val)
                        except (ValueError, TypeError) as ex:
                            log.warning("Invalid Retry-After header value '%s': %s", ra, ex)

                if _should_retry_on_status(status):
                    if attempt == retries:
                        debug_msg = f"Saved debug to {json_path}, {raw_path}" if (json_path or raw_path) else "Debug files not saved"
                        raise RuntimeError(f"Upload failed with status {status} after {retries} attempts. {debug_msg}")
                    _sleep_backoff(attempt)
                    continue
                else:
                    debug_msg = f"See debug files: {json_path}, {raw_path}" if (json_path or raw_path) else "Check logs for details"
                    raise RuntimeError(f"Upload failed with status {status}. {debug_msg}")

            # success path
            try:
                result = resp.json()
            except (ValueError, TypeError) as ex:
                raise RuntimeError(f"Failed to parse JSON response. HTTP {status}: {text[:200]}") from ex

            if result.get("status") != "ok":
                error_msg = result.get("message", "Unknown error")
                raise RuntimeError(f"GoFile API error: {error_msg} (status: {result.get('status', 'unknown')})")

            data = result.get("data", {})
            link = data.get("downloadPage")
            if not link:
                raise RuntimeError("No downloadPage in response. Response data: " + str(data)[:200])

            log.info("Upload successful: %s", link)
            return link

        except (FileNotFoundError, RuntimeError) as e:
            # Don't retry on these errors
            raise
        except Exception as e:
            last_exception = e
            log.warning("Attempt %d/%d failed: %s", attempt, retries, e, exc_info=log.isEnabledFor(logging.DEBUG))
            if attempt == retries:
                raise RuntimeError(f"Upload failed after {retries} attempts. Last error: {e}") from e
            _sleep_backoff(attempt)

    # This should never be reached, but included for type safety
    raise RuntimeError("Upload failed after all retries") from last_exception

