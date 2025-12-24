import os
import time
import requests
from datetime import datetime, timezone

BASE_URL = "https://api.pachca.com/api/shared/v1"
TOKEN = os.environ.get("PACHCA_TOKEN")
CHANNEL_CHAT_ID = os.environ.get("PACHCA_CHAT_ID")  # id канала

if not TOKEN or not CHANNEL_CHAT_ID:
    raise SystemExit("Нужны переменные окружения PACHCA_TOKEN и PACHCA_CHAT_ID")

HEADERS = {"Authorization": f"Bearer {TOKEN}"}


def iso_to_local(iso_str: str) -> str:
    # API отдает UTC (Z). Для txt хватит UTC, но можно и локализовать при желании.
    dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def fetch_all_users() -> dict[int, str]:
    # /users uses cursor pagination
    users = {}
    cursor = None
    while True:
        params = {"limit": 50}
        if cursor:
            params["cursor"] = cursor
        r = requests.get(f"{BASE_URL}/users", headers=HEADERS, params=params, timeout=60)
        r.raise_for_status()
        payload = r.json()
        for u in payload.get("data", []):
            uid = u["id"]
            name = (u.get("first_name", "") + " " + u.get("last_name", "")).strip()
            if not name:
                name = u.get("nickname") or f"user_{uid}"
            users[uid] = name

        cursor = (payload.get("meta", {}) or {}).get("paginate", {}).get("next_page")
        if not cursor:
            break
    return users


def fetch_messages(chat_id: str) -> list[dict]:
    # /messages uses page/per (max per=50) and returns newest first by default.
    # Берем все страницы, потом развернем в хронологию.
    all_msgs = []
    page = 1
    while True:
        params = {"chat_id": chat_id, "per": 50, "page": page}
        r = requests.get(f"{BASE_URL}/messages", headers=HEADERS, params=params, timeout=60)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            break
        all_msgs.extend(data)
        page += 1

        # маленькая пауза, чтобы не упереться в лимиты
        time.sleep(0.15)

    # хронология: от старых к новым
    all_msgs.sort(key=lambda m: m["id"])
    return all_msgs


def render_message(m: dict, users: dict[int, str]) -> str:
    author = users.get(m.get("user_id"), f"user_{m.get('user_id')}")
    ts = iso_to_local(m.get("created_at", ""))
    content = (m.get("content") or "").strip()
    url = m.get("url") or ""
    files = m.get("files") or []

    out = []
    out.append(f"[{ts}] {author}")
    if url:
        out.append(f"link: {url}")
    if content:
        out.append(content)
    if files:
        out.append("files:")
        for f in files:
            out.append(f"- {f.get('name')} ({f.get('file_type')}): {f.get('url')}")
    return "\n".join(out)


def export_channel_with_threads(channel_chat_id: str, out_path: str):
    users = fetch_all_users()
    channel_msgs = fetch_messages(channel_chat_id)

    lines = []
    lines.append(f"# Export from Pachca chat_id={channel_chat_id}")
    lines.append(f"# exported_at={datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
    lines.append("")

    for m in channel_msgs:
        # отсекаем “служебку” при желании: но лучше сначала выгрузить всё
        lines.append("## MESSAGE")
        lines.append(render_message(m, users))

        thread = m.get("thread")
        if thread and thread.get("chat_id"):
            t_chat_id = str(thread["chat_id"])
            lines.append("")
            lines.append(f"### THREAD (thread_chat_id={t_chat_id})")
            thread_msgs = fetch_messages(t_chat_id)
            for tm in thread_msgs:
                lines.append(render_message(tm, users))
                lines.append("")

        lines.append("\n---\n")

    with open(out_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    export_channel_with_threads(str(CHANNEL_CHAT_ID), "pachca_export.txt")
    print("OK: pachca_export.txt created")
