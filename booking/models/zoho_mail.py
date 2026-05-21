"""
booking/models/zoho_mail.py

Zoho Mail API wrapper for rusvin@xpresshealth.ie
Docs: https://www.zoho.com/mail/help/api/
"""
import requests
from datetime import datetime


ZOHO_API_BASE = "https://www.zohoapis.eu"   # EU data centre – change to zoho.com if US


class ZohoMail:
    def __init__(self, access_token: str, account_email: str = "rusvin@xpresshealth.ie"):
        """
        access_token  – OAuth 2.0 bearer token (short-lived; refresh before each request set)
        account_email – the mailbox to operate on
        """
        self.access_token = access_token
        self.account_email = account_email
        self._account_id: str | None = None   # resolved lazily

    # ------------------------------------------------------------------ #
    #  Internal helpers
    # ------------------------------------------------------------------ #

    def _headers(self) -> dict:
        return {
            "Authorization": f"Zoho-oauthtoken {self.access_token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _get_account_id(self) -> str:
        """
        Resolve the numeric Zoho account-id for self.account_email.
        Result is cached for the lifetime of this instance.
        """
        if self._account_id:
            return self._account_id

        url = f"{ZOHO_API_BASE}/accounts"
        resp = requests.get(url, headers=self._headers(), timeout=10)
        resp.raise_for_status()

        accounts = resp.json().get("data", [])
        for acc in accounts:
            if acc.get("mailAddress", "").lower() == self.account_email.lower():
                self._account_id = str(acc["accountId"])
                return self._account_id

        # Fallback: use the first account
        if accounts:
            self._account_id = str(accounts[0]["accountId"])
            return self._account_id

        raise ValueError(f"No Zoho account found for {self.account_email}")

    # ------------------------------------------------------------------ #
    #  Public API
    # ------------------------------------------------------------------ #

    def get_folders(self) -> list[dict]:
        """Return all mail folders for the account."""
        account_id = self._get_account_id()
        url = f"{ZOHO_API_BASE}/accounts/{account_id}/folders"
        resp = requests.get(url, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        return resp.json().get("data", [])

    def get_messages(
        self,
        folder_id: str = None,
        search: str = "",
        page: int = 1,
        per_page: int = 20,
    ) -> tuple[list[dict], int]:
        """
        Fetch a paginated list of email summaries.

        Returns (messages, total_count).
        Each message dict is normalised for easy template rendering.
        """
        account_id = self._get_account_id()

        # If no folder supplied, use INBOX
        if not folder_id:
            folder_id = self._get_inbox_folder_id(account_id)

        start = (page - 1) * per_page
        params = {
            "start": start,
            "limit": per_page,
        }
        if search:
            params["searchKey"] = search

        url = f"{ZOHO_API_BASE}/accounts/{account_id}/messages/view"
        resp = requests.get(url, headers=self._headers(), params=params, timeout=15)
        resp.raise_for_status()

        raw = resp.json()
        messages_raw = raw.get("data", [])
        total = int(raw.get("data", {}).get("total", len(messages_raw))) if isinstance(raw.get("data"), dict) else len(messages_raw)

        messages = [self._normalise_message(m) for m in messages_raw if isinstance(m, dict)]
        return messages, total

    def get_message_detail(self, message_id: str) -> dict:
        """Fetch full email content (body + headers)."""
        account_id = self._get_account_id()
        url = f"{ZOHO_API_BASE}/accounts/{account_id}/messages/{message_id}/content"
        resp = requests.get(url, headers=self._headers(), timeout=15)
        resp.raise_for_status()
        data = resp.json().get("data", {})
        return self._normalise_message(data, full=True)

    # ------------------------------------------------------------------ #
    #  Private helpers
    # ------------------------------------------------------------------ #

    def _get_inbox_folder_id(self, account_id: str) -> str:
        folders = self.get_folders()
        for f in folders:
            if f.get("folderName", "").upper() == "INBOX":
                return str(f["folderId"])
        if folders:
            return str(folders[0]["folderId"])
        raise ValueError("No folders found on account")

    @staticmethod
    def _normalise_message(raw: dict, full: bool = False) -> dict:
        """Map raw Zoho API fields → clean dict used by views & templates."""
        sent_ts = raw.get("sentDateInGMT") or raw.get("receivedTime", "")
        try:
            sent_dt = datetime.utcfromtimestamp(int(sent_ts) / 1000) if sent_ts else None
        except (ValueError, TypeError):
            sent_dt = None

        msg = {
            "message_id": raw.get("messageId", ""),
            "subject": raw.get("subject") or "(no subject)",
            "from_email": raw.get("fromAddress", ""),
            "from_name": raw.get("sender", raw.get("fromAddress", "")),
            "to_email": raw.get("toAddress", ""),
            "date": sent_dt.strftime("%d %b %Y, %H:%M") if sent_dt else "—",
            "date_raw": sent_dt,
            "is_read": raw.get("isRead", True),
            "has_attachment": raw.get("hasAttachment", False),
            "folder": raw.get("folderId", ""),
            "summary": raw.get("summary", ""),
            "flag": raw.get("flagid", 0),
        }
        if full:
            msg["body_html"] = raw.get("htmlBody", "")
            msg["body_text"] = raw.get("textBody", "")
        return msg