#!/usr/bin/env python3
"""
DOC Automation Scheduler - Runs workflows every 3 hours for More Retail
"""

import os
import json
import base64
import tempfile
import time
import logging
import schedule
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import re
import warnings
import io

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# Add LlamaParse import
try:
    from llama_cloud_services import LlamaExtract
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False
    print("WARNING: LlamaParse not available. Install with: pip install llama-cloud-services")

warnings.filterwarnings("ignore")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('doc_scheduler.log'),
        logging.StreamHandler()
    ]
)

# Hardcoded configuration for DOC platform
CONFIG = {
    'mail': {
        'gdrive_folder_id': '1C251csI1oOeX_skv7mfqpZB0NbyLLd9d',
        'sender': 'docs@more.in',
        'search_term': 'grn',
        'attachment_filter': '',
        'days_back': 3,
        'max_results': 1000
    },
    'sheet': {
        'llama_api_key': '',  # Will be set from environment
        'llama_agent': 'More retail Agent',
        'drive_folder_id': '1C251csI1oOeX_skv7mfqpZB0NbyLLd9d',
        'spreadsheet_id': '16y9DAK2tVHgnZNnPeRoSSPPE2NcspW_qqMF8ZR8OOC0',
        'sheet_range': 'mrgrn',
        'days_back': 3,
        'max_files': 1000
    },
    'workflow_log': {
        'spreadsheet_id': '16y9DAK2tVHgnZNnPeRoSSPPE2NcspW_qqMF8ZR8OOC0',
        'sheet_range': 'docs_workflow_logs'
    },
    'notifications': {
        'recipients': ['keyur@thebakersdozen.in'],
        'sender_email': 'sneha.p@thebakersdozen.in'  # Will be auto-populated from authenticated user
    },
    'credentials_path': 'credentials.json',
    'token_path': 'token.json'
}


class DocAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None
        
        # API scopes
        self.gmail_scopes = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.send'
        ]
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']
        
        # Stats tracking
        self.stats = {
            'mail': {
                'emails_checked': 0,
                'attachments_found': 0,
                'attachments_uploaded': 0,
                'attachments_skipped': 0,
                'upload_failed': 0
            },
            'sheet': {
                'files_found': 0,
                'files_processed': 0,
                'files_skipped': 0,
                'files_failed': 0,
                'rows_added': 0
            }
        }
    
    def log(self, message: str, level: str = "INFO"):
        """Log message with appropriate level"""
        if level.upper() == "ERROR":
            logging.error(message)
        elif level.upper() == "WARNING":
            logging.warning(message)
        else:
            logging.info(message)
    
    def authenticate(self):
        """Authenticate using local credentials file"""
        try:
            self.log("Starting authentication process...", "INFO")
            
            creds = None
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))
            
            # Load token if exists
            if os.path.exists(CONFIG['token_path']):
                creds = Credentials.from_authorized_user_file(CONFIG['token_path'], combined_scopes)
            
            # Refresh or get new credentials
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    self.log("Refreshing expired token...", "INFO")
                    creds.refresh(Request())
                else:
                    if not os.path.exists(CONFIG['credentials_path']):
                        self.log(f"Credentials file not found: {CONFIG['credentials_path']}", "ERROR")
                        return False
                    
                    self.log("Starting new OAuth flow...", "INFO")
                    flow = InstalledAppFlow.from_client_secrets_file(
                        CONFIG['credentials_path'], combined_scopes)
                    creds = flow.run_local_server(port=0)
                
                # Save credentials
                with open(CONFIG['token_path'], 'w') as token:
                    token.write(creds.to_json())
                self.log("Token saved successfully", "INFO")
            
            # Build services
            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)
            
            # Get authenticated user's email for sender
            try:
                profile = self.gmail_service.users().getProfile(userId='me').execute()
                CONFIG['notifications']['sender_email'] = profile['emailAddress']
                self.log(f"Authenticated as: {profile['emailAddress']}", "INFO")
            except Exception as e:
                self.log(f"Could not get user profile: {str(e)}", "WARNING")
            
            self.log("Authentication successful!", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False
    
    def send_email_notification(self, summary_data: dict):
        """Send email notification with workflow summary"""
        try:
            self.log("Preparing email notification...", "INFO")
            
            # Get sender email from authenticated user
            sender_email = CONFIG['notifications']['sender_email']
            
            # Create email body in the required format
            subject = f"DOC Automation Workflow Summary - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Format the email body as requested
            body_lines = [
                "BigBasket Automation Workflow Summary",
                f"Workflow Time: {summary_data.get('workflow_start', '')} to {summary_data.get('workflow_end', '')}",
                "",
                f"Duration: {summary_data.get('total_duration', '0')}",
                "",
                f"Status: {summary_data.get('status', 'Unknown')}",
                "",
                "📧 Mail to Drive Workflow",
                f"Days Back Parameter: {summary_data.get('mail_days_back', 7)} days",
                f"Number of Mails Checked: {summary_data.get('mail_emails_checked', 0)}",
                f"Number of Attachments Found: {summary_data.get('mail_attachments_found', 0)}",
                f"Number of Attachments Uploaded: {summary_data.get('mail_attachments_uploaded', 0)}",
                f"Number of Attachments Skipped: {summary_data.get('mail_attachments_skipped', 0)}",
                f"Failed to Upload: {summary_data.get('mail_upload_failed', 0)}",
                "",
                "📊 Drive to Sheet Workflow",
                f"Days Back Parameter: {summary_data.get('sheet_days_back', 7)} days",
                f"Number of Files Found: {summary_data.get('sheet_files_found', 0)}",
                f"Number of Files Processed: {summary_data.get('sheet_files_processed', 0)}",
                f"Number of Files Skipped: {summary_data.get('sheet_files_skipped', 0)}",
                f"Number of Files Failed to Process: {summary_data.get('sheet_files_failed', 0)}",
                f"Duplicate Records Removed: {summary_data.get('sheet_duplicates_removed', 0)}",
                "",
                "=" * 50,
                "This is an automated report from DOC Automation Scheduler.",
                ""
            ]
            
            email_body = "\n".join(body_lines)
            
            # Create message
            message = self.create_email_message(
                sender=sender_email,
                to=CONFIG['notifications']['recipients'],
                subject=subject,
                body_text=email_body
            )
            
            # Send email
            sent_message = self.gmail_service.users().messages().send(
                userId='me',
                body=message
            ).execute()
            
            self.log(f"Email notification sent successfully! Message ID: {sent_message['id']}", "INFO")
            return True
            
        except Exception as e:
            self.log(f"Failed to send email notification: {str(e)}", "ERROR")
            return False
    
    def create_email_message(self, sender: str, to: list, subject: str, body_text: str) -> dict:
        """Create an email message in Gmail format"""
        # Create email headers
        message_parts = [
            f"From: {sender}",
            f"To: {', '.join(to)}",
            f"Subject: {subject}",
            "Content-Type: text/plain; charset=utf-8",
            "MIME-Version: 1.0",
            "",
            body_text
        ]
        
        message = "\n".join(message_parts)
        
        # Encode message in base64
        encoded_message = base64.urlsafe_b64encode(message.encode("utf-8")).decode("utf-8")
        
        return {
            'raw': encoded_message
        }
    
    def search_emails(self, sender: str = "", search_term: str = "", 
                     days_back: int = 7, max_results: int = 50) -> List[Dict]:
        """Search for emails with attachments"""
        try:
            query_parts = ["has:attachment"]
            
            if sender:
                query_parts.append(f'from:"{sender}"')  
            
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            
            query = " ".join(query_parts)
            self.log(f"[SEARCH] Searching Gmail with query: {query}")
            
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            
            messages = result.get('messages', [])
            self.log(f"[SEARCH] Found {len(messages)} emails matching criteria")
            
            return messages
            
        except Exception as e:
            self.log(f"[ERROR] Email search failed: {str(e)}")
            return []
    
    def get_email_details(self, message_id: str) -> Dict:
        """Get email details including sender and subject"""
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            
            headers = message['payload'].get('headers', [])
            
            details = {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
            
            return details
            
        except Exception as e:
            self.log(f"[ERROR] Failed to get email details for {message_id}: {str(e)}")
            return {}
    
    def sanitize_filename(self, filename: str) -> str:
        """Clean up filenames to be safe for all operating systems"""
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            name_parts = cleaned.split('.')
            if len(name_parts) > 1:
                extension = name_parts[-1]
                base_name = '.'.join(name_parts[:-1])
                cleaned = f"{base_name[:95]}.{extension}"
            else:
                cleaned = cleaned[:100]
        return cleaned
    
    def classify_extension(self, filename: str) -> str:
        """Categorize file by extension"""
        if not filename or '.' not in filename:
            return "Other"
            
        ext = filename.split(".")[-1].lower()
        
        type_map = {
            "pdf": "PDFs",
            "doc": "Documents", "docx": "Documents", "txt": "Documents",
            "xls": "Spreadsheets", "xlsx": "Spreadsheets", "csv": "Spreadsheets",
            "jpg": "Images", "jpeg": "Images", "png": "Images", "gif": "Images",
            "ppt": "Presentations", "pptx": "Presentations",
            "zip": "Archives", "rar": "Archives", "7z": "Archives",
        }
        
        return type_map.get(ext, "Other")
    
    def create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        """Create a folder in Google Drive"""
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                folder_id = files[0]['id']
                self.log(f"[DRIVE] Using existing folder: {folder_name} (ID: {folder_id})")
                return folder_id
            
            folder_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            
            folder = self.drive_service.files().create(
                body=folder_metadata,
                fields='id'
            ).execute()
            
            folder_id = folder.get('id')
            self.log(f"[DRIVE] Created Google Drive folder: {folder_name} (ID: {folder_id})")
            
            return folder_id
            
        except Exception as e:
            self.log(f"[ERROR] Failed to create folder {folder_name}: {str(e)}")
            return ""
    
    def upload_to_drive(self, file_data: bytes, filename: str, folder_id: str) -> bool:
        """Upload file to Google Drive"""
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            files = existing.get('files', [])
            
            if files:
                self.log(f"[DRIVE] File already exists, skipping: {filename}")
                return True
            
            file_metadata = {
                'name': filename,
                'parents': [folder_id] if folder_id else []
            }
            
            media = MediaIoBaseUpload(
                io.BytesIO(file_data),
                mimetype='application/octet-stream',
                resumable=True
            )
            
            file = self.drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            self.log(f"[DRIVE] Uploaded to Drive: {filename}")
            return True
            
        except Exception as e:
            self.log(f"[ERROR] Failed to upload {filename}: {str(e)}")
            return False
    
    def extract_attachments_from_email(self, message_id: str, payload: Dict, 
                                     sender_info: Dict, search_term: str, 
                                     base_folder_id: str, attachment_filter: str) -> Dict:
        """Recursively extract all attachments from an email, returns stats"""
        stats = {
            'success': 0,
            'skipped': 0,
            'failed': 0,
            'total': 0
        }
        
        if "parts" in payload:
            for part in payload["parts"]:
                part_stats = self.extract_attachments_from_email(
                    message_id, part, sender_info, search_term, base_folder_id, attachment_filter
                )
                stats['success'] += part_stats['success']
                stats['skipped'] += part_stats['skipped']
                stats['failed'] += part_stats['failed']
                stats['total'] += part_stats['total']
        
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            
            # Check if attachment contains 'GRN' in filename
            if attachment_filter and attachment_filter.lower() not in filename.lower():
                self.log(f"[SKIPPED] Attachment {filename} does not contain '{attachment_filter}'")
                stats['skipped'] += 1
                stats['total'] += 1
                return stats
            
            try:
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                
                if not att.get("data"):
                    stats['failed'] += 1
                    stats['total'] += 1
                    return stats
                
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))
                
                # Skip folder creation and upload directly to the provided folder
                clean_filename = self.sanitize_filename(filename)
                
                success = self.upload_to_drive(file_data, clean_filename, base_folder_id)
                
                if success:
                    stats['success'] += 1
                    self.log(f"[SUCCESS] Uploaded attachment directly to folder: {filename}")
                else:
                    stats['failed'] += 1
                
                stats['total'] += 1
                
            except Exception as e:
                self.log(f"[ERROR] Failed to process attachment {filename}: {str(e)}")
                stats['failed'] += 1
                stats['total'] += 1
        
        return stats
    
    def process_mail_to_drive_workflow(self, config: dict) -> Dict:
        """Process Mail to Drive workflow, returns detailed stats"""
        try:
            self.log("[START] Starting Gmail to Google Drive automation")
            
            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            
            self.stats['mail']['emails_checked'] = len(emails)
            
            if not emails:
                self.log("[INFO] No emails found matching criteria")
                return {
                    'success': True, 
                    'emails_checked': 0,
                    'attachments_found': 0,
                    'attachments_skipped': 0,
                    'attachments_uploaded': 0,
                    'upload_failed': 0
                }
            
            # Use the provided folder ID directly (no extra folder creation)
            base_folder_id = config.get('gdrive_folder_id')
            
            if not base_folder_id:
                self.log("[ERROR] No Google Drive folder ID provided in config")
                return {
                    'success': False, 
                    'emails_checked': len(emails),
                    'attachments_found': 0,
                    'attachments_skipped': 0,
                    'attachments_uploaded': 0,
                    'upload_failed': 0
                }
            
            self.log(f"[PROCESS] Using provided Google Drive folder ID: {base_folder_id}")
            
            total_stats = {
                'total_attachments': 0,
                'successful_uploads': 0,
                'skipped_attachments': 0,
                'failed_uploads': 0
            }
            
            self.log(f"[PROCESS] Processing {len(emails)} emails...")
            
            for i, email in enumerate(emails, 1):
                try:
                    sender_info = self.get_email_details(email['id'])
                    if not sender_info:
                        continue
                    
                    # **FIX: Check if email subject contains GRN (not GDN)**
                    subject = sender_info.get('subject', '').upper()
                    
                    # Skip if subject contains GDN
                    if 'GDN' in subject:
                        self.log(f"[SKIPPED] Email with GDN subject: {subject[:50]}")
                        continue
                    
                    # Only process if subject contains GRN
                    if 'GRN' not in subject:
                        self.log(f"[SKIPPED] Email does not contain GRN in subject: {subject[:50]}")
                        continue
                    
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id']
                    ).execute()
                    
                    if not message or not message.get('payload'):
                        continue
                    
                    attachment_stats = self.extract_attachments_from_email(
                        email['id'], message['payload'], sender_info, 
                        config['search_term'], base_folder_id, config['attachment_filter']
                    )
                    
                    total_stats['total_attachments'] += attachment_stats['total']
                    total_stats['successful_uploads'] += attachment_stats['success']
                    total_stats['skipped_attachments'] += attachment_stats['skipped']
                    total_stats['failed_uploads'] += attachment_stats['failed']
                    
                    subject_display = sender_info.get('subject', 'No Subject')[:50]
                    self.log(f"[PROCESS] Email: {subject_display} - Success: {attachment_stats['success']}, Skipped: {attachment_stats['skipped']}, Failed: {attachment_stats['failed']}")
                    
                except Exception as e:
                    self.log(f"[ERROR] Failed to process email {email.get('id', 'unknown')}: {str(e)}")
                    total_stats['failed_uploads'] += 1
            
            # Update stats
            self.stats['mail']['attachments_found'] = total_stats['total_attachments']
            self.stats['mail']['attachments_uploaded'] = total_stats['successful_uploads']
            self.stats['mail']['attachments_skipped'] = total_stats['skipped_attachments']
            self.stats['mail']['upload_failed'] = total_stats['failed_uploads']
            
            self.log("[COMPLETE] Mail to Drive workflow complete!")
            self.log(f"[STATS] Emails checked: {len(emails)}")
            self.log(f"[STATS] Total attachments found: {total_stats['total_attachments']}")
            self.log(f"[STATS] Attachments uploaded: {total_stats['successful_uploads']}")
            self.log(f"[STATS] Attachments skipped: {total_stats['skipped_attachments']}")
            self.log(f"[STATS] Attachments failed: {total_stats['failed_uploads']}")
            
            return {
                'success': True, 
                'emails_checked': len(emails),
                'attachments_found': total_stats['total_attachments'],
                'attachments_skipped': total_stats['skipped_attachments'],
                'attachments_uploaded': total_stats['successful_uploads'],
                'upload_failed': total_stats['failed_uploads']
            }
            
        except Exception as e:
            self.log(f"Mail to Drive workflow failed: {str(e)}", "ERROR")
            return {
                'success': False, 
                'emails_checked': 0,
                'attachments_found': 0,
                'attachments_skipped': 0,
                'attachments_uploaded': 0,
                'upload_failed': 0
            }
    
    def list_drive_files(self, folder_id: str, days_back: int = 7) -> List[Dict]:
        """List all PDF files in a Google Drive folder filtered by creation date"""
        try:
            start_datetime = datetime.utcnow() - timedelta(days=days_back - 1)
            start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
            query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false and createdTime >= '{start_str}'"
            
            files = []
            page_token = None

            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc",
                    pageToken=page_token,
                    pageSize=100
                ).execute()
                
                files.extend(results.get('files', []))
                page_token = results.get('nextPageToken', None)
                
                if page_token is None:
                    break

            self.log(f"[DRIVE] Found {len(files)} PDF files in folder {folder_id} (last {days_back} days)")
            
            return files
        except Exception as e:
            self.log(f"[ERROR] Failed to list files in folder {folder_id}: {str(e)}")
            return []
    
    def download_from_drive(self, file_id: str, file_name: str) -> bytes:
        """Download a file from Google Drive"""
        try:
            request = self.drive_service.files().get_media(fileId=file_id)
            file_data = request.execute()
            return file_data
        except Exception as e:
            self.log(f"[ERROR] Failed to download file {file_name}: {str(e)}")
            return b""
    
    def get_existing_source_files(self, spreadsheet_id: str, sheet_range: str) -> set:
        """Get set of existing source_file from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=sheet_range,
                majorDimension="ROWS"
            ).execute()
            
            values = result.get('values', [])
            if not values:
                return set()
            
            headers = values[0]
            if "source_file" not in headers:
                self.log("No 'source_file' column found in sheet", "WARNING")
                return set()
            
            name_index = headers.index("source_file")
            existing_names = {row[name_index] for row in values[1:] if len(row) > name_index and row[name_index]}
            
            return existing_names
            
        except Exception as e:
            self.log(f"Failed to get existing file names: {str(e)}", "ERROR")
            return set()
    
    def append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        """Append data to a Google Sheet with retry mechanism"""
        max_retries = 3
        wait_time = 2
        
        for attempt in range(1, max_retries + 1):
            try:
                body = {'values': values}
                result = self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id, 
                    range=range_name,
                    valueInputOption='USER_ENTERED', 
                    body=body
                ).execute()
                
                updated_cells = result.get('updates', {}).get('updatedCells', 0)
                self.log(f"[SHEETS] Appended {updated_cells} cells to Google Sheet")
                return True
            except Exception as e:
                if attempt < max_retries:
                    self.log(f"[SHEETS] Attempt {attempt} failed: {str(e)}")
                    time.sleep(wait_time)
                else:
                    self.log(f"[ERROR] Failed to append to Google Sheet: {str(e)}")
                    return False
        return False
    
    def get_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> List[str]:
        """Get existing headers from Google Sheet"""
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:Z1",
                majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            return values[0] if values else []
        except Exception as e:
            self.log(f"[SHEETS] No existing headers or error: {str(e)}")
            return []
    
    def update_headers(self, spreadsheet_id: str, sheet_name: str, new_headers: List[str]) -> bool:
        """Update the header row with new columns"""
        try:
            body = {'values': [new_headers]}
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{chr(64 + len(new_headers))}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            self.log(f"Updated headers with {len(new_headers)} columns")
            return True
        except Exception as e:
            self.log(f"[ERROR] Failed to update headers: {str(e)}")
            return False
    
    def safe_extract(self, agent, file_path: str, retries: int = 3, wait_time: int = 2):
        """Retry-safe extraction to handle server disconnections"""
        for attempt in range(1, retries + 1):
            try:
                result = agent.extract(file_path)
                return result
            except Exception as e:
                self.log(f"Attempt {attempt} failed for {file_path}: {e}")
                time.sleep(wait_time)
        raise Exception(f"Extraction failed after {retries} attempts for {file_path}")
    
    def debug_extraction_result(self, extraction_result, filename: str):
        """Debug helper to see what's being extracted"""
        self.log(f"\n[DEBUG] Extraction result for {filename}:")
        self.log(f"Type: {type(extraction_result)}")
        
        if hasattr(extraction_result, 'data'):
            data = extraction_result.data
            self.log(f"Has .data attribute: True")
        else:
            data = extraction_result
            self.log(f"Has .data attribute: False")
        
        if isinstance(data, dict):
            self.log(f"Keys in data: {list(data.keys())}")
            
            # Check for items
            for key in ["items", "product_items", "line_items", "products", "grn_items"]:
                if key in data:
                    items = data[key]
                    if isinstance(items, list):
                        self.log(f"Found '{key}' with {len(items)} items")
                        if items and isinstance(items[0], dict):
                            self.log(f"First item keys: {list(items[0].keys())}")
                    else:
                        self.log(f"Found '{key}' but it's not a list: {type(items)}")
            
            # Log sample values
            for key in ["grn_date", "po_number", "supplier", "shipping_address"]:
                if key in data:
                    self.log(f"{key}: {data[key][:100] if isinstance(data[key], str) else data[key]}")
                    
        elif isinstance(data, list):
            self.log(f"Data is a list with {len(data)} elements")
            if data:
                if isinstance(data[0], dict):
                    self.log(f"First element keys: {list(data[0].keys())}")
                    # Check for items in first element
                    for key in ["items", "product_items", "line_items", "products", "grn_items"]:
                        if key in data[0]:
                            items = data[0][key]
                            if isinstance(items, list):
                                self.log(f"Found '{key}' in first element with {len(items)} items")
                else:
                    self.log(f"First element type: {type(data[0])}")
        
        self.log("[DEBUG] End of extraction debug\n")
    
    def process_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        """Process extracted data for More Retail DOCS to match sheet structure"""
        rows = []
        
        # Debug what we got
        self.log(f"[DEBUG] Processing extracted data for {file_info['name']}")
        self.log(f"[DEBUG] Available keys: {list(extracted_data.keys())}")
        
        # Look for items in different possible keys
        items = []
        for possible_key in ["items", "product_items", "line_items", "products", "grn_items", "line_items"]:
            if possible_key in extracted_data:
                items_data = extracted_data[possible_key]
                if isinstance(items_data, list):
                    items = items_data
                    self.log(f"[DEBUG] Found {len(items)} items in key '{possible_key}'")
                    break
        
        # If no items found, check if the data itself is a list
        if not items and isinstance(extracted_data, list):
            items = extracted_data
        
        # If still no items, create single row
        if not items:
            self.log(f"[DEBUG] No items list found, creating single row from main data")
            row = self.create_base_row(extracted_data, file_info)
            if row:
                rows.append(row)
            return rows
        
        # Extract document-level data (once for all items)
        doc_data = {
            "grn_date": extracted_data.get("grn_date", extracted_data.get("date", extracted_data.get("document_date", ""))),
            "supplier": extracted_data.get("supplier", extracted_data.get("vendor_name", extracted_data.get("vendor", ""))),
            "po_number": extracted_data.get("po_number", extracted_data.get("purchase_order_number", extracted_data.get("po_no", ""))),
            "shipping_address": extracted_data.get("shipping_address", extracted_data.get("delivery_address", extracted_data.get("address", ""))),
            "vendor_invoice_number": extracted_data.get("vendor_invoice_number", extracted_data.get("invoice_number", extracted_data.get("invoice_no", "")))
        }
        
        # Process each item
        for i, item in enumerate(items):
            if not isinstance(item, dict):
                self.log(f"[DEBUG] Item {i} is not a dict, skipping")
                continue
            
            # Create comprehensive row with ALL possible fields
            row = {
                # Document-level fields
                "grn_date": doc_data["grn_date"],
                "grndate": doc_data["grn_date"],  # Duplicate for compatibility
                "source_file": file_info['name'],
                "processed_date": time.strftime("%Y-%m-%d %H:%M:%S"),
                "supplier": doc_data["supplier"],
                "po_number": doc_data["po_number"],
                "shipping_address": doc_data["shipping_address"],
                "vendor_invoice_number": doc_data["vendor_invoice_number"],
                "drive_file_id": file_info['id'],
                
                # Item-level fields with multiple fallback names
                "item_description": item.get("item_description", 
                                           item.get("description", 
                                           item.get("product_description", 
                                           item.get("product_name", "")))),
                
                "rcv_qty": item.get("rcv_qty", 
                                  item.get("received_quantity", 
                                  item.get("received_qty", ""))),
                
                "ord_qty": item.get("ord_qty", 
                                  item.get("ordered_quantity", 
                                  item.get("ordered_qty", 
                                  item.get("quantity", 
                                  item.get("qty", ""))))),
                "ord.qty": item.get("ord.qty", 
                                  item.get("ord_qty", 
                                  item.get("ordered_quantity", ""))),  # Alternate column name
                
                "uom": item.get("uom", 
                              item.get("unit_of_measure", 
                              item.get("unit", ""))),
                
                "sku": item.get("sku", 
                              item.get("sku_code", 
                              item.get("item_code", ""))),
                
                "variant_ean": item.get("variant_ean", 
                                       item.get("ean", 
                                       item.get("barcode", ""))),
                "variant.ean": item.get("variant.ean", 
                                       item.get("ean", 
                                       item.get("barcode", ""))),  # Alternate column name
                
                "hsn_code": item.get("hsn_code", 
                                   item.get("hsn", 
                                   item.get("tax_code", ""))),
                
                "unit_cost": item.get("unit_cost", 
                                    item.get("unit_price", 
                                    item.get("price_per_unit", 
                                    item.get("rate", "")))),
                
                "tax_amount": item.get("tax_amount", 
                                     item.get("tax", "")),
                "tax amount": item.get("tax amount", 
                                     item.get("tax_amount", 
                                     item.get("tax", ""))),  # Alternate column name
                
                "tax_percentage": item.get("tax_percentage", 
                                         item.get("tax_percent", 
                                         item.get("tax_rate", ""))),
                
                "mrp": item.get("mrp", 
                              item.get("maximum_retail_price", 
                              item.get("retail_price", ""))),
                
                "net_value": item.get("net_value", 
                                    item.get("net_amount", 
                                    item.get("total_amount", 
                                    item.get("amount", ""))))
            }
            
            # Clean up values - convert everything to string and strip
            cleaned_row = {}
            for key, value in row.items():
                if value is None:
                    value = ""
                # Convert to string, handle numbers properly
                if isinstance(value, (int, float)):
                    cleaned_row[key] = str(value)
                else:
                    cleaned_row[key] = str(value).strip() if value != "" else ""
            
            rows.append(cleaned_row)
            
            # Log first item for debugging
            if i == 0:
                self.log(f"[DEBUG] First row created with {len(cleaned_row)} fields")
                self.log(f"[DEBUG] Sample fields: {dict(list(cleaned_row.items())[:8])}")
        
        self.log(f"[DEBUG] Created {len(rows)} rows from {len(items)} items")
        return rows
    
    def create_base_row(self, extracted_data: Dict, file_info: Dict) -> Dict:
        """Create a base row when no items are found"""
        row = {
            "grn_date": extracted_data.get("grn_date", extracted_data.get("date", "")),
            "grndate": extracted_data.get("grn_date", extracted_data.get("date", "")),
            "source_file": file_info['name'],
            "processed_date": time.strftime("%Y-%m-%d %H:%M:%S"),
            "supplier": extracted_data.get("supplier", extracted_data.get("vendor_name", "")),
            "po_number": extracted_data.get("po_number", extracted_data.get("purchase_order_number", "")),
            "shipping_address": extracted_data.get("shipping_address", extracted_data.get("delivery_address", "")),
            "vendor_invoice_number": extracted_data.get("vendor_invoice_number", extracted_data.get("invoice_number", "")),
            "drive_file_id": file_info['id'],
            "item_description": extracted_data.get("item_description", ""),
            "rcv_qty": extracted_data.get("rcv_qty", ""),
            "ord_qty": extracted_data.get("ord_qty", extracted_data.get("quantity", "")),
            "ord.qty": extracted_data.get("ord_qty", ""),
            "uom": extracted_data.get("uom", ""),
            "sku": extracted_data.get("sku", ""),
            "variant_ean": extracted_data.get("ean", ""),
            "variant.ean": extracted_data.get("ean", ""),
            "hsn_code": extracted_data.get("hsn_code", ""),
            "unit_cost": extracted_data.get("unit_cost", ""),
            "tax_amount": extracted_data.get("tax_amount", ""),
            "tax amount": extracted_data.get("tax_amount", ""),
            "tax_percentage": extracted_data.get("tax_percentage", ""),
            "mrp": extracted_data.get("mrp", ""),
            "net_value": extracted_data.get("net_value", "")
        }
        
        # Clean the row
        cleaned_row = {}
        for key, value in row.items():
            if value is None:
                value = ""
            if isinstance(value, (int, float)):
                cleaned_row[key] = str(value)
            else:
                cleaned_row[key] = str(value).strip() if value != "" else ""
        
        return cleaned_row
    
    def process_drive_to_sheet_workflow(self, config: dict, skip_existing: bool = True) -> Dict:
        """Process Drive to Sheet workflow for DOC platform"""
        stats = {
            'files_found': 0,
            'files_processed': 0,
            'files_failed': 0,
            'files_skipped': 0,
            'rows_added': 0
        }
        
        if not LLAMA_AVAILABLE:
            self.log("[ERROR] LlamaParse not available. Install with: pip install llama-cloud-services")
            return stats
        
        try:
            self.log("Starting Drive to Sheet workflow with LlamaParse", "INFO")
            
            # Set Llama API key from config
            os.environ["LLAMA_CLOUD_API_KEY"] = config['llama_api_key']
            extractor = LlamaExtract()
            agent = extractor.get_agent(name=config['llama_agent'])
            
            if agent is None:
                self.log(f"[ERROR] Could not find agent '{config['llama_agent']}'. Check dashboard.")
                return stats
            
            self.log("LlamaParse agent found", "SUCCESS")
            
            sheet_name = config['sheet_range'].split('!')[0] if '!' in config['sheet_range'] else config['sheet_range']
            
            existing_names = set()
            if skip_existing:
                existing_names = self.get_existing_source_files(config['spreadsheet_id'], config['sheet_range'])
                self.log(f"Skipping {len(existing_names)} already processed files", "INFO")
            
            pdf_files = self.list_drive_files(config['drive_folder_id'], config.get('days_back', 7))
            stats['files_found'] = len(pdf_files)
            
            if skip_existing:
                original_count = len(pdf_files)
                pdf_files = [f for f in pdf_files if f['name'] not in existing_names]
                stats['files_skipped'] = original_count - len(pdf_files)
                self.log(f"After filtering, {len(pdf_files)} PDFs to process", "INFO")
            
            max_files = config.get('max_files')
            if max_files is not None:
                pdf_files = pdf_files[:max_files]
                self.log(f"Limited to {len(pdf_files)} PDFs after max_files limit", "INFO")
            
            if not pdf_files:
                self.log("[INFO] No PDF files found to process")
                return stats
            
            self.log(f"Found {len(pdf_files)} PDF files to process")
            
            # Define expected columns based on your sheet
            expected_columns = [
                "item_description", "vendor_invoice_number", "rcv_qty", "grn_date", 
                "source_file", "processed_date", "supplier", "uom", "variant_ean", 
                "hsn_code", "ord_qty", "po_number", "tax_amount", "shipping_address", 
                "sku", "unit_cost", "tax_percentage", "drive_file_id", "mrp", 
                "net_value", "grndate", "variant.ean", "ord.qty", "tax amount"
            ]
            
            # Ensure headers are set correctly
            headers = self.get_sheet_headers(config['spreadsheet_id'], sheet_name)
            if not headers:
                # Write the expected headers
                self.update_headers(config['spreadsheet_id'], sheet_name, expected_columns)
                headers = expected_columns
            else:
                # Check if we need to add missing columns
                missing_columns = [col for col in expected_columns if col not in headers]
                if missing_columns:
                    new_headers = headers + missing_columns
                    self.update_headers(config['spreadsheet_id'], sheet_name, new_headers)
                    headers = new_headers
            
            self.log(f"[SHEETS] Using headers: {headers}")

            for pdf_file in pdf_files:
                try:
                    self.log(f"Processing: {pdf_file['name']}")
                    
                    file_data = self.download_from_drive(pdf_file['id'], pdf_file['name'])
                    if not file_data:
                        self.log(f"[ERROR] Failed to download {pdf_file['name']}")
                        stats['files_failed'] += 1
                        continue
                    
                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                        tmp_file.write(file_data)
                        tmp_path = tmp_file.name
                    
                    try:
                        extraction_result = self.safe_extract(agent, tmp_path)
                        
                        # Debug the extraction result
                        self.debug_extraction_result(extraction_result, pdf_file['name'])
                        
                        # Handle extraction result
                        extracted_data = extraction_result.data if hasattr(extraction_result, 'data') else extraction_result
                        
                        all_rows = []
                        
                        if isinstance(extracted_data, list):
                            # Process multiple pages/chunks
                            for chunk in extracted_data:
                                if isinstance(chunk, dict):
                                    chunk_rows = self.process_extracted_data(chunk, pdf_file)
                                    all_rows.extend(chunk_rows)
                        else:
                            # Single document
                            all_rows = self.process_extracted_data(extracted_data, pdf_file)
                        
                        if not all_rows:
                            self.log(f"[SKIP] No valid data extracted from {pdf_file['name']}")
                            stats['files_failed'] += 1
                            continue
                        
                        self.log(f"[DEBUG] Created {len(all_rows)} rows for {pdf_file['name']}")
                        
                        # Prepare rows for Google Sheets
                        sheet_rows = []
                        for row_dict in all_rows:
                            row_values = []
                            for header in headers:
                                # Get value or empty string if not found
                                value = row_dict.get(header, "")
                                # Convert to string if not already
                                if value is None:
                                    value = ""
                                row_values.append(str(value).strip())
                            sheet_rows.append(row_values)
                        
                        if sheet_rows:
                            # Append to Google Sheet
                            if self.append_to_google_sheet(config['spreadsheet_id'], config['sheet_range'], sheet_rows):
                                stats['rows_added'] += len(sheet_rows)
                                stats['files_processed'] += 1
                                self.log(f"[SUCCESS] Processed {pdf_file['name']}: {len(sheet_rows)} rows added")
                            else:
                                stats['files_failed'] += 1
                                self.log(f"[ERROR] Failed to append data for {pdf_file['name']}")
                        else:
                            stats['files_failed'] += 1
                            self.log(f"[ERROR] No rows to append for {pdf_file['name']}")
                    
                    finally:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                
                except Exception as e:
                    self.log(f"[ERROR] Failed to process {pdf_file.get('name', 'unknown')}: {str(e)}", "ERROR")
                    stats['files_failed'] += 1
            
            # Update stats
            self.stats['sheet']['files_found'] = stats['files_found']
            self.stats['sheet']['files_processed'] = stats['files_processed']
            self.stats['sheet']['files_skipped'] = stats['files_skipped']
            self.stats['sheet']['files_failed'] = stats['files_failed']
            self.stats['sheet']['rows_added'] = stats['rows_added']
            
            self.log("[COMPLETE] Drive to Sheet workflow complete!")
            self.log(f"[STATS] Files found: {stats['files_found']}")
            self.log(f"[STATS] Files processed: {stats['files_processed']}")
            self.log(f"[STATS] Files skipped (duplicates): {stats['files_skipped']}")
            self.log(f"[STATS] Files failed: {stats['files_failed']}")
            self.log(f"[STATS] Total rows added: {stats['rows_added']}")
            
            return stats
            
        except Exception as e:
            self.log(f"Drive to Sheet workflow failed: {str(e)}", "ERROR")
            return stats
    
    def log_workflow_to_sheet(self, workflow_name: str, start_time: datetime, 
                             end_time: datetime, stats: dict):
        """Log workflow execution details to Google Sheet"""
        try:
            duration = (end_time - start_time).total_seconds()
            duration_str = f"{duration:.2f}s"
            
            if duration >= 60:
                minutes = int(duration // 60)
                seconds = int(duration % 60)
                duration_str = f"{minutes}m {seconds}s"
            
            log_row = [
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                duration_str,
                workflow_name,
                stats.get('processed', stats.get('files_processed', 0)),
                stats.get('total_items', stats.get('rows_added', 0)),
                stats.get('failed', stats.get('files_failed', 0)),
                stats.get('skipped', stats.get('files_skipped', 0)),
                "Success" if stats.get('success', stats.get('files_processed', 0) > 0) else "Failed"
            ]
            
            log_config = CONFIG['workflow_log']
            
            headers = self.get_sheet_headers(log_config['spreadsheet_id'], log_config['sheet_range'])
            if not headers:
                header_row = [
                    "Start Time", "End Time", "Duration", "Workflow", 
                    "Processed", "Total Items", "Failed", "Skipped", "Status"
                ]
                self.append_to_google_sheet(
                    log_config['spreadsheet_id'], 
                    log_config['sheet_range'], 
                    [header_row]
                )
            
            self.append_to_google_sheet(
                log_config['spreadsheet_id'],
                log_config['sheet_range'],
                [log_row]
            )
            
            self.log(f"[WORKFLOW LOG] Logged workflow: {workflow_name}")
            
        except Exception as e:
            self.log(f"[ERROR] Failed to log workflow: {str(e)}")
    
    def run_scheduled_workflow(self):
        """Run both workflows in sequence, log results, and send email summary"""
        try:
            self.log("=" * 80)
            self.log("STARTING DOC AUTOMATION WORKFLOW RUN")
            self.log("=" * 80)
            
            overall_start = datetime.now(timezone.utc)
            workflow_start_str = overall_start.strftime('%Y-%m-%d %H:%M:%S')
            
            # Workflow 1: Mail to Drive
            self.log("\n[WORKFLOW 1/2] Starting Mail to Drive workflow...")
            mail_start = datetime.now(timezone.utc)
            
            # Get Llama API key from environment
            CONFIG['sheet']['llama_api_key'] = os.environ.get('LLAMA_CLOUD_API_KEY', '')
            
            mail_stats = self.process_mail_to_drive_workflow(CONFIG['mail'])
            mail_end = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("Mail to Drive", mail_start, mail_end, mail_stats)
            
            # Small delay between workflows
            time.sleep(5)
            
            # Workflow 2: Drive to Sheet
            self.log("\n[WORKFLOW 2/2] Starting Drive to Sheet workflow...")
            sheet_start = datetime.now(timezone.utc)
            sheet_stats = self.process_drive_to_sheet_workflow(CONFIG['sheet'], skip_existing=True)
            sheet_end = datetime.now(timezone.utc)
            
            sheet_stats_for_log = {
                'files_processed': sheet_stats['files_processed'],
                'rows_added': sheet_stats['rows_added'],
                'files_failed': sheet_stats['files_failed'],
                'files_skipped': sheet_stats['files_skipped'],
                'success': sheet_stats['files_processed'] > 0
            }
            self.log_workflow_to_sheet("Drive to Sheet", sheet_start, sheet_end, sheet_stats_for_log)
            
            overall_end = datetime.now(timezone.utc)
            workflow_end_str = overall_end.strftime('%Y-%m-%d %H:%M:%S')
            total_duration = (overall_end - overall_start).total_seconds()
            
            # Format duration for display
            duration_minutes = total_duration / 60.0
            duration_str = f"{duration_minutes:.2f} minutes"
            
            # Prepare summary data for email
            summary_data = {
                'workflow_start': workflow_start_str,
                'workflow_end': workflow_end_str,
                'total_duration': duration_str,
                'status': 'Completed Successfully' if (mail_stats.get('success', False) or sheet_stats['files_processed'] > 0) else 'Partially Completed',
                'mail_days_back': CONFIG['mail']['days_back'],
                'mail_emails_checked': self.stats['mail']['emails_checked'],
                'mail_attachments_found': self.stats['mail']['attachments_found'],
                'mail_attachments_uploaded': self.stats['mail']['attachments_uploaded'],
                'mail_attachments_skipped': self.stats['mail']['attachments_skipped'],
                'mail_upload_failed': self.stats['mail']['upload_failed'],
                'sheet_days_back': CONFIG['sheet']['days_back'],
                'sheet_files_found': self.stats['sheet']['files_found'],
                'sheet_files_processed': self.stats['sheet']['files_processed'],
                'sheet_files_skipped': self.stats['sheet']['files_skipped'],
                'sheet_files_failed': self.stats['sheet']['files_failed'],
                'sheet_duplicates_removed': self.stats['sheet']['files_skipped']  # Using skipped as duplicates removed
            }
            
            # Send email notification
            self.log("\n[SENDING EMAIL] Preparing and sending workflow summary...")
            email_sent = self.send_email_notification(summary_data)
            
            if email_sent:
                self.log("[EMAIL] Summary email sent successfully!")
            else:
                self.log("[EMAIL WARNING] Failed to send summary email")
            
            self.log("\n" + "=" * 80)
            self.log("DOC AUTOMATION WORKFLOW RUN COMPLETED")
            self.log(f"Total Duration: {duration_str}")
            self.log(f"Mail to Drive: {self.stats['mail']['emails_checked']} emails checked, {self.stats['mail']['attachments_uploaded']} attachments uploaded")
            self.log(f"Drive to Sheet: {self.stats['sheet']['files_processed']} PDFs processed, {self.stats['sheet']['rows_added']} rows added")
            self.log("=" * 80 + "\n")
            
            return summary_data
            
        except Exception as e:
            self.log(f"[ERROR] Scheduled workflow failed: {str(e)}", "ERROR")
            return None


def main():
    """Main function to run the scheduler"""
    print("=" * 80)
    print("DOC AUTOMATION SCHEDULER")
    print("Runs every 3 hours: Mail to Drive → Drive to Sheet")
    print("=" * 80)
    
    automation = DocAutomation()
    
    # Authenticate
    print("\nAuthenticating...")
    if not automation.authenticate():
        print("ERROR: Authentication failed. Please check credentials.")
        return
    
    print("Authentication successful!")
    
    # Run immediately on start
    print("\nRunning initial workflow...")
    summary = automation.run_scheduled_workflow()
    
    if summary:
        print("\nWorkflow Summary:")
        print(f"  Duration: {summary['total_duration']}")
        print(f"  Mail to Drive: {summary['mail_attachments_uploaded']} attachments uploaded")
        print(f"  Drive to Sheet: {summary['sheet_files_processed']} files processed")
        print(f"  Email sent to: {', '.join(CONFIG['notifications']['recipients'])}")
    
    # Schedule to run every 3 hours
    schedule.every(3).hours.do(automation.run_scheduled_workflow)
    
    print(f"\nScheduler started. Next run in 3 hours.")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Press Ctrl+C to stop the scheduler\n")
    
    # Keep running
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Check every minute
    except KeyboardInterrupt:
        print("\n\nScheduler stopped by user.")
        print("=" * 80)


if __name__ == "__main__":
    main()
