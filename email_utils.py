import os
import base64
import mimetypes
import zipfile
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Cc, Bcc,
    Content, Attachment, FileContent, FileName, FileType, Disposition
)

from dotenv import load_dotenv
load_dotenv()

# === CONFIGURABLE MAPPINGS ===
OFFICE_MANAGER_EMAILS = {
    "army": "erick@marketingleads.com.mx",
    "Tepic": "felipe@marketingleads.com.mx",
    "west": "wesley@marketingleads.com.mx",
    "egypt": "marwan@marketingleads.com.mx",
    "Spanish": "javier@marketingleads.com.mx",
    "pakistan": "erick@marketingleads.com.mx",
    "Sp & Prime": "manny@marketingleads.com.mx"
}

CEO_AND_DIRECTORS = [
    "", # Here we need Erick's email
    "carlos@marketingleads.com.mx",
    "erick@marketingleads.com.mx"
]

DEV_TEAM = ["dipepere@truedata.com.mx", "cecilio@marketingleads.com.mx"]

CHANGED_OFFICE_AGENTS = {
    "abilly": "wbilly@marketingstormleads.com",
    "ajudith": "wjudith@marketingstormleads.com",
    "azambo": "spzambo@marketingstormleads.com",
    "spgalloway1": "ngalloway1@marketingstormleads.com"
}

# This is the email setup for email sending in SendGrid
SENDER_EMAIL = "" 
SENDER_NAME = "MSL Analytics Dashboard"

AGENT_DOMAIN = "marketingstormleads.com"

def send_email(subject, html_content, to_list, cc_list=None, file_path=None):
    message = Mail(
        from_email=Email(SENDER_EMAIL, name=SENDER_NAME),
        to_emails=[To(email) for email in to_list],
        subject=subject,
        html_content=Content("text/html", html_content),
    )

    if cc_list:
        message.cc = [Cc(email) for email in cc_list]

    if file_path and os.path.exists(file_path):
        max_size_bytes = 7 * 1024 * 1024  # 7MB
        original_size = os.path.getsize(file_path)

        # Determine if we need to zip the file
        if original_size > max_size_bytes:
            zip_path = file_path.replace(".pdf", ".zip")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
                zipf.write(file_path, arcname=os.path.basename(file_path))
            file_path = zip_path
            mime_type = "application/zip"
        else:
            mime_type, _ = mimetypes.guess_type(file_path)
            mime_type = mime_type or "application/octet-stream"

        with open(file_path, "rb") as f:
            encoded_file = base64.b64encode(f.read()).decode()
            attachment = Attachment(
                FileContent(encoded_file),
                FileName(os.path.basename(file_path)),
                FileType(mime_type),
                Disposition("attachment")
            )
            message.attachment = attachment

    try:
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY"))
        response = sg.send(message)
        return response.status_code
    except Exception as e:
        return f"Error: {e}"

def send_agent_email(agent_name, office, pdf_path, date_str):
    if not os.path.exists(pdf_path):
        return "File not found"

    if agent_name in CHANGED_OFFICE_AGENTS:
        agent_name = CHANGED_OFFICE_AGENTS[agent_name]
    else:
        agent_email = f"{agent_name}@{AGENT_DOMAIN}"

    manager_email = OFFICE_MANAGER_EMAILS.get(office)
    if not manager_email:
        raise ValueError(f"No manager email found for office: {office}")

    subject = f"[Agent Report] {agent_name} – {date_str}"
    html_body = f"""
        <p>Hi {agent_name},</p>
        <p>Attached is your performance summary for <strong>{date_str}</strong>.</p>
        <p>Best regards,<br/>Analytics Team</p>
    """
    to_list = [agent_email]
    cc_list = [manager_email] + DEV_TEAM
    return send_email(subject, html_body, to_list, cc_list, pdf_path)

def send_office_email(office, pdf_path, date_str):
    manager_email = OFFICE_MANAGER_EMAILS.get(office)
    if not manager_email:
        raise ValueError(f"No manager email found for office: {office}")

    subject = f"[Office Report] {office} – {date_str}"
    html_body = f"""
        <p>Hi,</p>
        <p>Attached is the full report for <strong>{office}</strong> on {date_str}, including all agents.</p>
        <p>Best regards,<br/>Analytics Team</p>
    """
    to_list = [manager_email]
    cc_list = DEV_TEAM
    return send_email(subject, html_body, to_list, cc_list, pdf_path)

def send_full_company_email(pdf_path, date_str):
    subject = f"[Global Report] All Offices – {date_str}"
    html_body = f"""
        <p>Hi all,</p>
        <p>Attached is the full summary report for all offices on {date_str}.</p>
        <p>Best regards,<br/>Analytics Team</p>
    """
    to_list = CEO_AND_DIRECTORS
    cc_list = DEV_TEAM
    return send_email(subject, html_body, to_list, cc_list, pdf_path)

