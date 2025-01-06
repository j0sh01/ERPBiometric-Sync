# Copyright (c) 2025, Joshua Joseph Michael and contributors
# For license information, please see license.txt

import frappe
from frappe.model.document import Document
from frappe.utils import now


class BiometricDataStaging(Document):
	pass

# ------------------------- Core Functions -------------------------
@frappe.whitelist()
def enqueue_process_biometric_logs():
    """
    Enqueue the process_biometric_logs function to run in the background.
    """
    frappe.enqueue(
        'erpbiometric_sync.erpbiometric_sync.doctype.biometric_data_staging.biometric_data_staging.process_biometric_logs',
        queue='long',
        job_name='Process Biometric Logs',
        timeout=3600
    )
    return "Employee Checkin synchronization has started in the background."

def process_biometric_logs():
    """
    Process unsynced logs in Biometric Data Staging Doctype
    and move them to Employee Checkin while updating status.
    """
    unsynced_logs = frappe.get_all(
        "Biometric Data Staging",
        filters={"status": "Pending"},
        fields=["name", "attendance_device_id", "timestamp", "punch_type", "device_id"]
    )
    for log in unsynced_logs:
        try:
            # Match attendance_device_id to Employee
            employee = frappe.get_value("Employee", {"attendance_device_id": log.attendance_device_id}, "name")
            if employee:
                # Check for duplicate entries
                duplicate_check = frappe.db.exists(
                    "Employee Checkin",
                    {"employee": employee, "time": log.timestamp}
                )
                if duplicate_check:
                    frappe.db.set_value("Biometric Data Staging", log.name, "status", "Duplicate")
                    continue
                
                # Create Employee Checkin
                checkin = frappe.get_doc({
                    "doctype": "Employee Checkin",
                    "employee": employee,
                    "time": log.timestamp,
                    "log_type": log.punch_type,
                    "device_id": log.device_id
                })
                checkin.insert(ignore_permissions=True)

                # Mark log as Processed
                frappe.db.set_value("Biometric Data Staging", log.name, "status", "Processed")
            else:
                # Mark log as Ignored if Employee is not found
                frappe.db.set_value("Biometric Data Staging", log.name, "status", "Ignored")
        except Exception as e:
            frappe.log_error(f"Failed to process log {log.name}: {str(e)}", "Biometric Log Processing Error")
    frappe.db.commit()

def get_recipients_by_roles(roles):
    """
    Fetch email addresses of users assigned to the given roles.
    """
    recipients = frappe.db.sql(
        """
        SELECT DISTINCT u.email
        FROM `tabUser` u
        JOIN `tabHas Role` hr ON u.name = hr.parent
        WHERE u.enabled = 1
        AND u.user_type = 'System User'
        AND hr.role IN %(roles)s
        AND u.email IS NOT NULL
        """,
        {"roles": roles},
        as_dict=True,
    )
    return [recipient["email"] for recipient in recipients]

def send_exceptional_report():
    """
    Generate and email an exceptional report to System Managers and HRs.
    """
    # Fetch report data
    report_data = frappe.db.sql("""
        SELECT status, COUNT(*) as count 
        FROM `tabBiometric Data Staging` 
        WHERE status IN ('Pending', 'Ignored', 'Processed') 
        AND DATE(timestamp) = CURDATE()
        GROUP BY status
    """, as_dict=True)

    # Skip if no data to report
    if not report_data:
        print("No exceptional records to report.")
        return

    # Compose email body
    email_body = "<h3>Exceptional Report Summary</h3>"
    email_body += "<table border='1' style='border-collapse: collapse;'>"
    email_body += "<tr><th>Status</th><th>Count</th></tr>"
    for record in report_data:
        email_body += f"<tr><td>{record['status']}</td><td>{record['count']}</td></tr>"
    email_body += "</table>"

    # Fetch recipients (System Managers and HRs)
    roles = ["System Manager", "HR Manager"]
    recipients = get_recipients_by_roles(roles)

    # Log error if no recipients found
    if not recipients:
        frappe.log_error("No recipients found for the exceptional report", "Exceptional Report")
        print("No recipients found for the exceptional report.")
        return

    # Retrieve sender email
    sender = frappe.db.get_value("Email Account", {"default_outgoing": 1}, "email_id")
    if not sender:
        frappe.log_error("No default sender email is configured.", "Exceptional Report")
        print("No default sender email is configured.")
        return

    # Email subject and message content
    subject = "Exceptional Report - Biometric Data Staging"

    # Log communication in ERPNext's Communication doctype
    communication = frappe.get_doc({
        "doctype": "Communication",
        "communication_type": "Automated Message",
        "communication_medium": "Email",
        "subject": subject,
        "content": email_body,
        "sender": sender,
        "recipients": ", ".join(recipients),
        "reference_doctype": "Biometric Data Staging",
        "status": "Linked"
    })
    communication.insert(ignore_permissions=True)

    # Send the email
    try:
        frappe.sendmail(
            recipients=recipients,
            subject=subject,
            sender=sender,
            message=email_body,
            reference_doctype="Communication",
            reference_name=communication.name,
            expose_recipients="header"
        )
        print(f"Exceptional report email sent to: {', '.join(recipients)}")

    except Exception as e:
        print(f"Failed to send exceptional report email: {str(e)}")
        frappe.log_error(f"Failed to send email: {str(e)}", "Exceptional Report")

# ------------------------- Scheduled Jobs -------------------------
def setup_scheduled_job():
    """Create or update the scheduled job for biometric data synchronization."""
    job = frappe.get_doc({
        "doctype": "Scheduled Job Type",
        "method": "hrms.hr.doctype.biometric_data_staging.biometric_data_staging.process_biometric_logs",
        "frequency": "Hourly",  # Runs every hour
        "docstatus": 0,
        "name": "Hourly Biometric Data Sync",
        "status": "Active",
        "enabled": 1,
        "create_log": 1
    })

    try:
        # Check if the job already exists
        existing_job = frappe.get_doc("Scheduled Job Type", "Hourly Biometric Data Sync")
        existing_job.update(job)
        existing_job.save()
        frappe.db.commit()
        print("Updated existing scheduled job for biometric data synchronization.")
    except frappe.DoesNotExistError:
        # Create a new job if it doesn't exist
        job.insert()
        frappe.db.commit()
        print("Created new scheduled job for biometric data synchronization.")

def execute_scheduled_job():
    """Wrapper function to handle scheduled job execution."""
    try:
        from erpbiometric_sync.erpbiometric_sync.doctype.biometric_data_staging.biometric_data_staging import process_biometric_logs
        process_biometric_logs()
    except Exception as e:
        frappe.log_error(
            f"Error in scheduled biometric data synchronization: {str(e)}",
            "Scheduled Job Error"
        )
        raise

def setup_scheduled_job_for_exceptional_report():
    """Create or update the scheduled job for sending exceptional reports."""
    job = frappe.get_doc({
        "doctype": "Scheduled Job Type",
        "method": "hrms.hr.doctype.biometric_data_staging.biometric_data_staging.send_exceptional_report",
        "frequency": "Daily",  # Runs daily
        "docstatus": 0,
        "name": "Daily Exceptional Report",
        "status": "Active",
        "enabled": 1,
        "create_log": 1
    })

    try:
        # Check if the job already exists
        existing_job = frappe.get_doc("Scheduled Job Type", "Daily Exceptional Report")
        existing_job.update(job)
        existing_job.save()
        frappe.db.commit()
        print("Updated existing scheduled job for exceptional report.")
    except frappe.DoesNotExistError:
        # Create a new job if it doesn't exist
        job.insert()
        frappe.db.commit()
        print("Created new scheduled job for exceptional report.")

def execute_scheduled_exceptional_report():
    """Wrapper function to handle scheduled exceptional report execution."""
    try:
        from erpbiometric_sync.erpbiometric_sync.doctype.biometric_data_staging.biometric_data_staging import send_exceptional_report
        send_exceptional_report()
    except Exception as e:
        frappe.log_error(
            f"Error in scheduled exceptional report: {str(e)}",
            "Scheduled Job Error"
        )
        raise
