import os, boto3
from botocore.exceptions import ClientError

SES_REGION = os.getenv("SES_REGION", "us-east-1")
MAIL_SENDER = os.getenv("MAIL_SENDER")
MAIL_RECIPIENT = os.getenv("MAIL_RECIPIENT")

_ses = boto3.client("ses", region_name=SES_REGION)

def send_submission(subj: str, body_text: str) -> None:
    if not (MAIL_SENDER and MAIL_RECIPIENT):
        raise RuntimeError("MAIL_SENDER and/or MAIL_RECIPIENT not set")
    try:
        _ses.send_email(
            Source=MAIL_SENDER,
            Destination={"ToAddresses": [MAIL_RECIPIENT]},
            Message={
                "Subject": {"Data": subj},
                "Body": {"Text": {"Data": body_text}},
            },
            ReplyToAddresses=[MAIL_RECIPIENT],
        )
    except ClientError as e:
        raise RuntimeError(f"SES send failed: {e.response['Error'].get('Message')}")
