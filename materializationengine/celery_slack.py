import os

import requests
from celery.utils.log import get_task_logger

celery_logger = get_task_logger(__name__)


def post_message_to_slack(text: str, attachment: dict = None):
    """Post slack message

    Args:
        text (str): text message to post
        attachment (dict, optional): Append extra info. Defaults to None.
    """
    slack_webhook = os.environ.get("SLACK_WEBHOOK")

    payload = {"status": text}
    if attachment is not None:
        payload.update(attachment)
    try:
        r = requests.post(slack_webhook, json=payload)
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        raise f"An Http Error occurred:{repr(e)}"
    except requests.exceptions.ConnectionError as e:
        raise f"An Error Connecting to the API occurred:{repr(e)}"
    except requests.exceptions.Timeout as e:
        raise f"A Timeout Error occurred:{repr(e)}"
    except requests.exceptions.RequestException as e:
        raise f"An Unknown Error occurred{repr(e)}"


def post_to_slack_on_task_failure(self, exc, task_id, args, kwargs, einfo):
    """Post a failure message to slack on task failure."""
    message = f"Task failure: {self.name}"
    queue, task_name = self.name.split(":")
    datastack = kwargs.get("Datastack")

    failure_info = f"""
    
        Name: *{task_name}*
        Datastack: {datastack}
        Queue: {queue}
        Task ID: {task_id}
        task_args: {str(args)}
        task_kwargs: {str(kwargs)}
        Exception: {str(exc)}
        Info: {str(einfo)}
    """
    attachment = {
        "attachments": [
            {
                "fallback": message,
                "color": "#D00001",
                "text": failure_info,
                "title": message,
                "title_link": task_id,
                "mrkdwn_in": ["text"],
            }
        ],
        "text": "",
    }
    post_message_to_slack(failure_info, attachment)
