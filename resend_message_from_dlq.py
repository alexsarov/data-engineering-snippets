import asyncio
import json
import os
import time
import traceback
from typing import Any

from slack_sdk.web.async_client import AsyncWebClient

from azure.servicebus import ServiceBusMessage, ServiceBusSubQueue
from azure.servicebus.aio import ServiceBusClient, AutoLockRenewer
from azure.servicebus.management import ServiceBusAdministrationClient


SERVICE_BUS_CONN_STR = os.environ["SERVICE_BUS_CONN_STR"]
SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_CHANNEL_ID = os.environ["SLACK_CHANNEL_ID"]

MAX_MESSAGES_PER_RECEIVE = int(os.getenv("MAX_MESSAGES_PER_RECEIVE", "200"))
MAX_LOCK_RENEWAL_SECONDS = int(os.getenv("MAX_LOCK_RENEWAL_SECONDS", "300"))


async def send_message_to_slack(message: str) -> None:
    slack_client = AsyncWebClient(token=SLACK_BOT_TOKEN)
    await slack_client.chat_postMessage(
        channel=SLACK_CHANNEL_ID,
        text=message,
    )


def get_dlq_topics(conn_str: str) -> list[dict[str, Any]]:
    admin_client = ServiceBusAdministrationClient.from_connection_string(conn_str)

    result = []

    # Scan all topics and subscriptions and find DLQs with messages.
    for topic in admin_client.list_topics():
        topic_name = topic.name

        for subscription in admin_client.list_subscriptions(topic_name):
            subscription_name = subscription.name

            runtime_props = admin_client.get_subscription_runtime_properties(
                topic_name,
                subscription_name,
            )

            dead_letter_count = runtime_props.dead_letter_message_count

            if dead_letter_count > 0:
                result.append(
                    {
                        "topic": topic_name,
                        "subscription": subscription_name,
                        "dlq_count": dead_letter_count,
                    }
                )

    return result

def build_replay_message(message, subscription_name: str) -> ServiceBusMessage:
    ignored_properties = {
        "DeadLetterReason",
        "DeadLetterErrorDescription",
    }

    user_properties = {}

    # Copy custom application properties except DLQ system-related fields.
    for key, value in (message.application_properties or {}).items():
        prop_key = key.decode() if isinstance(key, bytes) else str(key)

        if prop_key in ignored_properties:
            continue

        user_properties[prop_key] = (
            value.decode() if isinstance(value, bytes) else value
        )

    application_properties = {
        **user_properties,
        "sub_name": subscription_name,
        "enqueued_time_utc_original": user_properties.get(
            "enqueued_time_utc_original",
            message.enqueued_time_utc.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        ),
        "dlq_replayed": True,
        "original_message_id": message.message_id,
    }

    return ServiceBusMessage(
        str(message),
        session_id=message.session_id,
        application_properties=application_properties,
    )

async def resend_messages(
    servicebus_client: ServiceBusClient,
    topic_name: str,
    subscription_name: str,) -> dict[str, Any]:
    result = {
        "topic": topic_name,
        "subscription": subscription_name,
        "processed": 0,
        "failed": 0,
    }

    lock_renewer = AutoLockRenewer()

    receiver = servicebus_client.get_subscription_receiver(
        topic_name=topic_name,
        subscription_name=subscription_name,
        sub_queue=ServiceBusSubQueue.DEAD_LETTER,
        max_wait_time=5,
    )

    sender = servicebus_client.get_topic_sender(topic_name=topic_name)

    try:
        async with receiver, sender:
            messages = await receiver.receive_messages(
                max_message_count=MAX_MESSAGES_PER_RECEIVE,
                max_wait_time=5,
            )

            for message in messages:
                try:
                    lock_renewer.register(
                        receiver,
                        message,
                        max_lock_renewal_duration=MAX_LOCK_RENEWAL_SECONDS,
                    )

                    replay_message = build_replay_message(
                        message=message,
                        subscription_name=subscription_name,
                    )

                    await sender.send_messages(replay_message)

                    # Complete the DLQ message only after successful resend.
                    await receiver.complete_message(message)

                    result["processed"] += 1

                except Exception as ex:
                    result["failed"] += 1
                    result.setdefault("errors", []).append(
                        {
                            "message_id": message.message_id,
                            "session_id": message.session_id,
                            "error": repr(ex),
                        }
                    )

                    # Do not complete the message if resend failed.
                    # It will remain in DLQ for the next retry.

    except Exception as ex:
        result["error"] = repr(ex)

    finally:
        await lock_renewer.close()

    return result


async def do() -> None:
    start_time = time.time()

    try:
        dlq_topics = get_dlq_topics(SERVICE_BUS_CONN_STR)

        if not dlq_topics:
            #stop and do nothing
            return

        async with ServiceBusClient.from_connection_string(
            conn_str=SERVICE_BUS_CONN_STR,
            logging_enable=False,
        ) as servicebus_client:
            tasks = [
                resend_messages(
                    servicebus_client=servicebus_client,
                    topic_name=item["topic"],
                    subscription_name=item["subscription"],
                )
                for item in dlq_topics
            ]

            results = await asyncio.gather(*tasks, return_exceptions=True)

        safe_results = [
            repr(item) if isinstance(item, Exception) else item
            for item in results
        ]

        await send_message_to_slack(
            "Messages from DLQ Service Bus were handled:\n"
            f"```{json.dumps(safe_results, indent=2, default=str)}```\n"
            f"TimeInSec: {round(time.time() - start_time, 1)}"
        )

    except Exception:
        await send_message_to_slack(
            "DLQ replay script failed:\n"
            f"```{traceback.format_exc()}```"
        )


if __name__ == "__main__":
    asyncio.run(do())
