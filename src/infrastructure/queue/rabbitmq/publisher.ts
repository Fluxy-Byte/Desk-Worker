import type { Channel } from "amqplib";
import { assertQueueWithDlq } from "./connection";

export const QUEUE_OUTBOUND_MESSAGE_SEND = "outbound.message.send";

interface OutboundMessagePayload {
  target: unknown;
  whatsappChannel: unknown;
  messagingSession: unknown;
  answer: { text: string; audio: string; image: string };
  finishesProcessing: boolean;
  origin: "SYSTEM" | "ATTENDANT";
  ticketId?: string;
}

async function publish(channel: Channel, queue: string, payload: object): Promise<void> {
  await assertQueueWithDlq(channel, queue);
  channel.sendToQueue(queue, Buffer.from(JSON.stringify(payload)), { persistent: true });
}

export async function publishOutboundMessage(channel: Channel, payload: OutboundMessagePayload): Promise<void> {
  await publish(channel, QUEUE_OUTBOUND_MESSAGE_SEND, payload);
}
