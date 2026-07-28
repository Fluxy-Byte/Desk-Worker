import type { Channel } from "amqplib";
import { assertQueueWithDlq } from "./connection";

export const QUEUE_OUTBOUND_MESSAGE_SEND = "outbound.message.send";
export const QUEUE_OUTBOUND_MESSAGE_MARK_READ = "outbound.message.mark-read";

interface OutboundMessagePayload {
  target: unknown;
  whatsappChannel: unknown;
  messagingSession: unknown;
  answer: { text: string; audio: string; image: string };
  messageType?: "TEXT" | "AUDIO" | "IMAGE" | "DOCUMENT" | "STICKER";
  mediaUrl?: string;
  finishesProcessing: boolean;
  origin: "SYSTEM" | "ATTENDANT";
  ticketId?: string;
}

async function publish(channel: Channel, queue: string, payload: object): Promise<void> {
  await assertQueueWithDlq(channel, queue);
  const ok = channel.sendToQueue(queue, Buffer.from(JSON.stringify(payload)), { persistent: true });
  console.log(`[DESK-MSG][publisher] enviado para fila "${queue}" — sendToQueue retornou ${ok} — payload=${JSON.stringify(payload)}`);
}

export async function publishOutboundMessage(channel: Channel, payload: OutboundMessagePayload): Promise<void> {
  await publish(channel, QUEUE_OUTBOUND_MESSAGE_SEND, payload);
}

interface MarkReadPayload {
  phoneNumberId: string;
  externalMessageId: string;
  typingIndicator: boolean;
}

/// Pede pro Outbound-Worker marcar a última mensagem do cliente como lida e
/// ligar o "digitando..." — quem de fato chama a Graph API é o
/// Outbound-Worker (best-effort, ver outbound-mark-read-consumer.ts).
export async function publishMarkRead(channel: Channel, payload: MarkReadPayload): Promise<void> {
  await publish(channel, QUEUE_OUTBOUND_MESSAGE_MARK_READ, payload);
}
