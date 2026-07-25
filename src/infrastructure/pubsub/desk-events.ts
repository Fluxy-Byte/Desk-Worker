import { redis } from "../cache/redis/client";

export type DeskEventType = "ticket_new" | "ticket_message" | "ticket_updated";

interface DeskEvent {
  type: DeskEventType;
  queueId?: string;
  ticketId?: string;
  payload: unknown;
}

const DESK_EVENTS_CHANNEL = "desk:events";

/// Publica no Redis Pub/Sub (não RabbitMQ — é notificação efêmera de UI,
/// consumida pelo Desk-API pra repassar por WebSocket aos atendentes).
export async function publishDeskEvent(event: DeskEvent): Promise<void> {
  await redis.publish(DESK_EVENTS_CHANNEL, JSON.stringify(event));
}
