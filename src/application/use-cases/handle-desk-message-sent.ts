import { prisma } from "../../infrastructure/database/prisma/client";
import { publishDeskEvent } from "../../infrastructure/pubsub/desk-events";

interface DeskMessageSentPayload {
  ticketId: string;
  mongoMessageId: string;
  sentAt?: string;
}

/// Consome `desk.message.sent` — publicado pelo Outbound-Worker depois de
/// enviar com sucesso uma mensagem origin="ATTENDANT" e gravá-la no Mongo.
/// Reconciliação FIFO: casa com o TicketMessage otimista mais antigo ainda sem
/// mongoMessageId (mesmo padrão do fluxy-desk-api deprecado).
export async function handleDeskMessageSent(payload: DeskMessageSentPayload): Promise<void> {
  const pending = await prisma.ticketMessage.findFirst({
    where: { ticketId: payload.ticketId, senderType: "ATTENDANT", mongoMessageId: null },
    orderBy: { createdAt: "asc" },
  });

  if (!pending) return;

  await prisma.ticketMessage.update({
    where: { id: pending.id },
    data: { mongoMessageId: payload.mongoMessageId },
  });

  // A partir daqui o front consegue casar a mensagem com o waStatus vindo do
  // Mongo (mongoMessageId preenchido) — os próximos ticks de entregue/lido já
  // chegam sozinhos via Notification-Worker (evento "message_status").
  await publishDeskEvent({ type: "ticket_message", ticketId: payload.ticketId, payload: { ticketId: payload.ticketId } });
}
