import { prisma } from "../../infrastructure/database/prisma/client";

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
}
