import { prisma } from "../../infrastructure/database/prisma/client";
import { getRabbitChannel } from "../../infrastructure/queue/rabbitmq/connection";
import { publishOutboundMessage } from "../../infrastructure/queue/rabbitmq/publisher";

interface DeskMessageOutboundPayload {
  ticketId: string;
  text: string;
  attendantUserId: string;
  messageType?: "TEXT" | "AUDIO" | "IMAGE" | "DOCUMENT" | "STICKER";
}

/// Consome `desk.message.outbound` — publicado pelo Desk-API quando um
/// atendente envia uma mensagem. mongoMessageId começa null e é preenchido
/// depois via `desk.message.sent` (reconciliação, ver handle-desk-message-sent).
export async function handleDeskMessageOutbound(payload: DeskMessageOutboundPayload): Promise<void> {
  const ticket = await prisma.ticket.findUnique({
    where: { id: payload.ticketId },
    include: { target: { include: { whatsappChannel: true } }, messagingSession: true },
  });

  if (!ticket) {
    console.error(`desk.message.outbound: ticket ${payload.ticketId} não encontrado — ignorando.`);
    return;
  }

  if (ticket.status !== "IN_PROGRESS") {
    console.error(`desk.message.outbound: ticket ${payload.ticketId} não está IN_PROGRESS (status=${ticket.status}) — ignorando.`);
    return;
  }

  await prisma.ticketMessage.create({
    data: {
      ticketId: ticket.id,
      senderType: "ATTENDANT",
      senderUserId: payload.attendantUserId,
      messageType: payload.messageType ?? "TEXT",
      mongoMessageId: null,
    },
  });

  await prisma.messagingSession.update({
    where: { id: ticket.messagingSessionId },
    data: { lastAttendantMessageAt: new Date() },
  });

  const channel = await getRabbitChannel();
  await publishOutboundMessage(channel, {
    target: ticket.target,
    whatsappChannel: ticket.target.whatsappChannel,
    messagingSession: ticket.messagingSession,
    answer: { text: payload.text, audio: "", image: "" },
    finishesProcessing: true,
    origin: "ATTENDANT",
    ticketId: ticket.id,
  });
}
