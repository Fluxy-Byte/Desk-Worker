import { prisma } from "../../infrastructure/database/prisma/client";
import { setLastInboundMessage } from "../../infrastructure/cache/redis/last-inbound-message";
import { getRabbitChannel } from "../../infrastructure/queue/rabbitmq/connection";
import { publishOutboundMessage } from "../../infrastructure/queue/rabbitmq/publisher";
import { publishDeskEvent } from "../../infrastructure/pubsub/desk-events";
import { findOrCreateOpenTicket } from "./find-or-create-open-ticket";

const WAITING_MESSAGE_THROTTLE_MS = 5 * 60 * 1000;

interface DeskMessageInboundPayload {
  target: { id: string; organizationId?: string; [key: string]: unknown };
  whatsappChannel: { id: string; [key: string]: unknown };
  messagingSession: { id: string; [key: string]: unknown };
  agent: { id: string; name: string };
  message: { mongoMessageId?: string; externalMessageId?: string; type: string; text?: string; timestamp?: string };
}

/// Consome `desk.message.inbound` — publicado pelo Inbound-Service quando
/// Target.status === "HUMAN". Implementa as regras do Desk Worker: ticket
/// IN_PROGRESS só atualiza e notifica; ticket WAITING reenvia a transferMessage
/// com throttle; sem ticket aberto (ou CLOSED) vira uma criação nova.
export async function handleDeskMessageInbound(payload: DeskMessageInboundPayload): Promise<void> {
  const ticket = await prisma.ticket.findFirst({
    where: { messagingSessionId: payload.messagingSession.id },
    orderBy: { createdAt: "desc" },
  });

  if (!ticket || ticket.status === "CLOSED") {
    const channel = await prisma.whatsappChannel.findUnique({
      where: { id: payload.whatsappChannel.id },
      select: { organizationId: true },
    });
    if (!channel) throw new Error(`WhatsappChannel ${payload.whatsappChannel.id} não encontrado.`);

    // Precisamos de uma fila — usamos a fila do ticket anterior se existir,
    // senão a fila padrão do agente.
    let queueId = ticket?.queueId;
    if (!queueId) {
      const agent = await prisma.agent.findUnique({ where: { id: payload.agent.id } });
      queueId = agent?.defaultQueueId ?? undefined;
    }
    if (!queueId) throw new Error("Nenhuma fila disponível para abrir o ticket (sem fila padrão configurada).");

    await findOrCreateOpenTicket({
      organizationId: channel.organizationId,
      queueId,
      targetId: payload.target.id,
      messagingSessionId: payload.messagingSession.id,
      target: payload.target,
      whatsappChannel: payload.whatsappChannel,
      messagingSession: payload.messagingSession,
      agentId: payload.agent.id,
    });
    return;
  }

  await prisma.ticketMessage.create({
    data: {
      ticketId: ticket.id,
      senderType: "CUSTOMER",
      mongoMessageId: payload.message.mongoMessageId ?? null,
      messageType: normalizeMessageType(payload.message.type),
    },
  });

  if (payload.message.externalMessageId) {
    await setLastInboundMessage(payload.messagingSession.id, payload.message.externalMessageId);
  }

  await prisma.messagingSession.update({
    where: { id: payload.messagingSession.id },
    data: { lastCustomerMessageAt: new Date() },
  });

  await prisma.ticket.update({ where: { id: ticket.id }, data: { updatedAt: new Date() } });

  if (ticket.status === "WAITING") {
    const throttled =
      ticket.lastWaitingMessageSentAt &&
      Date.now() - ticket.lastWaitingMessageSentAt.getTime() < WAITING_MESSAGE_THROTTLE_MS;

    if (!throttled) {
      const agent = await prisma.agent.findUnique({ where: { id: payload.agent.id } });
      const channel = await getRabbitChannel();

      await publishOutboundMessage(channel, {
        target: payload.target,
        whatsappChannel: payload.whatsappChannel,
        messagingSession: payload.messagingSession,
        answer: { text: agent?.transferMessage ?? "", audio: "", image: "" },
        finishesProcessing: true,
        origin: "SYSTEM",
      });

      await prisma.ticket.update({ where: { id: ticket.id }, data: { lastWaitingMessageSentAt: new Date() } });
    }
  }

  await publishDeskEvent({ type: "ticket_message", ticketId: ticket.id, payload: { ticketId: ticket.id } });
}

function normalizeMessageType(type: string): "TEXT" | "AUDIO" | "IMAGE" | "DOCUMENT" | "STICKER" {
  const upper = type.toUpperCase();
  if (upper === "TEXT" || upper === "AUDIO" || upper === "IMAGE" || upper === "DOCUMENT" || upper === "STICKER") {
    return upper;
  }
  return "TEXT";
}
