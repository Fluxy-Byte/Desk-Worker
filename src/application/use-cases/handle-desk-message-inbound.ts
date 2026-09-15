import { prisma } from "../../infrastructure/database/prisma/client";
import { setLastInboundMessage } from "../../infrastructure/cache/redis/last-inbound-message";
import { getRabbitChannel } from "../../infrastructure/queue/rabbitmq/connection";
import { publishOutboundMessage } from "../../infrastructure/queue/rabbitmq/publisher";
import { publishDeskEvent } from "../../infrastructure/pubsub/desk-events";
import { DEFAULT_TRANSFER_MESSAGE, findOrCreateOpenTicket } from "./find-or-create-open-ticket";

const WAITING_MESSAGE_THROTTLE_MS = 5 * 60 * 1000;

interface DeskMessageInboundPayload {
  target: { id: string; organizationId?: string; [key: string]: unknown };
  channel: { id: string; [key: string]: unknown };
  messagingSession: { id: string; [key: string]: unknown };
  /// Ausente quando o canal não tem agente de IA vinculado (openAgent=false
  /// desde a origem, ver Inbound-Service webhook-service.ts).
  agent: { id: string; name: string } | null;
  /// Fila (Queue) a usar quando não há staleOpenTicket.queueId nem agente pra
  /// resolver defaultQueueId — mandado pelo Inbound-Service a partir de
  /// Channel.idServiceIslandDefault quando openAgent=false.
  defaultQueueId?: string | null;
  message: { mongoMessageId?: string; externalMessageId?: string; type: string; text?: string; timestamp?: string };
}

/// Consome `desk.message.inbound` — publicado pelo Inbound-Service quando
/// Target.status === "HUMAN". Implementa as regras do Desk Worker: ticket
/// IN_PROGRESS só atualiza e notifica; ticket WAITING reenvia a transferMessage
/// com throttle; sem ticket aberto (ou CLOSED) pra ESTA sessão vira uma criação
/// nova — recuperando fila/atendente do ticket anterior do target e, se ele
/// ainda estava aberto (a janela de 24h venceu sem ninguém fechar), fechando-o
/// agora como SESSION_EXPIRED em vez de deixá-lo órfão pra sempre.
export async function handleDeskMessageInbound(payload: DeskMessageInboundPayload): Promise<void> {
  const ticket = await prisma.ticket.findFirst({
    where: { messagingSessionId: payload.messagingSession.id },
    orderBy: { createdAt: "desc" },
  });

  if (!ticket || ticket.status === "CLOSED") {
    // Nenhum ticket ABERTO pra esta sessão — pode ser sessão nova (rollover
    // de 24h no Inbound-Service) ou o ticket desta sessão já foi fechado.
    // Busca o último ticket do target em QUALQUER sessão: é dele que
    // recuperamos fila/atendente pra manter a continuidade do atendimento.
    const latestTicket = await prisma.ticket.findFirst({
      where: { targetId: payload.target.id },
      orderBy: { createdAt: "desc" },
    });

    // Só é "stale" (janela expirou com o ticket ainda aberto) se realmente
    // ninguém fechou — se já está CLOSED (resolvido/transferido), não mexe:
    // mantém o comportamento de sempre (cai pra fila padrão do agente).
    const staleOpenTicket = latestTicket && latestTicket.status !== "CLOSED" ? latestTicket : null;

    if (staleOpenTicket) {
      await prisma.ticket.update({
        where: { id: staleOpenTicket.id },
        data: { status: "CLOSED", closedAt: new Date(), closeReason: "SESSION_EXPIRED" },
      });
      console.log(
        `[DESK-MSG][handle-desk-message-inbound] ticket ${staleOpenTicket.id} fechado como SESSION_EXPIRED (janela expirou, cliente voltou a escrever) targetId=${payload.target.id}`,
      );
    }

    const channel = await prisma.channel.findUnique({
      where: { id: payload.channel.id },
      select: { organizationId: true },
    });
    if (!channel) throw new Error(`Channel ${payload.channel.id} não encontrado.`);

    // Precisamos de uma fila — usamos a fila do ticket que acabamos de
    // reabrir (SESSION_EXPIRED) pra manter o cliente com o mesmo
    // atendente/fila; senão payload.defaultQueueId (canal com openAgent=false,
    // ver Inbound-Service) e só por último a fila padrão do agente
    // (comportamento anterior, handoff de IA).
    let queueId = staleOpenTicket?.queueId ?? payload.defaultQueueId ?? undefined;
    if (!queueId && payload.agent) {
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
      channel: payload.channel,
      messagingSession: payload.messagingSession,
      agentId: payload.agent?.id,
      assignedUserId:
        staleOpenTicket?.status === "IN_PROGRESS" ? (staleOpenTicket.assignedUserId ?? undefined) : undefined,
      transferredFromTicketId: staleOpenTicket?.id,
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
      const agent = payload.agent ? await prisma.agent.findUnique({ where: { id: payload.agent.id } }) : null;
      const channel = await getRabbitChannel();

      await publishOutboundMessage(channel, {
        target: payload.target,
        channel: payload.channel,
        messagingSession: payload.messagingSession,
        answer: { text: agent?.transferMessage ?? DEFAULT_TRANSFER_MESSAGE, audio: "", image: "" },
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
