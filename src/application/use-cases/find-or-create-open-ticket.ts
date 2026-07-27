import { Prisma } from "../../../generated/prisma/client";
import { prisma } from "../../infrastructure/database/prisma/client";
import { getRabbitChannel } from "../../infrastructure/queue/rabbitmq/connection";
import { publishOutboundMessage } from "../../infrastructure/queue/rabbitmq/publisher";
import { publishDeskEvent } from "../../infrastructure/pubsub/desk-events";

interface FindOrCreateOpenTicketInput {
  organizationId: string;
  queueId: string;
  targetId: string;
  messagingSessionId: string;
  target: unknown;
  whatsappChannel: unknown;
  messagingSession: unknown;
  agentId: string;
}

async function createTicketUnderCounter(input: FindOrCreateOpenTicketInput) {
  return prisma.$transaction(async (tx) => {
    const counter = await tx.ticketCounter.upsert({
      where: { organizationId: input.organizationId },
      create: { organizationId: input.organizationId, lastNumber: 1 },
      update: { lastNumber: { increment: 1 } },
    });

    const ticket = await tx.ticket.create({
      data: {
        organizationId: input.organizationId,
        queueId: input.queueId,
        targetId: input.targetId,
        messagingSessionId: input.messagingSessionId,
        ticketNumber: counter.lastNumber,
        status: "WAITING",
      },
    });

    // Sem isso o Target nunca sai de "AI": o Inbound-Service decide o roteamento
    // (fila do agente vs. desk.message.inbound) checando Target.status === "HUMAN",
    // e o ticket-service (Desk-API) só devolve pra "AI" quando o ticket fecha —
    // então a transição pra "HUMAN" tem que acontecer aqui, atomicamente com a
    // criação do ticket, senão as próximas mensagens do cliente continuam indo
    // pro pipeline de IA mesmo com atendente aberto.
    await tx.target.update({ where: { id: input.targetId }, data: { status: "HUMAN" } });
    console.log(`[DESK-MSG][find-or-create-open-ticket] targetId=${input.targetId} status atualizado para HUMAN — ticketId=${ticket.id}`);

    return ticket;
  });
}

/// REGRA MAIS IMPORTANTE do Desk Worker: nunca mais de um ticket aberto
/// (WAITING | IN_PROGRESS) por MessagingSession. Garantido em dois níveis:
/// (1) checagem prévia (rápida, cobre o caso comum), (2) índice único parcial
/// no Postgres (uq_ticket_open_per_session) — a violação de constraint é o
/// sinal definitivo de corrida perdida, tratada aqui como sucesso (idempotente).
export async function findOrCreateOpenTicket(
  input: FindOrCreateOpenTicketInput,
): Promise<{ ticket: Awaited<ReturnType<typeof createTicketUnderCounter>>; created: boolean }> {
  const existing = await prisma.ticket.findFirst({
    where: { messagingSessionId: input.messagingSessionId, status: { not: "CLOSED" } },
  });
  if (existing) return { ticket: existing, created: false };

  try {
    const ticket = await createTicketUnderCounter(input);
    await sendTransferMessageAndNotify(ticket, input);
    return { ticket, created: true };
  } catch (error) {
    if (error instanceof Prisma.PrismaClientKnownRequestError && error.code === "P2002") {
      // Perdemos a corrida contra outro consumidor criando pra mesma sessão —
      // idempotente: busca quem venceu, não reenvia transferMessage de novo.
      const winner = await prisma.ticket.findFirst({
        where: { messagingSessionId: input.messagingSessionId, status: { not: "CLOSED" } },
      });
      if (winner) return { ticket: winner, created: false };
    }
    throw error;
  }
}

async function sendTransferMessageAndNotify(
  ticket: { id: string; queueId: string },
  input: FindOrCreateOpenTicketInput,
): Promise<void> {
  const agent = await prisma.agent.findUnique({ where: { id: input.agentId } });
  const channel = await getRabbitChannel();

  await publishOutboundMessage(channel, {
    target: input.target,
    whatsappChannel: input.whatsappChannel,
    messagingSession: input.messagingSession,
    answer: { text: agent?.transferMessage ?? "", audio: "", image: "" },
    finishesProcessing: true,
    origin: "SYSTEM",
  });

  await publishDeskEvent({
    type: "ticket_new",
    queueId: ticket.queueId,
    ticketId: ticket.id,
    payload: ticket,
  });
}
